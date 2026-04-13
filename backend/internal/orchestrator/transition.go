package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
	"gastrolog/internal/system"

	"github.com/google/uuid"
)

// transitionChunk streams all records from a sealed chunk to the next tier
// in the vault's tier chain, then deletes the source chunk. This is the
// inter-tier data movement mechanism: records flow from hotter to colder
// tiers, each tier independently chunking and sealing.
//
// Both local and remote transitions use the same model: stream an iterator
// to the destination tier's ImportRecords (one pass, one sealed chunk).
// The destination tier handles its own follower replication.
func (r *retentionRunner) transitionChunk(id chunk.ChunkID) {
	ctx := context.Background()

	sys, err := r.orch.loadSystem(ctx)
	if err != nil {
		r.logger.Error("transition: failed to load config",
			"vault", r.vaultID, "tier", r.tierID, "chunk", id.String(), "error", err)
		return
	}

	nextTierID, nextTierCfg := r.resolveNextTier(&sys.Config)
	if nextTierCfg == nil {
		return // logged inside resolveNextTier
	}

	cursor, err := r.cm.OpenCursor(id)
	if err != nil {
		if errors.Is(err, chunk.ErrChunkSuspect) {
			r.logger.Warn("transition: chunk suspect (blob not found in cloud), skipping",
				"vault", r.vaultID, "tier", r.tierID, "chunk", id.String())
			return
		}
		r.logger.Error("transition: failed to open cursor",
			"vault", r.vaultID, "tier", r.tierID, "chunk", id.String(), "error", err)
		r.markUnreadable(id, err)
		return
	}
	defer func() { _ = cursor.Close() }()

	nextLeaderNodeID := system.LeaderNodeID(sys.Runtime.TierPlacements[nextTierCfg.ID], sys.Runtime.NodeStorageConfigs)
	remote := nextLeaderNodeID != "" && nextLeaderNodeID != r.orch.localNodeID

	var streamErr error
	if remote {
		if r.orch.transferrer == nil {
			r.logger.Error("transition: no remote transferrer configured",
				"vault", r.vaultID, "tier", r.tierID, "chunk", id.String())
			return
		}
		streamErr = r.orch.transferrer.StreamToTier(ctx, nextLeaderNodeID, r.vaultID, nextTierID, chunk.CursorIterator(cursor))
	} else {
		streamErr = r.streamLocal(cursor, nextTierID)
	}
	if streamErr != nil {
		r.logger.Error("transition: stream failed",
			"vault", r.vaultID, "tier", r.tierID, "chunk", id.String(),
			"next_tier", nextTierID, "remote", remote, "error", streamErr)
		// Only mark the chunk unreadable if the error came from the
		// source cursor's record iterator (ErrSourceRead). Transient
		// destination errors — network blips, peer timeouts, gRPC
		// Unavailable, cluster forwarder per-call timeouts (see
		// gastrolog-4rp6i) — must NOT permanently retire the chunk;
		// the next retention sweep will retry. See gastrolog-50271.
		if errors.Is(streamErr, cluster.ErrSourceRead) {
			r.markUnreadable(id, streamErr)
		}
		return
	}

	// Write a receipt to the DESTINATION tier's Raft confirming it has
	// received the records from this source chunk. The Raft commit gives
	// majority-durable confirmation across the destination's nodes.
	destTier := r.findDestTierInstance(nextTierID)
	if destTier != nil && destTier.ApplyRaftTransitionReceived != nil {
		if err := destTier.ApplyRaftTransitionReceived(id); err != nil {
			r.logger.Warn("transition: failed to write receipt to destination",
				"vault", r.vaultID, "tier", r.tierID, "chunk", id.String(),
				"dest_tier", nextTierID, "error", err)
			// Fall through — mark as streamed and let the confirmation
			// sweep retry the receipt check.
		}
	}

	// Mark the chunk as streamed rather than deleting it immediately.
	// The retention sweep will check whether the destination tier has
	// the receipt before expiring the source copy. See gastrolog-4913n.
	//
	// In single-node mode (no tier Raft), fall back to immediate expire
	// since there are no followers to wait for.
	if r.applyRaftTransitionStreamed != nil {
		if err := r.applyRaftTransitionStreamed(id); err != nil {
			r.logger.Error("transition: failed to mark as streamed",
				"vault", r.vaultID, "tier", r.tierID, "chunk", id.String(), "error", err)
			return
		}
		r.logger.Debug("transition: streamed, awaiting destination receipt",
			"vault", r.vaultID, "tier", r.tierID, "chunk", id.String(),
			"next_tier", nextTierID, "remote", remote)
	} else {
		r.expireChunk(id)
		r.logger.Debug("transition: completed (single-node, immediate expire)",
			"vault", r.vaultID, "tier", r.tierID, "chunk", id.String(),
			"next_tier", nextTierID)
	}
}

// findDestTierInstance looks up the destination tier in the orchestrator's vault registry.
func (r *retentionRunner) findDestTierInstance(destTierID uuid.UUID) *TierInstance {
	if r.orch == nil {
		return nil
	}
	vault := r.orch.vaults[r.vaultID]
	if vault == nil {
		return nil
	}
	for _, t := range vault.Tiers {
		if t.TierID == destTierID {
			return t
		}
	}
	return nil
}

// resolveNextTier delegates to resolveNextTierInChain.
func (r *retentionRunner) resolveNextTier(cfg *system.Config) (uuid.UUID, *system.TierConfig) {
	nextID, nextCfg, err := resolveNextTierInChain(cfg, r.vaultID, r.tierID)
	if err != nil {
		r.logger.Warn("transition: "+err.Error(), "vault", r.vaultID, "tier", r.tierID)
	}
	return nextID, nextCfg
}

// resolveNextTierInChain finds the next tier in a vault's chain after the given tier.
// Returns an error string if the tier is terminal or not found.
func resolveNextTierInChain(cfg *system.Config, vaultID, tierID uuid.UUID) (uuid.UUID, *system.TierConfig, error) {
	var vaultCfg *system.VaultConfig
	for i := range cfg.Vaults {
		if cfg.Vaults[i].ID == vaultID {
			vaultCfg = &cfg.Vaults[i]
			break
		}
	}
	if vaultCfg == nil {
		return uuid.UUID{}, nil, fmt.Errorf("vault %s not found in config", vaultID)
	}

	tierIDs := system.VaultTierIDs(cfg.Tiers, vaultID)
	idx := slices.Index(tierIDs, tierID)
	if idx < 0 {
		return uuid.UUID{}, nil, fmt.Errorf("tier %s not found in vault's tier chain", tierID)
	}
	if idx == len(tierIDs)-1 {
		return uuid.UUID{}, nil, errors.New("terminal tier has no next tier")
	}

	nextTierID := tierIDs[idx+1]
	nextTierCfg := findTierConfig(cfg.Tiers, nextTierID)
	if nextTierCfg == nil {
		return uuid.UUID{}, nil, fmt.Errorf("next tier %s config not found", nextTierID)
	}
	return nextTierID, nextTierCfg, nil
}

// confirmStreamedTransitions checks chunks in the transitionStreamed state and
// expires those whose destination tier has committed the chunk to its Raft
// manifest. The Raft commit means a majority of the destination tier's nodes
// have acknowledged the chunk — the strongest guarantee available with the
// current node count. See gastrolog-4913n.
//
// If the destination tier has no local instance on this node (fully remote
// tier placement), the check can't be performed locally. In that case, chunks
// are expired after a grace period to avoid blocking forever.
func (r *retentionRunner) confirmStreamedTransitions(cfg *system.Config) {
	if r.orch == nil {
		return
	}

	// Collect streamed chunk IDs from the tier Raft FSM.
	r.mu.Lock()
	tier := r.findTierInstance()
	r.mu.Unlock()
	if tier == nil || tier.ListTransitionStreamed == nil {
		return
	}
	streamed := tier.ListTransitionStreamed()
	if len(streamed) == 0 {
		return
	}

	// Resolve the destination tier (for logging only — see comment below
	// about why we can't check the destination's manifest).
	nextTierID, _ := r.resolveNextTier(cfg)
	if nextTierID == uuid.Nil {
		return
	}

	destTier := r.findDestTierInstance(nextTierID)

	for _, id := range streamed {
		if r.isReceiptConfirmed(destTier, id) {
			r.expireChunk(id)
			r.logger.Debug("transition: receipt confirmed, expired",
				"vault", r.vaultID, "tier", r.tierID, "chunk", id.String(),
				"dest_tier", nextTierID)
		}
	}
}

// isReceiptConfirmed checks whether the destination tier has committed a
// transition receipt for the given source chunk ID. If the destination has
// no local tier instance or no Raft, falls back to true — the stream
// succeeded and we can't check further from this node.
func (r *retentionRunner) isReceiptConfirmed(destTier *TierInstance, sourceChunkID chunk.ChunkID) bool {
	if destTier == nil || destTier.HasTransitionReceipt == nil {
		// No local destination instance or no Raft — accept stream
		// success as confirmation (remote-only destination).
		return true
	}
	return destTier.HasTransitionReceipt(sourceChunkID)
}

// findTierInstance looks up this runner's tier in the orchestrator's vault registry.
func (r *retentionRunner) findTierInstance() *TierInstance {
	if r.orch == nil {
		return nil
	}
	vault := r.orch.vaults[r.vaultID]
	if vault == nil {
		return nil
	}
	for _, t := range vault.Tiers {
		if t.TierID == r.tierID {
			return t
		}
	}
	return nil
}

// TransitionChunk transitions a single sealed chunk from the given tier to the
// next tier in the vault's chain. Exported for integration tests that need to
// trigger transitions from outside the package.
func (o *Orchestrator) TransitionChunk(vaultID, tierID uuid.UUID, chunkID chunk.ChunkID) {
	vault := o.vaults[vaultID]
	if vault == nil {
		return
	}
	var tier *TierInstance
	for _, t := range vault.Tiers {
		if t.TierID == tierID {
			tier = t
			break
		}
	}
	if tier == nil {
		return
	}
	r := &retentionRunner{
		isLeader:        true,
		vaultID:         vaultID,
		tierID:          tierID,
		cm:              tier.Chunks,
		im:              tier.Indexes,
		orch:            o,
		followerTargets: tier.FollowerTargets,
		now:             time.Now,
		logger:          o.logger,
	}
	r.transitionChunk(chunkID)
}

// streamLocal appends records from a cursor to a local tier via AppendToTier.
func (r *retentionRunner) streamLocal(cursor chunk.RecordCursor, nextTierID uuid.UUID) error {
	for {
		rec, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read source chunk: %w", err)
		}
		if err := r.orch.AppendToTier(r.vaultID, nextTierID, chunk.ChunkID{}, rec); err != nil {
			return err
		}
	}
}



