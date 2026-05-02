package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"slices"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
	"gastrolog/internal/system"
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
	// Close-on-return as a safety net; we also Close explicitly post-
	// stream below so that the per-chunk read lock the cursor holds
	// (gastrolog-26zu1) is released before any later code path tries
	// to take the per-chunk write lock on the same chunk. Specifically:
	// the single-node fallback expires the source chunk via
	// expireChunk → Delete → deleteInternal, which acquires the write
	// lock; if the cursor's read lock were still held by this same
	// goroutine, the RLock→Lock upgrade would deadlock the goroutine
	// on itself. Close is idempotent (sets fields nil).
	cursorClosed := false
	defer func() {
		if !cursorClosed {
			_ = cursor.Close()
		}
	}()

	nextLeaderNodeID := system.LeaderNodeID(sys.Runtime.TierPlacements[nextTierCfg.ID], sys.Runtime.NodeStorageConfigs)
	remote := nextLeaderNodeID != "" && nextLeaderNodeID != r.orch.localNodeID

	// Mark the source chunk as TransitionStreamed BEFORE streaming starts.
	// This is the load-bearing change for histogram correctness: with the
	// flag in place, the source is filtered out of every count-based
	// aggregator the moment streaming begins, so the records appearing
	// on the destination as the stream progresses never get double-counted.
	// The previous "stream then mark" order left the entire streaming
	// window (which can be seconds for large chunks) double-counted on the
	// histogram. See gastrolog-66b7x.
	//
	// Crash safety: if streaming fails after this flag commits, retention
	// is gated on the destination receipt (see gastrolog-4913n) so the
	// source copy stays put. The next retention sweep retries the whole
	// transition (re-stream from disk, re-apply receipt). The flag is
	// idempotent — re-applying TransitionStreamed on an already-flagged
	// chunk is a no-op.
	//
	// Single-node mode (no vault-ctl Raft) keeps the post-stream expire
	// path because there's no replicated flag to set; the bump doesn't
	// apply because there are no replicas observing the source.
	if r.applyRaftTransitionStreamed != nil {
		if err := r.applyRaftTransitionStreamed(id); err != nil {
			r.logger.Error("transition: failed to mark as streamed (pre-stream)",
				"vault", r.vaultID, "tier", r.tierID, "chunk", id.String(), "error", err)
			return
		}
	}

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

	// Streaming complete — release the source cursor's per-chunk read
	// lock NOW so the single-node fallback below can take the write
	// lock via expireChunk → Delete without an RLock→Lock self-deadlock.
	// See gastrolog-26zu1.
	_ = cursor.Close()
	cursorClosed = true

	// Successful retry: a chunk previously flagged unreadable just
	// streamed cleanly. Clear the backoff entry and the alert so the
	// chunk doesn't carry a stale "unreadable" badge once the retention
	// sweep removes it. See gastrolog-25vur.
	r.clearUnreadable(id)

	// Write a receipt to the DESTINATION tier's Raft confirming it has
	// received the records from this source chunk. The Raft commit gives
	// majority-durable confirmation across the destination's nodes.
	// Retention on the source is gated on this receipt being committed.
	destTier := r.findDestTierInstance(nextTierID)
	if destTier != nil && destTier.ApplyRaftTransitionReceived != nil {
		if err := destTier.ApplyRaftTransitionReceived(id); err != nil {
			r.logger.Warn("transition: failed to write receipt to destination",
				"vault", r.vaultID, "tier", r.tierID, "chunk", id.String(),
				"dest_tier", nextTierID, "error", err)
			// Fall through — the confirmation sweep retries the receipt
			// check until it commits.
		}
	}

	if r.applyRaftTransitionStreamed != nil {
		r.logger.Debug("transition: streamed, awaiting destination receipt",
			"vault", r.vaultID, "tier", r.tierID, "chunk", id.String(),
			"next_tier", nextTierID, "remote", remote)
	} else {
		// Single-node fallback: no vault-ctl Raft to coordinate retention,
		// so expire the source immediately.
		r.expireChunk(id, "transition-source-expire")
		r.logger.Debug("transition: completed (single-node, immediate expire)",
			"vault", r.vaultID, "tier", r.tierID, "chunk", id.String(),
			"next_tier", nextTierID)
	}
}

// findDestTierInstance looks up the destination tier in the orchestrator's vault registry.
func (r *retentionRunner) findDestTierInstance(destTierID glid.GLID) *TierInstance {
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
func (r *retentionRunner) resolveNextTier(cfg *system.Config) (glid.GLID, *system.TierConfig) {
	nextID, nextCfg, err := resolveNextTierInChain(cfg, r.vaultID, r.tierID)
	if err != nil {
		r.logger.Warn("transition: "+err.Error(), "vault", r.vaultID, "tier", r.tierID)
	}
	return nextID, nextCfg
}

// resolveNextTierInChain finds the next tier in a vault's chain after the given tier.
// Returns an error string if the tier is terminal or not found.
func resolveNextTierInChain(cfg *system.Config, vaultID, tierID glid.GLID) (glid.GLID, *system.TierConfig, error) {
	var vaultCfg *system.VaultConfig
	for i := range cfg.Vaults {
		if cfg.Vaults[i].ID == vaultID {
			vaultCfg = &cfg.Vaults[i]
			break
		}
	}
	if vaultCfg == nil {
		return glid.GLID{}, nil, fmt.Errorf("vault %s not found in config", vaultID)
	}

	tierIDs := system.VaultTierIDs(cfg.Tiers, vaultID)
	idx := slices.Index(tierIDs, tierID)
	if idx < 0 {
		return glid.GLID{}, nil, fmt.Errorf("tier %s not found in vault's tier chain", tierID)
	}
	if idx == len(tierIDs)-1 {
		return glid.GLID{}, nil, errors.New("terminal tier has no next tier")
	}

	nextTierID := tierIDs[idx+1]
	nextTierCfg := findTierConfig(cfg.Tiers, nextTierID)
	if nextTierCfg == nil {
		return glid.GLID{}, nil, fmt.Errorf("next tier %s config not found", nextTierID)
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
// tier placement), isReceiptConfirmed treats the stream as confirmed so the
// source can expire — there is no local receipt state to consult.
//
// When the destination is local, transitionChunk may have failed to apply the
// receipt once (logged as a warning) while still marking TransitionStreamed.
// Those chunks would otherwise be excluded from TTL sweeps forever; this pass
// retries ApplyRaftTransitionReceived until the receipt commits and the
// source-expire receipt protocol fires. The receipt-retry covers both the
// initial-failure case and the longer-tail recovery case where the receipt
// commit was lost — there is no separate staleness watchdog (the
// maxTransitionStreamedStaleness watchdog was removed in gastrolog-51gme step 6
// because the receipt protocol does not benefit from a fallback "delete the
// source anyway" decision: if the destination never confirmed receipt, deleting
// the source would risk data loss).
func (r *retentionRunner) confirmStreamedTransitions(cfg *system.Config) {
	if r.orch == nil {
		return
	}

	// Collect streamed chunk IDs from the tier FSM.
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
	if nextTierID == glid.Nil {
		return
	}

	destTier := r.findDestTierInstance(nextTierID)

	for _, id := range streamed {
		if destTier != nil && destTier.ApplyRaftTransitionReceived != nil &&
			!r.isReceiptConfirmed(destTier, id) {
			if err := destTier.ApplyRaftTransitionReceived(id); err != nil {
				r.logger.Warn("transition: retry transition receipt on dest tier failed",
					"vault", r.vaultID, "source_tier", r.tierID, "chunk", id.String(),
					"dest_tier", nextTierID, "error", err)
			}
		}
		if r.isReceiptConfirmed(destTier, id) {
			r.expireChunk(id, "transition-source-expire")
			r.logger.Debug("transition: receipt confirmed, expired",
				"vault", r.vaultID, "tier", r.tierID, "chunk", id.String(),
				"dest_tier", nextTierID)
		}
	}
}

// confirmStreamedOne is the single-chunk version of
// confirmStreamedTransitions, called from the event-driven path
// (gastrolog-1g6br) so the source-side expire happens within
// milliseconds of the destination committing the receipt instead of
// waiting up to a full retention sweep tick.
//
// No-ops if this runner does not currently hold the chunk in
// transitionStreamed state — every cross-tier callback fan-out runs
// confirmStreamedOne on every source-tier candidate in the vault, so
// the runner that doesn't hold the chunk just bails cheaply.
//
// Idempotent against the periodic confirmStreamedTransitions sweep:
// if the sweep already expired the chunk, the second pass becomes a
// no-op (transitionStreamed list no longer contains the ID).
func (r *retentionRunner) confirmStreamedOne(cfg *system.Config, id chunk.ChunkID) {
	if r.orch == nil {
		return
	}
	r.mu.Lock()
	tier := r.findTierInstance()
	r.mu.Unlock()
	if tier == nil || tier.ListTransitionStreamed == nil {
		return
	}
	if !slices.Contains(tier.ListTransitionStreamed(), id) {
		return
	}
	nextTierID, _ := r.resolveNextTier(cfg)
	if nextTierID == glid.Nil {
		return
	}
	destTier := r.findDestTierInstance(nextTierID)
	if destTier != nil && destTier.ApplyRaftTransitionReceived != nil &&
		!r.isReceiptConfirmed(destTier, id) {
		if err := destTier.ApplyRaftTransitionReceived(id); err != nil {
			r.logger.Warn("transition: retry transition receipt failed (event-driven)",
				"vault", r.vaultID, "source_tier", r.tierID, "chunk", id.String(),
				"dest_tier", nextTierID, "error", err)
		}
	}
	if r.isReceiptConfirmed(destTier, id) {
		r.expireChunk(id, "transition-source-expire")
		r.logger.Debug("transition: receipt confirmed (event-driven), expired",
			"vault", r.vaultID, "tier", r.tierID, "chunk", id.String(),
			"dest_tier", nextTierID)
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

// TransitionChunkForTesting synchronously transitions a single sealed chunk
// from the given tier to the next tier in the vault's chain. Assumes the
// caller is the tier leader — no leader check, no readiness gate. Production
// code goes through the retention sweep's transition path.
//
// Only call from tests.
func (o *Orchestrator) TransitionChunkForTesting(vaultID, tierID glid.GLID, chunkID chunk.ChunkID) {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	var tier *TierInstance
	if vault != nil {
		for _, t := range vault.Tiers {
			if t.TierID == tierID {
				tier = t
				break
			}
		}
	}
	o.mu.RUnlock()
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
//
// Read-side errors from the source cursor are wrapped in
// cluster.ErrSourceRead so transitionChunk's classifier can retire the
// chunk as unreadable instead of looping forever on the same corruption
// (see gastrolog-3ayz3). AppendToTier errors on the destination stay
// plain — those are retryable per the comment in transitionChunk.
func (r *retentionRunner) streamLocal(cursor chunk.RecordCursor, nextTierID glid.GLID) error {
	for {
		rec, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("%w: read source chunk: %w", cluster.ErrSourceRead, err)
		}
		if err := r.orch.AppendToTier(r.vaultID, nextTierID, chunk.ChunkID{}, rec); err != nil {
			return err
		}
	}
}
