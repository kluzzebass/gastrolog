package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"gastrolog/internal/chunk"
	"gastrolog/internal/config"

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

	cfg, err := r.orch.loadConfig(ctx)
	if err != nil {
		r.logger.Error("transition: failed to load config",
			"vault", r.vaultID, "tier", r.tierID, "chunk", id.String(), "error", err)
		return
	}

	nextTierID, nextTierCfg := r.resolveNextTier(cfg)
	if nextTierCfg == nil {
		return // logged inside resolveNextTier
	}

	cursor, err := r.cm.OpenCursor(id)
	if err != nil {
		r.logger.Error("transition: failed to open cursor",
			"vault", r.vaultID, "tier", r.tierID, "chunk", id.String(), "error", err)
		r.markUnreadable(id, err)
		return
	}
	defer func() { _ = cursor.Close() }()

	nextLeaderNodeID := nextTierCfg.LeaderNodeID(cfg.NodeStorageConfigs)
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
		r.markUnreadable(id, streamErr)
		return
	}

	r.expireChunk(id)
	r.logger.Info("transition: completed",
		"vault", r.vaultID, "tier", r.tierID, "chunk", id.String(),
		"next_tier", nextTierID, "remote", remote)
}

// resolveNextTier delegates to resolveNextTierInChain.
func (r *retentionRunner) resolveNextTier(cfg *config.Config) (uuid.UUID, *config.TierConfig) {
	nextID, nextCfg, err := resolveNextTierInChain(cfg, r.vaultID, r.tierID)
	if err != nil {
		r.logger.Warn("transition: "+err.Error(), "vault", r.vaultID, "tier", r.tierID)
	}
	return nextID, nextCfg
}

// resolveNextTierInChain finds the next tier in a vault's chain after the given tier.
// Returns an error string if the tier is terminal or not found.
func resolveNextTierInChain(cfg *config.Config, vaultID, tierID uuid.UUID) (uuid.UUID, *config.TierConfig, error) {
	var vaultCfg *config.VaultConfig
	for i := range cfg.Vaults {
		if cfg.Vaults[i].ID == vaultID {
			vaultCfg = &cfg.Vaults[i]
			break
		}
	}
	if vaultCfg == nil {
		return uuid.UUID{}, nil, fmt.Errorf("vault %s not found in config", vaultID)
	}

	idx := slices.Index(vaultCfg.TierIDs, tierID)
	if idx < 0 {
		return uuid.UUID{}, nil, fmt.Errorf("tier %s not found in vault's tier chain", tierID)
	}
	if idx == len(vaultCfg.TierIDs)-1 {
		return uuid.UUID{}, nil, errors.New("terminal tier has no next tier")
	}

	nextTierID := vaultCfg.TierIDs[idx+1]
	nextTierCfg := findTierConfig(cfg.Tiers, nextTierID)
	if nextTierCfg == nil {
		return uuid.UUID{}, nil, fmt.Errorf("next tier %s config not found", nextTierID)
	}
	return nextTierID, nextTierCfg, nil
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



