package orchestrator

import (
	"context"
	"slices"

	"gastrolog/internal/chunk"
	"gastrolog/internal/config"

	"github.com/google/uuid"
)

// transitionChunk streams all records from a sealed chunk to the next tier
// in the vault's tier chain, then deletes the source chunk. This is the
// inter-tier data movement mechanism: records flow from hotter to colder
// tiers, each tier independently chunking and sealing.
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
		return
	}
	defer func() { _ = cursor.Close() }()

	sameNode := nextTierCfg.NodeID == "" || nextTierCfg.NodeID == r.orch.localNodeID
	var recordCount int64
	var ok bool

	if sameNode {
		recordCount, ok = r.transitionLocal(id, cursor, nextTierID)
	} else {
		recordCount, ok = r.transitionRemote(ctx, id, cursor, nextTierCfg.NodeID, nextTierID)
	}
	if !ok {
		return // error logged inside; chunk retained for retry
	}

	r.expireChunk(id)
	r.logger.Info("transition: completed",
		"vault", r.vaultID, "tier", r.tierID, "chunk", id.String(),
		"records", recordCount, "next_tier", nextTierID, "remote", !sameNode)
}

// resolveNextTier finds the next tier in the vault's chain after this runner's tier.
// Returns the next tier's ID and config, or nil if this is the terminal tier.
func (r *retentionRunner) resolveNextTier(cfg *config.Config) (uuid.UUID, *config.TierConfig) {
	var vaultCfg *config.VaultConfig
	for i := range cfg.Vaults {
		if cfg.Vaults[i].ID == r.vaultID {
			vaultCfg = &cfg.Vaults[i]
			break
		}
	}
	if vaultCfg == nil {
		r.logger.Error("transition: vault not found in config",
			"vault", r.vaultID, "tier", r.tierID)
		return uuid.UUID{}, nil
	}

	idx := slices.Index(vaultCfg.TierIDs, r.tierID)
	if idx < 0 {
		r.logger.Error("transition: tier not found in vault's tier chain",
			"vault", r.vaultID, "tier", r.tierID)
		return uuid.UUID{}, nil
	}
	if idx == len(vaultCfg.TierIDs)-1 {
		r.logger.Warn("transition: terminal tier has no next tier, skipping",
			"vault", r.vaultID, "tier", r.tierID)
		return uuid.UUID{}, nil
	}

	nextTierID := vaultCfg.TierIDs[idx+1]
	nextTierCfg := findTierConfig(cfg.Tiers, nextTierID)
	if nextTierCfg == nil {
		r.logger.Error("transition: next tier config not found",
			"vault", r.vaultID, "tier", r.tierID, "next_tier", nextTierID)
		return uuid.UUID{}, nil
	}
	return nextTierID, nextTierCfg
}

// transitionLocal streams records to a tier on the same node.
func (r *retentionRunner) transitionLocal(id chunk.ChunkID, cursor chunk.RecordCursor, nextTierID uuid.UUID) (int64, bool) {
	var count int64
	for {
		rec, _, err := cursor.Next()
		if err != nil {
			break // cursor exhausted
		}
		if err := r.orch.AppendToTier(r.vaultID, nextTierID, rec); err != nil {
			r.logger.Error("transition: local append failed",
				"vault", r.vaultID, "chunk", id.String(),
				"next_tier", nextTierID, "error", err)
			return count, false
		}
		count++
	}
	return count, true
}

// transitionRemote buffers and sends records to a tier on a different node.
func (r *retentionRunner) transitionRemote(ctx context.Context, id chunk.ChunkID, cursor chunk.RecordCursor, nodeID string, nextTierID uuid.UUID) (int64, bool) {
	if r.orch.transferrer == nil {
		r.logger.Error("transition: no remote transferrer configured",
			"vault", r.vaultID, "tier", r.tierID, "chunk", id.String())
		return 0, false
	}

	const batchSize = 1000
	var batch []chunk.Record
	var total int64

	for {
		rec, _, err := cursor.Next()
		if err != nil {
			break // cursor exhausted
		}
		batch = append(batch, rec)
		total++

		if len(batch) >= batchSize {
			if err := r.orch.transferrer.ForwardTierAppend(ctx, nodeID, r.vaultID, nextTierID, batch); err != nil {
				r.logger.Error("transition: remote append failed",
					"vault", r.vaultID, "chunk", id.String(),
					"node", nodeID, "next_tier", nextTierID, "error", err)
				return total, false
			}
			batch = batch[:0]
		}
	}

	// Flush remaining.
	if len(batch) > 0 {
		if err := r.orch.transferrer.ForwardTierAppend(ctx, nodeID, r.vaultID, nextTierID, batch); err != nil {
			r.logger.Error("transition: remote append failed (final batch)",
				"vault", r.vaultID, "chunk", id.String(),
				"node", nodeID, "next_tier", nextTierID, "error", err)
			return total, false
		}
	}

	return total, true
}
