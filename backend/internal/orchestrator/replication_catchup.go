package orchestrator

import (
	"context"
	"errors"
	"fmt"

	"gastrolog/internal/chunk"

	"github.com/google/uuid"
)

// ScheduleCatchupForTier finds the vault containing the given tier and
// schedules catchup for the specified secondaries.
func (o *Orchestrator) ScheduleCatchupForTier(tierID uuid.UUID, secondaryNodeIDs []string) {
	o.mu.RLock()
	for vaultID, vault := range o.vaults {
		for _, t := range vault.Tiers {
			if t.TierID == tierID && !t.IsSecondary {
				o.mu.RUnlock()
				o.scheduleCatchup(vaultID, tierID, secondaryNodeIDs)
				return
			}
		}
	}
	o.mu.RUnlock()
}

// scheduleCatchup schedules background jobs to replicate existing sealed chunks
// from the primary to newly added secondary nodes.
func (o *Orchestrator) scheduleCatchup(vaultID, tierID uuid.UUID, newSecondaries []string) {
	for _, nodeID := range newSecondaries {
		name := "replication-catchup:" + vaultID.String() + ":" + tierID.String() + ":" + nodeID
		node := nodeID // capture for closure
		if err := o.scheduler.RunOnce(name, o.catchupSecondary, context.Background(), vaultID, tierID, node); err != nil {
			o.logger.Warn("failed to schedule replication catchup", "name", name, "error", err)
		}
		o.scheduler.Describe(name, "Replicate sealed chunks to secondary "+nodeID[:8])
	}
}

// catchupSecondary copies all sealed chunks from the primary's tier to a
// secondary node. Each chunk's records are streamed via TransferRecords,
// producing an identical sealed chunk on the secondary.
func (o *Orchestrator) catchupSecondary(ctx context.Context, vaultID, tierID uuid.UUID, nodeID string) error {
	tier := o.findLocalTier(vaultID, tierID)
	if tier == nil {
		return fmt.Errorf("tier %s not found in vault %s", tierID, vaultID)
	}
	if tier.IsSecondary {
		return nil // only primary initiates catchup
	}
	if o.transferrer == nil {
		return errors.New("no remote transferrer configured")
	}

	metas, err := tier.Chunks.List()
	if err != nil {
		return fmt.Errorf("list chunks: %w", err)
	}

	// Only replicate chunks where post-seal is complete. For file tiers,
	// this means compressed — uncompressed sealed chunks are still in the
	// post-seal pipeline (compress → index → replicate), which will handle
	// replication itself. For memory tiers, all sealed chunks are ready.
	var sealed []chunk.ChunkMeta
	isFileTier := tier.Type == "file"
	for _, m := range metas {
		if !m.Sealed {
			continue
		}
		if isFileTier && !m.Compressed {
			continue // post-seal pipeline will replicate after compression
		}
		sealed = append(sealed, m)
	}

	if len(sealed) == 0 {
		o.logger.Info("replication catchup: no sealed chunks to copy",
			"vault", vaultID, "tier", tierID, "secondary", nodeID)
		return nil
	}

	o.logger.Info("replication catchup: starting",
		"vault", vaultID, "tier", tierID, "secondary", nodeID, "chunks", len(sealed))

	for _, meta := range sealed {
		if err := o.replicateToSecondary(ctx, vaultID, tierID, meta.ID, tier.Chunks, nodeID); err != nil {
			o.logger.Warn("replication catchup: transfer failed",
				"chunk", meta.ID.String(), "secondary", nodeID, "error", err)
			continue
		}
		o.logger.Info("replication catchup: chunk transferred",
			"vault", vaultID, "tier", tierID, "chunk", meta.ID.String(), "secondary", nodeID,
			"records", meta.RecordCount)
	}

	o.logger.Info("replication catchup: completed",
		"vault", vaultID, "tier", tierID, "secondary", nodeID, "chunks", len(sealed))
	return nil
}
