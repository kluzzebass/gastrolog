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

	var sealed []chunk.ChunkMeta
	for _, m := range metas {
		if m.Sealed {
			sealed = append(sealed, m)
		}
	}

	if len(sealed) == 0 {
		o.logger.Info("replication catchup: no sealed chunks to copy",
			"vault", vaultID, "tier", tierID, "secondary", nodeID)
		return nil
	}

	o.logger.Info("replication catchup: starting",
		"vault", vaultID, "tier", tierID, "secondary", nodeID, "chunks", len(sealed))

	for _, meta := range sealed {
		cursor, err := tier.Chunks.OpenCursor(meta.ID)
		if err != nil {
			o.logger.Warn("replication catchup: open cursor failed",
				"chunk", meta.ID.String(), "error", err)
			continue
		}

		iter := chunk.CursorIterator(cursor)
		if err := o.transferrer.TransferRecords(ctx, nodeID, vaultID, iter); err != nil {
			_ = cursor.Close()
			o.logger.Warn("replication catchup: transfer failed",
				"chunk", meta.ID.String(), "secondary", nodeID, "error", err)
			continue
		}
		_ = cursor.Close()

		o.logger.Info("replication catchup: chunk transferred",
			"vault", vaultID, "tier", tierID, "chunk", meta.ID.String(), "secondary", nodeID,
			"records", meta.RecordCount)
	}

	o.logger.Info("replication catchup: completed",
		"vault", vaultID, "tier", tierID, "secondary", nodeID, "chunks", len(sealed))
	return nil
}
