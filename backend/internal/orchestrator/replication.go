package orchestrator

import (
	"context"

	"gastrolog/internal/chunk"

	"github.com/google/uuid"
)

// replicateRecord sends a record to all secondary nodes for a tier.
// Async and non-blocking — uses the RecordForwarder's per-node buffered channels.
// Only called on the primary; gated by !tier.IsSecondary in the caller.
func (o *Orchestrator) replicateRecord(vaultID, tierID uuid.UUID, secondaryNodeIDs []string, rec chunk.Record) {
	for _, nodeID := range secondaryNodeIDs {
		if err := o.forwarder.ForwardToTier(context.Background(), nodeID, vaultID, tierID, []chunk.Record{rec}); err != nil {
			o.logger.Warn("replication: forward failed", "node", nodeID, "vault", vaultID, "tier", tierID, "error", err)
		}
	}
}

// sealSecondaries sends a seal command to all secondary nodes for a tier.
// Synchronous — seals are infrequent and boundary correctness is critical.
// Only called on the primary after detecting a seal in appendRecord/AppendToTier.
func (o *Orchestrator) sealSecondaries(vaultID, tierID uuid.UUID, chunkID chunk.ChunkID, secondaryNodeIDs []string) {
	if o.transferrer == nil {
		return
	}
	for _, nodeID := range secondaryNodeIDs {
		if err := o.transferrer.ForwardSealTier(context.Background(), nodeID, vaultID, tierID, chunkID); err != nil {
			o.logger.Warn("replication: seal forward failed",
				"node", nodeID, "vault", vaultID, "tier", tierID, "chunk", chunkID.String(), "error", err)
		}
	}
}

// SealActiveTier seals the active chunk for a specific tier.
// Used by the ForwardSealTier handler on secondary nodes to seal at the
// same boundary as the primary.
func (o *Orchestrator) SealActiveTier(vaultID, tierID uuid.UUID, expectedChunkID chunk.ChunkID) error {
	tier := o.findLocalTier(vaultID, tierID)
	if tier == nil {
		return ErrVaultNotFound
	}
	active := tier.Chunks.Active()
	if active == nil {
		return nil // nothing to seal
	}
	if active.ID != expectedChunkID {
		o.logger.Warn("replication: seal chunk ID mismatch",
			"vault", vaultID, "tier", tierID,
			"expected", expectedChunkID.String(), "actual", active.ID.String())
		// Seal anyway to stay in sync — divergence is worse than a mismatched boundary.
	}
	chunkID := active.ID
	if err := tier.Chunks.Seal(); err != nil {
		return err
	}
	o.postSealWork(vaultID, tier.Chunks, chunkID)
	return nil
}
