package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gastrolog/internal/chunk"

	"github.com/google/uuid"
)

// SealActiveTier seals the active chunk for a specific tier.
// Used by the ForwardSealTier handler on secondary nodes.
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
		o.logger.Debug("replication: seal skipped — chunk already rotated",
			"vault", vaultID, "tier", tierID,
			"expected", expectedChunkID.String(), "active", active.ID.String())
		return nil
	}
	chunkID := active.ID
	if err := tier.Chunks.Seal(); err != nil {
		return err
	}
	o.postSealWork(vaultID, tier.Chunks, chunkID)
	return nil
}

// BufferForTier appends a record to a secondary tier's durability buffer.
// The buffer is invisible to queries and the inspector. It exists purely
// for active-chunk durability — if the primary dies, the buffer holds the
// records. Cleared when sealed-chunk replication delivers the canonical version.
func (o *Orchestrator) BufferForTier(vaultID, tierID uuid.UUID, rec chunk.Record) error {
	o.mu.RLock()
	defer o.mu.RUnlock()

	vault := o.vaults[vaultID]
	if vault == nil {
		return nil // not registered locally — ignore silently
	}
	for _, tier := range vault.Tiers {
		if tier.TierID == tierID && tier.IsSecondary {
			tier.BufferRecord(rec)
			return nil
		}
	}
	return nil // not a secondary for this tier — ignore
}

// clearDurabilityBuffer clears the durability buffer for a tier.
func (o *Orchestrator) clearDurabilityBuffer(vaultID, tierID uuid.UUID) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	vault := o.vaults[vaultID]
	if vault == nil {
		return
	}
	for _, tier := range vault.Tiers {
		if tier.TierID == tierID {
			tier.ClearDurabilityBuffer()
			return
		}
	}
}

// scheduleReplication schedules a separate job to replicate a sealed chunk.
// Decoupled from the post-seal pipeline — never blocks compression or indexing.
func (o *Orchestrator) scheduleReplication(vaultID, tierID uuid.UUID, chunkID chunk.ChunkID, secondaryNodeIDs []string) {
	if len(secondaryNodeIDs) == 0 {
		return
	}
	name := fmt.Sprintf("replicate:%s:%s", vaultID, chunkID)
	if err := o.scheduler.RunOnce(name, func() {
		// 10s network deadline per chunk — enough for any healthy transfer,
		// short enough to release the gRPC connection when a secondary is down.
		// Created inside the closure so the timeout starts when the job executes,
		// not when it's scheduled.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		o.replicateSealedChunk(ctx, vaultID, tierID, chunkID, secondaryNodeIDs)
	}); err != nil {
		o.logger.Warn("failed to schedule replication", "name", name, "error", err)
	}
	o.scheduler.Describe(name, fmt.Sprintf("Replicate chunk %s to %d secondaries", chunkID, len(secondaryNodeIDs)))
}

// replicateSealedChunk copies a sealed chunk from the primary to all secondaries.
// Runs as its own scheduler job — decoupled from post-seal.
// Each secondary receives the chunk via ForwardImportRecords with the original
// chunk ID preserved — no ID mismatch, no ordering issues, no races.
func (o *Orchestrator) replicateSealedChunk(ctx context.Context, vaultID, tierID uuid.UUID, chunkID chunk.ChunkID, secondaryNodeIDs []string) {
	if o.transferrer == nil || len(secondaryNodeIDs) == 0 {
		return
	}

	tier := o.findLocalTier(vaultID, tierID)
	if tier == nil {
		o.logger.Warn("replication: tier not found for sealed chunk",
			"vault", vaultID, "tier", tierID, "chunk", chunkID.String())
		return
	}

	for _, nodeID := range secondaryNodeIDs {
		if err := o.replicateToSecondary(ctx, vaultID, tierID, chunkID, tier.Chunks, nodeID); err != nil {
			o.logger.Warn("replication: sealed chunk failed",
				"node", nodeID, "vault", vaultID, "tier", tierID,
				"chunk", chunkID.String(), "error", err)
		} else {
			o.logger.Info("replication: sealed chunk sent",
				"node", nodeID, "vault", vaultID, "tier", tierID,
				"chunk", chunkID.String())
		}
	}
}

// replicateToSecondary streams a single sealed chunk to one secondary node.
// Validates that the chunk is readable before opening the network stream —
// corrupted chunks fail fast without touching the wire.
func (o *Orchestrator) replicateToSecondary(ctx context.Context, vaultID, tierID uuid.UUID, chunkID chunk.ChunkID, cm chunk.ChunkManager, nodeID string) error {
	// Pre-flight: open and read the first record to confirm the chunk is intact.
	// Corrupted compressed data fails here instantly — no network round-trip.
	probe, err := cm.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
	}
	_, _, probeErr := probe.Next()
	_ = probe.Close()
	if probeErr != nil && !errors.Is(probeErr, chunk.ErrNoMoreRecords) {
		return fmt.Errorf("chunk unreadable: %w", probeErr)
	}

	// Chunk is readable — open a fresh cursor for the actual transfer.
	cursor, err := cm.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
	}
	defer func() { _ = cursor.Close() }()

	iter := chunk.CursorIterator(cursor)
	return o.transferrer.ReplicateSealedChunk(ctx, nodeID, vaultID, tierID, chunkID, iter)
}
