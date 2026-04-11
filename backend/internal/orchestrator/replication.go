package orchestrator

import (
	"context"
	"errors"
	"fmt"

	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
	"gastrolog/internal/config"

	"github.com/google/uuid"
)

// SealActiveTier seals the active chunk for a specific tier.
// Used by the TierReplication seal command on follower nodes.
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

// ackAfterReplication does sync forwarding to followers and sync
// cross-node forwarding for ack-gated records, then sends the ack.
// Runs in a goroutine — doesn't block the writeLoop.
//
// Two kinds of sync work are processed in sequence:
//
//  1. Local tier follower replication via TierReplicator. If nil
//     (single-node mode) the replication step is skipped.
//  2. Cross-node forwarding via RecordForwarder.ForwardSync for records
//     that matched filters targeting vaults on other nodes. See
//     gastrolog-27zvt: before this, ack-gated records routed to remote
//     vaults were fire-and-forget forwarded, giving a durability
//     guarantee that could be silently broken by a full forward buffer.
//
// The first error from either phase is sent to the ack channel and
// remaining work is skipped.
func (o *Orchestrator) ackAfterReplication(ack chan<- error, pa *pendingAcks, rec chunk.Record) {
	if pa == nil {
		ack <- nil
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), cluster.ReplicationTimeout)
	defer cancel()

	if o.tierReplicator != nil {
		for _, t := range pa.replication {
			for _, tgt := range t.targets {
				if err := o.tierReplicator.AppendRecords(ctx, tgt.NodeID, t.vaultID, t.tierID, t.chunkID, []chunk.Record{rec}); err != nil {
					ack <- fmt.Errorf("ack-gated replication to %s: %w", tgt.NodeID, err)
					return
				}
			}
		}
	}

	if o.forwarder != nil {
		for _, f := range pa.forwards {
			if err := o.forwarder.ForwardSync(ctx, f.nodeID, f.vaultID, []chunk.Record{rec}); err != nil {
				ack <- fmt.Errorf("ack-gated forward to %s: %w", f.nodeID, err)
				return
			}
		}
	}

	ack <- nil
}

// scheduleReplication schedules a separate job to replicate a sealed chunk.
// Decoupled from the post-seal pipeline — never blocks compression or indexing.
func (o *Orchestrator) scheduleReplication(vaultID, tierID uuid.UUID, chunkID chunk.ChunkID, targets []config.ReplicationTarget) {
	if len(targets) == 0 {
		return
	}
	name := fmt.Sprintf("replicate:%s:%s", vaultID, chunkID)
	if err := o.scheduler.RunOnce(name, func() {
		// Created inside the closure so the timeout starts when the job executes,
		// not when it's scheduled.
		ctx, cancel := context.WithTimeout(context.Background(), cluster.ReplicationTimeout)
		defer cancel()
		o.replicateSealedChunk(ctx, vaultID, tierID, chunkID, targets)
	}); err != nil {
		o.logger.Warn("failed to schedule replication", "name", name, "error", err)
	}
	o.scheduler.Describe(name, fmt.Sprintf("Replicate chunk %s to %d followers", chunkID, len(targets)))
}

// replicateSealedChunk copies a sealed chunk from the leader to all follower
// targets. Each target is a (nodeID, storageID) pair — multiple targets on the
// same node are distinct (different file storages for same-node replication).
//
// Cloud-backed chunks are skipped: the data is in shared S3, so followers don't
// need record streaming. The tier Raft FSM's OnUpload callback registers the
// chunk in each follower's cloud index (see wireTierFSMOnUpload).
func (o *Orchestrator) replicateSealedChunk(ctx context.Context, vaultID, tierID uuid.UUID, chunkID chunk.ChunkID, targets []config.ReplicationTarget) {
	if o.transferrer == nil || len(targets) == 0 {
		return
	}

	tier := o.findLocalTier(vaultID, tierID)
	if tier == nil {
		o.logger.Warn("replication: tier not found for sealed chunk",
			"vault", vaultID, "tier", tierID, "chunk", chunkID.String())
		return
	}

	// Cloud-backed chunks live in shared object storage (S3/GCS/Azure).
	// Followers learn about them via the tier Raft FSM's OnUpload callback
	// and read directly from the bucket — no record streaming needed.
	meta, err := tier.Chunks.Meta(chunkID)
	if err == nil && meta.CloudBacked {
		o.logger.Debug("replication: skipping cloud-backed chunk (shared bucket)",
			"vault", vaultID, "tier", tierID, "chunk", chunkID.String())
		return
	}

	for _, tgt := range targets {
		o.replicateToTarget(ctx, vaultID, tierID, chunkID, tier.Chunks, tgt)
	}
}

// replicateToTarget sends a sealed chunk to one target. Same-node targets
// use local ImportToTierStorage; cross-node targets use gRPC.
func (o *Orchestrator) replicateToTarget(ctx context.Context, vaultID, tierID uuid.UUID, chunkID chunk.ChunkID, sourceCM chunk.ChunkManager, tgt config.ReplicationTarget) {
	if tgt.NodeID == o.localNodeID {
		if err := o.replicateLocally(ctx, vaultID, tierID, tgt.StorageID, chunkID, sourceCM); err != nil {
			o.logger.Warn("replication: local copy failed",
				"vault", vaultID, "tier", tierID, "storage", tgt.StorageID,
				"chunk", chunkID.String(), "error", err)
		} else {
			o.logger.Debug("replication: local copy done",
				"vault", vaultID, "tier", tierID, "storage", tgt.StorageID,
				"chunk", chunkID.String())
		}
		return
	}
	if err := o.replicateToFollower(ctx, vaultID, tierID, chunkID, sourceCM, tgt.NodeID); err != nil {
		o.logger.Warn("replication: sealed chunk failed",
			"node", tgt.NodeID, "vault", vaultID, "tier", tierID,
			"chunk", chunkID.String(), "error", err)
	} else {
		o.logger.Debug("replication: sealed chunk sent",
			"node", tgt.NodeID, "vault", vaultID, "tier", tierID,
			"chunk", chunkID.String())
	}
}

// replicateLocally copies a sealed chunk to a different storage-specific
// tier instance on the same node. Opens a cursor on the source, then
// imports into the target via ImportToTierStorage.
func (o *Orchestrator) replicateLocally(ctx context.Context, vaultID, tierID uuid.UUID, storageID string, chunkID chunk.ChunkID, sourceCM chunk.ChunkManager) error {
	cursor, err := sourceCM.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
	}
	defer func() { _ = cursor.Close() }()

	iter := chunk.CursorIterator(cursor)
	return o.ImportToTierStorage(ctx, vaultID, tierID, storageID, chunkID, iter)
}

// replicateToFollower streams a single sealed chunk to one follower node.
// Validates that the chunk is readable before opening the network stream —
// corrupted chunks fail fast without touching the wire.
func (o *Orchestrator) replicateToFollower(ctx context.Context, vaultID, tierID uuid.UUID, chunkID chunk.ChunkID, cm chunk.ChunkManager, nodeID string) error {
	if o.tierReplicator == nil {
		return errors.New("replicateToFollower: tier replicator not configured")
	}
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

	var records []chunk.Record
	for {
		rec, _, recErr := cursor.Next()
		if errors.Is(recErr, chunk.ErrNoMoreRecords) {
			break
		}
		if recErr != nil {
			return fmt.Errorf("read chunk: %w", recErr)
		}
		records = append(records, rec)
	}
	return o.tierReplicator.ImportSealedChunk(ctx, nodeID, vaultID, tierID, chunkID, records)
}
