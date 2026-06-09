package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"log/slog"

	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
	"gastrolog/internal/system"
)

// scheduleReplication schedules a separate job to replicate a sealed chunk.
// Decoupled from the post-seal pipeline — never blocks compression or indexing.
func (o *Orchestrator) scheduleReplication(vaultID glid.GLID, chunkID chunk.ChunkID, targets []system.ReplicationTarget) {
	if len(targets) == 0 {
		return
	}
	name := fmt.Sprintf("replicate:%s:%s", vaultID, chunkID)
	if err := o.scheduler.RunOnce(name, func() {
		// Created inside the closure so the timeout starts when the job executes,
		// not when it's scheduled.
		ctx, cancel := context.WithTimeout(context.Background(), cluster.ReplicationTimeout)
		defer cancel()
		o.replicateSealedChunk(ctx, vaultID, chunkID, targets)
	}); err != nil {
		o.replicationLogger.Warn("failed to schedule replication", "name", name, "error", err)
	}
	o.scheduler.Describe(name, fmt.Sprintf("Replicate chunk %s to %d followers", chunkID, len(targets)))
}

// replicateSealedChunk copies a sealed chunk from the leader to all follower
// targets. Each target is a (nodeID, storageID) pair — multiple targets on the
// same node are distinct (different file storages for same-node replication).
//
// Cloud-backed chunks are skipped: the data is in shared S3, so followers don't
// need record streaming. The vault-ctl FSM's OnUpload callback registers the
// chunk in each follower's cloud index (see wireVaultFSMOnUpload).
func (o *Orchestrator) replicateSealedChunk(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID, targets []system.ReplicationTarget) {
	if o.transferrer == nil || len(targets) == 0 {
		return
	}

	vaultInst := o.findLocalVaultInstance(vaultID)
	if vaultInst == nil {
		o.replicationLogger.Warn("replication: vault not found for sealed chunk",
			"vault", vaultID, "chunk", chunkID.String())
		return
	}

	// If retention deleted the chunk while this replication job was queued,
	// the vault-ctl FSM now holds a tombstone for it. Skip the replication —
	// sending ImportSealedChunk to followers would recreate a chunk the
	// cluster has already decided to forget (ghost chunk). Closes the
	// retention-beats-replication ordering at the leader; the receiver-side
	// tombstone check closes the reverse ordering. See gastrolog-11rzz.
	if vaultInst.IsTombstoned != nil && vaultInst.IsTombstoned(chunkID) {
		o.replicationLogger.Debug("replication: skipping tombstoned chunk (retention beat replication)",
			"vault", vaultID, "chunk", chunkID.String())
		return
	}

	// Cloud-backed chunks live in shared object storage (S3/GCS/Azure).
	// Followers learn about them via the vault-ctl FSM's OnUpload callback
	// and read directly from the bucket — no record streaming needed.
	meta, err := vaultInst.Chunks.Meta(chunkID)
	if err == nil && meta.CloudBacked {
		o.replicationLogger.Debug("replication: skipping cloud-backed chunk (shared bucket)",
			"vault", vaultID, "chunk", chunkID.String())
		return
	}

	// Track per-target outcomes so the caller / logs reflect the actual
	// replica count rather than the target count. Without this, a single
	// unhealthy follower silently caps every chunk at less-than-RF and
	// the only signal is the chunk's replica_count column. See gastrolog-3tn5g.
	var (
		succeeded   int
		failedNodes []string
	)
	for _, tgt := range targets {
		if err := o.replicateToTarget(ctx, vaultID, chunkID, vaultInst.Chunks, tgt); err != nil {
			failedNodes = append(failedNodes, tgt.NodeID)
		} else {
			succeeded++
		}
	}
	// Replication on the leader counts toward RF too — the chunk lives
	// locally on the leader's storage by virtue of being sealed there.
	// Total replicas = leader (1) + successful follower copies.
	totalReplicas := 1 + succeeded
	expectedReplicas := 1 + len(targets)
	if len(failedNodes) > 0 {
		// Placement churn means the peer evicted its vault instance —
		// expected during reconfiguration, not a degraded-replication signal.
		// We don't know per-target whether failure was churn-shaped here, so
		// log at WARN with actual counts; the per-target log inside
		// replicateToTarget already classified each individual error.
		o.replicationLogger.Warn("replication: chunk replicated to fewer followers than expected",
			"vault", vaultID, "chunk", chunkID.String(),
			"replicas", totalReplicas, "expected", expectedReplicas,
			"failed_nodes", failedNodes)
	} else {
		o.replicationLogger.Debug("replication: chunk fully replicated",
			"vault", vaultID, "chunk", chunkID.String(),
			"replicas", totalReplicas)
	}
}

// replicateToTarget sends a sealed chunk to one target. Same-node targets
// use local ImportToInstanceStorage; cross-node targets use gRPC. Returns nil
// on success so the caller can count actual replicas vs configured targets.
func (o *Orchestrator) replicateToTarget(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID, sourceCM chunk.ChunkManager, tgt system.ReplicationTarget) error {
	if tgt.NodeID == o.localNodeID {
		if err := o.replicateLocally(ctx, vaultID, tgt.StorageID, chunkID, sourceCM); err != nil {
			o.replicationLogger.Warn("replication: local copy failed",
				"vault", vaultID, "storage", tgt.StorageID,
				"chunk", chunkID.String(), "error", err)
			return err
		}
		o.replicationLogger.Debug("replication: local copy done",
			"vault", vaultID, "storage", tgt.StorageID,
			"chunk", chunkID.String())
		return nil
	}
	if err := o.replicateToFollower(ctx, vaultID, chunkID, sourceCM, tgt.NodeID); err != nil {
		// Placement churn (peer evicted the vault instance) is expected
		// during reconfiguration and gets logged at Debug rather than
		// WARN-spamming the operator dashboard. See gastrolog-5z607.
		level := slog.LevelWarn
		if IsPlacementChurnErr(err) {
			level = slog.LevelDebug
		}
		o.replicationLogger.Log(ctx, level, "replication: sealed chunk failed",
			"node", tgt.NodeID, "vault", vaultID,
			"chunk", chunkID.String(), "error", err)
		return err
	}
	o.replicationLogger.Debug("replication: sealed chunk sent",
		"node", tgt.NodeID, "vault", vaultID,
		"chunk", chunkID.String())
	return nil
}

// replicateLocally copies a sealed chunk to a different storage-targeted
// vault instance on the same node. Opens a cursor on the source, then
// imports into the target via ImportToInstanceStorage.
func (o *Orchestrator) replicateLocally(ctx context.Context, vaultID glid.GLID, storageID string, chunkID chunk.ChunkID, sourceCM chunk.ChunkManager) error {
	cursor, err := sourceCM.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
	}
	defer func() { _ = cursor.Close() }()

	iter := chunk.CursorIterator(cursor)
	return o.ImportToInstanceStorage(ctx, vaultID, storageID, chunkID, iter)
}

// replicateToFollower streams a single sealed chunk to one follower node.
// Validates that the chunk is readable before opening the network stream —
// corrupted chunks fail fast without touching the wire.
func (o *Orchestrator) replicateToFollower(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID, cm chunk.ChunkManager, nodeID string) error {
	if o.chunkReplicator == nil {
		return errors.New("replicateToFollower: chunk replicator not configured")
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
	// The cursor streams records on demand; ImportSealedChunk consumes it
	// via a RecordIterator, so nothing proportional to chunk size lands on
	// the leader's heap during the push. See gastrolog-4yvhh.
	cursor, err := cm.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
	}
	defer func() { _ = cursor.Close() }()

	// Final tombstone check right before sending: retention may have
	// deleted this chunk while we were validating readability. Without
	// the recheck, a late ImportSealed would still land on the follower
	// after the follower has already processed the delete via vault-ctl
	// Raft, and the follower's post-import cleanup only catches the
	// case where the tombstone is on its own FSM. This leader-side
	// recheck short-circuits the RPC entirely when the leader already
	// knows the chunk is gone. See gastrolog-11rzz.
	vaultInst := o.findLocalVaultInstance(vaultID)
	if vaultInst != nil && vaultInst.IsTombstoned != nil && vaultInst.IsTombstoned(chunkID) {
		o.replicationLogger.Debug("replication: chunk tombstoned after probe, aborting send",
			"vault", vaultID, "chunk", chunkID.String(), "node", nodeID)
		return nil
	}

	return o.chunkReplicator.ImportSealedChunk(ctx, nodeID, vaultID, chunkID, chunk.CursorIterator(cursor))
}
