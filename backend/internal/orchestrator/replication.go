package orchestrator

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"log/slog"

	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
	"gastrolog/internal/system"

	"golang.org/x/sync/errgroup"
)

// SealActiveTier seals the active chunk for a specific tier, on a **follower**
// node, as the local effect of a tier-leader-originated SealTier replication
// command. Use SealActive on the leader-triggered path.
//
// Role: follower-side. Caller is typically the TierReplicator handler that
// receives the seal command from the leader. Validates expectedChunkID to
// avoid sealing the wrong chunk if rotation raced the seal command.
//
// Readiness: no Vault.ReadinessErr gate — this call executes a replicated
// command that the leader already authorized. The follower's own FSM
// manifest may lag, but the physical seal (flush + close file) is local
// and safe regardless.
//
// Do not merge with SealActive: the two paths run on different nodes with
// different invariants. SealActive (leader) fans out replication; this
// function is the target of that fan-out on followers.
func (o *Orchestrator) SealActiveTier(vaultID, tierID glid.GLID, expectedChunkID chunk.ChunkID) error {
	tier := o.findLocalTier(vaultID, tierID)
	if tier == nil {
		return fmt.Errorf("%w: tier %s in vault %s", ErrTierNotLocal, tierID, vaultID)
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
// All tier follower AppendRecords and all cross-node ForwardSync calls run
// concurrently under one deadline (cluster.ReplicationTimeout). The first
// error wins and is sent to the ack channel; errgroup cancels the shared
// context so other RPCs stop promptly.
//
// Cross-node forwarding uses RecordForwarder.ForwardSync for records that
// matched filters targeting vaults on other nodes. See gastrolog-27zvt:
// before that fix, ack-gated remote routes were fire-and-forget and could
// be silently dropped on a full forward buffer.
func (o *Orchestrator) ackAfterReplication(ack chan<- error, pa *pendingAcks, rec chunk.Record) {
	if pa == nil {
		ack <- nil
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), cluster.ReplicationTimeout)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	if o.tierReplicator != nil {
		for _, t := range pa.replication {
			for _, tgt := range t.targets {
				g.Go(func() error {
					if err := o.tierReplicator.AppendRecords(ctx, tgt.NodeID, t.vaultID, t.tierID, t.chunkID, []chunk.Record{rec}); err != nil {
						return fmt.Errorf("ack-gated replication to %s: %w", tgt.NodeID, err)
					}
					return nil
				})
			}
		}
	}

	if o.forwarder != nil {
		for _, f := range pa.forwards {
			g.Go(func() error {
				if err := o.forwarder.ForwardSync(ctx, f.nodeID, f.vaultID, []chunk.Record{rec}); err != nil {
					return fmt.Errorf("ack-gated forward to %s: %w", f.nodeID, err)
				}
				return nil
			})
		}
	}

	ack <- g.Wait()
}

// scheduleReplication schedules a separate job to replicate a sealed chunk.
// Decoupled from the post-seal pipeline — never blocks compression or indexing.
func (o *Orchestrator) scheduleReplication(vaultID, tierID glid.GLID, chunkID chunk.ChunkID, targets []system.ReplicationTarget) {
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
// need record streaming. The tier FSM's OnUpload callback registers the
// chunk in each follower's cloud index (see wireTierFSMOnUpload).
func (o *Orchestrator) replicateSealedChunk(ctx context.Context, vaultID, tierID glid.GLID, chunkID chunk.ChunkID, targets []system.ReplicationTarget) {
	if o.transferrer == nil || len(targets) == 0 {
		return
	}

	tier := o.findLocalTier(vaultID, tierID)
	if tier == nil {
		o.logger.Warn("replication: tier not found for sealed chunk",
			"vault", vaultID, "tier", tierID, "chunk", chunkID.String())
		return
	}

	// If retention deleted the chunk while this replication job was queued,
	// the tier FSM now holds a tombstone for it. Skip the replication —
	// sending ImportSealedChunk to followers would recreate a chunk the
	// cluster has already decided to forget (ghost chunk). Closes the
	// retention-beats-replication ordering at the leader; the receiver-side
	// tombstone check closes the reverse ordering. See gastrolog-11rzz.
	if tier.IsTombstoned != nil && tier.IsTombstoned(chunkID) {
		o.logger.Debug("replication: skipping tombstoned chunk (retention beat replication)",
			"vault", vaultID, "tier", tierID, "chunk", chunkID.String())
		return
	}

	// Cloud-backed chunks live in shared object storage (S3/GCS/Azure).
	// Followers learn about them via the tier FSM's OnUpload callback
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
func (o *Orchestrator) replicateToTarget(ctx context.Context, vaultID, tierID glid.GLID, chunkID chunk.ChunkID, sourceCM chunk.ChunkManager, tgt system.ReplicationTarget) {
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
		// Placement churn (peer evicted the tier instance) is expected
		// during reconfiguration and gets logged at Debug rather than
		// WARN-spamming the operator dashboard. See gastrolog-5z607.
		level := slog.LevelWarn
		if IsPlacementChurnErr(err) {
			level = slog.LevelDebug
		}
		o.logger.Log(ctx, level, "replication: sealed chunk failed",
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
func (o *Orchestrator) replicateLocally(ctx context.Context, vaultID, tierID glid.GLID, storageID string, chunkID chunk.ChunkID, sourceCM chunk.ChunkManager) error {
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
func (o *Orchestrator) replicateToFollower(ctx context.Context, vaultID, tierID glid.GLID, chunkID chunk.ChunkID, cm chunk.ChunkManager, nodeID string) error {
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

	// Binary replication path (gastrolog-3o5b4): if the chunk manager has
	// a sealed `data.glcb` blob to copy, stream it directly via ImportBlob.
	// Production (chunk/file.Manager) takes this path. Memory-backed test
	// harnesses don't implement ChunkBlobReader and fall back to the
	// legacy per-record ImportSealedChunk path below.
	if blobReader, ok := cm.(chunk.ChunkBlobReader); ok {
		body, size, err := blobReader.OpenSealedBlob(chunkID)
		if err != nil {
			return fmt.Errorf("open sealed blob: %w", err)
		}
		defer func() { _ = body.Close() }()

		// Final tombstone check right before sending: retention may have
		// deleted this chunk while we were opening the blob. The
		// follower's tombstone check (in AdoptSealedBlobToTier) is the
		// second line of defence; this leader-side recheck short-circuits
		// the RPC entirely when the leader already knows the chunk is gone.
		// See gastrolog-11rzz.
		tier := o.findLocalTier(vaultID, tierID)
		if tier != nil && tier.IsTombstoned != nil && tier.IsTombstoned(chunkID) {
			o.logger.Debug("replication: chunk tombstoned after blob open, aborting send",
				"vault", vaultID, "tier", tierID, "chunk", chunkID.String(), "node", nodeID)
			return nil
		}

		digest, err := o.tierReplicator.ImportBlob(ctx, nodeID, vaultID, tierID, chunkID, size, body)
		if err != nil {
			return fmt.Errorf("ImportBlob to %s: %w", nodeID, err)
		}
		o.logger.Debug("replication: ImportBlob ack",
			"vault", vaultID, "tier", tierID, "chunk", chunkID.String(), "node", nodeID,
			"size", size, "digest", hex.EncodeToString(digest[:8]))
		return nil
	}

	// Legacy per-record path. Reached only by memory-backed test harnesses
	// (no on-disk data.glcb to copy). Production replication never lands
	// here. Will be removed alongside ImportSealedChunk once the test
	// harness is migrated.
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

	tier := o.findLocalTier(vaultID, tierID)
	if tier != nil && tier.IsTombstoned != nil && tier.IsTombstoned(chunkID) {
		o.logger.Debug("replication: chunk tombstoned after cursor read, aborting send",
			"vault", vaultID, "tier", tierID, "chunk", chunkID.String(), "node", nodeID)
		return nil
	}

	return o.tierReplicator.ImportSealedChunk(ctx, nodeID, vaultID, tierID, chunkID, records)
}
