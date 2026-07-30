package orchestrator

import (
	"context"
	"fmt"

	"gastrolog/internal/glid"

	"gastrolog/internal/chunk"
	"gastrolog/internal/system"
)

// postSealWork schedules the post-seal pipeline for a newly sealed chunk.
// Safe to call from any context (sealed-chunk import, lifecycle reconciler,
// leader-triggered SealActive) — acquires the orchestrator lock internally.
func (o *Orchestrator) postSealWork(vaultID glid.GLID, cm chunk.ChunkManager, chunkID chunk.ChunkID) {
	o.schedulePostSeal(vaultID, cm, chunkID)
	// Notify WatchChunks subscribers: the chunk's sealed flag has changed.
	// Fetch the post-seal meta so the event carries the final state instead
	// of forcing a refetch on the client.
	if meta, err := cm.Meta(chunkID); err == nil {
		o.EmitChunkSealed(vaultID, meta)
	} else {
		o.NotifyChunkChange() // fall back to bare wake-up if meta lookup failed
	}
}

// schedulePostSeal schedules the unified post-seal pipeline (compress → index → upload).
// If the chunk manager implements ChunkPostSealProcessor, the entire pipeline runs
// as one sequential job. Otherwise falls back to compress-only for non-file managers.
// After the pipeline completes, sealed-chunk replication is triggered for leader vaults.
//
// This runs for pipeline-registered vaults too. The chunk manager's Seal()
// fires AnnounceBeginSeal (Active → Sealing) and the matching AnnounceSeal
// (Sealing → Sealed) lives inside PostSealProcess — the two are halves of one
// FSM transition, so skipping the post-seal pipeline parks the manifest entry
// in Sealing forever. A vault-level "is this a pipeline vault" gate used to
// short-circuit here, which stranded every chunk-manager seal on a pipeline
// vault (i.e. on every vault with a local instance, since reloadPipelineFromConfig
// registers each vault this node homes). Pipeline-produced chunks never reach
// this function: they live in the pipeline chunk root, the pipeline commits
// CmdSealChunk for them itself, and every caller here passes a chunk the local
// chunk manager holds. The one thing the pipeline genuinely replaces —
// follower record-stream replication — stays gated inside scheduleReplication.
//
// Self-locking: callers must NOT hold o.mu. The old contract (callers wrap in
// RLock for the vault-map read, while isPipelineIngestVault re-RLocked
// internally) deadlocked the node whenever a writer (DrainVault, retention
// sweep, config reload) queued between the two acquisitions — RWMutex blocks
// recursive RLock behind a waiting writer. Found via a 10-minute hang in
// TestDrainConcurrentWithIngestion.
func (o *Orchestrator) schedulePostSeal(vaultID glid.GLID, cm chunk.ChunkManager, chunkID chunk.ChunkID) {
	// The pipeline check reads the lock-free snapshot, so a pipeline vault
	// reaches the scheduling below without touching o.mu at all — the same
	// hazard as above, one layer out: a Raft apply pump that blocks here
	// stalls every apply for the group, and a caller holding o.mu while
	// awaiting one of those applies then never returns.
	// Only the legacy follower-replication branch still needs the lock.
	_, pipeline := o.lookupPipelineVault(vaultID)
	var followerTargets []system.ReplicationTarget
	if !pipeline {
		o.mu.RLock()
		followerTargets = o.followerReplicationTargetsLocked(vaultID, cm)
		o.mu.RUnlock()
	}

	if processor, ok := cm.(chunk.ChunkPostSealProcessor); ok {
		o.schedulePostSealProcessing(vaultID, cm, processor, chunkID, followerTargets)
		return
	}

	// No post-processing — schedule replication directly.
	o.scheduleReplication(vaultID, chunkID, followerTargets)
}

// postSealJobName is the scheduler name claimed for one chunk's post-seal. The
// claim IS this string, so anything that needs to know whether a post-seal is
// already in flight has to build it the same way — hence one function rather
// than two format calls that can drift.
func postSealJobName(vaultID glid.GLID, chunkID chunk.ChunkID) string {
	return fmt.Sprintf("post-seal:%s:%s", vaultID, chunkID)
}

// postSealInFlight reports whether a post-seal job for this chunk is currently
// registered — i.e. RunOnceIfAbsent would decline a second one.
func (o *Orchestrator) postSealInFlight(vaultID glid.GLID, chunkID chunk.ChunkID) bool {
	if o == nil || o.scheduler == nil {
		return false
	}
	return o.scheduler.HasJob(postSealJobName(vaultID, chunkID))
}

// schedulePostSealProcessing registers the post-seal pipeline for one chunk.
// Split out of schedulePostSeal to keep that function's nesting within budget.
func (o *Orchestrator) schedulePostSealProcessing(
	vaultID glid.GLID,
	cm chunk.ChunkManager,
	processor chunk.ChunkPostSealProcessor,
	chunkID chunk.ChunkID,
	followerTargets []system.ReplicationTarget,
) {
	// Describe BEFORE scheduling — see scheduleReplication for why (missing
	// label on the Scheduled event, leaked descriptions entry when the job
	// finishes first).
	name := postSealJobName(vaultID, chunkID)
	o.scheduler.Describe(name, fmt.Sprintf("Post-seal pipeline for chunk %s", chunkID))
	wrappedFn := func(ctx context.Context, id chunk.ChunkID) error {
		if err := processor.PostSealProcess(ctx, id); err != nil {
			return err
		}
		// Notify: compression/indexing done, chunk meta changed again
		// (DiskBytes, etc.). Carry the fresh meta so clients can patch
		// their cache without a refetch.
		if meta, err := cm.Meta(id); err == nil {
			o.EmitChunkSealed(vaultID, meta)
		} else {
			o.NotifyChunkChange()
		}
		// Schedule replication as a separate job — never blocks the
		// post-seal scheduler slot.
		o.scheduleReplication(vaultID, id, followerTargets)
		return nil
	}

	// Claim the name. Several independent paths reach post-seal for one chunk
	// with no coordination between them — postSealWork from ingest, two direct
	// calls in vault_ops, vault_drain, and the lifecycle reconciler's
	// sealLocalActive / sealMetadataOnlyOrphan — and RunOnce would overwrite the
	// registry entry without stopping the job already running, so the work would
	// run twice. It is not idempotent: sealToGLCB rebuilds the GLCB from the
	// record cursor with no already-built short-circuit, then re-announces and
	// re-enters the upload path — the shape that produces duplicate S3 PUTs and
	// duplicate Raft commands. The cloud-upload path claims its name the same
	// way.
	//
	// A claim on IN-FLIGHT work only: once the job completes the name frees, so
	// a later post-seal of the same chunk still runs.
	scheduled, err := o.scheduler.RunOnceIfAbsent(name, wrappedFn, context.Background(), chunkID)
	if err != nil {
		o.logger.Warn("failed to schedule post-seal", "name", name, "error", err)
	} else if !scheduled {
		o.logger.Debug("post-seal already in flight for this chunk; not scheduling a second",
			"vault", vaultID, "chunk", chunkID)
	}
}

// followerReplicationTargetsLocked returns the follower targets for the vault
// that owns the given ChunkManager. Returns nil if not found or if the vault
// is a follower (followers don't replicate further). Caller holds o.mu.
func (o *Orchestrator) followerReplicationTargetsLocked(vaultID glid.GLID, cm chunk.ChunkManager) []system.ReplicationTarget {
	vault := o.vaults[vaultID]
	if vault == nil {
		return nil
	}
	if vaultInst := vault.Instance; vaultInst != nil && vaultInst.Chunks == cm && vaultInst.ShouldForwardToFollowers() {
		return vaultInst.FollowerTargets
	}
	return nil
}
