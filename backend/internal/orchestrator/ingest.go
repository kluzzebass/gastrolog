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
// recursive RLock behind a waiting writer. Found via the gastrolog-38snf4
// gate forensics (TestDrainConcurrentWithIngestion 10-minute hang).
func (o *Orchestrator) schedulePostSeal(vaultID glid.GLID, cm chunk.ChunkManager, chunkID chunk.ChunkID) {
	o.mu.RLock()
	_, pipeline := o.pipelineVaults[vaultID]
	var followerTargets []system.ReplicationTarget
	if !pipeline {
		followerTargets = o.followerReplicationTargetsLocked(vaultID, cm)
	}
	o.mu.RUnlock()

	processor, ok := cm.(chunk.ChunkPostSealProcessor)
	if ok {
		name := fmt.Sprintf("post-seal:%s:%s", vaultID, chunkID)
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
		if err := o.scheduler.RunOnce(name, wrappedFn, context.Background(), chunkID); err != nil {
			o.logger.Warn("failed to schedule post-seal", "name", name, "error", err)
		}
		o.scheduler.Describe(name, fmt.Sprintf("Post-seal pipeline for chunk %s", chunkID))
		return
	}

	// No post-processing — schedule replication directly. The legacy
	// ChunkCompressor fallback is gone (gastrolog-24m1t step 7e); only
	// chunkfile.Manager implemented it, and it now goes through the
	// PostSealProcess branch above.
	o.scheduleReplication(vaultID, chunkID, followerTargets)
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
