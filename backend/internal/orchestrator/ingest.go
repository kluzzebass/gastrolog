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
	o.mu.RLock()
	defer o.mu.RUnlock()
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
func (o *Orchestrator) schedulePostSeal(vaultID glid.GLID, cm chunk.ChunkManager, chunkID chunk.ChunkID) {
	if o.isPipelineIngestVault(vaultID) {
		return
	}
	followerTargets := o.followerReplicationTargets(vaultID, cm)

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

// followerReplicationTargets returns the follower targets for the vault that
// owns the given ChunkManager. Returns nil if not found or if the vault is a
// follower (followers don't replicate further).
func (o *Orchestrator) followerReplicationTargets(vaultID glid.GLID, cm chunk.ChunkManager) []system.ReplicationTarget {
	vault := o.vaults[vaultID]
	if vault == nil {
		return nil
	}
	if vaultInst := vault.Instance; vaultInst != nil && vaultInst.Chunks == cm && vaultInst.ShouldForwardToFollowers() {
		return vaultInst.FollowerTargets
	}
	return nil
}
