package orchestrator

import (
	"context"
	"fmt"

	"gastrolog/internal/glid"

	"gastrolog/internal/chunk"
)

// postSealWork schedules the post-seal pipeline for a newly sealed chunk and
// notifies WatchChunks subscribers. Takes no orchestrator lock, so it is safe
// to call from the lifecycle reconciler or with o.mu held.
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
// as one sequential job; other managers have no post-seal work.
//
// This runs for pipeline-registered vaults too. The chunk manager's Seal()
// fires AnnounceBeginSeal (Active → Sealing) and the matching AnnounceSeal
// (Sealing → Sealed) lives inside PostSealProcess — the two are halves of one
// FSM transition, so skipping the post-seal pipeline parks the manifest entry
// in Sealing forever. Pipeline-produced chunks never reach this function:
// they live in the pipeline chunk root, the pipeline commits CmdSealChunk for
// them itself, and every caller here passes a chunk the local chunk manager
// holds.
//
// Lock-free: callers may hold o.mu or run on a Raft apply pump, and nothing
// here takes o.mu.
func (o *Orchestrator) schedulePostSeal(vaultID glid.GLID, cm chunk.ChunkManager, chunkID chunk.ChunkID) {
	if processor, ok := cm.(chunk.ChunkPostSealProcessor); ok {
		o.schedulePostSealProcessing(vaultID, cm, processor, chunkID)
	}
}

// postSealJobName is the scheduler name claimed for one chunk's post-seal. The
// claim IS this string, so anything that needs to know whether a post-seal is
// already in flight has to build it the same way — hence one function rather
// than two format calls that can drift.
func postSealJobName(vaultID glid.GLID, chunkID chunk.ChunkID) string {
	return postSealJobPrefix(vaultID) + chunkID.String()
}

func postSealJobPrefix(vaultID glid.GLID) string {
	return "post-seal:" + vaultID.String() + ":"
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
) {
	// Describe BEFORE scheduling: the description is read into the Scheduled
	// event's JobInfo, and completeOneTimeJob deletes the entry when the job
	// finishes, so a Describe after scheduling loses the label on the event
	// and re-adds an entry nothing is left to remove.
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
		return nil
	}

	// Claim the name. Several independent paths reach post-seal for one chunk
	// with no coordination between them — postSealWork, two direct calls in
	// vault_ops, and the lifecycle reconciler's
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
