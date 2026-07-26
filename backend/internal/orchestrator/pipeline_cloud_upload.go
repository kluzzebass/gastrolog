package orchestrator

import (
	"fmt"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// cloudUploadJobName is the scheduler job name for "upload this chunk to the
// vault's cloud store". It is deliberately SHARED by both paths that can ask
// for that work — the live seal effect (schedulePipelineCloudUpload) and the
// catch-up sweep (backfillCloudUploads) — because it is the idempotency key
// they claim through Scheduler.RunOnceIfAbsent.
//
// The two paths used to name the same work differently
// ("pipeline-cloud-upload:…" vs "cloud-backfill:…"), so they deduplicated
// against themselves but never against each other: a seal effect and a
// catch-up sweep landing on the same chunk each enqueued their own upload job
// and the chunk was uploaded twice. One name is what makes them mutually
// exclusive. See gastrolog-3hwngy.
func cloudUploadJobName(vaultID glid.GLID, chunkID chunk.ChunkID) string {
	return cloudUploadJobPrefix(vaultID) + ":" + chunkID.String()
}

// cloudUploadJobPrefix is the per-vault name prefix for every cloud-upload
// job, used by vault teardown/unregister to cancel outstanding uploads with
// RemoveJobsByPrefix before the chunk manager closes.
func cloudUploadJobPrefix(vaultID glid.GLID) string {
	return "cloud-upload:" + vaultID.String()
}

// schedulePipelineCloudUpload schedules uploading a pipeline-built sealed chunk
// to object storage when the vault has cloud backing on this node. Leader-only;
// followers adopt via vault-ctl CmdUploadChunk. See gastrolog-34azvz.
func (o *Orchestrator) schedulePipelineCloudUpload(vaultID glid.GLID, chunkID chunk.ChunkID) {
	if !o.isPipelineIngestVault(vaultID) {
		return
	}
	// No o.mu here: findLocalVaultInstance takes its own read lock, and an
	// outer RLock made this a recursive read acquisition — with a writer
	// queued between the two, the second RLock parks behind the writer,
	// the writer waits on the first hold, and the node wedges. This exact
	// shape deadlocked node-2 (gastrolog-1ug3rq) and node-1; the lock
	// tracker named this line. Everything below works on the returned
	// instance and the scheduler, which lock for themselves.
	vaultInst := o.findLocalVaultInstance(vaultID)
	if vaultInst == nil {
		return
	}
	if !vaultInstCanUploadToCloud(vaultInst) {
		return
	}
	uploader, ok := vaultInst.Chunks.(chunk.ChunkCloudUploader)
	if !ok {
		return
	}
	// The FSM manifest entry is the cluster-wide seal/upload gate: skip a
	// chunk the cluster hasn't finished sealing (GLCB not yet committed) or
	// has already uploaded. ManifestEntry is nil only in memory mode, where
	// cloud upload is already gated off above (vaultInstCanUploadToCloud).
	if vaultInst.ManifestEntry != nil {
		e, ok := vaultInst.ManifestEntry(chunkID)
		if !ok || e.State != chunk.ChunkStateSealed || e.CloudBacked {
			return
		}
	}

	// Describe BEFORE scheduling: the description is read into the Scheduled
	// event's JobInfo, and completeOneTimeJob deletes the entry when the job
	// finishes. Describing afterwards both lost the label on the event and
	// leaked one descriptions entry per chunk for jobs that finished first.
	name := cloudUploadJobName(vaultID, chunkID)
	o.scheduler.Describe(name, fmt.Sprintf("Pipeline cloud upload for chunk %s", chunkID))
	// Claim-or-skip in one step. A HasPendingPrefix check followed by RunOnce
	// is a check-then-act race: this path and the catch-up sweep can both
	// observe "nothing pending" and both enqueue an upload for one chunk.
	if _, err := o.scheduler.RunOnceIfAbsent(name, func(id chunk.ChunkID) error {
		err := uploader.UploadToCloud(id)
		if err != nil {
			o.cloudHealthLogger.Warn("pipeline cloud upload failed",
				"vault", vaultID, "chunk", id, "error", err)
		}
		return err
	}, chunkID); err != nil {
		o.cloudHealthLogger.Warn("failed to schedule pipeline cloud upload",
			"name", name, "error", err)
	}
}
