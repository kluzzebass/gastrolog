package orchestrator

import (
	"context"
	"errors"
	"fmt"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
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
// exclusive.
func cloudUploadJobName(vaultID glid.GLID, chunkID chunk.ChunkID) string {
	return cloudUploadJobPrefix(vaultID) + chunkID.String()
}

// cloudUploadJobPrefix names one vault's cloud-upload jobs.
func cloudUploadJobPrefix(vaultID glid.GLID) string {
	return "cloud-upload:" + vaultID.String() + ":"
}

// schedulePipelineCloudUpload schedules the cloud upload of a sealed pipeline
// chunk whose agreed GLCB has landed on this home (onGLCBLanded), when this
// node owes it (cloudUploadOwed). The job asks again when it runs: an upload
// still queued after the uploader moved is left to the new uploader, whose
// catch-up sweep finds the chunk.
func (o *Orchestrator) schedulePipelineCloudUpload(vaultID glid.GLID, chunkID chunk.ChunkID) {
	if !o.isPipelineIngestVault(vaultID) {
		return
	}
	// No o.mu here: findLocalVaultInstance takes its own read lock, and a
	// recursive read acquisition wedges the node once a writer queues
	// between the two. Everything below works on the returned instance and
	// the scheduler, which lock for themselves.
	vaultInst := o.findLocalVaultInstance(vaultID)
	if vaultInst == nil {
		return
	}
	uploader, ok := vaultInst.Chunks.(chunk.ChunkCloudUploader)
	if !ok || !o.cloudUploadOwed(vaultInst, chunkID) {
		return
	}

	// Describe BEFORE scheduling: the description is read into the Scheduled
	// event's JobInfo, and completeOneTimeJob deletes the entry when the job
	// finishes, so describing afterwards loses the label on the event and
	// leaks one descriptions entry per chunk for jobs that finished first.
	name := cloudUploadJobName(vaultID, chunkID)
	o.scheduler.Describe(name, fmt.Sprintf("Pipeline cloud upload for chunk %s", chunkID))
	// Claim-or-skip in one step: this path and the catch-up sweep share the
	// name, so a chunk has at most one upload job on this node.
	if _, err := o.scheduler.RunOnceIfAbsent(name, func(id chunk.ChunkID) error {
		if !o.cloudUploadOwed(vaultInst, id) {
			return nil
		}
		err := uploader.UploadToCloud(id)
		if errors.Is(err, chunk.ErrNotCloudUploader) {
			return nil
		}
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

// cloudUploadOwed reports whether this node owes a chunk its cloud upload
// now: it uploads for the vault (vaultInstRunsCloudBackfill), and the
// vault-ctl FSM has the chunk sealed and not yet cloud-backed. ManifestEntry
// is nil only in memory mode, where there is no FSM to consult.
func (o *Orchestrator) cloudUploadOwed(vaultInst *VaultInstance, id chunk.ChunkID) bool {
	if !o.vaultInstRunsCloudBackfill(vaultInst) {
		return false
	}
	if vaultInst.ManifestEntry == nil {
		return true
	}
	e, ok := vaultInst.ManifestEntry(id)
	return ok && e.State == chunk.ChunkStateSealed && !e.CloudBacked
}

// isCloudUploader reports whether this node uploads the vault's sealed chunks
// to its cloud store, read from the replicated config at the moment of asking:
// the uploader is the vault's placement leader. Without a system loader there
// is no replicated placement, and the node uploads the vaults it runs.
func (o *Orchestrator) isCloudUploader(vaultID glid.GLID) bool {
	loader := o.systemLoader()
	if loader == nil {
		return true
	}
	sys, err := loader.Load(context.Background())
	if err != nil || sys == nil {
		return false
	}
	return system.LeaderNodeID(sys.PlacementsFor(vaultID), sys.Runtime.NodeStorageConfigs) == o.localNodeID
}

// installCloudUploaderCheck gates every upload the instance's chunk manager
// performs — the post-seal pipeline's included — on isCloudUploader.
func (o *Orchestrator) installCloudUploaderCheck(inst *VaultInstance, vaultID glid.GLID) {
	if inst == nil || inst.Chunks == nil {
		return
	}
	if setter, ok := inst.Chunks.(chunk.CloudUploaderSetter); ok {
		setter.SetCloudUploader(func() bool { return o.isCloudUploader(vaultID) })
	}
}
