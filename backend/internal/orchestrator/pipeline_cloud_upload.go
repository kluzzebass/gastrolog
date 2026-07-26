package orchestrator

import (
	"fmt"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

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

	name := fmt.Sprintf("pipeline-cloud-upload:%s:%s", vaultID, chunkID)
	if o.scheduler.HasPendingPrefix(name) {
		return
	}
	if err := o.scheduler.RunOnce(name, func(id chunk.ChunkID) error {
		err := uploader.UploadToCloud(id)
		if err != nil {
			o.cloudHealthLogger.Warn("pipeline cloud upload failed",
				"vault", vaultID, "chunk", id, "error", err)
		}
		return err
	}, chunkID); err != nil {
		o.cloudHealthLogger.Warn("failed to schedule pipeline cloud upload",
			"name", name, "error", err)
		return
	}
	o.scheduler.Describe(name, fmt.Sprintf("Pipeline cloud upload for chunk %s", chunkID))
}
