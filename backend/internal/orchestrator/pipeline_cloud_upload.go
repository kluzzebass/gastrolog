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
	o.mu.RLock()
	defer o.mu.RUnlock()

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
	if vaultInst.ManifestEntry != nil {
		e, ok := vaultInst.ManifestEntry(chunkID)
		if !ok || e.State != chunk.ChunkStateSealed || e.CloudBacked {
			return
		}
	} else if vaultInst.OverlayFromFSM != nil {
		meta, err := vaultInst.Chunks.Meta(chunkID)
		if err != nil {
			return
		}
		meta = vaultInst.OverlayFromFSM(meta)
		if !meta.Sealed || meta.CloudBacked {
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
