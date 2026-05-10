package orchestrator

import (
	"fmt"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
)

// cloudHealthChecker is an optional interface implemented by chunk managers
// that have a cloud backing store. The orchestrator polls this every 5s
// to raise/clear a "cloud-store:<vaultID>" alert.
type cloudHealthChecker interface {
	CloudDegraded() bool
	CloudDegradedError() string
}

// evaluateCloudHealth checks every instance's cloud health and sets/clears
// alerts. When an instance transitions from degraded → healthy, schedules
// post-seal work for sealed chunks that are missing their cloud upload.
// Runs in the rate alert evaluator loop (every 5s).
func (o *Orchestrator) evaluateCloudHealth() {
	if o.alerts == nil {
		return
	}
	o.mu.RLock()
	defer o.mu.RUnlock()

	for _, vault := range o.vaults {
		vaultInst := vault.Instance
		if vaultInst == nil || vaultInst.Type != "cloud" {
			continue
		}
		o.evaluateVaultCloudHealth(vaultInst)
	}
}

// evaluateVaultCloudHealth checks a single cloud instance's health and runs
// backfill on the vault leader only. Followers skip backfill — they learn
// about cloud-backed chunks via the vault-ctl FSM.
func (o *Orchestrator) evaluateVaultCloudHealth(vaultInst *VaultInstance) {
	chk, ok := vaultInst.Chunks.(cloudHealthChecker)
	if !ok {
		return
	}
	alertID := fmt.Sprintf("cloud-store:%s", vaultInst.VaultID)
	if chk.CloudDegraded() {
		o.alerts.Set(alertID, alert.Error, "cloud",
			fmt.Sprintf("Cloud store unreachable for vault %s: %s",
				vaultInst.VaultID.String()[:8], chk.CloudDegradedError()))
	} else {
		o.alerts.Clear(alertID)
	}
	if vaultInst.IsRaftLeader != nil && vaultInst.IsRaftLeader() {
		o.backfillCloudUploads(vaultInst)
	}
}

// backfillCloudUploads reconciles sealed chunks against the vault-ctl FSM
// (the single source of truth for CloudBacked). For every sealed chunk
// where the FSM says CloudBacked=false, it schedules an UploadToCloud job.
// UploadToCloud does a Head check — if the blob already exists in S3, it
// adopts and fires AnnounceUpload to update the FSM. If not, it uploads.
//
// The local CloudBacked flag from List() is intentionally ignored — only
// the FSM decides whether a chunk needs work. See gastrolog-68fqk.
func (o *Orchestrator) backfillCloudUploads(vaultInst *VaultInstance) {
	uploader, ok := vaultInst.Chunks.(chunk.ChunkCloudUploader)
	if !ok {
		return
	}

	metas, err := vaultInst.Chunks.List()
	if err != nil {
		return
	}

	var backfilled int
	for _, m := range metas {
		// Phase 3 (gastrolog-1huz5): gate on FSM-Sealed, not local.
		// During the Sealing window the leader has closed active-form
		// files but data.glcb does not exist yet — uploading would
		// fail with no-such-file. Overlaying through the FSM makes us
		// wait for AnnounceSeal in PostSealProcess.
		if vaultInst.OverlayFromFSM != nil {
			m = vaultInst.OverlayFromFSM(m)
		}
		if !m.Sealed || chunkIsCloudBacked(vaultInst, m) {
			continue
		}
		name := fmt.Sprintf("cloud-backfill:%s:%s", vaultInst.VaultID, m.ID)
		if o.scheduler.HasPendingPrefix(name) {
			continue
		}
		if err := o.scheduler.RunOnce(name, func(id chunk.ChunkID) error {
			return uploader.UploadToCloud(id)
		}, m.ID); err == nil {
			backfilled++
		}
		o.scheduler.Describe(name, fmt.Sprintf("Cloud backfill upload for chunk %s", m.ID))
	}
	if backfilled > 0 {
		o.logger.Debug("cloud backfill: scheduled uploads",
			"vault", vaultInst.VaultID, "count", backfilled)
	}
}

// chunkIsCloudBacked checks the FSM (single source of truth) for CloudBacked.
// Falls back to local state when no FSM exists (single-node / memory mode).
func chunkIsCloudBacked(vaultInst *VaultInstance, m chunk.ChunkMeta) bool {
	if vaultInst.OverlayFromFSM != nil {
		return vaultInst.OverlayFromFSM(m).CloudBacked
	}
	return m.CloudBacked
}
