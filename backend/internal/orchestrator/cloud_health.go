package orchestrator

import (
	"fmt"

	"gastrolog/internal/chunk"
	"os"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
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
		if vaultInst == nil || !vaultInstanceHasCloudBacking(vaultInst) {
			continue
		}
		o.evaluateVaultCloudHealth(vaultInst)
	}
}

// vaultInstanceHasCloudBacking reports whether this instance participates in
// cloud upload / health monitoring: dedicated cloud vaults, or file vaults with
// a wired CloudStore on the placement leader. Followers (CloudReadOnly) are excluded.
func vaultInstanceHasCloudBacking(vi *VaultInstance) bool {
	return vaultInstCanUploadToCloud(vi) || (vi != nil && vi.Type == "cloud")
}

// vaultInstCanUploadToCloud reports whether this node's vault instance can
// perform S3 uploads (placement leader with CloudStore). Vault-ctl Raft
// leadership alone is insufficient — followers keep CloudReadOnly even when
// they are the ctl leader. See gastrolog-34azvz.
func vaultInstCanUploadToCloud(vi *VaultInstance) bool {
	if vi == nil || vi.Chunks == nil {
		return false
	}
	cs, ok := vi.Chunks.(interface{ CloudStoreConfigured() bool })
	return ok && cs.CloudStoreConfigured()
}

// evaluateVaultCloudHealth checks a single cloud instance's health and runs
// backfill on the vault leader only. Followers skip backfill — they learn
// about cloud-backed chunks via the vault-ctl FSM.
func (o *Orchestrator) evaluateVaultCloudHealth(vaultInst *VaultInstance) {
	chk, ok := vaultInst.Chunks.(cloudHealthChecker)
	if !ok {
		return
	}
	if chk.CloudDegraded() {
		o.alerts.Raise("cloud-store", vaultInst.VaultID.String(),
			fmt.Sprintf("Cloud store unreachable for vault %s: %s",
				vaultInst.VaultID.String()[:8], chk.CloudDegradedError()))
	} else {
		o.alerts.Clear("cloud-store", vaultInst.VaultID.String())
	}
	if vaultInstRunsCloudBackfill(vaultInst) {
		o.backfillCloudUploads(vaultInst)
	}
}

// vaultInstRunsCloudBackfill reports whether this node should schedule cloud
// upload backfill for a vault. File/cloud-backed pipeline vaults upload from
// the placement leader (CloudStore configured). Legacy type=cloud vaults keep
// the vault-ctl Raft leader gate.
func vaultInstRunsCloudBackfill(vi *VaultInstance) bool {
	if vi == nil {
		return false
	}
	if vi.Type == "cloud" {
		return vi.IsRaftLeader == nil || vi.IsRaftLeader()
	}
	return vaultInstCanUploadToCloud(vi)
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
	if !vaultInstRunsCloudBackfill(vaultInst) {
		return
	}
	uploader, ok := vaultInst.Chunks.(chunk.ChunkCloudUploader)
	if !ok {
		return
	}

	metas, err := vaultInst.Chunks.List()
	if err != nil {
		return
	}
	// Sealed manifest entries not yet lazily resolved by the manager
	// (post-restart) still need upload backfill (gastrolog-2kmgj6).
	metas = appendUnlistedManifestSealed(metas, vaultInst)

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
			err := uploader.UploadToCloud(id)
			if err != nil {
				o.logBackfillFailure(vaultInst.VaultID, id, err)
			}
			return err
		}, m.ID); err == nil {
			backfilled++
		}
		o.scheduler.Describe(name, fmt.Sprintf("Cloud backfill upload for chunk %s", m.ID))
	}
	if backfilled > 0 {
		o.cloudHealthLogger.Debug("cloud backfill: scheduled uploads",
			"vault", vaultInst.VaultID, "count", backfilled)
	}
}

// logBackfillFailure reports a failed backfill upload with the signal an
// operator needs: whether the GLCB exists on disk. Present-but-unuploadable
// means the chunk manager lost its registration (a bug — the restart
// registration gap produced 1,500 bare 'chunk not found' warns); absent
// means the local build simply hasn't finished (normal on followers,
// Debug). Both throttled per vault: retries every 5s flood otherwise.
func (o *Orchestrator) logBackfillFailure(vaultID glid.GLID, id chunk.ChunkID, err error) {
	onDisk := false
	if root, ok := o.pipelineVaultChunkRoot(vaultID); ok {
		if _, statErr := os.Stat(chunking.ChunkGLCBPath(root, id)); statErr == nil {
			onDisk = true
		}
	}
	n, allow := o.backfillLogThrottle.Allow(vaultID.String())
	if !allow {
		return
	}
	if onDisk {
		o.cloudHealthLogger.Warn("cloud backfill failed for chunk present on disk — chunk manager registration missing",
			"vault", vaultID, "chunk", id, "error", err, "suppressed", n)
		return
	}
	o.cloudHealthLogger.Debug("cloud backfill awaiting local build",
		"vault", vaultID, "chunk", id, "error", err, "suppressed", n)
}

// chunkIsCloudBacked checks the FSM (single source of truth) for CloudBacked.
// Falls back to local state when no FSM exists (single-node / memory mode).
func chunkIsCloudBacked(vaultInst *VaultInstance, m chunk.ChunkMeta) bool {
	if vaultInst.OverlayFromFSM != nil {
		return vaultInst.OverlayFromFSM(m).CloudBacked
	}
	return m.CloudBacked
}
