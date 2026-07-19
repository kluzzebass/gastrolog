package orchestrator

import (
	"errors"
	"fmt"
	"time"

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

	// A chunk that dropped out of this vault's raw candidate view since the
	// last sweep (retention destroyed it, or anything else made it vanish)
	// must not keep its backoff state or alarm — that would strand both
	// forever, since a gone chunk is never visited by the loop below again.
	o.pruneVanishedBackfillFailures(vaultInst.VaultID, metas)

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
		// A chunk with an unexpired backoff window from a prior failure is
		// not due for retry yet. Skipping the schedule entirely — not just
		// the upload — is what stops the schedule/complete INFO pair from
		// flooding the job journal every 5s for a known-failing chunk
		// (gastrolog-4ryguo).
		if !o.backfillDue(m.ID) {
			continue
		}
		name := fmt.Sprintf("cloud-backfill:%s:%s", vaultInst.VaultID, m.ID)
		if o.scheduler.HasPendingPrefix(name) {
			continue
		}
		if err := o.scheduler.RunOnce(name, func(id chunk.ChunkID) error {
			err := uploader.UploadToCloud(id)
			if err != nil {
				err = o.repairAndRetryBackfill(vaultInst, id, err, uploader)
			}
			if err != nil {
				o.logBackfillFailure(vaultInst.VaultID, id, err)
				o.markBackfillFailure(vaultInst.VaultID, id, err)
				return err
			}
			o.clearBackfillFailure(id)
			return nil
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

// repairAndRetryBackfill detects the registration-missing signature that
// made this bug permanent (gastrolog-4ryguo): the chunk is FSM-sealed with
// its GLCB verifiably present on disk, but UploadToCloud failed because the
// local chunk manager has no registration for it — the lazy-resolution gap
// the gastrolog-2kmgj6 fix left open (appendUnlistedManifestSealed made the
// chunk schedulable for backfill, not uploadable). Repairs the registration
// via the same primitive pipeline sealing uses to register a freshly-built
// GLCB (VaultLifecycleReconciler.registerPipelineGLCB) and retries the
// upload once.
//
// Returns the original error untouched whenever repair does not apply:
// wrong error, no reconciler, no manifest entry, or the GLCB genuinely
// absent from disk (e.g. deleted out from under the manifest entry). That
// last case is not repairable — falling through to backoff instead of
// retrying here is what keeps it from tight-looping.
func (o *Orchestrator) repairAndRetryBackfill(vaultInst *VaultInstance, id chunk.ChunkID, uploadErr error, uploader chunk.ChunkCloudUploader) error {
	if !errors.Is(uploadErr, chunk.ErrChunkNotFound) {
		return uploadErr
	}
	if vaultInst.Reconciler == nil || vaultInst.ManifestEntry == nil {
		return uploadErr
	}
	root, ok := o.pipelineVaultChunkRoot(vaultInst.VaultID)
	if !ok {
		return uploadErr
	}
	if _, statErr := os.Stat(chunking.ChunkGLCBPath(root, id)); statErr != nil {
		// Bytes genuinely absent — nothing to repair.
		return uploadErr
	}
	entry, ok := vaultInst.ManifestEntry(id)
	if !ok {
		return uploadErr
	}
	vaultInst.Reconciler.registerPipelineGLCB(entry)
	return uploader.UploadToCloud(id)
}

// backfillFailureEntry tracks per-chunk retry backoff for cloud-backfill
// uploads that failed and were not resolved by registration repair. Mirrors
// retention's unreadableEntry (retention.go): a failure schedules the next
// attempt via the same unreadableBackoff schedule and raises the
// cloud-backfill-stuck alarm — the catalog's DelayOn keeps a blip that
// clears on the very next retry from ever annunciating, while a chunk stuck
// past it does.
type backfillFailureEntry struct {
	vaultID   glid.GLID
	failCount int
	nextRetry time.Time
}

// backfillDue reports whether a chunk's cloud-backfill retry is due: no
// failure on record (never failed, or already cleared), or its backoff
// window has elapsed.
func (o *Orchestrator) backfillDue(id chunk.ChunkID) bool {
	o.backfillMu.Lock()
	defer o.backfillMu.Unlock()
	entry := o.backfillFailures[id]
	if entry == nil {
		return true
	}
	return !o.now().Before(entry.nextRetry)
}

// markBackfillFailure records a cloud-backfill upload failure that
// registration repair did not resolve, schedules the next retry via
// unreadableBackoff, and raises the cloud-backfill-stuck alarm (subject to
// the catalog's DelayOn suppression).
func (o *Orchestrator) markBackfillFailure(vaultID glid.GLID, id chunk.ChunkID, cause error) {
	o.backfillMu.Lock()
	if o.backfillFailures == nil {
		o.backfillFailures = make(map[chunk.ChunkID]*backfillFailureEntry)
	}
	entry := o.backfillFailures[id]
	if entry == nil {
		entry = &backfillFailureEntry{vaultID: vaultID}
		o.backfillFailures[id] = entry
	}
	entry.vaultID = vaultID
	entry.failCount++
	entry.nextRetry = o.now().Add(unreadableBackoff(entry.failCount))
	nextRetry := entry.nextRetry
	o.backfillMu.Unlock()

	if o.alerts != nil {
		o.alerts.Raise("cloud-backfill-stuck", id.String(),
			fmt.Sprintf("Chunk %s in vault %s failed cloud backfill: %v (next retry %s)",
				id, vaultID, cause, nextRetry.Format(time.RFC3339)))
	}
}

// clearBackfillFailure drops a chunk's backfill backoff state and clears
// its alarm — called on upload success, whether the original attempt
// succeeded outright or a registration repair's retry did.
func (o *Orchestrator) clearBackfillFailure(id chunk.ChunkID) {
	o.backfillMu.Lock()
	_, had := o.backfillFailures[id]
	delete(o.backfillFailures, id)
	o.backfillMu.Unlock()
	if had && o.alerts != nil {
		o.alerts.Clear("cloud-backfill-stuck", id.String())
	}
}

// pruneVanishedBackfillFailures drops backoff/alarm state for chunks that
// no longer appear in this vault's raw candidate view (chunk manager List()
// plus unlisted-manifest-sealed) — the chunk was deleted (retention or
// otherwise) and is gone for good. Without this, a chunk that fails backfill
// and is then destroyed would strand its backoff entry and alarm forever:
// nothing would ever revisit it to clear them. metas is deliberately the
// pre-filter list (before the Sealed/CloudBacked gate) so a chunk that
// simply became cloud-backed is still "present" here — its entry clears via
// the upload-success path instead, not by pruning.
func (o *Orchestrator) pruneVanishedBackfillFailures(vaultID glid.GLID, metas []chunk.ChunkMeta) {
	present := make(map[chunk.ChunkID]bool, len(metas))
	for _, m := range metas {
		present[m.ID] = true
	}
	o.backfillMu.Lock()
	var vanished []chunk.ChunkID
	for id, entry := range o.backfillFailures {
		if entry.vaultID != vaultID || present[id] {
			continue
		}
		vanished = append(vanished, id)
	}
	for _, id := range vanished {
		delete(o.backfillFailures, id)
	}
	o.backfillMu.Unlock()
	if o.alerts == nil {
		return
	}
	for _, id := range vanished {
		o.alerts.Clear("cloud-backfill-stuck", id.String())
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
