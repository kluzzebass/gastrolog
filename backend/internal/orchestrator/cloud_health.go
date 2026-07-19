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
//
// Also GCs cloud-backfill failure/backoff state the same way
// retentionSweepAll GCs retention runners: a vault this node no longer runs
// backfill for (leadership moved, placement changed, vault removed from
// config) is never visited by backfillCloudUploads again, so nothing else
// would ever clear its stranded backoff entries or alarms. See
// gastrolog-4ryguo review follow-up.
func (o *Orchestrator) evaluateCloudHealth() {
	if o.alerts == nil {
		return
	}
	o.mu.RLock()
	defer o.mu.RUnlock()

	active := make(map[glid.GLID]bool)
	for _, vault := range o.vaults {
		vaultInst := vault.Instance
		if vaultInst == nil || !vaultInstanceHasCloudBacking(vaultInst) {
			continue
		}
		o.evaluateVaultCloudHealth(vaultInst)
		if vaultInstRunsCloudBackfill(vaultInst) {
			active[vaultInst.VaultID] = true
		}
	}
	o.purgeBackfillFailuresForInactiveVaults(active)
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
		if !m.Sealed {
			continue
		}
		if chunkIsCloudBacked(vaultInst, m) {
			// The PRIMARY path (schedulePipelineCloudUpload / onSeal) may
			// have resolved this chunk before this sweep's RunOnce ever
			// ran again — that upload never went through
			// markBackfillFailure/clearBackfillFailure, so without this the
			// chunk's backoff entry and alarm would strand here forever:
			// this continue is the only place backfillCloudUploads visits
			// an already-resolved chunk again. See gastrolog-4ryguo review
			// follow-up.
			o.clearBackfillFailure(m.ID)
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
			return o.runBackfillUpload(vaultInst, id, uploader)
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

// runBackfillUpload performs one chunk's upload attempt — including
// registration repair and failure-track bookkeeping — as the scheduler job
// body. Extracted from backfillCloudUploads to keep the sweep loop small;
// this is where the review follow-up's build-lag gate lives: onDisk (the
// registration-missing signature — GLCB verifiably present on disk) is
// computed once and gates BOTH repair applicability and entry into the
// backoff/alarm track. A chunk whose local build simply hasn't finished yet
// (onDisk false) fails the same not-found error but is owned by the
// primary upload path (schedulePipelineCloudUpload / onSeal) and resolves
// itself in seconds — pushing it into the 5-minute backoff track would
// pollute it with build-lag noise that was never actually stuck. See
// gastrolog-4ryguo review follow-up.
func (o *Orchestrator) runBackfillUpload(vaultInst *VaultInstance, id chunk.ChunkID, uploader chunk.ChunkCloudUploader) error {
	err := uploader.UploadToCloud(id)
	if err != nil {
		onDisk := o.backfillChunkOnDisk(vaultInst.VaultID, id)
		if onDisk {
			err = o.repairAndRetryBackfill(vaultInst, id, err, uploader, onDisk)
		}
		if err != nil {
			// Both onDisk cases get a failure entry with the same
			// exponential backoff — one map, one strand-safe lifecycle
			// (cross-path clear, the vault-scoped purges, and the
			// vanished-candidate prune all apply regardless of onDisk).
			// onDisk gates ALARM ELIGIBILITY only: build-lag and a
			// genuinely-deleted GLCB are indistinguishable from an
			// os.Stat, so neither pages an operator — build-lag entries
			// clear entirely via the chunkIsCloudBacked cross-path once
			// the primary upload lands, and a genuinely-deleted GLCB backs
			// off to the cap without flooding the scheduler journal
			// (backfillDue still gates it) or ever alarming for state the
			// primary path owns. See gastrolog-4ryguo review follow-up.
			o.logBackfillFailure(vaultInst.VaultID, id, err, onDisk)
			o.markBackfillFailure(vaultInst.VaultID, id, err, onDisk)
			return err
		}
	}
	o.clearBackfillFailure(id)
	return nil
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
// retrying here is what keeps it from tight-looping. onDisk is the caller's
// already-computed backfillChunkOnDisk result, passed in rather than
// re-derived via a second os.Stat.
func (o *Orchestrator) repairAndRetryBackfill(vaultInst *VaultInstance, id chunk.ChunkID, uploadErr error, uploader chunk.ChunkCloudUploader, onDisk bool) error {
	if !errors.Is(uploadErr, chunk.ErrChunkNotFound) {
		return uploadErr
	}
	if vaultInst.Reconciler == nil || vaultInst.ManifestEntry == nil {
		return uploadErr
	}
	if !onDisk {
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

// backfillChunkOnDisk reports whether a chunk's GLCB is verifiably present
// on disk under this vault's pipeline chunk root — the registration-missing
// signature predicate shared by three call sites: the repair-applicability
// check, the failure-track entry gate (only a registration-missing-shaped
// failure backs off/alarms; a build-lag failure does not), and the
// operator-facing on-disk/awaiting-build log split. False whenever there is
// no pipeline home registration for the vault on this node (root not found)
// as well as when the file itself is absent.
func (o *Orchestrator) backfillChunkOnDisk(vaultID glid.GLID, id chunk.ChunkID) bool {
	root, ok := o.pipelineVaultChunkRoot(vaultID)
	if !ok {
		return false
	}
	_, statErr := os.Stat(chunking.ChunkGLCBPath(root, id))
	return statErr == nil
}

// backfillFailureEntry tracks per-chunk retry backoff for cloud-backfill
// uploads that failed. Mirrors retention's unreadableEntry (retention.go):
// every failure — registration-missing or GLCB-absent alike — schedules the
// next attempt via the same unreadableBackoff schedule, one map, one
// strand-safe lifecycle. alarmEligible is the ONLY distinction between the
// two shapes: only a registration-missing failure (GLCB verifiably on
// disk) escalates to the cloud-backfill-stuck alarm (subject to the
// catalog's DelayOn suppression). A GLCB-absent failure — build-lag or a
// GLCB genuinely deleted out from under the manifest entry, indistinguishable
// by an os.Stat — backs off the same way but never alarms: build-lag
// entries clear via the chunkIsCloudBacked cross-path once the primary
// upload lands, and a permanently-missing GLCB backs off to the cap
// without paging an operator for state the primary path owns. See
// gastrolog-4ryguo review follow-up.
type backfillFailureEntry struct {
	vaultID       glid.GLID
	failCount     int
	nextRetry     time.Time
	alarmEligible bool
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

// markBackfillFailure records a cloud-backfill upload failure and schedules
// the next retry via unreadableBackoff — for BOTH registration-missing and
// GLCB-absent failures alike (one map, one strand-safe lifecycle).
// alarmEligible (the caller's backfillChunkOnDisk observation) decides only
// whether this raises the cloud-backfill-stuck alarm (subject to the
// catalog's DelayOn suppression): a GLCB-absent failure backs off silently,
// never alarming. alarmEligible tracks the LATEST observation, not the
// first — if a later failure's eligibility differs from what a prior
// standing alarm assumed, the alarm is raised/cleared to match.
func (o *Orchestrator) markBackfillFailure(vaultID glid.GLID, id chunk.ChunkID, cause error, alarmEligible bool) {
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
	entry.alarmEligible = alarmEligible
	nextRetry := entry.nextRetry
	o.backfillMu.Unlock()

	if o.alerts == nil {
		return
	}
	if !alarmEligible {
		o.alerts.Clear("cloud-backfill-stuck", id.String())
		return
	}
	o.alerts.Raise("cloud-backfill-stuck", id.String(),
		fmt.Sprintf("Chunk %s in vault %s failed cloud backfill: %v (next retry %s)",
			id, vaultID, cause, nextRetry.Format(time.RFC3339)))
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

// purgeBackfillFailuresWhere drops backoff state and clears the alarm for
// every backfillFailures entry the predicate matches. The single locked
// scan-and-delete primitive behind pruneVanishedBackfillFailures (per-sweep,
// vault-scoped, keyed by which chunks vanished from the candidate view),
// purgeBackfillFailuresForInactiveVaults (per-sweep, node-wide, keyed by
// which vaults this node still runs backfill for), and
// purgeBackfillFailuresForVault (immediate, on vault teardown/unregister).
func (o *Orchestrator) purgeBackfillFailuresWhere(match func(id chunk.ChunkID, entry *backfillFailureEntry) bool) {
	o.backfillMu.Lock()
	var vanished []chunk.ChunkID
	for id, entry := range o.backfillFailures {
		if !match(id, entry) {
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

// pruneVanishedBackfillFailures drops backoff/alarm state for chunks that
// no longer appear in this vault's raw candidate view (chunk manager List()
// plus unlisted-manifest-sealed) — the chunk was deleted (retention or
// otherwise) and is gone for good. Without this, a chunk that fails backfill
// and is then destroyed would strand its backoff entry and alarm forever:
// nothing would ever revisit it to clear them. metas is deliberately the
// pre-filter list (before the Sealed/CloudBacked gate) so a chunk that
// simply became cloud-backed is still "present" here — its entry clears via
// the upload-success path (the chunkIsCloudBacked continue in
// backfillCloudUploads) instead, not by pruning.
func (o *Orchestrator) pruneVanishedBackfillFailures(vaultID glid.GLID, metas []chunk.ChunkMeta) {
	present := make(map[chunk.ChunkID]bool, len(metas))
	for _, m := range metas {
		present[m.ID] = true
	}
	o.purgeBackfillFailuresWhere(func(id chunk.ChunkID, entry *backfillFailureEntry) bool {
		return entry.vaultID == vaultID && !present[id]
	})
}

// purgeBackfillFailuresForInactiveVaults drops backoff/alarm state for
// every entry whose vault is not in active — a vault this node no longer
// runs cloud backfill for (leadership moved, placement changed, vault
// removed from config). Without this, a vault that fell out of the active
// set would never have backfillCloudUploads called for it again, so nothing
// would ever revisit and clear its stranded entries. Called once per
// evaluateCloudHealth sweep. Mirrors retentionSweepAll's runner GC.
func (o *Orchestrator) purgeBackfillFailuresForInactiveVaults(active map[glid.GLID]bool) {
	o.purgeBackfillFailuresWhere(func(_ chunk.ChunkID, entry *backfillFailureEntry) bool {
		return !active[entry.vaultID]
	})
}

// purgeBackfillFailuresForVault immediately drops every backoff/alarm entry
// for one vault — called from teardownVault/removeVaultJobs so a vault
// leaving this node (deleted, unregistered, drained) doesn't leave its
// alarms standing until the next evaluateCloudHealth sweep notices.
func (o *Orchestrator) purgeBackfillFailuresForVault(vaultID glid.GLID) {
	o.purgeBackfillFailuresWhere(func(_ chunk.ChunkID, entry *backfillFailureEntry) bool {
		return entry.vaultID == vaultID
	})
}

// logBackfillFailure reports a failed backfill upload with the signal an
// operator needs: whether the GLCB exists on disk (onDisk, computed once by
// the caller via backfillChunkOnDisk and shared with the failure-track
// gate). Present-but-unuploadable means the chunk manager lost its
// registration (a bug — the restart registration gap produced 1,500 bare
// 'chunk not found' warns); absent means the local build simply hasn't
// finished (normal, Debug). Both throttled per vault: retries every 5s
// flood otherwise.
func (o *Orchestrator) logBackfillFailure(vaultID glid.GLID, id chunk.ChunkID, err error, onDisk bool) {
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
