package orchestrator

// retention_transfer.go implements the "transfer" retention disposition
// (gastrolog-2l918): a fired retention event re-homes the sealed chunk to
// a target vault UNCHANGED — no record decode, no routing, no re-ingest.
//
// Execution shape (docs/retention-transfer-disposition-design.md):
//  1. resolve target vault + validate current state (else defer);
//  2. propose/ensure the destination FSM entry (announce-import) so
//     destination homes learn the chunk and pull it, reusing the replica
//     catch-up pull path against the destination's group (glcb_catchup.go
//     generalized to address transfer-introduced entries at the SOURCE
//     vault via ManifestEntry.TransferSourceVaultID);
//  3. wait bounded for destination receipts (watchdog pattern) — a
//     stalled transfer defers like a stalled fan-out, one-shot NOT
//     consumed (the 5034va ordering: nothing is marked on the source
//     until the destination confirms);
//  4. on destination receipts >= dest RF: return true so the caller marks
//     retention-pending and calls expireChunk exactly as today — now a
//     pure local-copy delete, since the data lives on at the destination.
//
// Reused primitives (house rule: reuse the deepest existing seam that
// honestly fits, never build a parallel one):
//   - announce-import reuses CmdRepatriateChunk / MarshalRepatriateChunk
//     verbatim (orphan_repatriation.go's mechanism for introducing a
//     manifest entry the FSM doesn't yet know about). applyRepatriate's
//     existing "already in manifest" refusal IS the idempotent-retry /
//     ID-collision-is-success path (spec decision #7) — no new command.
//   - byte transfer reuses verifyAndPromoteGLCB (glcb_catchup.go) for
//     both the local-node fast path and, via the generalized
//     pullMissingGLCB, the cross-node replica catch-up pull.
//   - destination admission reuses vaultAdmissionGate — the same check
//     routed records are gated on (disk_guard.go) — not a parallel check.
//   - receipts gate reuses AckChunkHolder / ManifestEntry.Holders (the
//     same holder-receipt truth GLCB replica catch-up and
//     SegmentSuperseded read) instead of a new ack protocol.
//   - source deletion reuses expireChunk / the reconciler's
//     CmdRequestDelete receipt protocol, called by the same caller
//     (tryRetainChunk) that calls it for route and delete dispositions.

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// Defer-cause categories for deferTransfer's throttle key (gastrolog-2l918
// review finding 6). Bounded and fixed regardless of chunk/target
// identity — the throttle key must never embed variable text (chunk IDs,
// record counts) or the idleLog map grows unboundedly and every distinct
// failure re-logs instead of throttling. The full, variable-text cause
// still reaches the operator via noteRetentionDeferral (alarm detail) and
// the log line itself; only the THROTTLE KEY is category-bounded.
const (
	deferCatTargetUnconfigured = "target-unconfigured"
	deferCatConfigLoadFailed   = "config-load-failed"
	deferCatTargetDisabled     = "target-disabled"
	deferCatTargetNotFileVault = "target-not-file-vault"
	deferCatTargetNotFound     = "target-not-found"
	deferCatAdmission          = "target-admission"
	deferCatNoHandle           = "no-vault-ctl-handle"
	deferCatCorruption         = "corruption"
	deferCatTombstoned         = "tombstoned"
	deferCatAnnounceFailed     = "announce-failed"
	deferCatEntryNotVisible    = "entry-not-visible"
	deferCatReceiptsStall      = "receipts-stall"
	deferCatTargetStalled      = "target-stalled-this-sweep"
)

// transferReceiptsPollInterval is how often fireTransferEvent polls the
// destination FSM's holder count while waiting for destination RF to be
// met. Production value; tests override via
// retentionRunner.transferReceiptTick so the wait is driven without
// wall-clock sleeps.
const transferReceiptsPollInterval = 2 * time.Second

// transferReceiptsMaxStallTicks bounds how many consecutive polls may pass
// with no NEW destination holder before a transfer aborts and the chunk is
// retained for a later sweep — the watchdog pattern (retention_watchdog.go)
// applied to the receipts wait instead of fan-out submit progress. At the
// production poll interval this is 2 minutes, matching
// retentionFanOutStallWindow.
const transferReceiptsMaxStallTicks = 60

// fireTransferEvent runs the per-chunk transfer protocol: resolve and
// validate the target, announce-import the chunk into the destination
// vault-ctl FSM, wait for destination holder receipts to reach the
// destination's replication factor, and report success. Returns false
// (defer, chunk retained, one-shot NOT consumed) at any point the transfer
// cannot currently proceed — the same contract fireRetentionEvent honors
// for route disposition. See applyRetentionDispositionToChunk.
func (r *retentionRunner) fireTransferEvent(id chunk.ChunkID) bool {
	if r.orch == nil {
		// No orchestrator: nothing to transfer into. Retain rather than
		// silently succeed — a disposition that cannot reach a
		// destination must never destroy the source's only copy
		// (gastrolog-2l918 review finding 6).
		return false
	}
	if r.orch.shuttingDown() {
		return false
	}
	// Drain-gate pre-check, same as route fan-out (fireRetentionEvent):
	// transfer is drain work — its transient cost is the destination's
	// disk plus receipts, the source only frees. Below the disk floor
	// nothing may consume disk, not even the drain itself.
	if r.orch.diskDeferWrites() {
		if n, ok := r.idleLog.Allow("disk-protect"); ok {
			r.logger.Warn("retention: transfer deferred — drain gate engaged below the disk floor; chunk retained for a later sweep",
				"vault", r.vaultID, "suppressed", n)
		}
		r.noteRetentionDeferral("drain gate engaged (node below its disk floor)")
		return false
	}

	if r.transferTarget == nil {
		r.deferTransfer(id, deferCatTargetUnconfigured, "retention_transfer_target_vault_id is not set")
		return false
	}
	targetID := *r.transferTarget

	// Per-sweep circuit breaker (gastrolog-2l918 review finding 2): a
	// target that already stalled a DIFFERENT chunk's receipts wait this
	// sweep is not going to un-stall for this chunk either. Defer
	// immediately instead of burning another full stall window — without
	// this, N chunks queued against one stalled destination serially eat
	// N stall windows (2 minutes each in production), freezing every
	// OTHER chunk's retention on this vault for hours.
	if cause, stalled := r.transferTargetStalledThisSweep(targetID); stalled {
		r.deferTransfer(id, deferCatTargetStalled, cause)
		return false
	}

	targetCfg, category, deferCause := r.resolveTransferTarget(targetID)
	if targetCfg == nil {
		r.deferTransfer(id, category, deferCause)
		return false
	}

	// Destination admission: the same disk-guard / size-budget check that
	// gates routed records gates transfer intake — a capped or protected
	// destination vault defers, it does not overfill (reuse
	// vaultAdmissionGate, not a parallel check).
	if err := r.orch.vaultAdmissionGate(targetID); err != nil {
		r.deferTransfer(id, deferCatAdmission, fmt.Sprintf("transfer target vault %q: %v", targetCfg.Name, err))
		return false
	}

	meta, err := r.cm.Meta(id)
	if err != nil {
		r.markUnreadable(id, fmt.Errorf("read meta for transfer: %w", err))
		return false
	}

	destFSM, _, _, hasHandle := r.orch.vaultCtlHandle(targetID)
	if !hasHandle || destFSM == nil {
		r.deferTransfer(id, deferCatNoHandle, fmt.Sprintf("transfer target vault %q has no local vault-ctl handle on this node", targetCfg.Name))
		return false
	}

	entry, category, deferCause := r.ensureDestManifestEntry(destFSM, targetID, id, meta)
	if entry == nil {
		r.deferTransfer(id, category, deferCause)
		return false
	}

	// Accelerate convergence when this node is itself a home of the
	// target vault: copy bytes locally right now instead of waiting for
	// the destination's own catch-up sweep tick. Required for single-node
	// clusters, where no OTHER node will ever perform this pull; a no-op
	// (falls through to the cross-node catch-up sweep, generalized in
	// glcb_catchup.go) when this node is not a destination home.
	r.tryLocalTransferCopy(targetID, id, *entry)

	destRF := max(int(targetCfg.ReplicationFactor), 1)
	tick := r.transferReceiptTick
	stop := func() {}
	if tick == nil {
		ticker := time.NewTicker(transferReceiptsPollInterval)
		tick = ticker.C
		stop = ticker.Stop
	}
	ok := r.waitForDestHolders(destFSM, id, destRF, tick)
	stop()
	if !ok {
		r.markTransferTargetStalledThisSweep(targetID)
		r.deferTransfer(id, deferCatReceiptsStall, fmt.Sprintf(
			"destination vault %q did not reach %d holder receipt(s) for the transferred chunk within the stall window",
			targetCfg.Name, destRF))
		return false
	}

	// Destination RF is met: clear the manifest entry's transfer-source
	// pointer BEFORE the caller expires the source's local copy
	// (gastrolog-2l918 review finding 1). Left set, every future replica-
	// repair pull for this chunk would keep addressing itself at THIS
	// (source) vault's placement peers — who are about to delete their
	// copies — permanently defeating self-healing for the transferred
	// chunk. Defense in depth, not the only guard: if this apply fails
	// (leader hiccup, forwarding error), completion still proceeds —
	// runGLCBPull's holder-set fallback (glcb_catchup.go) is the second
	// line of defense for a miss here.
	if err := r.orch.ApplyVaultControlPlane(targetID, vaultraft.MarshalVaultChunkCommand(targetID, vaultctlfsm.MarshalClearTransferSource(id))); err != nil {
		r.logger.Warn("retention: transfer completed but failed to clear the destination's transfer-source pointer — replica repair falls back to the destination's own holders if this is ever needed",
			"vault", r.vaultID, "target", targetCfg.Name, "chunk", id, "error", err)
	}

	r.noteRetentionProgress()
	return true
}

// deferTransfer folds a defer cause into the shared deferral streak AND
// logs it (throttled, one line per distinct CATEGORY per idleLog
// interval) — parity with fireRetentionEvent's per-branch r.logger.Warn
// calls, so an operator (or a debugging session) watching this node's
// logs sees why a transfer stalled without waiting 3 sweeps for the alarm
// to name it. category is a small, fixed defer-cause enum (deferCat*
// constants) used ONLY for the throttle key; cause is the full,
// variable-text operator-facing message (chunk/target identity included)
// carried in the alarm detail and the log line. Splitting them matters
// (gastrolog-2l918 review finding 6): a throttle key built from the
// formatted cause text embeds per-chunk data (record counts, chunk IDs)
// and grows the idleLog map without bound, defeating the throttle for
// every distinct chunk instead of collapsing repeats of the same failure
// mode.
func (r *retentionRunner) deferTransfer(id chunk.ChunkID, category, cause string) {
	r.noteRetentionDeferral(cause)
	if n, ok := r.idleLog.Allow("transfer:" + category); ok {
		r.logger.Warn("retention: transfer deferred; chunk retained for a later sweep",
			"vault", r.vaultID, "chunk", id, "category", category, "cause", cause, "suppressed", n)
	}
}

// transferTargetStalledThisSweep reports whether targetID's receipts wait
// already stalled once during the CURRENT sweep — the per-sweep circuit
// breaker (gastrolog-2l918 review finding 2).
func (r *retentionRunner) transferTargetStalledThisSweep(targetID glid.GLID) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.sweepStalledTransferTargets[targetID] {
		return "", false
	}
	return fmt.Sprintf("transfer target vault %s stalled earlier this sweep — deferring immediately rather than re-waiting", targetID), true
}

// markTransferTargetStalledThisSweep records that targetID's receipts
// wait stalled this sweep, tripping the circuit breaker for every OTHER
// chunk targeting the same vault for the rest of THIS sweep. Reset to nil
// at the start of every sweep() call.
func (r *retentionRunner) markTransferTargetStalledThisSweep(targetID glid.GLID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sweepStalledTransferTargets == nil {
		r.sweepStalledTransferTargets = make(map[glid.GLID]bool)
	}
	r.sweepStalledTransferTargets[targetID] = true
}

// resolveTransferTarget re-validates the transfer target at sweep time —
// PutVault already enforced target-exists/file/not-self at config-write
// time, but config can drift after validation (target vault deleted,
// disabled, or its type changed is not possible for type but disabled is).
// Returns (nil, category, cause) when the target is not currently usable.
func (r *retentionRunner) resolveTransferTarget(targetID glid.GLID) (*system.VaultConfig, string, string) {
	sys, err := r.orch.loadSystem(context.Background())
	if err != nil || sys == nil {
		return nil, deferCatConfigLoadFailed, "failed to load config to resolve transfer target"
	}
	for i := range sys.Config.Vaults {
		if sys.Config.Vaults[i].ID != targetID {
			continue
		}
		targetCfg := &sys.Config.Vaults[i]
		if !targetCfg.Enabled {
			return nil, deferCatTargetDisabled, fmt.Sprintf("transfer target vault %q is disabled", targetCfg.Name)
		}
		if targetCfg.Type != system.VaultTypeFile || targetCfg.IsCloud() {
			return nil, deferCatTargetNotFileVault, fmt.Sprintf("transfer target vault %q is no longer a plain file vault", targetCfg.Name)
		}
		return targetCfg, "", ""
	}
	return nil, deferCatTargetNotFound, fmt.Sprintf("transfer target vault %s no longer exists", targetID)
}

// ensureDestManifestEntry announces the chunk into the destination vault's
// FSM manifest (CmdRepatriateChunk, reused verbatim) if it isn't already
// there, stamping a FRESH SealedAt so a shorter destination TTL does not
// re-fire retention the moment the chunk lands (spec decision #6) and
// TransferSourceVaultID so the destination's replica catch-up sweep knows
// where to pull the bytes from. If the entry already exists — a previous
// transfer attempt reached announce-import — this is the idempotent-retry
// path (spec decision #7): a matching record count proceeds straight to
// the receipts wait; a mismatch is corruption and defers with an alarm
// cause naming it. If the destination has TOMBSTONED this chunk ID (a
// prior transfer to this same destination was retracted — see the
// abandoned-announce GC in vault_lifecycle_reconciler.go, finding 4 — or
// an operator delete), the announce is refused with a NAMED cause distinct
// from corruption (finding 3b): the transfer defers until the tombstone
// prunes rather than looping on a dead entry forever. Returns
// (entry, category, cause); category is a bounded deferCat* constant for
// deferTransfer's throttle key, cause is the full operator-facing text.
func (r *retentionRunner) ensureDestManifestEntry(destFSM *vaultctlfsm.FSM, targetID glid.GLID, id chunk.ChunkID, meta chunk.ChunkMeta) (*vaultctlfsm.ManifestEntry, string, string) {
	if existing := destFSM.Get(id); existing != nil {
		if existing.RecordCount != meta.RecordCount {
			return nil, deferCatCorruption, fmt.Sprintf(
				"transfer target already holds a DIFFERENT chunk under ID %s (record count %d != source %d) — corruption, not re-transferring",
				id, existing.RecordCount, meta.RecordCount)
		}
		return existing, "", ""
	}
	if destFSM.IsTombstoned(id) {
		return nil, deferCatTombstoned, fmt.Sprintf(
			"transfer target vault: chunk %s is tombstoned at the destination — deferred until the destination's tombstone prunes (see docs/retention-transfer-disposition-design.md \"Cycles and tombstones\")",
			id)
	}

	entry := r.sourceManifestEntryForTransfer(id, meta)
	entry.SealedAt = r.now()
	entry.TransferSourceVaultID = r.vaultID
	entry.Holders = nil
	entry.RetentionPending = false

	cmdData, err := vaultctlfsm.MarshalRepatriateChunk(entry)
	if err != nil {
		return nil, deferCatAnnounceFailed, fmt.Sprintf("marshal transfer announce-import: %v", err)
	}
	applyErr := r.orch.ApplyVaultControlPlane(targetID, vaultraft.MarshalVaultChunkCommand(targetID, cmdData))
	// Authoritative check regardless of what the apply error says
	// (gastrolog-2l918 review finding 6): a concurrent or earlier attempt
	// landing the exact same entry is success (idempotent retry) whether
	// or not the error text happens to say so. Re-read the FSM directly
	// instead of pattern-matching error text across the RPC boundary.
	if got := destFSM.Get(id); got != nil {
		if got.RecordCount != meta.RecordCount {
			return nil, deferCatCorruption, fmt.Sprintf(
				"transfer target already holds a DIFFERENT chunk under ID %s (record count %d != source %d) — corruption, not re-transferring",
				id, got.RecordCount, meta.RecordCount)
		}
		return got, "", ""
	}
	if applyErr != nil {
		return nil, deferCatAnnounceFailed, fmt.Sprintf("announce-import to transfer target failed: %v", applyErr)
	}
	return nil, deferCatEntryNotVisible, "announce-import applied but the destination FSM does not show the entry yet"
}

// sourceManifestEntryForTransfer returns the manifest entry to announce at
// the destination: a full copy of the SOURCE vault-ctl FSM's own entry
// when this node has a local handle on it — preserving Hash, KeyScheme,
// IngestTSMonotonic, and the GLCB section-offset fields that rebuilding
// from ChunkMeta alone drops (gastrolog-2l918 review finding 5) — falling
// back to chunkMetaToManifestEntry only when no local source FSM handle
// exists (bare unit-test harnesses that build a retentionRunner without an
// Orchestrator).
func (r *retentionRunner) sourceManifestEntryForTransfer(id chunk.ChunkID, meta chunk.ChunkMeta) vaultctlfsm.ManifestEntry {
	if r.orch != nil {
		if srcFSM, _, _, ok := r.orch.vaultCtlHandle(r.vaultID); ok && srcFSM != nil {
			if e := srcFSM.Get(id); e != nil {
				return *e
			}
		}
	}
	return chunkMetaToManifestEntry(meta)
}

// tryLocalTransferCopy copies the chunk's GLCB directly from this node's
// local source-vault chunk root into the destination vault's chunk root
// when this node holds both — the common case for single-node clusters
// and any multi-node placement where the source leader overlaps a
// destination home. No-op (falls through to the cross-node catch-up
// sweep) when this node doesn't hold the source bytes locally or isn't a
// destination home. Errors are logged and swallowed: the cross-node path
// is the correctness backstop, this is purely a convergence accelerator.
func (r *retentionRunner) tryLocalTransferCopy(targetID glid.GLID, id chunk.ChunkID, entry vaultctlfsm.ManifestEntry) {
	if r.orch == nil {
		return
	}
	sourceRoot, ok := r.orch.pipelineVaultChunkRoot(r.vaultID)
	if !ok {
		return
	}
	destRoot, ok := r.orch.pipelineVaultChunkRoot(targetID)
	if !ok {
		return
	}
	srcPath := chunking.ChunkGLCBPath(sourceRoot, id)
	destPath := chunking.ChunkGLCBPath(destRoot, id)
	if _, err := os.Stat(destPath); err != nil {
		if err := localCopyAndPromoteGLCB(srcPath, destPath, entry); err != nil {
			r.logger.Debug("retention: local transfer copy deferred to cross-node catch-up",
				"vault", r.vaultID, "target", targetID, "chunk", id, "error", err)
			return
		}
	}

	r.orch.mu.RLock()
	var rec *VaultLifecycleReconciler
	if vault := r.orch.vaults[targetID]; vault != nil && vault.Instance != nil {
		rec = vault.Instance.Reconciler
	}
	r.orch.mu.RUnlock()
	if rec != nil {
		// Same registration + holder-receipt seam runGLCBPull uses after a
		// successful cross-node pull (registerPipelineGLCB stats the file,
		// registers it with the local chunk manager, and proposes this
		// node's AckChunkHolder receipt).
		rec.registerPipelineGLCB(entry)
	}
}

// localCopyAndPromoteGLCB copies srcPath into a dot-temp file next to
// destPath (same staging convention pullGLCBFromNode uses for network
// pulls) and verify-before-promotes it via the shared verifyAndPromoteGLCB
// — identical integrity guarantee whether the bytes crossed the network or
// just crossed vault directories on the same disk.
func localCopyAndPromoteGLCB(srcPath, destPath string, e vaultctlfsm.ManifestEntry) error {
	if err := os.MkdirAll(filepath.Dir(destPath), 0o750); err != nil {
		return err
	}
	src, err := os.Open(filepath.Clean(srcPath))
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()

	f, err := os.CreateTemp(filepath.Dir(destPath), ".glcb.pull.*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	_, copyErr := io.Copy(f, src)
	if copyErr == nil {
		copyErr = f.Sync()
	}
	closeErr := f.Close()
	if copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		_ = os.Remove(tmp) //nolint:gosec // G703: CreateTemp name in our own chunk dir, not untrusted input
		return copyErr
	}
	return verifyAndPromoteGLCB(tmp, destPath, e)
}

// waitForDestHolders blocks until the destination FSM reports at least
// need holders for chunkID, or the receipts wait stalls — no NEW holder
// for transferReceiptsMaxStallTicks consecutive ticks. tick is injected so
// tests drive the wait without wall-clock sleeps (the same pattern
// retention_watchdog.go uses for the route fan-out's progress monitor);
// production passes a real time.Ticker channel. Returns immediately,
// without consuming a tick, when the destination already meets need (the
// idempotent-retry and single-node local-copy fast paths both land here).
func (r *retentionRunner) waitForDestHolders(fsm *vaultctlfsm.FSM, chunkID chunk.ChunkID, need int, tick <-chan time.Time) bool {
	holderCount := func() int {
		e := fsm.Get(chunkID)
		if e == nil {
			return 0
		}
		return len(e.Holders)
	}
	last := holderCount()
	if last >= need {
		return true
	}
	stalled := 0
	for {
		<-tick
		cur := holderCount()
		if cur >= need {
			return true
		}
		if cur != last {
			last = cur
			stalled = 0
		} else {
			stalled++
		}
		if stalled >= transferReceiptsMaxStallTicks {
			return false
		}
	}
}
