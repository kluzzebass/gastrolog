package orchestrator

// gastrolog-51gme — VaultLifecycleReconciler.
//
// One reconciler per VaultInstance. Owns chunk-lifecycle execution
// uniformly: every FSM apply event goes through here, and every
// chunk-file deletion in steady state ends here. This file is the
// single home for "what just happened in the FSM, and what should the
// local chunk manager do about it?"
//
// Migration roadmap status:
//   step 4 (retention-ttl via deleteChunk): done.
//   step 5 (disk-vs-manifest sweep removed):
//     done. The receipt protocol's pendingDeletes (preserved across
//     snapshot install + processed by ReconcileFromSnapshot) is the
//     primary catchup path. SweepLocalOrphans (added after the initial
//     step-5 landing) covers the snapshot-restore gap that pendingDeletes
//     alone misses: a delete that finalized while this node was offline
//     leaves the FSM with only a tombstone, and the local file is
//     orphaned with no obligation to drive cleanup. The orphan sweep
//     uses tombstone presence as positive proof that a finalize was
//     applied — a freshly-created chunk with announce in flight has no
//     tombstone and is left alone.
//   step 6 (archival sweep + drop maxTransitionStreamedStaleness):
//     done. Archival expiry, archival suspect expiry, and transition
//     source-expire all route through deleteChunk; the staleness
//     watchdog was deleted because the receipt protocol does not benefit
//     from a fallback "delete the source anyway" decision.
//   step 7 (manual-delete RPC via deleteChunkFromInstance):
//     done. The CLI/UI delete now routes through the reconciler with
//     reason "manual-delete-rpc". The active-chunk seal-first behavior
//     is preserved so deletes targeting the active chunk still seal it
//     before propagation.
//   step 8 (FSM-sealed projection + drop the manager.go heuristic):
//     done. onSeal and ReconcileFromSnapshot project FSM-sealed state
//     onto the local chunk Manager via chunk.SealEnsurer.EnsureSealed.
//     The "multiple unsealed → seal all but newest" startup heuristic
//     in file.Manager was deleted; sealed-state divergence (e.g.
//     gastrolog-uccg6) is now resolved by replaying the FSM truth.
//   step 9 (lint ban on direct DeleteNoAnnounce / DeleteSilent):
//     done. The forbidigo linter rejects new direct callers outside a
//     small allow-list (this file + vault teardown + replaceForwardedChunk
//     + chunk-package internals). New paths must funnel through
//     deleteChunk so the receipt protocol stays the single execution API.
//   step 10 (membership-change cleanup): done. CmdPruneNode (FSM cmd 12)
//     drops a decommissioned node from every pendingDeletes entry's
//     ExpectedFrom; the apply returns the chunkIDs whose ExpectedFrom
//     became empty. The vault-ctl leader manager's onMemberRemoved hook
//     fans CmdPruneNode out across the vault's instance sub-FSMs after a
//     successful RemoveServer call; the reconciler's onPruneNode handler
//     (leader-only) proposes CmdFinalizeDelete for each finalizable
//     chunk so deletes don't pin pendingDeletes forever.
//   step 11 (deprecate CmdDeleteChunk): done. The dead production
//     plumbing (VaultInstance.ApplyRaftDelete, vaultRaftCallbacks.applyDelete,
//     buildVaultRaftCallbacks's MarshalDeleteChunk producer,
//     retentionRunner.applyRaftDelete + clusterMode branch) was removed.
//     CmdDeleteChunk + applyDeleteChunk + MarshalDeleteChunk stay in the
//     FSM for WAL replay backward-compat, but a forbidigo rule blocks
//     new MarshalDeleteChunk callers. The wireVaultFSMOnDelete callback
//     and the legacy forwardDeletionToFollowers RPC chain stay too —
//     they're reachable only from the older cluster harness in
//     transition_test.go, which doesn't go through buildInstance and
//     therefore has no reconciler attached. Migrating that harness onto
//     the reconciler with a fake-FSM-applier is a follow-up refactor.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// VaultLifecycleReconciler owns chunk-lifecycle execution for a single
// VaultInstance. Created during instance wiring (reconfig_vaults.go), wired
// to the vault's FSM via Wire(), and torn down with the vault instance.
//
// The reconciler is the canonical caller of `chunk.DeleteNoAnnounce`
// and the SilentDeleter shortcut. A forbidigo lint rule (step 9)
// blocks direct calls from anywhere else in the orchestrator package
// outside a small allow-list (vault teardown, replaceForwardedChunk).
type VaultLifecycleReconciler struct {
	vaultID     glid.GLID
	vaultInst        *VaultInstance
	localNodeID string
	logger      *slog.Logger

	// orch is the parent orchestrator, kept so the reconciler can fan
	// local deletes out to same-node sibling TIs (mirrors the legacy
	// deleteFromFollowers path) and bump WatchChunks subscribers.
	orch *Orchestrator

	// fsm is the instance sub-FSM this reconciler is bound to. Stored on
	// Wire() so onAckDelete can read remaining ExpectedFrom without
	// having to re-resolve the FSM through the Raft group.
	fsm *vaultctlfsm.FSM

	mu sync.Mutex

	// sweepInFlight guards against stacking up SweepPendingObligations
	// goroutines when the leader's apply queue is slow. Atomically set
	// to 1 when a sweep starts, 0 when it finishes. Subsequent ticks
	// observe the bit and skip — better to lose a tick than pile up
	// concurrent goroutines fighting for the same Apply queue.
	sweepInFlight atomic.Int32

	// postSealHook overrides the production schedule path used by
	// resumeSealingFromFSM. Defaults nil; tests inject a recorder to
	// observe what would have been scheduled without spinning up the
	// full orchestrator scheduler. Phase 3 (gastrolog-1huz5).
	postSealHook func(vaultID glid.GLID, cm chunk.ChunkManager, id chunk.ChunkID)
}

// NewVaultLifecycleReconciler creates a reconciler for a vault instance.
// localNodeID is required so the reconciler can recognize when its own
// node ID appears in a CmdRequestDelete's ExpectedFrom set (and ack)
// or doesn't (and ignore).
//
// orch may be nil in tests that exercise the reconciler in isolation;
// when nil, the same-node sibling cleanup path is skipped and chunk-
// change notifications are dropped.
func NewVaultLifecycleReconciler(orch *Orchestrator, vaultID glid.GLID, vaultInst *VaultInstance, localNodeID string, logger *slog.Logger) *VaultLifecycleReconciler {
	return &VaultLifecycleReconciler{
		vaultID:     vaultID,
		vaultInst:        vaultInst,
		localNodeID: localNodeID,
		orch:        orch,
		logger:      compVaultLifecycle.Apply(logger).With("vault", vaultID),
	}
}

// Wire installs the reconciler's callbacks on the given vault-ctl FSM. Must
// be called once after the FSM is constructed. Idempotent — repeat
// calls just rebind the callback bindings.
//
// Each callback fires outside the FSM lock, so handlers can call back
// into the chunk manager / Raft applier without risking the
// FSM-mutex / orchestrator-mutex inversion that's been a recurring
// problem (see gastrolog-5oofa, gastrolog-1s3mf).
func (r *VaultLifecycleReconciler) Wire(fsm *vaultctlfsm.FSM) {
	if fsm == nil {
		return
	}
	r.fsm = fsm
	fsm.SetOnSeal(r.onSeal)
	fsm.SetOnRetentionPending(r.onRetentionPending)
	fsm.SetOnRequestDelete(r.onRequestDelete)
	fsm.SetOnAckDelete(r.onAckDelete)
	fsm.SetOnFinalizeDelete(r.onFinalizeDelete)
	fsm.SetOnPruneNode(r.onPruneNode)
	fsm.SetOnPublishFence(r.onPublishFence)
	// Note: onDelete and onUpload remain wired by their existing call
	// sites (file/manager.go). Migrating those into the reconciler
	// happens during steps 4-7 alongside the path-by-path deletions.
}

// ReconcileFromSnapshot runs once after the FSM has been Restore'd from
// a snapshot. Walks the FSM's pendingDeletes and processes any
// obligations this node owes — same code path as the steady-state
// onRequestDelete handler. Also projects the FSM's sealed state onto
// the local chunk Manager (gastrolog-51gme step 8): when an entry is
// flagged sealed in the FSM but the local chunk Manager has it as
// unsealed, EnsureSealed seals it on disk. This replaces the legacy
// "multiple unsealed → seal all but newest" startup heuristic.
//
// Both passes are idempotent. The pending-deletes pass owns the
// receipt-protocol catchup; the sealed-projection pass owns
// gastrolog-uccg6 (FSM-sealed but local-still-active divergence).
//
// IMPORTANT: this is fired from the vault-ctl FSM's after-restore
// hook, which runs on the Raft apply-pump goroutine (Restore and
// Apply share the same hraft runFSM goroutine). Each fulfillObligation
// proposes CmdAckDelete via applier.Apply — on the leader, Apply
// posts to the queue we are currently draining and would deadlock
// the apply pump waiting for our own ack to commit. Snapshot the
// pending list, then dispatch the obligations on a goroutine so the
// apply pump can drain.
func (r *VaultLifecycleReconciler) ReconcileFromSnapshot(fsm *vaultctlfsm.FSM) {
	if fsm == nil {
		return
	}
	pending := fsm.PendingDeletes()
	if len(pending) > 0 {
		r.logger.Info("reconcile-from-snapshot: processing pending deletes",
			"pending_count", len(pending))
	} else {
		r.logger.Debug("reconcile-from-snapshot: no pending deletes")
	}

	// Sealed-state projection acquires the chunk Manager mutex but
	// does not propose Raft applies, so it is safe to run inline.
	r.projectAllSealedFromFSM(fsm)
	r.projectAllCloudBackedFromFSM(fsm)
	// Phase 3 (gastrolog-1huz5): chunks left in Sealing state on the
	// FSM after a leader crash mid-PostSealProcess need their assembly
	// resumed. Re-runs the post-seal pipeline so sealToGLCB completes
	// and AnnounceSeal fires.
	r.resumeSealingFromFSM(fsm)

	if len(pending) == 0 {
		return
	}
	// Snapshot under the FSM read in PendingDeletes() above already
	// returned copies, so no aliasing concern. Defer ack-side Applies
	// to a goroutine to avoid the apply-pump self-cycle.
	go func() {
		for _, p := range pending {
			if !p.ExpectedFrom[r.localNodeID] {
				continue
			}
			r.fulfillObligation(p.ChunkID, p.Reason, "snapshot-restore")
		}
	}()
}

// projectAllSealedFromFSM iterates every entry in the FSM and projects
// the sealed flag onto the local chunk Manager. Used by
// ReconcileFromSnapshot after Restore — at that point the FSM has been
// fully reloaded but the local Manager has only the on-disk flag bits,
// which may have missed CmdSealChunk replays. Idempotent: chunks that
// are already sealed locally, or that don't exist locally, are no-ops.
func (r *VaultLifecycleReconciler) projectAllSealedFromFSM(fsm *vaultctlfsm.FSM) {
	if r.vaultInst == nil || r.vaultInst.Chunks == nil {
		return
	}
	ensurer, ok := r.vaultInst.Chunks.(chunk.SealEnsurer)
	if !ok {
		return
	}
	for _, e := range fsm.List() {
		if !e.IsSealed() {
			continue
		}
		if err := ensurer.EnsureSealed(e.ID); err != nil {
			r.logger.Warn("reconcile-from-snapshot: EnsureSealed failed",
				"chunk", e.ID, "error", err)
		}
	}
}

// resumeSealingFromFSM iterates every Sealing entry in the FSM and
// re-runs the post-seal pipeline so sealToGLCB completes and the
// Sealing → Sealed transition (CmdSealChunk + AnnounceSeal) fires.
//
// Phase 3 (gastrolog-1huz5) splits the seal lifecycle: sealActiveLocked
// fires CmdBeginSeal (Active → Sealing), then PostSealProcess runs
// sealToGLCB and only on success fires CmdSealChunk (Sealing → Sealed).
// If the leader crashes between CmdBeginSeal applying and PostSealProcess
// completing, the FSM is left with State=Sealing entries while the local
// chunk Manager has the active-form files closed and sealed but no
// data.glcb. Anything gating on Sealed (cloud upload, retention,
// replication catchup) waits forever for the second announcement that
// won't come without help.
//
// Recovery: schedule PostSealProcess for each Sealing entry whose chunk
// the local Manager still holds. PostSealProcess is idempotent —
// sealToGLCB writes via .tmp + atomic rename, so a partial GLCB from
// the prior crashed run gets cleanly replaced. The post-seal scheduler
// chains into AnnounceSeal once the GLCB lands.
//
// Skipped silently when the chunk isn't in the local Manager (a
// follower-turned-leader that never had the active-form files cannot
// re-derive a GLCB; the chunk in that case is unrecoverable through
// this path and must come in via SweepMissingReplicas — but with 1:1:1
// placement the leader holding the FSM Sealing entry is the only node
// that ever held the chunk locally, so this is the right place).
func (r *VaultLifecycleReconciler) resumeSealingFromFSM(fsm *vaultctlfsm.FSM) {
	if r.vaultInst == nil || r.vaultInst.Chunks == nil {
		return
	}
	schedule := r.postSealHook
	if schedule == nil {
		if r.orch == nil {
			return
		}
		schedule = r.orch.schedulePostSeal
	}
	// Build a local-chunk index so we don't repeatedly walk the Manager
	// for each FSM entry — the seal-resume pass should be cheap.
	localMetas, err := r.vaultInst.Chunks.List()
	if err != nil {
		r.logger.Warn("reconcile-from-snapshot: list chunks failed for sealing-resume",
			"error", err)
		return
	}
	localByID := make(map[chunk.ChunkID]chunk.ChunkMeta, len(localMetas))
	for _, m := range localMetas {
		localByID[m.ID] = m
	}

	resumed := 0
	for _, e := range fsm.List() {
		if e.State != chunk.ChunkStateSealing {
			continue
		}
		local, ok := localByID[e.ID]
		if !ok {
			// No local active-form files to assemble from. Either a
			// follower-turned-leader (unreachable in 1:1:1) or the
			// chunk has been retired without a Sealed transition. Log
			// and move on — operator-visible because this is genuinely
			// unrecoverable through this path.
			r.logger.Warn("reconcile-from-snapshot: Sealing entry has no local chunk; cannot resume",
				"chunk", e.ID)
			continue
		}
		if !local.Sealed {
			// Active-form files weren't sealed at crash time. The seal
			// flag is only flipped after sealActiveLocked closes the
			// files, so this state is a deeper bug — the FSM advanced
			// past CmdBeginSeal but the local sealed-flag write was
			// lost. Log loudly; the next ingest+rotate cycle will
			// re-run the full path on a different chunk.
			r.logger.Warn("reconcile-from-snapshot: Sealing entry but local chunk not sealed on disk",
				"chunk", e.ID)
			continue
		}
		r.logger.Info("reconcile-from-snapshot: resuming PostSealProcess for Sealing chunk",
			"chunk", e.ID)
		schedule(r.vaultID, r.vaultInst.Chunks, e.ID)
		resumed++
	}
	if resumed > 0 {
		r.logger.Info("reconcile-from-snapshot: scheduled seal resumption",
			"count", resumed)
	}
}

// projectAllCloudBackedFromFSM iterates every cloud-backed entry in the
// FSM and registers the chunk in the local chunk Manager's cloud index
// via RegisterCloudChunk. Used by ReconcileFromSnapshot after Restore —
// the per-apply onUpload effect (which fires the same RegisterCloudChunk
// for live CmdUploadChunk replication) does NOT fire during snapshot
// install (Restore replaces f.chunks wholesale, no per-entry effects),
// so cloud chunks that arrived during snapshot install would otherwise
// be present in the FSM but absent from cm.cloudIdx — making
// cm.OpenCursor return ErrChunkNotFound and aborting search streams.
// RegisterCloudChunk is idempotent (skips if already in m.metas or
// m.cloudIdx), so calling it for every cloud-backed entry is safe.
// See gastrolog-3ukgz.
func (r *VaultLifecycleReconciler) projectAllCloudBackedFromFSM(fsm *vaultctlfsm.FSM) {
	if r.vaultInst == nil || r.vaultInst.Chunks == nil {
		return
	}
	registrar, ok := r.vaultInst.Chunks.(chunk.CloudChunkRegistrar)
	if !ok {
		return
	}
	for _, e := range fsm.List() {
		if !e.CloudBacked {
			continue
		}
		info := chunk.CloudChunkInfo{
			WriteStart:        e.WriteStart,
			WriteEnd:          e.WriteEnd,
			IngestStart:       e.IngestStart,
			IngestEnd:         e.IngestEnd,
			SourceStart:       e.SourceStart,
			SourceEnd:         e.SourceEnd,
			RecordCount:       e.RecordCount,
			Bytes:             e.Bytes,
			DiskBytes:         e.DiskBytes,
			IngestIdxOffset:   e.IngestIdxOffset,
			IngestIdxSize:     e.IngestIdxSize,
			SourceIdxOffset:   e.SourceIdxOffset,
			SourceIdxSize:     e.SourceIdxSize,
			IngestTSMonotonic: e.IngestTSMonotonic,
		}
		if err := registrar.RegisterCloudChunk(e.ID, info); err != nil {
			r.logger.Warn("reconcile-from-snapshot: RegisterCloudChunk failed",
				"chunk", e.ID, "error", err)
		}
	}
}

// ---------- FSM apply event handlers ----------
//
// All seven handlers run outside the FSM mutex (see Wire()). They take
// the reconciler's own mu when they need to serialize writes against
// each other or against ReconcileFromSnapshot, but never hold it across
// a Raft Apply or a chunk-manager I/O call to avoid the lock-inversion
// trap.

// onSeal fires when CmdSealChunk applies on this node. Projects the
// FSM-sealed state onto the local chunk Manager via the SealEnsurer
// interface. The Manager's EnsureSealed contract handles the cases
// where the chunk is already sealed, doesn't exist locally, or is the
// local active chunk — only the unsealed-on-disk case results in a
// header rewrite. See gastrolog-51gme step 8 / gastrolog-uccg6.
//
// Fires NotifyChunkChange unconditionally: the FSM's authoritative
// view of this chunk just changed (the seal flag flipped), so the
// inspector's WatchChunks subscribers on this node need to refresh.
// Local EnsureSealed failure does not gate the notification — the
// inspector reflects FSM state, not on-disk state. See gastrolog-2ob86.
func (r *VaultLifecycleReconciler) onSeal(e vaultctlfsm.ManifestEntry) {
	r.logger.Debug("onSeal", "chunk", e.ID, "records", e.RecordCount)
	defer func() {
		if r.orch == nil {
			return
		}
		// Emit SEALED with the FSM ManifestEntry as the authoritative
		// source: every cluster node's FSM applies CmdSealChunk with the
		// same payload, so every node emits the same final RecordCount /
		// Bytes / IngestStart / etc. Using local Manager.Meta instead
		// would produce per-node variance (followers lag the leader's
		// record stream), making the inspector flicker through stale
		// counts as N+1 SEALED events arrive in sequence. See
		// gastrolog-3pf9w.
		r.orch.EmitChunkSealed(r.vaultID, manifestEntryToChunkMeta(e, true))
	}()
	if r.vaultInst == nil || r.vaultInst.Chunks == nil {
		return
	}
	ensurer, ok := r.vaultInst.Chunks.(chunk.SealEnsurer)
	if !ok {
		return
	}
	if err := ensurer.EnsureSealed(e.ID); err != nil {
		r.logger.Warn("onSeal: EnsureSealed failed",
			"chunk", e.ID, "error", err)
	}
}

// onPublishFence fires when CmdPublishFence applies on this node. Schedules
// local spool-to-chunk materialization for sequenced write-model vaults.
func (r *VaultLifecycleReconciler) onPublishFence(rec vaultctlfsm.FenceRecord) {
	if r.orch == nil {
		return
	}
	r.orch.scheduleMaterializeFence(r.vaultID, rec)
}

func (r *VaultLifecycleReconciler) onRetentionPending(id chunk.ChunkID) {
	r.logger.Debug("onRetentionPending", "chunk", id)
	// Audit-only. The actual cleanup goes through CmdRequestDelete.
}

// onRequestDelete fires on every node when CmdRequestDelete commits.
// Each node in ExpectedFrom owes one ack: delete the local chunk if
// it exists, then propose CmdAckDelete. Idempotent on the FSM side —
// duplicate / unknown-node acks are silently dropped, so a partial
// failure here just means we'll retry on the next ReconcileFromSnapshot
// (or the next time the obligation is re-observed).
//
// IMPORTANT: this runs on the FSM apply goroutine. The local-delete
// portion is safe to do inline (no Raft round-trip). The ack itself
// MUST happen in a separate goroutine — proposing CmdAckDelete on the
// leader posts to the same Raft apply queue we're currently draining,
// which would deadlock the leader's apply pump waiting for its own
// queued ack to apply. See gastrolog-51gme follow-up: apply-pump
// self-cycle stall observed in the 4-node test cluster.
func (r *VaultLifecycleReconciler) onRequestDelete(p vaultctlfsm.PendingDelete) {
	if !p.ExpectedFrom[r.localNodeID] {
		r.logger.Debug("onRequestDelete: not in expectedFrom",
			"chunk", p.ChunkID, "reason", p.Reason)
		return
	}
	go r.fulfillObligation(p.ChunkID, p.Reason, "request-delete")
}

// onAckDelete fires on every node when CmdAckDelete commits.
//
// Audit-only post gastrolog-15fm8: applyAckDelete now finalizes the
// delete atomically inside the same apply when ExpectedFrom drains to
// empty. The FSM's onFinalizeDelete callback fires from the same
// apply dispatch, so post-finalize bookkeeping happens through that
// path. This callback retains only the per-ack observability signal.
func (r *VaultLifecycleReconciler) onAckDelete(chunkID chunk.ChunkID, ackingNodeID string) {
	r.logger.Debug("onAckDelete", "chunk", chunkID, "node", ackingNodeID)
}

func (r *VaultLifecycleReconciler) onFinalizeDelete(chunkID chunk.ChunkID) {
	r.logger.Debug("onFinalizeDelete", "chunk", chunkID)
	// Audit-only. The pending entry was removed inside applyFinalizeDelete
	// before this callback fired.
}

// onPruneNode fires on every node when CmdPruneNode commits.
//
// Audit-only post gastrolog-15fm8: applyPruneNode now finalizes
// chunks whose ExpectedFrom drained as a result of the prune
// atomically inside the same apply. The FSM's onFinalizeDelete
// callback fires per chunk from the same apply dispatch. This
// callback retains only the per-prune observability signal.
//
// Pre-fix (gastrolog-51gme step 10), the leader proposed
// CmdFinalizeDelete for each chunk in a goroutine; leadership
// transfer between the prune apply and the goroutine running could
// strand pendingDeletes entries forever. Folding the finalize into
// applyPruneNode closes that leak — see gastrolog-3qr8z for the
// disease pattern.
func (r *VaultLifecycleReconciler) onPruneNode(prunedNodeID string, finalizable []chunk.ChunkID) {
	r.logger.Debug("onPruneNode",
		"node", prunedNodeID, "finalizable_count", len(finalizable))
}

// SweepPendingObligations walks the FSM's pendingDeletes and runs
// fulfillObligation for any entry where this node is still in
// ExpectedFrom. The orchestrator schedules this on a 20s cron tick
// (offset from the retention sweep) so deletes that the steady-state
// onRequestDelete callback missed — apply-pump wedge, callback error,
// node pause, plain restart without snapshot install — eventually
// converge.
//
// Idempotent: pendingDeletes is local FSM state replicated across
// every node, so each node consults its OWN copy and decides
// independently. No leader involvement; the FSM's applyAckDelete
// drops duplicate / already-pruned acks. Snapshot the list under
// PendingDeletes() (which already returns copies) and fire
// fulfillObligation in a goroutine to avoid blocking the cron
// scheduler if the leader's apply queue is slow.
func (r *VaultLifecycleReconciler) SweepPendingObligations() {
	if r.fsm == nil {
		return
	}
	// Skip if a previous sweep is still in flight. Prevents goroutine
	// pile-up when the leader's apply queue is slow — better to lose a
	// tick than have multiple concurrent sweeps fighting for the same
	// Apply slots and amplifying the saturation.
	if !r.sweepInFlight.CompareAndSwap(0, 1) {
		r.logger.Debug("pending-delete sweep: previous sweep still in flight, skipping")
		return
	}
	pending := r.fsm.PendingDeletes()
	if len(pending) == 0 {
		r.sweepInFlight.Store(0)
		return
	}
	// Count obligations this node still owes — the rest are someone
	// else's problem and shouldn't pollute the per-node sweep log.
	owed := 0
	for _, p := range pending {
		if p.ExpectedFrom[r.localNodeID] {
			owed++
		}
	}
	if owed > 0 {
		r.logger.Info("pending-delete sweep: fulfilling obligations",
			"owed", owed, "total_pending", len(pending))
	} else {
		r.logger.Debug("pending-delete sweep: no obligations owed by this node",
			"total_pending", len(pending))
	}
	go func() {
		defer r.sweepInFlight.Store(0)
		for _, p := range pending {
			if !p.ExpectedFrom[r.localNodeID] {
				continue
			}
			r.fulfillObligation(p.ChunkID, p.Reason, "periodic-sweep")
		}
	}()
}

// SweepLocalOrphans walks local sealed chunks and deletes any whose FSM
// state proves they were finalize-deleted while this node was offline.
// This fills the snapshot-restore gap that pendingDeletes alone cannot
// cover: when a delete cycle ran to completion (CmdRequestDelete →
// CmdAckDelete from every reachable node → CmdFinalizeDelete) while
// this node was paused or partitioned, snapshot install brings the
// FSM forward to the post-finalize state — tombstone present,
// pendingDeletes entry gone, manifest entry gone. The local file is
// then orphaned with no receipt obligation to drive cleanup.
//
// Safety invariants — ALL must hold before the local file is touched:
//
//   - chunk MUST be sealed locally. Active (unsealed) chunks may be
//     mid-rotation and have no FSM presence yet; never act on those.
//   - chunk MUST be absent from the FSM manifest (fsm.Get returns nil).
//     FSM-known live entries stay regardless of replication state.
//   - chunk MUST be absent from pendingDeletes. Active deletes are
//     SweepPendingObligations' responsibility — let the receipt
//     protocol drive them.
//   - chunk MUST be tombstoned in the FSM. Tombstone presence is
//     positive proof that an applyFinalizeDelete ran for this chunk.
//     A freshly-created chunk with announce-in-flight has no tombstone
//     and would not be touched, so we cannot mistake "not yet known"
//     for "deleted".
//
// Logged at INFO level so the deletion is visible in cluster.log
// without per-component log-level overrides — the whole point of this
// sweep is operator-visible recovery.
func (r *VaultLifecycleReconciler) SweepLocalOrphans() {
	if r.fsm == nil || r.vaultInst == nil || r.vaultInst.Chunks == nil {
		return
	}
	metas, err := r.vaultInst.Chunks.List()
	if err != nil {
		r.logger.Warn("local-orphan sweep: list chunks failed", "error", err)
		return
	}
	ensurer, _ := r.vaultInst.Chunks.(chunk.SealEnsurer) // optional
	// Chunks freshly created on this node but whose CmdCreateChunk hasn't
	// applied yet would also fail fsm.Get / IsTombstoned. We don't want to
	// race-delete them. Use seal age as a coarse "old enough that announce
	// would have applied by now" guard for the no-tombstone branch — if a
	// chunk has been sealed for longer than this, the Create-then-Delete
	// pair on the FSM has had ample time to converge.
	const ghostAgeThreshold = 5 * time.Minute
	now := time.Now()
	var deleted int
	for _, meta := range metas {
		if r.fsm.Get(meta.ID) != nil {
			continue
		}
		if r.fsm.PendingDelete(meta.ID) != nil {
			continue
		}
		// Two paths to deletion eligibility:
		//  - Tombstoned: FSM positively recorded a finalize-delete. Always safe.
		//  - Ghost (rotation artifact): FSM has no entry AND no tombstone,
		//    RecordCount == 0 (no real data — never finished a record append),
		//    sealed long enough ago that a pending Create can't still be
		//    in-flight. The retention sweep otherwise re-transitions these
		//    ghosts every minute and pollutes downstream vaults. See
		//    gastrolog-66b7x.
		//
		// A third class — data-bearing chunks the FSM doesn't recognize —
		// is handled separately below: those raise an operator alert and
		// are preserved on disk per the no-auto-delete-of-unknown-orphans
		// invariant (docs/disk-authority-audit.md, gastrolog-3y8py).
		// Auto-deleting them removes the recovery surface for FSM-glitch
		// scenarios.
		tombstoned := r.fsm.IsTombstoned(meta.ID)
		ghost := !tombstoned && meta.Sealed && !meta.WriteEnd.IsZero() &&
			meta.RecordCount == 0 &&
			now.Sub(meta.WriteEnd) > ghostAgeThreshold
		unknownOrphan := !tombstoned && meta.Sealed && meta.RecordCount > 0
		if unknownOrphan {
			r.alertUnknownOrphan(meta)
			continue
		}
		if !tombstoned && !ghost {
			continue
		}
		// Local-active + FSM-tombstoned (gastrolog-533l9): the
		// chunk was active on this node at crash time; while
		// offline, the cluster sealed → retention-deleted →
		// finalized it; no live obligation remains in the FSM
		// (only the tombstone). Demote local active first so the
		// subsequent deleteLocalCopy doesn't bounce off
		// ErrActiveChunk. Same demote-then-delete sequence as
		// fulfillObligation (gastrolog-2yeht).
		if !meta.Sealed {
			if ensurer == nil {
				r.logger.Warn("local-orphan sweep: chunk is local active but Manager has no SealEnsurer; skipping",
					"chunk", meta.ID)
				continue
			}
			if err := ensurer.EnsureSealed(meta.ID); err != nil {
				r.logger.Warn("local-orphan sweep: pre-demote failed",
					"chunk", meta.ID, "error", err)
				continue
			}
		}
		r.logger.Info("local-orphan sweep: deleting tombstoned local chunk",
			"chunk", meta.ID)
		if err := r.deleteLocalCopy(meta.ID); err != nil {
			r.logger.Warn("local-orphan sweep: delete failed",
				"chunk", meta.ID, "error", err)
			continue
		}
		deleted++
	}
	if deleted > 0 {
		r.logger.Info("local-orphan sweep: cleaned up tombstoned orphans",
			"deleted", deleted)
	}
}

// alertUnknownOrphan raises an operator-visible alert for a chunk
// that is sealed, has records, but is not recognized by the FSM
// (no manifest entry, no pendingDeletes, no tombstone). The
// no-auto-delete-of-unknown-orphans invariant
// (docs/disk-authority-audit.md) keeps these files on disk so they
// remain available as a recovery surface for FSM-glitch scenarios
// (bugs, operator error, restore-from-backup desync).
//
// The alert is keyed per (vault, chunk) so each unknown orphan
// appears as its own row in the operator UI. The "unknown orphan"
// language deliberately matches the audit doc terminology so future
// readers find the same concept in one search.
//
// Why this is conservative: a chunk with RecordCount > 0 took
// committed appends; either it was part of the cluster at some
// point (and the FSM lost the record — recoverable), or it's been
// orphaned by an aborted ingest path (rare). Either way, deleting
// it removes information the cluster has no other copy of.
func (r *VaultLifecycleReconciler) alertUnknownOrphan(meta chunk.ChunkMeta) {
	if r.orch == nil || r.orch.alerts == nil {
		return
	}
	alertID := fmt.Sprintf("unknown-orphan:%s:%s", r.vaultID, meta.ID)
	r.orch.alerts.Set(alertID, alert.Warning, "vault",
		fmt.Sprintf("Vault %s: chunk %s on disk with %d records but not recognized by FSM; preserved for recovery (do not manually delete without operator review)",
			r.vaultID, meta.ID, meta.RecordCount))
}

// SweepMissingReplicas walks the FSM's sealed-chunk manifest and asks
// every placement peer to re-push any sealed chunks this node should
// have but doesn't. This is the create-side mirror of SweepLocalOrphans:
// where SweepLocalOrphans cleans up local files the FSM has tombstoned,
// SweepMissingReplicas pulls in local files the FSM expects but the
// disk lacks. Together with SweepPendingObligations these three sweeps
// give a node "every 20s, reconcile my local store against my replicated
// FSM in both directions."
//
// Failure modes this sweep recovers from:
//
//   - Leader sealed a chunk while a follower was offline;
//     replicateToFollower's gRPC push failed; no retry queue exists at
//     the leader. Vault-ctl Raft caught the follower's FSM up via
//     snapshot install or log replay on rejoin so the manifest entry is
//     present, but the actual chunk records aren't on disk
//     (gastrolog-2dgvj).
//
//   - Leadership transferred to a node that joined the placement set
//     after some chunks were written. The new leader's FSM has the
//     manifest entry (replicated via vault-ctl Raft) but its local
//     chunk manager never received the bytes. The old chunks live on
//     the prior replica set; the new leader must pull them from a peer
//     instead of declaring them lost to the stale-fsm sweep
//     (gastrolog-19241).
//
// Both roles run this sweep. The peer set is asymmetric by role:
// followers ask the leader (FollowerTargets is empty on followers,
// LeaderNodeID points at the leader); the leader asks every follower
// (FollowerTargets enumerates them). Cloud-backed chunks live in shared
// object storage and are skipped — they are not a local-replica concern.
func (r *VaultLifecycleReconciler) SweepMissingReplicas() {
	if r.fsm == nil || r.vaultInst == nil || r.vaultInst.Chunks == nil {
		return
	}
	if r.orch == nil || r.orch.chunkReplicator == nil {
		return // no transport wired; cluster mode requires a replicator
	}

	peers := r.replicationPeers()
	if len(peers) == 0 {
		return // nothing to ask
	}

	// Local index of what's on disk so the diff is O(N+M) not O(N×M).
	localMetas, err := r.vaultInst.Chunks.List()
	if err != nil {
		r.logger.Warn("missing-replica sweep: list chunks failed", "error", err)
		return
	}
	have := make(map[chunk.ChunkID]bool, len(localMetas))
	for _, m := range localMetas {
		have[m.ID] = true
	}

	// Walk the FSM manifest and collect the missing-locally subset.
	var missing []chunk.ChunkID
	for _, e := range r.fsm.List() {
		if !e.IsSealed() {
			continue
		}
		if e.CloudBacked {
			continue // shared bucket; no local replica needed
		}
		if have[e.ID] {
			continue
		}
		missing = append(missing, e.ID)
	}

	if len(missing) == 0 {
		return
	}

	r.logger.Info("missing-replica sweep: requesting catchup",
		"peers", peers, "missing", len(missing))

	// Ask every peer. Whichever peer has a given chunk schedules the
	// push; peers that don't have it return scheduled=0 silently. The
	// receiver dedupes if multiple peers push the same chunk because
	// the second arrival hits the local-already-present gate.
	for _, peerNodeID := range peers {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		scheduled, err := r.orch.chunkReplicator.RequestReplicaCatchup(
			ctx, peerNodeID, r.vaultID, missing, r.localNodeID)
		cancel()
		if err != nil {
			// The next sweep tick will retry. Causes: peer changed after
			// we resolved placement state, peer unreachable, peer connection
			// warming up. None terminal — the FSM diff is local state, so
			// we converge on the next tick.
			r.logger.Warn("missing-replica sweep: request failed",
				"peer", peerNodeID, "missing", len(missing), "error", err)
			continue
		}
		r.logger.Info("missing-replica sweep: peer scheduled pushes",
			"peer", peerNodeID, "scheduled", scheduled, "requested", len(missing))
	}
}

// replicationPeers returns the placement-peer node IDs this reconciler
// should ask for missing chunks. Followers ask the leader; leaders ask
// every follower target. Self is always excluded. Empty IDs (transient
// unknown state during placement transitions) are filtered so we don't
// dial nowhere.
func (r *VaultLifecycleReconciler) replicationPeers() []string {
	if r.vaultInst == nil {
		return nil
	}
	if r.vaultInst.IsFollower {
		if r.vaultInst.LeaderNodeID == "" || r.vaultInst.LeaderNodeID == r.localNodeID {
			return nil
		}
		return []string{r.vaultInst.LeaderNodeID}
	}
	peers := make([]string, 0, len(r.vaultInst.FollowerTargets))
	seen := map[string]bool{r.localNodeID: true}
	for _, t := range r.vaultInst.FollowerTargets {
		if t.NodeID == "" || seen[t.NodeID] {
			continue
		}
		peers = append(peers, t.NodeID)
		seen[t.NodeID] = true
	}
	return peers
}

// staleLeaderFSMGracePeriod is how long a sealed-but-not-on-disk-locally
// FSM entry stays around before SweepStaleLeaderFSMEntries proposes
// CmdRequestDelete to remove it. The grace lets followers replicate or
// leadership transfer to a node that DOES have the chunk before we
// declare it unrecoverable. One hour is well past every transient
// failure mode (gRPC retry, leader election, warm-cache fault) and
// matches the cluster's coarse-grained reconciliation cadence.
const staleLeaderFSMGracePeriod = 1 * time.Hour

// SweepStaleLeaderFSMEntries walks the FSM manifest on the leader of a
// non-cloud instance and proposes CmdRequestDelete for any sealed entry
// missing from the leader's local chunk manager AND past the grace
// period. The leader is the source of truth for non-cloud vaults
// (per SweepMissingReplicas's invariant); if the leader doesn't have
// the chunk and the chunk isn't recoverable from peers (the
// missing-replica catchup mechanism only works leader→follower, so a
// leader hole is unrecoverable in the current architecture), the FSM
// should reflect that the chunk is gone instead of letting search
// fan-out hit ErrChunkNotFound forever.
//
// Skips cloud-backed chunks: those live in shared object storage and
// have separate health logic (cloud_health.go). Skips chunks fresher
// than the grace period: a transient disk fault or in-flight seal
// shouldn't trigger delete. Skips chunks already in pendingDeletes:
// the receipt protocol is already running.
//
// See gastrolog-5nhwe.
func (r *VaultLifecycleReconciler) SweepStaleLeaderFSMEntries() {
	if r.fsm == nil || r.vaultInst == nil || r.vaultInst.Chunks == nil {
		return
	}
	if r.vaultInst.IsFollower {
		return // followers use SweepMissingReplicas to pull from leader
	}
	if r.vaultInst.ApplyRaftRequestDelete == nil {
		return // single-node / no Raft; no receipt protocol
	}

	localMetas, err := r.vaultInst.Chunks.List()
	if err != nil {
		r.logger.Warn("stale-fsm sweep: list chunks failed", "error", err)
		return
	}
	have := make(map[chunk.ChunkID]bool, len(localMetas))
	for _, m := range localMetas {
		have[m.ID] = true
	}

	now := time.Now()
	expectedFrom := r.placementMembership()
	stale := 0
	for _, e := range r.fsm.List() {
		// Sealed and Sealing chunks are both candidates here. A Sealing
		// entry whose chunk this leader doesn't have locally is the
		// classic "leader transferred mid-PostSealProcess and the new
		// leader never had the active-form files" case (gastrolog-1huz5):
		// no recovery path in 1:1:1 placement, so the entry must be
		// retired after grace period or it stays in Sealing forever.
		// Active entries are skipped — they're too transient to confuse
		// with stranded state, and a missing Active is properly handled
		// by other paths (vault readiness, ingest reroute).
		if e.State != chunk.ChunkStateSealed && e.State != chunk.ChunkStateSealing {
			continue
		}
		if e.CloudBacked {
			continue
		}
		if have[e.ID] {
			continue
		}
		// Grace period anchored on WriteEnd (the seal completion time)
		// for Sealed entries. Sealing entries don't have a WriteEnd yet
		// (CmdSealChunk never applied), so anchor on WriteStart instead
		// — the only timestamp the FSM has — to give the same "is this
		// genuinely stuck?" signal.
		anchor := e.WriteEnd
		if anchor.IsZero() {
			anchor = e.WriteStart
		}
		if !anchor.IsZero() && now.Sub(anchor) < staleLeaderFSMGracePeriod {
			continue
		}
		// PendingDelete check inside deleteChunk dedups but logs each
		// proposal; check up front to keep the log quiet for the
		// already-in-flight case.
		if r.fsm.PendingDelete(e.ID) != nil {
			continue
		}
		r.logger.Warn("stale-fsm sweep: proposing delete for unrecoverable chunk",
			"chunk", e.ID, "vault", r.vaultID,
			"state", e.State, "anchor", anchor, "age", now.Sub(anchor))
		if err := r.deleteChunk(e.ID, "stale-fsm-leader-missing", expectedFrom); err != nil {
			r.logger.Warn("stale-fsm sweep: deleteChunk failed",
				"chunk", e.ID, "error", err)
			continue
		}
		stale++
	}
	if stale > 0 {
		r.logger.Info("stale-fsm sweep: deletes proposed",
			"vault", r.vaultID, "count", stale)
	}
}

// SweepStalePendingDeleteAcks walks every pendingDelete entry and proposes
// CmdPruneNode for any node in ExpectedFrom that's no longer in the vault's
// current placement set. This unsticks retention-deletes whose ExpectedFrom
// references nodes removed from the vault's placement (e.g., after a
// kubernetes-contract or vault rebalance): those nodes can never ack
// because they have no vault instance running locally.
//
// Without this sweep, the receipt protocol deadlocks: g-1 was in the
// placement when CmdRequestDelete was proposed, the placement later
// changed to exclude g-1, g-1 lost its vault instance, the ExpectedFrom
// entry survives, and nothing can drive an ack. The retention-pending
// chunks sit forever, the receipt-protocol's stuck observation surfaces
// in the inspector as `pending-ack: gastrolog-1`, but no automatic
// recovery fires.
//
// Why use CmdPruneNode (not CmdAckDelete) for the cleanup: CmdPruneNode
// has the exact semantic we need — "node X is no longer in scope; drop
// it from every entry's ExpectedFrom, finalize entries whose Expected
// From drained as a result". CmdAckDelete would also work but requires
// per-chunk proposals; CmdPruneNode batches the whole prune into one
// apply per stale node.
//
// Leader-only (the same gate as SweepStaleLeaderFSMEntries): only one
// node should propose, and the vault-ctl leader is the natural single
// point because retention itself is leader-only.
//
// See gastrolog-2eclw follow-up: the live K8s cluster ended up with
// 8 stuck chunks whose ExpectedFrom contained only gastrolog-1 — a
// former placement member with no current vault instance. This sweep
// is the self-healing path.
func (r *VaultLifecycleReconciler) SweepStalePendingDeleteAcks() {
	if r.fsm == nil || r.vaultInst == nil {
		return
	}
	if r.vaultInst.IsFollower {
		return
	}
	if r.vaultInst.ApplyRaftPruneNode == nil {
		return
	}

	// Build the current-placement set: this node (always the leader
	// for the sweep — see follower gate above) plus every follower
	// target. Any nodeID in pendingDeletes ExpectedFrom that's NOT in
	// this set is stale.
	placement := make(map[string]bool, 1+len(r.vaultInst.FollowerTargets))
	if r.localNodeID != "" {
		placement[r.localNodeID] = true
	}
	for _, t := range r.vaultInst.FollowerTargets {
		if t.NodeID != "" {
			placement[t.NodeID] = true
		}
	}

	staleNodes := make(map[string]bool)
	for _, p := range r.fsm.PendingDeletes() {
		for nodeID := range p.ExpectedFrom {
			if !placement[nodeID] {
				staleNodes[nodeID] = true
			}
		}
	}
	if len(staleNodes) == 0 {
		return
	}

	for nodeID := range staleNodes {
		r.logger.Warn("stale-pending-ack sweep: proposing CmdPruneNode for stale ExpectedFrom",
			"vault", r.vaultID, "stale_node", nodeID,
			"current_placement_size", len(placement))
		if err := r.vaultInst.ApplyRaftPruneNode(nodeID); err != nil {
			r.logger.Warn("stale-pending-ack sweep: CmdPruneNode apply failed",
				"vault", r.vaultID, "stale_node", nodeID, "error", err)
			continue
		}
	}
	r.logger.Info("stale-pending-ack sweep: pruned stale ExpectedFrom",
		"vault", r.vaultID, "stale_count", len(staleNodes))
}

// idleActiveThreshold is how long an FSM-Active chunk can sit without
// receiving record appends (i.e., local WriteEnd hasn't advanced)
// before SweepIdleActiveChunks seals it. Targets the orphan-active
// case from gastrolog-2eclw: when leadership for a vault transfers
// to a new node, the previous leader's active chunk stops receiving
// appends and becomes permanently stranded — no rotation triggers
// (record-count / size) ever fire on a chunk that's frozen, retention
// skips !Sealed chunks, and the FSM keeps the active entry forever.
//
// Setting this threshold higher than the leader's typical
// rotation-policy MaxAge ensures the leader's own active chunk rotates
// via the normal rotation path first, leaving this fallback to catch
// ONLY orphans the rotation path no longer covers (chunks left in the
// FSM as Active after a leader transfer or after a missed AnnounceSeal
// on shutdown).
const idleActiveThreshold = 10 * time.Minute

// SweepIdleActiveChunks walks the FSM's Active manifest entries and
// seals any whose local copy has been idle (no record appends) for
// longer than idleActiveThreshold. Drives from the FSM rather than the
// chunk manager's singular m.active because multiple Active entries
// can exist simultaneously:
//
//   - On restart, file.Manager recovers all unsealed chunks into
//     m.metas but only opens the newest as m.active. Older unsealed
//     chunks sit in m.metas waiting for FSM projection — which never
//     comes if the FSM also says they're Active.
//   - Leader transfers and lost AnnounceSeal calls (the announcer
//     short-circuits during shutdown, see announcer.go) leave the FSM
//     with stale Active entries that no node's m.active is currently
//     advancing.
//
// Algorithm per FSM Active entry e:
//
//  1. Skip if this node doesn't hold the chunk locally. Some other
//     holder will run the same sweep and propose the seal — every
//     node runs this on every tick.
//  2. Require positive WriteEnd. A zero WriteEnd is a freshly-created
//     chunk that never saw an append; this isn't the orphan case.
//  3. Require age > threshold. Live chunks rotate via the normal
//     rotation path; only frozen WriteEnd indicates orphaning.
//  4. If the chunk is the local m.active, call Chunks.Seal() — that
//     fires AnnounceSeal cluster-wide and rotates a fresh active.
//     Otherwise EnsureSealed the local files and manually call the
//     announcer with the local metadata to propose CmdSealChunk.
//
// Idempotency: CmdSealChunk applies on every replica. If two holders
// race (both have local copies of the same Active entry and both
// detect it as idle), both propose; the FSM's applySeal overwrites
// with the second metadata. Metadata is consistent across replicas
// because the active was replicated when it was the leader's, so the
// race is harmless.
//
// See gastrolog-2eclw / gastrolog-3qr8z.
func (r *VaultLifecycleReconciler) SweepIdleActiveChunks() {
	if r.fsm == nil || r.vaultInst == nil || r.vaultInst.Chunks == nil {
		return
	}
	announcerGetter, ok := r.vaultInst.Chunks.(chunk.AnnouncerGetter)
	if !ok {
		return
	}
	announcer := announcerGetter.GetAnnouncer()
	if announcer == nil {
		return
	}

	var localActiveID chunk.ChunkID
	if a := r.vaultInst.Chunks.Active(); a != nil {
		localActiveID = a.ID
	}

	sealed := 0
	for _, e := range r.fsm.List() {
		if r.sealIfIdleActive(e, localActiveID, announcer) {
			sealed++
		}
	}
	if sealed > 0 {
		r.logger.Info("idle-active sweep: sealed stranded orphans",
			"vault", r.vaultID, "count", sealed)
	}
}

// sealIfIdleActive seals a single FSM Active entry if this node holds
// it locally, hasn't already locally sealed it, and its local WriteEnd
// has been frozen past the idle threshold. Returns true on a successful
// seal. Extracted from SweepIdleActiveChunks to keep cyclomatic /
// cognitive complexity manageable now that the sweep has two distinct
// seal paths.
//
// CRITICAL: the localMeta.Sealed guard breaks the runaway-loop the
// first implementation hit in K8s. Without it, every 20s tick re-fired
// AnnounceSeal AND postSealWork — which re-ran the full GLCB assembly
// + index rebuild every tick. On a 20MB orphan, that's ~60s of GC
// churn per tick and pushed RSS into the multi-GB range. The guard
// makes the sweep one-shot per orphan: EnsureSealed flips the local
// flag, the announce+post-seal fire once, and subsequent ticks
// short-circuit even if the FSM hasn't reached Sealed yet (which can
// happen when Apply forwards to a leader that's lost track of this
// vault-ctl group's membership).
func (r *VaultLifecycleReconciler) sealIfIdleActive(e vaultctlfsm.ManifestEntry, localActiveID chunk.ChunkID, announcer chunk.MetadataAnnouncer) bool {
	if e.State != chunk.ChunkStateActive {
		return false
	}
	localMeta, err := r.vaultInst.Chunks.Meta(e.ID)
	if err != nil {
		return false
	}
	if localMeta.Sealed {
		// Already sealed locally on a prior tick (or by some other path).
		// FSM is still Active either because the AnnounceSeal Apply
		// failed silently or the vault-ctl group's leader hasn't fully
		// converged. Either way, do NOT re-run the post-seal pipeline.
		return false
	}
	if localMeta.WriteEnd.IsZero() {
		return false
	}
	age := time.Since(localMeta.WriteEnd)
	if age < idleActiveThreshold {
		return false
	}

	r.logger.Warn("idle-active sweep: sealing stranded orphan",
		"chunk", e.ID, "vault", r.vaultID,
		"is_m_active", e.ID == localActiveID,
		"write_end", localMeta.WriteEnd, "age", age)

	if e.ID == localActiveID {
		return r.sealLocalActive(e.ID)
	}
	return r.sealMetadataOnlyOrphan(e.ID, localMeta, announcer)
}

// sealLocalActive seals the current m.active via Chunks.Seal(), which
// fires AnnounceSeal internally and rotates a fresh active. Used when
// the FSM Active entry matches the chunk manager's local m.active
// pointer (the steady-state orphan: this node's active stream
// stopped after a leader transfer).
func (r *VaultLifecycleReconciler) sealLocalActive(id chunk.ChunkID) bool {
	if err := r.vaultInst.Chunks.Seal(); err != nil {
		r.logger.Warn("idle-active sweep: local seal failed",
			"chunk", id, "error", err)
		return false
	}
	if r.orch != nil {
		r.orch.postSealWork(r.vaultID, r.vaultInst.Chunks, id)
	}
	return true
}

// sealMetadataOnlyOrphan handles the FSM-Active entry whose local
// copy exists in m.metas but isn't the m.active pointer (file.Manager
// startup recovery opens only the newest unsealed chunk as m.active;
// older unsealed chunks sit metadata-only). EnsureSealed flips the
// on-disk sealed flag so receipt-protocol deletes don't bounce off
// ErrActiveChunk; AnnounceSeal then proposes CmdSealChunk manually
// with the local metadata.
func (r *VaultLifecycleReconciler) sealMetadataOnlyOrphan(id chunk.ChunkID, localMeta chunk.ChunkMeta, announcer chunk.MetadataAnnouncer) bool {
	if ensurer, ok := r.vaultInst.Chunks.(chunk.SealEnsurer); ok {
		if err := ensurer.EnsureSealed(id); err != nil {
			r.logger.Warn("idle-active sweep: EnsureSealed failed",
				"chunk", id, "error", err)
			return false
		}
		if m, err := r.vaultInst.Chunks.Meta(id); err == nil {
			localMeta = m
		}
	}
	announcer.AnnounceSeal(
		id,
		localMeta.WriteEnd,
		localMeta.RecordCount,
		localMeta.Bytes,
		localMeta.IngestStart,
		localMeta.IngestEnd,
		localMeta.SourceEnd,
		localMeta.IngestTSMonotonic,
	)
	if r.orch != nil {
		r.orch.postSealWork(r.vaultID, r.vaultInst.Chunks, id)
	}
	return true
}

// placementMembership returns the expectedFrom set for delete
// proposals: the local node plus every replication target. Mirrored
// from orchestrator.placementMembership which takes an instance as input
// and is wired through r.instance directly here so the reconciler doesn't
// need an orchestrator back-pointer for this.
func (r *VaultLifecycleReconciler) placementMembership() []string {
	expected := make([]string, 0, 1+len(r.vaultInst.FollowerTargets))
	seen := map[string]bool{}
	if r.localNodeID != "" {
		expected = append(expected, r.localNodeID)
		seen[r.localNodeID] = true
	}
	for _, t := range r.vaultInst.FollowerTargets {
		if t.NodeID == "" || seen[t.NodeID] {
			continue
		}
		expected = append(expected, t.NodeID)
		seen[t.NodeID] = true
	}
	return expected
}

// fulfillObligation deletes the local copy of a chunk and then proposes
// CmdAckDelete. Used by onRequestDelete (steady state),
// ReconcileFromSnapshot (catchup after Restore), and
// SweepPendingObligations (periodic local sweep). source is a short
// label that distinguishes them for log triage.
//
// Force-demotes the chunk first if the local Manager still has it as
// the active pointer (gastrolog-2yeht). The FSM has authoritatively
// scheduled this chunk for deletion via the receipt protocol; the
// local stale active pointer must yield. Without this prelude,
// downstream-instance followers (no continuous record-stream to swap
// active naturally) would have fulfillObligation bouncing off
// ErrActiveChunk on every periodic-sweep tick, blocking finalize
// indefinitely.
func (r *VaultLifecycleReconciler) fulfillObligation(chunkID chunk.ChunkID, reason, source string) {
	if r.vaultInst != nil && r.vaultInst.Chunks != nil {
		if ensurer, ok := r.vaultInst.Chunks.(chunk.SealEnsurer); ok {
			if err := ensurer.EnsureSealed(chunkID); err != nil {
				r.logger.Warn("delete obligation: pre-demote failed",
					"chunk", chunkID, "reason", reason, "source", source, "error", err)
				// Continue to deleteLocalCopy — if the chunk is in fact
				// already sealed, that path will succeed; if not, it'll
				// produce its own diagnostic.
			}
		}
	}
	if err := r.deleteLocalCopy(chunkID); err != nil {
		// Don't ack: the FSM keeps the obligation, and we'll retry on
		// the next observation. Logging at warn lets retry storms show
		// up in operator dashboards.
		r.logger.Warn("delete obligation: local delete failed",
			"chunk", chunkID, "reason", reason, "source", source, "error", err)
		return
	}
	if r.vaultInst == nil || r.vaultInst.ApplyRaftAckDelete == nil {
		// No applier wired — nothing to ack against. Single-node mode
		// uses deleteChunk's local-only fallback and never reaches here.
		return
	}
	if err := r.vaultInst.ApplyRaftAckDelete(chunkID, r.localNodeID); err != nil {
		r.logger.Warn("delete obligation: ack failed",
			"chunk", chunkID, "reason", reason, "source", source, "error", err)
	}
}

// deleteLocalCopy removes a chunk's local on-disk state from this
// node. ErrChunkNotFound is treated as success — the chunk was already
// gone (concurrent OnDelete cascade, or this node never had it).
//
// No same-node sibling fan-out: in the receipt protocol every node
// runs its own per-TI reconciler, so each TI self-cleans via its own
// r.instance.Chunks. The legacy `deleteFromFollowers` walk only made sense
// when a single leader-side expireChunk had to clean up sibling
// follower TIs on the same node; per-TI reconcilers obsolete that, and
// per 1:1:1 placement there are no sibling TIs anyway. Calling
// deleteFromFollowers here would re-visit the same TI we just deleted
// from and log a spurious "chunk not found" warning. See the cluster
// log storm fixed alongside this change.
func (r *VaultLifecycleReconciler) deleteLocalCopy(chunkID chunk.ChunkID) error {
	if r.vaultInst == nil {
		return nil
	}
	if r.vaultInst.Indexes != nil {
		if err := r.vaultInst.Indexes.DeleteIndexes(chunkID); err != nil && !errors.Is(err, chunk.ErrChunkNotFound) {
			return fmt.Errorf("delete indexes: %w", err)
		}
	}
	if r.vaultInst.Chunks != nil {
		if err := chunk.DeleteNoAnnounce(r.vaultInst.Chunks, chunkID); err != nil && !errors.Is(err, chunk.ErrChunkNotFound) {
			return fmt.Errorf("delete chunk: %w", err)
		}
	}
	if r.orch != nil {
		// Carry the DELETED op so subscribers remove the cache entry.
		r.orch.EmitChunkDeleted(r.vaultID, chunkID)
	}
	return nil
}

// ---------- Single deletion entry point ----------

// deleteChunk is the canonical entry point for "delete this chunk
// across the cluster". All eight legacy cleanup paths converge here
// over steps 4-8. reason is a short free-form label that ends up in the
// FSM's pendingDeletes entry and in audit logs:
//
//   "retention-ttl"             retention rule fired
//   "transition-source-expire"  source after destination receipt
//   "manual-delete-rpc"         operator-initiated via CLI/UI
//   "archived-to-glacier"       archival sweep on cloud instance
//   "unreadable"                chunk classified as corrupt
//   "crash-recovery-orphan"     local-only orphan with no FSM entry
//
// expectedFrom is the set of node IDs that must ack before the entry
// finalizes. For cluster-wide deletes, pass placement-membership-at-
// decision-time. For local-only orphan cleanup (no FSM entry to
// reference), pass {localNodeID} so the propagation collapses to
// "this node only".
//
// In single-node / memory mode (no Raft applier wired), deleteChunk
// falls back to a direct local delete without going through the FSM.
//
// Skips proposing CmdRequestDelete when the FSM already has a
// pendingDeletes entry for this chunk. The FSM-side applyRequestDelete
// is idempotent (returns no-op for an existing entry), but each
// redundant proposal still costs a Raft round-trip + apply pump cycle.
// At scale (hundreds of stuck deletes re-evaluated per retention tick)
// this was a major contributor to leader-queue saturation. The
// SweepPendingObligations path retries acks for stalled obligations
// directly from local FSM state without going through this entry
// point, so dedup'ing here is safe.
func (r *VaultLifecycleReconciler) deleteChunk(chunkID chunk.ChunkID, reason string, expectedFrom []string) error {
	if r.vaultInst == nil {
		return errors.New("deleteChunk: nil vaultInst instance")
	}
	if r.vaultInst.ApplyRaftRequestDelete == nil {
		// Single-node fallback: no Raft, no receipt protocol. Just
		// delete locally and notify chunk-change subscribers.
		r.logger.Debug("deleteChunk: single-node fallback",
			"chunk", chunkID, "reason", reason)
		return r.deleteLocalCopy(chunkID)
	}
	if r.fsm != nil && r.fsm.PendingDelete(chunkID) != nil {
		r.logger.Debug("deleteChunk: pendingDelete entry already exists, skipping propose",
			"chunk", chunkID, "reason", reason)
		return nil
	}
	r.logger.Debug("deleteChunk: proposing CmdRequestDelete",
		"chunk", chunkID, "reason", reason, "expected_count", len(expectedFrom))
	return r.vaultInst.ApplyRaftRequestDelete(chunkID, reason, expectedFrom)
}
