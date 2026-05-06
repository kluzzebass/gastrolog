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
//   step 5 (drop reconcileTierDiskAgainstManifest / reconcileFollower):
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
//   step 7 (manual-delete RPC via deleteChunkFromTierInstance):
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
//     fans CmdPruneNode out across the vault's tier sub-FSMs after a
//     successful RemoveServer call; the reconciler's onPruneNode handler
//     (leader-only) proposes CmdFinalizeDelete for each finalizable
//     chunk so deletes don't pin pendingDeletes forever.
//   step 11 (deprecate CmdDeleteChunk): done. The dead production
//     plumbing (VaultInstance.ApplyRaftDelete, tierRaftCallbacks.applyDelete,
//     buildTierRaftCallbacks's MarshalDeleteChunk producer,
//     retentionRunner.applyRaftDelete + clusterMode branch) was removed.
//     CmdDeleteChunk + applyDeleteChunk + MarshalDeleteChunk stay in the
//     FSM for WAL replay backward-compat, but a forbidigo rule blocks
//     new MarshalDeleteChunk callers. The wireTierFSMOnDelete callback
//     and the legacy forwardDeletionToFollowers RPC chain stay too —
//     they're reachable only from the older cluster harness in
//     transition_test.go, which doesn't go through buildTierInstance and
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

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/tierfsm"
)

// VaultLifecycleReconciler owns chunk-lifecycle execution for a single
// VaultInstance. Created during tier wiring (reconfig_vaults.go), wired
// to the tier's FSM via Wire(), and torn down with the tier instance.
//
// The reconciler is the canonical caller of `chunk.DeleteNoAnnounce`
// and the SilentDeleter shortcut. A forbidigo lint rule (step 9)
// blocks direct calls from anywhere else in the orchestrator package
// outside a small allow-list (vault teardown, replaceForwardedChunk).
type VaultLifecycleReconciler struct {
	vaultID     glid.GLID
	tierID      glid.GLID
	tier        *VaultInstance
	localNodeID string
	logger      *slog.Logger

	// orch is the parent orchestrator, kept so the reconciler can fan
	// local deletes out to same-node sibling TIs (mirrors the legacy
	// deleteFromFollowers path) and bump WatchChunks subscribers.
	orch *Orchestrator

	// fsm is the tier sub-FSM this reconciler is bound to. Stored on
	// Wire() so onAckDelete can read remaining ExpectedFrom without
	// having to re-resolve the FSM through the Raft group.
	fsm *tierfsm.FSM

	mu sync.Mutex

	// sweepInFlight guards against stacking up SweepPendingObligations
	// goroutines when the leader's apply queue is slow. Atomically set
	// to 1 when a sweep starts, 0 when it finishes. Subsequent ticks
	// observe the bit and skip — better to lose a tick than pile up
	// concurrent goroutines fighting for the same Apply queue.
	sweepInFlight atomic.Int32
}

// NewTierLifecycleReconciler creates a reconciler for a tier instance.
// localNodeID is required so the reconciler can recognize when its own
// node ID appears in a CmdRequestDelete's ExpectedFrom set (and ack)
// or doesn't (and ignore).
//
// orch may be nil in tests that exercise the reconciler in isolation;
// when nil, the same-node sibling cleanup path is skipped and chunk-
// change notifications are dropped.
func NewTierLifecycleReconciler(orch *Orchestrator, vaultID, tierID glid.GLID, tier *VaultInstance, localNodeID string, logger *slog.Logger) *VaultLifecycleReconciler {
	return &VaultLifecycleReconciler{
		vaultID:     vaultID,
		tierID:      tierID,
		tier:        tier,
		localNodeID: localNodeID,
		orch:        orch,
		logger:      logger.With("component", "tier-lifecycle-reconciler", "vault", vaultID, "tier", tierID),
	}
}

// Wire installs the reconciler's callbacks on the given tier FSM. Must
// be called once after the FSM is constructed. Idempotent — repeat
// calls just rebind the callback bindings.
//
// Each callback fires outside the FSM lock, so handlers can call back
// into the chunk manager / Raft applier without risking the
// FSM-mutex / orchestrator-mutex inversion that's been a recurring
// problem (see gastrolog-5oofa, gastrolog-1s3mf).
func (r *VaultLifecycleReconciler) Wire(fsm *tierfsm.FSM) {
	if fsm == nil {
		return
	}
	r.fsm = fsm
	fsm.SetOnSeal(r.onSeal)
	fsm.SetOnRetentionPending(r.onRetentionPending)
	fsm.SetOnTransitionStreamed(r.onTransitionStreamed)
	fsm.SetOnTransitionReceived(r.onTransitionReceived)
	fsm.SetOnRequestDelete(r.onRequestDelete)
	fsm.SetOnAckDelete(r.onAckDelete)
	fsm.SetOnFinalizeDelete(r.onFinalizeDelete)
	fsm.SetOnPruneNode(r.onPruneNode)
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
func (r *VaultLifecycleReconciler) ReconcileFromSnapshot(fsm *tierfsm.FSM) {
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
func (r *VaultLifecycleReconciler) projectAllSealedFromFSM(fsm *tierfsm.FSM) {
	if r.tier == nil || r.tier.Chunks == nil {
		return
	}
	ensurer, ok := r.tier.Chunks.(chunk.SealEnsurer)
	if !ok {
		return
	}
	for _, e := range fsm.List() {
		if !e.Sealed {
			continue
		}
		if err := ensurer.EnsureSealed(e.ID); err != nil {
			r.logger.Warn("reconcile-from-snapshot: EnsureSealed failed",
				"chunk", e.ID, "error", err)
		}
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
func (r *VaultLifecycleReconciler) projectAllCloudBackedFromFSM(fsm *tierfsm.FSM) {
	if r.tier == nil || r.tier.Chunks == nil {
		return
	}
	registrar, ok := r.tier.Chunks.(chunk.CloudChunkRegistrar)
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
func (r *VaultLifecycleReconciler) onSeal(e tierfsm.ManifestEntry) {
	r.logger.Debug("onSeal", "chunk", e.ID, "records", e.RecordCount)
	defer func() {
		if r.orch != nil {
			r.orch.NotifyChunkChange()
		}
	}()
	if r.tier == nil || r.tier.Chunks == nil {
		return
	}
	ensurer, ok := r.tier.Chunks.(chunk.SealEnsurer)
	if !ok {
		return
	}
	if err := ensurer.EnsureSealed(e.ID); err != nil {
		r.logger.Warn("onSeal: EnsureSealed failed",
			"chunk", e.ID, "error", err)
	}
}

func (r *VaultLifecycleReconciler) onRetentionPending(id chunk.ChunkID) {
	r.logger.Debug("onRetentionPending", "chunk", id)
	// Audit-only. The actual cleanup goes through CmdRequestDelete.
}

// onTransitionStreamed fires on the source tier when the
// CmdTransitionStreamed apply commits. The chunk has finished
// streaming to the destination and is now eligible for expiration —
// pending the destination's transition receipt. Trigger an immediate
// confirm-and-expire pass on this chunk instead of waiting for the
// next retention sweep tick.
//
// If the destination's receipt hasn't committed yet (common — the
// receipt apply may still be propagating), confirmStreamedOne no-ops
// and the matching onTransitionReceived callback (or the periodic
// sweep, as a safety net) will retry. See gastrolog-1g6br.
//
// Must run in a goroutine: this is the FSM apply pump, and the path
// proposes CmdRequestDelete via reconciler.deleteChunk → posting on
// the same Raft we are draining would deadlock.
func (r *VaultLifecycleReconciler) onTransitionStreamed(id chunk.ChunkID) {
	r.logger.Debug("onTransitionStreamed", "chunk", id)
	if r.orch == nil {
		return
	}
	go r.orch.tryEventDrivenExpire(r.vaultID, r.tierID, id)
}

// onTransitionReceived fires on the destination tier when the
// CmdTransitionReceived apply commits. The receipt is now visible to
// every node hosting this tier. Notify the source-tier runner in the
// same vault to confirm and expire its copy immediately. Must run in
// a goroutine for the same reason as onTransitionStreamed (apply-pump
// deadlock avoidance). See gastrolog-1g6br.
func (r *VaultLifecycleReconciler) onTransitionReceived(sourceChunkID chunk.ChunkID) {
	r.logger.Debug("onTransitionReceived", "source_chunk", sourceChunkID)
	if r.orch == nil {
		return
	}
	go r.orch.notifyReceiptConfirmed(r.vaultID, sourceChunkID)
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
func (r *VaultLifecycleReconciler) onRequestDelete(p tierfsm.PendingDelete) {
	if !p.ExpectedFrom[r.localNodeID] {
		r.logger.Debug("onRequestDelete: not in expectedFrom",
			"chunk", p.ChunkID, "reason", p.Reason)
		return
	}
	go r.fulfillObligation(p.ChunkID, p.Reason, "request-delete")
}

// onAckDelete fires on every node when CmdAckDelete commits. Only the
// vault-ctl Raft leader proposes CmdFinalizeDelete; followers ignore
// the event. Reading the remaining ExpectedFrom set is safe because
// applyAckDelete fires the callback after the set has been mutated
// inside the FSM lock; the FSM read here just sees the post-state.
//
// The finalize Apply MUST happen in a goroutine — same reason as
// onRequestDelete's ack: posting CmdFinalizeDelete on the leader from
// inside the FSM apply pump would deadlock waiting for our own queued
// command to apply.
func (r *VaultLifecycleReconciler) onAckDelete(chunkID chunk.ChunkID, ackingNodeID string) {
	r.logger.Debug("onAckDelete", "chunk", chunkID, "node", ackingNodeID)
	if r.tier == nil || r.tier.IsRaftLeader == nil || !r.tier.IsRaftLeader() {
		return
	}
	if r.fsm == nil || r.tier.ApplyRaftFinalizeDelete == nil {
		return
	}
	p := r.fsm.PendingDelete(chunkID)
	if p == nil || len(p.ExpectedFrom) > 0 {
		return // still owed acks, or already finalized
	}
	go func() {
		if err := r.tier.ApplyRaftFinalizeDelete(chunkID); err != nil {
			r.logger.Warn("onAckDelete: finalize failed",
				"chunk", chunkID, "error", err)
		}
	}()
}

func (r *VaultLifecycleReconciler) onFinalizeDelete(chunkID chunk.ChunkID) {
	r.logger.Debug("onFinalizeDelete", "chunk", chunkID)
	// Audit-only. The pending entry was removed inside applyFinalizeDelete
	// before this callback fired.
}

// onPruneNode fires on every node when CmdPruneNode commits. Only the
// vault-ctl Raft leader proposes CmdFinalizeDelete for the chunkIDs
// whose ExpectedFrom became empty as a result of the prune. Followers
// observe the event but take no action — finalization is leader-only,
// matching onAckDelete.
//
// Without the post-prune finalize, deletes proposed before the
// decommissioned node left would stay in pendingDeletes forever: the
// FSM had already removed the node from ExpectedFrom (the prune did
// that synchronously), so onAckDelete never re-fires for those chunks.
// See gastrolog-51gme step 10.
func (r *VaultLifecycleReconciler) onPruneNode(prunedNodeID string, finalizable []chunk.ChunkID) {
	r.logger.Debug("onPruneNode",
		"node", prunedNodeID, "finalizable_count", len(finalizable))
	if r.tier == nil || r.tier.IsRaftLeader == nil || !r.tier.IsRaftLeader() {
		return
	}
	if r.tier.ApplyRaftFinalizeDelete == nil || len(finalizable) == 0 {
		return
	}
	// Finalize Applies MUST run off the FSM apply pump — same reason as
	// onAckDelete's goroutine. Snapshot the slice so the goroutine doesn't
	// race with a future re-use of the underlying array.
	ids := append([]chunk.ChunkID(nil), finalizable...)
	go func() {
		for _, id := range ids {
			if err := r.tier.ApplyRaftFinalizeDelete(id); err != nil {
				r.logger.Warn("onPruneNode: post-prune finalize failed",
					"chunk", id, "node", prunedNodeID, "error", err)
			}
		}
	}()
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
	if r.fsm == nil || r.tier == nil || r.tier.Chunks == nil {
		return
	}
	metas, err := r.tier.Chunks.List()
	if err != nil {
		r.logger.Warn("local-orphan sweep: list chunks failed", "error", err)
		return
	}
	ensurer, _ := r.tier.Chunks.(chunk.SealEnsurer) // optional
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
		//  - Ghost: FSM has no entry AND no tombstone — the receipt protocol
		//    never finalized this chunk, but the FSM also doesn't recognize
		//    it. Sealed long enough ago that a pending Create can't still be
		//    in-flight. The retention sweep otherwise re-transitions these
		//    ghosts every minute and pollutes downstream tiers. See
		//    gastrolog-66b7x.
		tombstoned := r.fsm.IsTombstoned(meta.ID)
		ghost := !tombstoned && meta.Sealed && !meta.WriteEnd.IsZero() &&
			now.Sub(meta.WriteEnd) > ghostAgeThreshold
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

// SweepMissingReplicas walks the FSM's sealed-chunk manifest and asks
// the placement leader to re-push any sealed chunks this node should
// have but doesn't. This is the create-side mirror of SweepLocalOrphans:
// where SweepLocalOrphans cleans up local files the FSM has tombstoned,
// SweepMissingReplicas pulls in local files the FSM expects but the
// disk lacks. Together with SweepPendingObligations these three sweeps
// give a node "every 20s, reconcile my local store against my replicated
// FSM in both directions."
//
// Failure mode this sweep recovers from: leader sealed a chunk while
// this node was offline; replicateToFollower's gRPC push to this node
// failed; no retry queue exists at the leader. Vault-ctl Raft caught
// our FSM up via snapshot install or log replay on rejoin so the
// manifest entry is present, but the actual chunk records aren't on
// disk. SweepMissingReplicas detects the gap and asks the leader to
// re-push via the existing RequestReplicaCatchup RPC.
//
// Only follower TIs perform this sweep. The leader's own local store
// is, by definition, the source of truth — if a chunk is in its FSM
// but not on its disk, the chunk has already been lost and no peer
// catchup will recover it. (That scenario is a leader-side disk
// failure outside this sweep's scope.) Cloud-backed chunks live in
// shared object storage and are skipped: they are not a local-replica
// concern. See gastrolog-2dgvj.
func (r *VaultLifecycleReconciler) SweepMissingReplicas() {
	if r.fsm == nil || r.tier == nil || r.tier.Chunks == nil {
		return
	}
	if !r.tier.IsFollower {
		return // leader's own disk is the source of truth
	}
	if r.tier.LeaderNodeID == "" {
		return // no known placement leader; nowhere to send the request
	}
	if r.orch == nil || r.orch.chunkReplicator == nil {
		return // no transport wired; cluster mode requires a replicator
	}

	// Local index of what's on disk so the diff is O(N+M) not O(N×M).
	localMetas, err := r.tier.Chunks.List()
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
		if !e.Sealed {
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
		"leader", r.tier.LeaderNodeID, "missing", len(missing))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	scheduled, err := r.orch.chunkReplicator.RequestReplicaCatchup(
		ctx, r.tier.LeaderNodeID, r.vaultID, r.tierID, missing, r.localNodeID)
	if err != nil {
		// The next sweep tick will retry. Possible causes: leader changed
		// after we resolved tier.LeaderNodeID, leader is unreachable, peer
		// connection still warming up. None of these are terminal — the
		// FSM diff is local state, so we converge on the next tick.
		r.logger.Warn("missing-replica sweep: request failed",
			"leader", r.tier.LeaderNodeID, "missing", len(missing), "error", err)
		return
	}
	r.logger.Info("missing-replica sweep: leader scheduled pushes",
		"leader", r.tier.LeaderNodeID, "scheduled", scheduled, "requested", len(missing))
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
// non-cloud tier and proposes CmdRequestDelete for any sealed entry
// missing from the leader's local chunk manager AND past the grace
// period. The leader is the source of truth for non-cloud tiers
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
	if r.fsm == nil || r.tier == nil || r.tier.Chunks == nil {
		return
	}
	if r.tier.IsFollower {
		return // followers use SweepMissingReplicas to pull from leader
	}
	if r.tier.ApplyRaftRequestDelete == nil {
		return // single-node / no Raft; no receipt protocol
	}

	localMetas, err := r.tier.Chunks.List()
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
		if !e.Sealed {
			continue
		}
		if e.CloudBacked {
			continue
		}
		if have[e.ID] {
			continue
		}
		// Grace period anchored on WriteEnd (the seal completion time)
		// rather than IngestEnd (record TS, can be much earlier than
		// real-world clock for backfilled streams).
		if !e.WriteEnd.IsZero() && now.Sub(e.WriteEnd) < staleLeaderFSMGracePeriod {
			continue
		}
		// PendingDelete check inside deleteChunk dedups but logs each
		// proposal; check up front to keep the log quiet for the
		// already-in-flight case.
		if r.fsm.PendingDelete(e.ID) != nil {
			continue
		}
		r.logger.Warn("stale-fsm sweep: proposing delete for unrecoverable chunk",
			"chunk", e.ID, "tier", r.tierID, "vault", r.vaultID,
			"write_end", e.WriteEnd, "age", now.Sub(e.WriteEnd))
		if err := r.deleteChunk(e.ID, "stale-fsm-leader-missing", expectedFrom); err != nil {
			r.logger.Warn("stale-fsm sweep: deleteChunk failed",
				"chunk", e.ID, "error", err)
			continue
		}
		stale++
	}
	if stale > 0 {
		r.logger.Info("stale-fsm sweep: deletes proposed",
			"tier", r.tierID, "vault", r.vaultID, "count", stale)
	}
}

// placementMembership returns the expectedFrom set for delete
// proposals: the local node plus every replication target. Mirrored
// from orchestrator.placementMembership which takes a tier as input
// and is wired through r.tier directly here so the reconciler doesn't
// need an orchestrator back-pointer for this.
func (r *VaultLifecycleReconciler) placementMembership() []string {
	expected := make([]string, 0, 1+len(r.tier.FollowerTargets))
	seen := map[string]bool{}
	if r.localNodeID != "" {
		expected = append(expected, r.localNodeID)
		seen[r.localNodeID] = true
	}
	for _, t := range r.tier.FollowerTargets {
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
// downstream-tier followers (no continuous record-stream to swap
// active naturally) would have fulfillObligation bouncing off
// ErrActiveChunk on every periodic-sweep tick, blocking finalize
// indefinitely.
func (r *VaultLifecycleReconciler) fulfillObligation(chunkID chunk.ChunkID, reason, source string) {
	if r.tier != nil && r.tier.Chunks != nil {
		if ensurer, ok := r.tier.Chunks.(chunk.SealEnsurer); ok {
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
	if r.tier == nil || r.tier.ApplyRaftAckDelete == nil {
		// No applier wired — nothing to ack against. Single-node mode
		// uses deleteChunk's local-only fallback and never reaches here.
		return
	}
	if err := r.tier.ApplyRaftAckDelete(chunkID, r.localNodeID); err != nil {
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
// r.tier.Chunks. The legacy `deleteFromFollowers` walk only made sense
// when a single leader-side expireChunk had to clean up sibling
// follower TIs on the same node; per-TI reconcilers obsolete that, and
// per 1:1:1 placement there are no sibling TIs anyway. Calling
// deleteFromFollowers here would re-visit the same TI we just deleted
// from and log a spurious "chunk not found" warning. See the cluster
// log storm fixed alongside this change.
func (r *VaultLifecycleReconciler) deleteLocalCopy(chunkID chunk.ChunkID) error {
	if r.tier == nil {
		return nil
	}
	if r.tier.Indexes != nil {
		if err := r.tier.Indexes.DeleteIndexes(chunkID); err != nil && !errors.Is(err, chunk.ErrChunkNotFound) {
			return fmt.Errorf("delete indexes: %w", err)
		}
	}
	if r.tier.Chunks != nil {
		if err := chunk.DeleteNoAnnounce(r.tier.Chunks, chunkID); err != nil && !errors.Is(err, chunk.ErrChunkNotFound) {
			return fmt.Errorf("delete chunk: %w", err)
		}
	}
	if r.orch != nil {
		// Notify WatchChunks subscribers: a chunk was removed.
		r.orch.NotifyChunkChange()
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
//   "archived-to-glacier"       archival sweep on cloud tier
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
	if r.tier == nil {
		return errors.New("deleteChunk: nil tier instance")
	}
	if r.tier.ApplyRaftRequestDelete == nil {
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
	return r.tier.ApplyRaftRequestDelete(chunkID, reason, expectedFrom)
}
