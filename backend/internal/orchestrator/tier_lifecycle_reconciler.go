package orchestrator

// gastrolog-51gme — TierLifecycleReconciler.
//
// One reconciler per TierInstance. Owns chunk-lifecycle execution
// uniformly: every FSM apply event goes through here, and every
// chunk-file deletion in steady state ends here. This file is the
// single home for "what just happened in the FSM, and what should the
// local chunk manager do about it?"
//
// Migration roadmap status:
//   step 4 (retention-ttl via deleteChunk): done.
//   step 5 (drop reconcileTierDiskAgainstManifest / reconcileFollower):
//     done. The receipt protocol's pendingDeletes (preserved across
//     snapshot install + processed by ReconcileFromSnapshot) replaces
//     the legacy disk-vs-manifest catchup sweep.
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
//     plumbing (TierInstance.ApplyRaftDelete, tierRaftCallbacks.applyDelete,
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
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	tierfsm "gastrolog/internal/tier/raftfsm"
)

// TierLifecycleReconciler owns chunk-lifecycle execution for a single
// TierInstance. Created during tier wiring (reconfig_vaults.go), wired
// to the tier's FSM via Wire(), and torn down with the tier instance.
//
// The reconciler is the canonical caller of `chunk.DeleteNoAnnounce`
// and the SilentDeleter shortcut. A forbidigo lint rule (step 9)
// blocks direct calls from anywhere else in the orchestrator package
// outside a small allow-list (vault teardown, replaceForwardedChunk).
type TierLifecycleReconciler struct {
	vaultID     glid.GLID
	tierID      glid.GLID
	tier        *TierInstance
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
}

// NewTierLifecycleReconciler creates a reconciler for a tier instance.
// localNodeID is required so the reconciler can recognize when its own
// node ID appears in a CmdRequestDelete's ExpectedFrom set (and ack)
// or doesn't (and ignore).
//
// orch may be nil in tests that exercise the reconciler in isolation;
// when nil, the same-node sibling cleanup path is skipped and chunk-
// change notifications are dropped.
func NewTierLifecycleReconciler(orch *Orchestrator, vaultID, tierID glid.GLID, tier *TierInstance, localNodeID string, logger *slog.Logger) *TierLifecycleReconciler {
	return &TierLifecycleReconciler{
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
func (r *TierLifecycleReconciler) Wire(fsm *tierfsm.FSM) {
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
func (r *TierLifecycleReconciler) ReconcileFromSnapshot(fsm *tierfsm.FSM) {
	if fsm == nil {
		return
	}

	pending := fsm.PendingDeletes()
	r.logger.Debug("reconcile-from-snapshot: starting",
		"pending_count", len(pending))
	for _, p := range pending {
		if !p.ExpectedFrom[r.localNodeID] {
			continue
		}
		r.fulfillObligation(p.ChunkID, p.Reason, "snapshot-restore")
	}

	r.projectAllSealedFromFSM(fsm)
}

// projectAllSealedFromFSM iterates every entry in the FSM and projects
// the sealed flag onto the local chunk Manager. Used by
// ReconcileFromSnapshot after Restore — at that point the FSM has been
// fully reloaded but the local Manager has only the on-disk flag bits,
// which may have missed CmdSealChunk replays. Idempotent: chunks that
// are already sealed locally, or that don't exist locally, are no-ops.
func (r *TierLifecycleReconciler) projectAllSealedFromFSM(fsm *tierfsm.FSM) {
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
func (r *TierLifecycleReconciler) onSeal(e tierfsm.Entry) {
	r.logger.Debug("onSeal", "chunk", e.ID, "records", e.RecordCount)
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

func (r *TierLifecycleReconciler) onRetentionPending(id chunk.ChunkID) {
	r.logger.Debug("onRetentionPending", "chunk", id)
	// Audit-only. The actual cleanup goes through CmdRequestDelete.
}

func (r *TierLifecycleReconciler) onTransitionStreamed(id chunk.ChunkID) {
	r.logger.Debug("onTransitionStreamed (skeleton no-op)", "chunk", id)
	// Step 6 fills this in: trigger CmdRequestDelete on the source
	// once the destination has acked CmdTransitionReceived.
}

func (r *TierLifecycleReconciler) onTransitionReceived(sourceChunkID chunk.ChunkID) {
	r.logger.Debug("onTransitionReceived (skeleton no-op)", "source_chunk", sourceChunkID)
	// Step 6 uses this on source-tier reconcilers paired with
	// onTransitionStreamed to drive the source delete via the receipt
	// protocol.
}

// onRequestDelete fires on every node when CmdRequestDelete commits.
// Each node in ExpectedFrom owes one ack: delete the local chunk if
// it exists, then propose CmdAckDelete. Idempotent on the FSM side —
// duplicate / unknown-node acks are silently dropped, so a partial
// failure here just means we'll retry on the next ReconcileFromSnapshot
// (or the next time the obligation is re-observed).
func (r *TierLifecycleReconciler) onRequestDelete(p tierfsm.PendingDelete) {
	if !p.ExpectedFrom[r.localNodeID] {
		r.logger.Debug("onRequestDelete: not in expectedFrom",
			"chunk", p.ChunkID, "reason", p.Reason)
		return
	}
	r.fulfillObligation(p.ChunkID, p.Reason, "request-delete")
}

// onAckDelete fires on every node when CmdAckDelete commits. Only the
// vault-ctl Raft leader proposes CmdFinalizeDelete; followers ignore
// the event. Reading the remaining ExpectedFrom set is safe because
// applyAckDelete fires the callback after the set has been mutated
// inside the FSM lock; the FSM read here just sees the post-state.
func (r *TierLifecycleReconciler) onAckDelete(chunkID chunk.ChunkID, ackingNodeID string) {
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
	if err := r.tier.ApplyRaftFinalizeDelete(chunkID); err != nil {
		r.logger.Warn("onAckDelete: finalize failed",
			"chunk", chunkID, "error", err)
	}
}

func (r *TierLifecycleReconciler) onFinalizeDelete(chunkID chunk.ChunkID) {
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
func (r *TierLifecycleReconciler) onPruneNode(prunedNodeID string, finalizable []chunk.ChunkID) {
	r.logger.Debug("onPruneNode",
		"node", prunedNodeID, "finalizable_count", len(finalizable))
	if r.tier == nil || r.tier.IsRaftLeader == nil || !r.tier.IsRaftLeader() {
		return
	}
	if r.tier.ApplyRaftFinalizeDelete == nil {
		return
	}
	for _, id := range finalizable {
		if err := r.tier.ApplyRaftFinalizeDelete(id); err != nil {
			r.logger.Warn("onPruneNode: post-prune finalize failed",
				"chunk", id, "node", prunedNodeID, "error", err)
		}
	}
}

// fulfillObligation deletes the local copy of a chunk and then proposes
// CmdAckDelete. Used by both onRequestDelete (steady state) and
// ReconcileFromSnapshot (catchup after Restore). source is a short
// label that distinguishes the two for log triage.
func (r *TierLifecycleReconciler) fulfillObligation(chunkID chunk.ChunkID, reason, source string) {
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
// gone (concurrent OnDelete cascade, or this node never had it). All
// same-node sibling TIs (rare, only when 1:1:1 placement is violated)
// are cleaned up too, mirroring the legacy deleteFromFollowers fan-out.
func (r *TierLifecycleReconciler) deleteLocalCopy(chunkID chunk.ChunkID) error {
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
		// Same-node sibling cleanup. With 1:1:1 placement there are no
		// siblings, but historically a node could host both a leader
		// and follower TI for the same tier; deleteFromFollowers handles
		// that case symmetrically with wireTierFSMOnDelete.
		r.orch.deleteFromFollowers(r.vaultID, r.tierID, chunkID)
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
func (r *TierLifecycleReconciler) deleteChunk(chunkID chunk.ChunkID, reason string, expectedFrom []string) error {
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
	r.logger.Debug("deleteChunk: proposing CmdRequestDelete",
		"chunk", chunkID, "reason", reason, "expected_count", len(expectedFrom))
	return r.tier.ApplyRaftRequestDelete(chunkID, reason, expectedFrom)
}
