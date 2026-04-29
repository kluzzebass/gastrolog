package orchestrator

// gastrolog-51gme step 3 — TierLifecycleReconciler skeleton.
//
// One reconciler per TierInstance. Owns chunk-lifecycle execution
// uniformly: every FSM apply event goes through here, and every
// chunk-file deletion in steady state ends here. This file is the
// single home for "what just happened in the FSM, and what should the
// local chunk manager do about it?"
//
// At step 3 the reconciler is wired but does not yet do work. Each
// callback handler is a method that logs and returns. The behavior
// migrations come in later steps:
//
//   step 4: expireChunk → reconciler.deleteChunk via CmdRequestDelete
//   step 5: delete reconcileTierDiskAgainstManifest
//   step 6: delete maxTransitionStreamedStaleness; archival sweep
//   step 7: RPC delete migration
//   step 8: delete the manager.go startup auto-seal heuristic
//
// The skeleton lands first so subsequent steps just shift bodies into
// existing methods rather than introducing infrastructure piecemeal.

import (
	"log/slog"
	"sync"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/tier/raftfsm"
)

// TierLifecycleReconciler owns chunk-lifecycle execution for a single
// TierInstance. Created during tier wiring (reconfig_vaults.go), wired
// to the tier's FSM via Wire(), and torn down with the tier instance.
//
// The reconciler is the only intended caller of `chunk.DeleteNoAnnounce`
// and `Manager.Delete` once the migration completes. A linter rule
// forbidding direct calls outside this file lands in step 9.
type TierLifecycleReconciler struct {
	vaultID  glid.GLID
	tierID   glid.GLID
	tier     *TierInstance
	localNodeID string
	logger   *slog.Logger

	mu sync.Mutex
}

// NewTierLifecycleReconciler creates a reconciler for a tier instance.
// localNodeID is required so the reconciler can recognize when its own
// node ID appears in a CmdRequestDelete's expectedFrom set (and ack)
// or doesn't (and ignore).
func NewTierLifecycleReconciler(vaultID, tierID glid.GLID, tier *TierInstance, localNodeID string, logger *slog.Logger) *TierLifecycleReconciler {
	return &TierLifecycleReconciler{
		vaultID:     vaultID,
		tierID:      tierID,
		tier:        tier,
		localNodeID: localNodeID,
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
func (r *TierLifecycleReconciler) Wire(fsm *raftfsm.FSM) {
	if fsm == nil {
		return
	}
	fsm.SetOnSeal(r.onSeal)
	fsm.SetOnRetentionPending(r.onRetentionPending)
	fsm.SetOnTransitionStreamed(r.onTransitionStreamed)
	fsm.SetOnTransitionReceived(r.onTransitionReceived)
	fsm.SetOnRequestDelete(r.onRequestDelete)
	fsm.SetOnAckDelete(r.onAckDelete)
	fsm.SetOnFinalizeDelete(r.onFinalizeDelete)
	// Note: onDelete and onUpload remain wired by their existing call
	// sites (file/manager.go). Migrating those into the reconciler
	// happens during steps 4-7 alongside the path-by-path deletions.
}

// ReconcileFromSnapshot runs once after the FSM has been Restore'd from
// a snapshot. Walks the FSM's pendingDeletes and processes any
// obligations this node owes — same code path as the steady-state
// onRequestDelete handler. This is the structural fix for the catchup
// boundary that defeated the old single-shot CmdDeleteChunk path.
//
// At step 3 this is a no-op stub. Step 4 fills in the body alongside
// the expireChunk migration so the same code path serves both
// steady-state and restore-time obligations.
func (r *TierLifecycleReconciler) ReconcileFromSnapshot(fsm *raftfsm.FSM) {
	if fsm == nil {
		return
	}
	pending := fsm.PendingDeletes()
	r.logger.Debug("reconcile-from-snapshot: starting", "pending_count", len(pending))
	for _, p := range pending {
		if !p.ExpectedFrom[r.localNodeID] {
			continue
		}
		// Step 4 will replace this stub with: handle local side
		// (delete file if present, no-op if absent), then propose
		// CmdAckDelete via the tier's vault-ctl Raft applier.
		r.logger.Debug("reconcile-from-snapshot: obligation owed (skeleton no-op)",
			"chunk", p.ChunkID, "reason", p.Reason)
	}
}

// ---------- FSM apply event handlers ----------
//
// All seven handlers run outside the FSM mutex (see Wire()). They take
// the reconciler's own mu when they need to serialize writes against
// each other or against ReconcileFromSnapshot, but never hold it across
// a Raft Apply or a chunk-manager I/O call to avoid the lock-inversion
// trap.
//
// Steps 4-8 fill in the bodies. At step 3 each is a logged no-op so
// the wiring can be exercised end-to-end before the migrations begin.

func (r *TierLifecycleReconciler) onSeal(e raftfsm.Entry) {
	r.logger.Debug("onSeal (skeleton no-op)", "chunk", e.ID, "records", e.RecordCount)
	// Step 8 fills this in: project FSM-side seal into the local
	// chunk manager's chunkMeta. Closes gastrolog-uccg6 structurally.
}

func (r *TierLifecycleReconciler) onRetentionPending(id chunk.ChunkID) {
	r.logger.Debug("onRetentionPending (skeleton no-op)", "chunk", id)
	// Step 4 may use this for log/audit; the actual cleanup goes
	// through CmdRequestDelete, not through retention-pending.
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

func (r *TierLifecycleReconciler) onRequestDelete(p raftfsm.PendingDelete) {
	r.logger.Debug("onRequestDelete (skeleton no-op)",
		"chunk", p.ChunkID, "reason", p.Reason, "expected_count", len(p.ExpectedFrom))
	// Step 4 fills this in: if r.localNodeID is in p.ExpectedFrom,
	// handle the local side (delete file if present, no-op if absent),
	// then propose CmdAckDelete.
}

func (r *TierLifecycleReconciler) onAckDelete(chunkID chunk.ChunkID, ackingNodeID string) {
	r.logger.Debug("onAckDelete (skeleton no-op)", "chunk", chunkID, "node", ackingNodeID)
	// Step 4 fills this in on the leader: when the FSM's expectedFrom
	// becomes empty for this chunk, propose CmdFinalizeDelete.
}

func (r *TierLifecycleReconciler) onFinalizeDelete(chunkID chunk.ChunkID) {
	r.logger.Debug("onFinalizeDelete (skeleton no-op)", "chunk", chunkID)
	// Step 4 fills this in: optional audit log line per finalized
	// delete. The actual entry-removal already happened in
	// applyFinalizeDelete; this is just the post-apply notification.
}

// ---------- Single deletion entry point (skeleton) ----------

// deleteChunk is the canonical entry point for "delete this chunk
// across the cluster". All eight current cleanup paths converge here
// over steps 4-8. Reason is a short free-form label that ends up in the
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
// At step 3 this is a stub. Step 4 fills in the body: propose
// CmdRequestDelete via the tier's Raft applier.
func (r *TierLifecycleReconciler) deleteChunk(chunkID chunk.ChunkID, reason string, expectedFrom []string) error {
	r.logger.Debug("deleteChunk (skeleton no-op)",
		"chunk", chunkID, "reason", reason, "expected_count", len(expectedFrom))
	return nil
}
