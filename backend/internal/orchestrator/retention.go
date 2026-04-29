package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/system"
)

const (
	defaultRetentionSchedule = "* * * * *" // every minute
	retentionJobName         = "retention"
)

// retentionKey returns a unique map key for a tier instance's retention state.
func retentionKey(tierID glid.GLID, storageID string) string {
	if storageID == "" {
		return tierID.String()
	}
	return tierID.String() + ":" + storageID
}

// retentionRule is a resolved rule: a compiled policy paired with an action.
type retentionRule struct {
	policy        chunk.RetentionPolicy
	action        system.RetentionAction
	ejectRouteIDs []glid.GLID // target route IDs, only for eject
}

// retentionRunner holds per-tier-instance state that persists across sweeps.
// Only leaders get runners — followers react to the tier FSM manifest
// via the ChunkFSM.OnDelete callback.
type retentionRunner struct {
	mu      sync.Mutex
	vaultID glid.GLID
	tierID  glid.GLID
	// Cached for job descriptions so the Jobs inspector can tell sweep
	// sub-jobs (transitions) apart by their vault/tier. Refreshed from
	// config on every sweep via retentionTargetForTier.
	vaultName    string
	tierPosition int
	tierType     string
	cm           chunk.ChunkManager
	im           index.IndexManager
	inflight     map[chunk.ChunkID]bool // chunks currently being processed
	unreadable   map[chunk.ChunkID]bool // chunks that failed to read — skipped until restart
	orch         *Orchestrator          // for eject/transition callbacks

	// applyRaftDelete applies CmdDeleteChunk to vault-ctl Raft before local
	// deletion, so followers learn about it via the manifest. Nil in
	// single-node / memory mode — local delete proceeds without Raft.
	applyRaftDelete             func(id chunk.ChunkID) error
	applyRaftRetentionPending   func(id chunk.ChunkID) error
	applyRaftTransitionStreamed func(id chunk.ChunkID) error

	// reconciler is the tier lifecycle reconciler that owns chunk-lifecycle
	// execution. Step 4 routes retention-ttl deletes through here via
	// CmdRequestDelete; the legacy applyRaftDelete path is preserved for
	// the other cleanup paths until they migrate in steps 5-8. Nil in
	// single-node / memory mode — local delete proceeds without Raft. See
	// gastrolog-51gme.
	reconciler *TierLifecycleReconciler

	// isLeader returns true if this node is the config leader for this tier.
	// Retention (expiry + transitions) only runs on the leader to prevent
	// all nodes from independently transitioning the same chunks.
	isLeader bool

	// followerTargets are the remote nodes that hold replicas of this tier's
	// chunks. Used to forward chunk deletions after retention expires them.
	followerTargets []system.ReplicationTarget

	now    func() time.Time
	logger *slog.Logger
}

type sweepTarget struct {
	runner *retentionRunner
	rules  []retentionRule
}

// retentionSweepAll is the single scheduled retention job. Runs on every node
// that hosts at least one tier instance.
//
// Per-tier role and readiness:
//   - Rule evaluation runs only when the tier is the leader (tier.IsLeader()).
//     Followers skip rule evaluation because rule results must be applied via
//     the vault control-plane Raft, which only the leader writes to.
//
// There is no per-vault Vault.ReadinessErr gate at this level because the
// per-tier IsLeader checks already cover the preconditions for each action.
// See vault_readiness.go for the canonical vault readiness definition used
// by ingest/query entry points.
//
// Disk-vs-manifest orphan cleanup was removed in gastrolog-51gme step 5:
// the receipt protocol's pendingDeletes (committed in vault-ctl Raft and
// preserved across snapshot install via TierLifecycleReconciler.
// ReconcileFromSnapshot) is now the only catchup path. Any node that
// missed a delete observation eventually applies the pending entry from
// the FSM and acks via the canonical onRequestDelete callback chain —
// no out-of-band disk sweep is needed.
func (o *Orchestrator) retentionSweepAll() {
	sys, err := o.loadSystem(context.Background())
	if err != nil {
		o.logger.Error("retention: failed to load config", "error", err)
		return
	}
	if sys == nil {
		return
	}
	cfg := &sys.Config

	var targets []sweepTarget
	active := make(map[string]bool)

	o.mu.Lock()
	for _, vaultCfg := range cfg.Vaults {
		vault := o.vaults[vaultCfg.ID]
		if vault == nil {
			continue
		}
		for _, tier := range vault.Tiers {
			// Only tier leaders evaluate retention rules.
			if !tier.IsLeader() {
				continue
			}
			if t := o.retentionTargetForTier(cfg, vaultCfg, tier, active); t != nil {
				targets = append(targets, *t)
			}
		}
	}
	for key := range o.retention {
		if !active[key] {
			delete(o.retention, key)
		}
	}
	o.mu.Unlock()

	// Leader: evaluate retention rules.
	for _, t := range targets {
		t.runner.sweep(t.rules)
	}

	// Leader: confirm streamed transitions. Chunks marked as
	// transitionStreamed have been streamed to the next tier but not yet
	// deleted — we wait for the destination to have adequate replicas
	// before expiring the source. See gastrolog-4913n.
	for _, t := range targets {
		t.runner.confirmStreamedTransitions(cfg)
	}

	// (Disk vs manifest orphan cleanup was removed in gastrolog-51gme step 5;
	// the receipt protocol's pendingDeletes is the catchup path now.)

	// Memory budget enforcement: transition oldest chunks when over budget.
	o.enforceMemoryBudgets(cfg)

	// Cache eviction: collect evictors under lock, run outside.
	// EvictCache does filesystem I/O — holding the lock would block Raft applies.
	var evictors []chunk.ChunkCacheEvictor
	o.mu.RLock()
	for _, vault := range o.vaults {
		for _, tier := range vault.Tiers {
			if evictor, ok := tier.Chunks.(chunk.ChunkCacheEvictor); ok {
				evictors = append(evictors, evictor)
			}
		}
	}
	o.mu.RUnlock()
	for _, evictor := range evictors {
		evictor.EvictCache()
	}
}

// enforceMemoryBudgets checks memory tiers for budget overruns and transitions
// the oldest sealed chunks to the next tier. Only runs on leaders.
func (o *Orchestrator) enforceMemoryBudgets(cfg *system.Config) {
	if cfg == nil {
		return
	}
	type budgetTarget struct {
		vaultID glid.GLID
		tierID  glid.GLID
		cm      chunk.ChunkManager
		excess  int64
	}

	var targets []budgetTarget
	o.mu.RLock()
	for _, vaultCfg := range cfg.Vaults {
		vault := o.vaults[vaultCfg.ID]
		if vault == nil {
			continue
		}
		for _, tier := range vault.Tiers {
			if !tier.IsLeader() {
				continue
			}
			monitor, ok := tier.Chunks.(chunk.ChunkBudgetMonitor)
			if !ok {
				continue
			}
			if excess := monitor.BudgetExceeded(); excess > 0 {
				targets = append(targets, budgetTarget{
					vaultID: vaultCfg.ID,
					tierID:  tier.TierID,
					cm:      tier.Chunks,
					excess:  excess,
				})
			}
		}
	}
	o.mu.RUnlock()

	for _, t := range targets {
		o.drainExcessChunks(t.vaultID, t.tierID, t.cm, t.excess)
	}
}

// drainExcessChunks transitions the oldest sealed chunks from a memory tier
// until the excess bytes are reclaimed (or no more sealed chunks remain).
func (o *Orchestrator) drainExcessChunks(vaultID, tierID glid.GLID, cm chunk.ChunkManager, excess int64) {
	metas, err := cm.List()
	if err != nil {
		return
	}

	// Sort oldest first (by WriteStart).
	slices.SortFunc(metas, func(a, b chunk.ChunkMeta) int {
		return a.WriteStart.Compare(b.WriteStart)
	})

	// Find the index manager for this tier.
	var im index.IndexManager
	o.mu.RLock()
	if vault := o.vaults[vaultID]; vault != nil {
		for _, tier := range vault.Tiers {
			if tier.TierID == tierID {
				im = tier.Indexes
				break
			}
		}
	}
	o.mu.RUnlock()

	var reclaimed int64
	for _, m := range metas {
		if reclaimed >= excess {
			break
		}
		if !m.Sealed {
			continue
		}

		runner := &retentionRunner{
			isLeader: true,
			vaultID:  vaultID,
			tierID:   tierID,
			cm:       cm,
			im:       im,
			orch:     o,
			now:      o.now,
			logger:   o.logger,
		}
		runner.transitionChunk(m.ID)
		reclaimed += m.Bytes
	}

	if reclaimed > 0 {
		o.logger.Info("memory budget enforcement: transitioned chunks",
			"vault", vaultID, "tier", tierID,
			"excess", excess, "reclaimed", reclaimed)
	}
}

// RetentionPendingChunks returns chunk IDs marked as retention-pending in the
// tier FSM for a vault. Visible to all nodes via Raft replication.
//
// Read-only accessor, callable from any-node. No Vault.ReadinessErr gate —
// observational use only. Decision-making callers should gate on
// Vault.ReadinessErr first.
func (o *Orchestrator) RetentionPendingChunks(vaultID glid.GLID) map[chunk.ChunkID]bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	vault := o.vaults[vaultID]
	if vault == nil {
		return nil
	}
	result := make(map[chunk.ChunkID]bool)
	for _, tier := range vault.Tiers {
		if tier.ListRetentionPending != nil {
			for _, id := range tier.ListRetentionPending() {
				result[id] = true
			}
		}
	}
	return result
}

// TransitionStreamedChunks returns chunk IDs on source tiers where records
// were streamed to the next tier but the local copy is not yet deleted
// (awaiting destination receipt). See gastrolog-4913n.
//
// Read-only accessor, callable from any-node. No Vault.ReadinessErr gate —
// results reflect whatever the local FSM has applied, which is acceptable for
// inspector/observational callers. For any path that makes decisions based
// on this set, gate on Vault.ReadinessErr first.
func (o *Orchestrator) TransitionStreamedChunks(vaultID glid.GLID) map[chunk.ChunkID]bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	vault := o.vaults[vaultID]
	if vault == nil {
		return nil
	}
	result := make(map[chunk.ChunkID]bool)
	for _, tier := range vault.Tiers {
		if tier.ListTransitionStreamed != nil {
			for _, id := range tier.ListTransitionStreamed() {
				result[id] = true
			}
		}
	}
	return result
}

// retentionTargetForTier resolves a single tier instance into a sweep target.
// Returns nil if the tier should be skipped (no rules, no leader, etc.).
func (o *Orchestrator) retentionTargetForTier(cfg *system.Config, vaultCfg system.VaultConfig, tier *TierInstance, active map[string]bool) *sweepTarget {
	if tier.HasRaftLeader != nil && !tier.HasRaftLeader() {
		return nil
	}
	// IsRaftLeader check removed: the tier apply forwarder transparently
	// routes applies to the vault-ctl Raft leader. The config placement leader
	// always runs retention regardless of vault-ctl Raft leadership.
	tierCfg := findTierConfig(cfg.Tiers, tier.TierID)
	if tierCfg == nil || len(tierCfg.RetentionRules) == 0 {
		return nil
	}
	rules, err := resolveRetentionRulesFromTier(cfg, vaultCfg, tierCfg)
	if err != nil {
		o.logger.Warn("retention: failed to resolve rules",
			"vault", vaultCfg.ID, "tier", tier.TierID, "error", err)
		return nil
	}
	if len(rules) == 0 {
		return nil
	}

	key := retentionKey(tier.TierID, tier.StorageID)
	active[key] = true

	runner := o.retention[key]
	if runner == nil {
		runner = &retentionRunner{
			vaultID: vaultCfg.ID,
			tierID:  tier.TierID,
			cm:      tier.Chunks,
			im:      tier.Indexes,
			orch:    o,
			now:     o.now,
			logger:  o.logger,
		}
		o.retention[key] = runner
	}
	runner.cm = tier.Chunks
	runner.im = tier.Indexes
	runner.applyRaftDelete = tier.ApplyRaftDelete
	runner.applyRaftRetentionPending = tier.ApplyRaftRetentionPending
	runner.applyRaftTransitionStreamed = tier.ApplyRaftTransitionStreamed
	runner.reconciler = tier.Reconciler
	runner.isLeader = tier.IsLeader()
	runner.followerTargets = tier.FollowerTargets
	runner.vaultName = vaultCfg.Name
	runner.tierType = string(tierCfg.Type)
	runner.tierPosition = tierPositionInVault(cfg, vaultCfg.ID, tier.TierID)
	return &sweepTarget{runner: runner, rules: rules}
}

// tierPositionInVault returns the 0-based index of tierID in the vault's
// ordered tier list, or -1 if the tier isn't found (shouldn't happen for
// an active sweep target).
func tierPositionInVault(cfg *system.Config, vaultID, tierID glid.GLID) int {
	tierIDs := system.VaultTierIDs(cfg.Tiers, vaultID)
	for i, id := range tierIDs {
		if id == tierID {
			return i
		}
	}
	return -1
}

// (gastrolog-51gme step 5: reconcileFollower and reconcileTierDiskAgainstManifest
// were removed. The disk-vs-manifest sweep was the legacy catchup mechanism for
// nodes that missed an OnDelete callback. The receipt protocol replaces it: every
// delete commits CmdRequestDelete to vault-ctl Raft with a pendingDeletes entry
// keyed by chunk ID + expectedFrom set. The entry survives snapshot install, and
// TierLifecycleReconciler.ReconcileFromSnapshot processes any obligations a
// rejoining node owes — same code path as steady-state onRequestDelete.)

// sweep evaluates retention rules on a leader and applies expire/eject/transition.
func (r *retentionRunner) sweep(rules []retentionRule) {
	// Only the config placement leader runs retention. Raft applies are
	// forwarded transparently to the Raft leader via TierApplyForwarder.
	// Config followers must not independently evaluate and transition chunks —
	// that causes N× duplication.
	if !r.isLeader {
		return
	}

	r.mu.Lock()
	if r.inflight == nil {
		r.inflight = make(map[chunk.ChunkID]bool)
	}
	r.mu.Unlock()

	if len(rules) == 0 {
		return
	}

	metas, err := r.cm.List()
	if err != nil {
		r.logger.Error("retention: failed to list chunks", "vault", r.vaultID, "error", err)
		return
	}

	r.mu.Lock()
	unreadable := r.unreadable
	tier := r.findTierInstance()
	r.mu.Unlock()

	// Build a set of chunks awaiting destination replication confirmation
	// so we don't re-stream them on every sweep. See gastrolog-4913n.
	streamed := make(map[chunk.ChunkID]bool)
	if tier != nil && tier.ListTransitionStreamed != nil {
		for _, id := range tier.ListTransitionStreamed() {
			streamed[id] = true
		}
	}

	var sealed []chunk.ChunkMeta
	for _, meta := range metas {
		if meta.Sealed && !unreadable[meta.ID] && !streamed[meta.ID] {
			sealed = append(sealed, meta)
		}
	}

	if len(sealed) == 0 {
		return
	}

	state := chunk.VaultState{
		Chunks: sealed,
		Now:    r.now(),
	}

	processed := make(map[chunk.ChunkID]bool)

	for _, b := range rules {
		matched := b.policy.Apply(state)
		for _, id := range matched {
			if processed[id] {
				continue
			}
			processed[id] = true
			r.tryRetainChunk(id, b)
		}
	}
}

// tryRetainChunk attempts to apply a retention action to a single chunk.
// Acquires the inflight lock, marks retention-pending via Raft, and
// dispatches to the action handler.
func (r *retentionRunner) tryRetainChunk(id chunk.ChunkID, b retentionRule) {
	r.mu.Lock()
	if r.inflight[id] {
		r.mu.Unlock()
		return
	}
	r.inflight[id] = true
	r.mu.Unlock()

	// Mark as retention-pending in vault-ctl Raft so all nodes see it.
	if r.applyRaftRetentionPending != nil {
		if err := r.applyRaftRetentionPending(id); err != nil {
			r.logger.Error("retention: failed to apply raft retention-pending",
				"vault", r.vaultID, "chunk", id, "error", err)
			r.clearInflight(id)
			return
		}
		// Notify: retention_pending flag changed — inspector shows this.
		if r.orch != nil {
			r.orch.NotifyChunkChange()
		}
	}

	switch b.action {
	case system.RetentionActionExpire:
		defer r.clearInflight(id)
		r.expireChunk(id, "retention-ttl")
	case system.RetentionActionEject:
		defer r.clearInflight(id)
		r.ejectChunk(id, b.ejectRouteIDs)
	case system.RetentionActionTransition:
		// Transitions can be slow (streaming large chunks to slow tiers).
		// Run as a one-shot scheduler job so the sweep isn't blocked.
		// The inflight guard stays set until the job completes — the
		// sweep won't re-schedule the same chunk.
		r.scheduleTransition(id)
	default:
		r.clearInflight(id)
		r.logger.Error("retention: unknown action", "vault", r.vaultID, "action", b.action)
	}
}

// scheduleTransition dispatches a tier transition as a one-shot scheduler job
// so that slow transitions don't block the retention sweep from processing
// other chunks. The inflight guard is cleared when the job completes.
func (r *retentionRunner) scheduleTransition(id chunk.ChunkID) {
	if r.orch == nil {
		// No orchestrator (test mode) — run inline.
		defer r.clearInflight(id)
		r.transitionChunk(id)
		return
	}
	name := fmt.Sprintf("transition:%s:%s:%s", r.vaultID, r.tierID, id)
	if err := r.orch.scheduler.RunOnce(name, func() {
		defer r.clearInflight(id)
		r.transitionChunk(id)
	}); err != nil {
		r.clearInflight(id)
		r.logger.Warn("retention: failed to schedule transition",
			"vault", r.vaultID, "tier", r.tierID, "chunk", id.String(), "error", err)
	}
	r.orch.scheduler.Describe(name, r.transitionJobDescription(id))
}

// transitionJobDescription formats a transition job's description for the
// Jobs inspector, including vault name and tier position/type so concurrent
// transitions from different vaults or tiers are distinguishable.
func (r *retentionRunner) transitionJobDescription(id chunk.ChunkID) string {
	vaultName := r.vaultName
	if vaultName == "" {
		vaultName = r.vaultID.String()
	}
	if r.tierPosition < 0 || r.tierType == "" {
		return fmt.Sprintf("Transition chunk %s in %q to next tier", id, vaultName)
	}
	return fmt.Sprintf("Transition chunk %s in %q tier %d (%s) to next tier",
		id, vaultName, r.tierPosition, r.tierType)
}

// clearInflight removes a chunk from the in-flight set.
func (r *retentionRunner) clearInflight(id chunk.ChunkID) {
	r.mu.Lock()
	delete(r.inflight, id)
	r.mu.Unlock()
}

// markUnreadable flags a chunk as unreadable so retention skips it on future sweeps.
func (r *retentionRunner) markUnreadable(id chunk.ChunkID, reason error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.unreadable == nil {
		r.unreadable = make(map[chunk.ChunkID]bool)
	}
	r.unreadable[id] = true
	if r.orch.alerts != nil {
		r.orch.alerts.Set(
			fmt.Sprintf("chunk-unreadable:%s", id),
			alert.Error, "retention",
			fmt.Sprintf("Chunk %s unreadable: %v", id, reason),
		)
	}
}

// expireChunk routes a chunk deletion through the lifecycle reconciler's
// receipt protocol when cluster Raft is wired, and falls back to a direct
// local delete otherwise. reason ends up in the FSM's pendingDeletes
// entry and in audit logs — see deleteChunk for the canonical reason
// catalog.
//
// Cluster path (gastrolog-51gme step 4):
//   reconciler.deleteChunk → CmdRequestDelete → onRequestDelete fires on
//   every node in expectedFrom (including this leader) and each one
//   deletes its local copy + acks. Once expectedFrom is empty the leader
//   proposes CmdFinalizeDelete and the FSM entry is removed. The
//   reconciler bumps NotifyChunkChange and walks same-node sibling TIs
//   itself, so retention only owns the rate-alert side-effect here.
//
// Single-node path:
//   reconciler.deleteChunk's local-only fallback handles the direct
//   delete (indexes + chunk + sibling TIs + chunk-change notify).
func (r *retentionRunner) expireChunk(id chunk.ChunkID, reason string) {
	if r.reconciler != nil {
		expectedFrom := r.expectedFromForExpire()
		if err := r.reconciler.deleteChunk(id, reason, expectedFrom); err != nil {
			r.logger.Warn("retention: reconciler deleteChunk failed, will retry",
				"vault", r.vaultID, "chunk", id.String(), "reason", reason, "error", err)
			return
		}
		if r.orch != nil && r.orch.retentionRates != nil {
			// Per-tier rate alert (see gastrolog-47qyw). Only counted on
			// the leader path (this function only runs on tier leaders)
			// so the rate reflects active expiration decisions, not
			// follower delete-cascade applications.
			r.orch.retentionRates.Record(r.tierID, r.orch.now())
		}
		r.logger.Debug("retention: requested chunk delete via reconciler",
			"vault", r.vaultID, "chunk", id.String(), "reason", reason)
		return
	}

	// Reconciler-less fallback: legacy direct-delete path.
	//
	// Reached by older test harnesses that build a retentionRunner without
	// going through buildTierInstance (so tier.Reconciler is nil). They wire
	// cross-node deletion via tierReplicator.DeleteChunk RPC fan-out instead
	// of vault-ctl Raft. We preserve that path here so the harness keeps
	// passing while production runs the receipt protocol.
	clusterMode := r.applyRaftDelete != nil
	if clusterMode {
		if err := r.applyRaftDelete(id); err != nil {
			r.logger.Warn("retention: raft delete failed, will retry",
				"vault", r.vaultID, "chunk", id.String(), "error", err)
			return
		}
	}
	if err := r.im.DeleteIndexes(id); err != nil {
		r.logger.Error("retention: failed to delete indexes",
			"vault", r.vaultID, "chunk", id.String(), "error", err)
		return
	}
	if err := r.cm.Delete(id); err != nil {
		if clusterMode && errors.Is(err, chunk.ErrChunkNotFound) {
			r.logger.Debug("retention: chunk already removed by OnDelete cascade",
				"vault", r.vaultID, "chunk", id.String())
		} else {
			r.logger.Error("retention: failed to delete chunk",
				"vault", r.vaultID, "chunk", id.String(), "error", err)
			return
		}
	}
	if r.orch != nil {
		if r.orch.retentionRates != nil {
			r.orch.retentionRates.Record(r.tierID, r.orch.now())
		}
		r.orch.NotifyChunkChange()
		r.orch.deleteFromFollowers(r.vaultID, r.tierID, id)
		r.forwardDeletionToFollowers(id, clusterMode)
	}
	r.logger.Debug("retention: deleted chunk (no-reconciler fallback)",
		"vault", r.vaultID, "chunk", id.String())
}

// expectedFromForExpire returns the placement-membership-at-decision-time
// for a retention-driven delete: the local node (always the leader, since
// retention only runs on leaders) plus every follower target's node ID.
// Duplicate node IDs (same-node follower placements) collapse on the FSM
// side via the map[string]bool encoding in MarshalRequestDelete.
//
// localNodeID is sourced from the reconciler — that is the canonical
// identity for "who fulfills obligations on this node". Sourcing it
// from the orchestrator would couple the retention test path to the
// orchestrator construction; the reconciler is already required for
// this code path so reusing its localNodeID is strictly tighter.
func (r *retentionRunner) expectedFromForExpire() []string {
	if r.reconciler == nil {
		return nil
	}
	localNodeID := r.reconciler.localNodeID
	expected := make([]string, 0, 1+len(r.followerTargets))
	expected = append(expected, localNodeID)
	for _, t := range r.followerTargets {
		if t.NodeID == "" || t.NodeID == localNodeID {
			continue
		}
		expected = append(expected, t.NodeID)
	}
	return expected
}

// (placementMembership lives on Orchestrator in vault_ops.go and serves
// the cluster paths that aren't routed through a retention runner —
// archival sweep, the cloud reconciliation suspect-expiry, etc.)

// forwardDeletionToFollowers sends an explicit delete RPC to each remote
// follower. Used only by the reconciler-less fallback path in expireChunk
// (test harnesses without a vault-ctl Raft group / TierLifecycleReconciler).
// Production runs through the receipt protocol and never reaches here. The
// RPC delete migration in gastrolog-51gme step 7 retires the underlying
// tierReplicator.DeleteChunk API and removes this chain entirely.
func (r *retentionRunner) forwardDeletionToFollowers(id chunk.ChunkID, _ bool) {
	for _, target := range r.followerTargets {
		if target.NodeID == r.orch.localNodeID {
			continue // already handled by deleteFromFollowers
		}
		r.forwardDeleteWithRetry(target.NodeID, id)
	}
}

// forwardDeleteWithRetry sends a chunk-delete RPC to a follower with up to
// 3 retries on transient failures. "chunk not found" means the chunk is
// already gone on the follower — goal achieved, no retry needed.
func (r *retentionRunner) forwardDeleteWithRetry(nodeID string, id chunk.ChunkID) {
	const maxAttempts = 3
	for attempt := range maxAttempts {
		err := r.sendDeleteToFollower(nodeID, id)
		if err == nil {
			return
		}
		if strings.Contains(err.Error(), "chunk not found") {
			r.logger.Debug("retention: chunk already gone on follower",
				"vault", r.vaultID, "chunk", id.String(), "follower", nodeID)
			return
		}
		if attempt < maxAttempts-1 {
			time.Sleep(time.Duration(attempt+1) * 50 * time.Millisecond)
			continue
		}
		r.logger.Warn("retention: failed to forward chunk deletion to follower",
			"vault", r.vaultID, "chunk", id.String(),
			"follower", nodeID, "error", err, "attempts", maxAttempts)
	}
}

// sendDeleteToFollower issues a single chunk-delete RPC via the tier
// replicator. Returns nil when no replicator is configured (single-node mode).
func (r *retentionRunner) sendDeleteToFollower(followerID string, id chunk.ChunkID) error {
	if r.orch.tierReplicator == nil {
		return nil
	}
	return r.orch.tierReplicator.DeleteChunk(
		context.Background(), followerID, r.vaultID, r.tierID, id,
	)
}
