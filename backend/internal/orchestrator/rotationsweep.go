package orchestrator

import (
	"context"
	"errors"
	"slices"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

const (
	// pipelineConfigReconcileJobName is the operator-visible name for the
	// pipeline-config reconcile safety net. Keep stable across releases.
	pipelineConfigReconcileJobName = "pipeline-config-reconcile"

	// pipelineConfigReconcileSchedule runs every 15 seconds. 6-field cron
	// (with-seconds). This is the ONE leg of the retired placement sweep that
	// stays periodic (gastrolog-29xpy): reloadPipelineFromConfig aligns each
	// vault-ctl group's desired Raft leader with the placement leader and
	// re-registers the pipeline vault as the vault-ctl handle/leadership
	// converges. Those are async Raft outcomes (elections, group readiness)
	// with no config event to trigger from, so they need a periodic pass —
	// exactly like the sibling vault-ctl-membership-reconcile (30s). Role and
	// FollowerTargets refreshes do have config events and are event-driven
	// (ReconcileVaultPlacement / ReconcilePlacements).
	pipelineConfigReconcileSchedule = "*/15 * * * * *"
)

// startPipelineConfigReconcile registers the pipeline-config reconcile safety
// net with the orchestrator's job scheduler. Each tick reloads the routing
// table + pipeline vault registrations from config and re-asserts each
// vault-ctl group's desired leader (reconcileFilters → reloadPipelineFromConfig).
//
// Called from ApplyConfig, i.e. on every config apply, so finding the job
// already registered is the normal case and not a failure. That is expressed
// by testing AddJob's ErrJobExists rather than by a HasJob pre-check: the
// pre-check was a check-then-act race (two applies could both see "absent",
// and only the loser's AddJob error would say so) and it was misleading, since
// AddJob already answers the same question atomically. See gastrolog-69sjlj.
func (o *Orchestrator) startPipelineConfigReconcile() error {
	if err := o.scheduler.AddJob(pipelineConfigReconcileJobName, pipelineConfigReconcileSchedule, o.pipelineConfigReconcile); err != nil {
		if errors.Is(err, ErrJobExists) {
			return nil // registered by an earlier apply; nothing to do
		}
		return err
	}
	o.scheduler.Describe(pipelineConfigReconcileJobName,
		"Pipeline-config reconcile safety net. Runs on every node every 15 seconds: reloads the routing table and pipeline vault registrations from config and re-asserts each vault-ctl Raft group's desired leader (the placement leader). Covers async vault-ctl leadership/handle convergence — an election that lands leadership on a non-home node leaves the chunking planner (home ∧ vault-ctl leader) running nowhere until this pass realigns it — which has no config event to trigger from. The placement role/FollowerTargets/routing-table refreshes are event-driven (see ReconcileVaultPlacement); this is not the retired 15s placement sweep, only its no-config-event leg.")
	return nil
}

// pipelineConfigReconcile is the scheduled task: load config and republish the
// routing table + pipeline registrations + vault-ctl desired leaders.
func (o *Orchestrator) pipelineConfigReconcile() {
	sys, err := o.loadSystem(context.Background())
	if err != nil {
		o.rotationLogger.Error("pipeline-config reconcile: failed to load config", "error", err)
		return
	}
	if sys == nil {
		return
	}
	o.reconcileFilters(sys)
}

// ReconcilePlacements aligns every local vault instance's role and (for
// leaders) sealed-chunk replication targets with the current placement
// config, then republishes the pipeline routing table. This is the
// event-driven successor to the retired 15s placement sweep (gastrolog-29xpy):
// the FSM config dispatcher calls it when a change lands that can move roles
// or targets across many vaults at once but is not scoped to a single vault —
// a node-storage config change (which remaps storage→node for FollowerTargets)
// and post-snapshot config replay (whose entries never fired onApply
// notifications). Single-vault placement/vault changes take the cheaper
// ReconcileVaultPlacement path.
//
// Active-chunk rotation is gone: the pipeline's chunking manager owns chunk
// sealing (event-driven thresholds + scheduler-driven cron via
// reconcileChunkCron), so no chunk-manager rotation runs here.
func (o *Orchestrator) ReconcilePlacements(ctx context.Context) {
	sys, err := o.loadSystem(ctx)
	if err != nil {
		o.rotationLogger.Error("placement reconcile: failed to load config", "error", err)
		return
	}
	if sys == nil {
		return
	}
	cfg := &sys.Config

	leaderless := make(map[glid.GLID]string)
	var moved []*VaultLifecycleReconciler
	o.mu.RLock()
	for vaultID, vault := range o.vaults {
		vaultInst := vault.Instance
		if vaultInst == nil {
			continue
		}
		vaultCfg := findVaultConfig(cfg.Vaults, vaultID)
		if vaultCfg == nil {
			continue
		}
		leaderResolved, roleChanged := o.reconcileInstanceRole(sys, *vaultCfg, vaultInst)
		if !leaderResolved {
			leaderless[vaultID] = vaultCfg.Name
		}
		targetsChanged := false
		if !vaultInst.IsFollower {
			targetsChanged = o.refreshFollowerTargets(sys, *vaultCfg, vaultInst)
		}
		if (roleChanged || targetsChanged) && vaultInst.Reconciler != nil {
			moved = append(moved, vaultInst.Reconciler)
		}
	}
	o.mu.RUnlock()

	o.wakeStalePendingAckReconcile(moved)

	// A vault whose placements resolve to no leader is beyond self-healing —
	// sustained, that is an operator problem (alarm after the catalog's
	// delay-on window; see updateLeaderlessAlarms). Passing the full outcome
	// map lets vaults that re-resolved this pass diff to a Clear.
	o.updateLeaderlessAlarms(leaderless)

	o.reconcileFilters(sys)
}

// ReconcileVaultPlacement aligns ONE local vault instance's role and (for a
// leader) sealed-chunk replication targets with the current placement config.
// The FSM config dispatcher calls it on every single-vault placement change
// (NotifyVaultPlacementsSet, NotifyVaultPut) so role and FollowerTargets move
// the instant placements change — the two edges the retired sweep used to
// close only on its next tick:
//
//   - Role: reconcileInstanceRole refuses to flip on a placement that resolves
//     to no leader (mid-flap partial state), so the event path can no longer
//     strand a vault leaderless the way the unguarded in-dispatch role write
//     once did — the race the sweep was built to heal.
//   - FollowerTargets: only ever refreshed at instance BUILD before this — a
//     leader that kept its role while its follower set changed carried stale
//     targets until the next 15s sweep. Now they refresh on the placement
//     event itself.
func (o *Orchestrator) ReconcileVaultPlacement(ctx context.Context, vaultID glid.GLID) {
	sys, err := o.loadSystem(ctx)
	if err != nil {
		o.rotationLogger.Error("placement reconcile: failed to load config", "vault", vaultID, "error", err)
		return
	}
	if sys == nil {
		return
	}
	vaultCfg := findVaultConfig(sys.Config.Vaults, vaultID)
	if vaultCfg == nil {
		return
	}

	hasInstance := false
	leaderResolved := true
	var moved []*VaultLifecycleReconciler
	o.mu.RLock()
	if vault := o.vaults[vaultID]; vault != nil && vault.Instance != nil {
		hasInstance = true
		roleChanged := false
		leaderResolved, roleChanged = o.reconcileInstanceRole(sys, *vaultCfg, vault.Instance)
		targetsChanged := false
		if !vault.Instance.IsFollower {
			targetsChanged = o.refreshFollowerTargets(sys, *vaultCfg, vault.Instance)
		}
		if (roleChanged || targetsChanged) && vault.Instance.Reconciler != nil {
			moved = append(moved, vault.Instance.Reconciler)
		}
	}
	o.mu.RUnlock()

	o.wakeStalePendingAckReconcile(moved)

	if hasInstance {
		o.updateLeaderlessAlarm(vaultID, vaultCfg.Name, !leaderResolved)
	}
}

// wakeStalePendingAckReconcile fires the reconciler's stale-pending-delete-ack
// category for every instance whose placement membership just moved — a role
// flip, a leader-pointer change, or a FollowerTargets reassignment.
//
// This closes the last periodic-only reconcile category (gastrolog-235dm7).
// gastrolog-3fu9t wired ReconcileMembershipCatchup to onVaultCtlLeadGained,
// which covers every membership edge that comes with a leadership change; it
// does NOT cover a rebalance under a STABLE leader — placements move a follower
// from node A to node B, the leader keeps both its placement role and its
// vault-ctl Raft leadership, and no lead-gained edge fires. The leader's
// pendingDeletes still name A in ExpectedFrom, so those deletes stay stuck
// until the periodic backstop tick notices. The reassignment IS the event; wake
// on it.
//
// Deliberately ONLY the ack category, not the whole ReconcileMembershipCatchup
// set. A placement move directly invalidates ExpectedFrom — that set is
// literally derived from FollowerTargets, so the reassignment is its exact
// upstream edge. The other three categories in that set are not this event's:
// missing-replica catchup for a newly added follower is already dispatcher-owned
// (catchupScheduler / newFollowersForInstance) and, on pipeline vaults, carries
// holder-receipt ack/revoke that must not race a GLCB build that is merely
// mid-flight; stale-leader-FSM and abandoned-transfer are grace-period GCs whose
// edge is elapsed time, not membership. They keep their existing lead-gained
// wake and the periodic backstop.
//
// Only instances that actually moved are woken: replicationTargetsEqual and the
// role diff gate this, so a no-op placement republish (the common case for an
// unrelated vault config edit) costs nothing. The category is internally
// role-gated (leader-only) and idempotent, so a wake on transient state is a
// safe no-op.
//
// Dispatched on auxWg for the same reason as onVaultCtlLeadGained: it proposes
// Raft applies, and the callers run under o.mu (and, via the config dispatcher,
// on the FSM apply path) where a synchronous Apply would deadlock.
func (o *Orchestrator) wakeStalePendingAckReconcile(moved []*VaultLifecycleReconciler) {
	for _, rec := range moved {
		o.auxWg.Go(rec.SweepStalePendingDeleteAcks)
	}
}

// reconcileFilters republishes the pipeline routing table from config under a
// write lock. Name kept for callsite stability across the Phase 5 refactor.
func (o *Orchestrator) reconcileFilters(sys *system.System) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.reloadRoutesFromConfig(sys); err != nil {
		o.rotationLogger.Warn("placement reconcile: routing-table reconciliation failed", "error", err)
	}
}

// replicationTargetsEqual compares two ReplicationTarget slices by (NodeID,
// StorageID) pairs. Order-insensitive. Used to detect FollowerTargets changes
// across placement reconciles so the audit log only fires when something
// actually moved.
func replicationTargetsEqual(a, b []system.ReplicationTarget) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]string, len(a))
	for _, t := range a {
		seen[t.NodeID] = t.StorageID
	}
	for _, t := range b {
		if got, ok := seen[t.NodeID]; !ok || got != t.StorageID {
			return false
		}
	}
	return true
}

// reconcileInstanceRole aligns a local instance's leader/follower role with
// the current placement config. It is the guarded, authoritative role
// computation: it refuses to flip roles when placements resolve to no leader
// (mid-flap partial state), so a raced dispatch cannot leave a vault leaderless
// (or doubly-led). Membership add/remove stays dispatch-owned; this only
// corrects the role of an instance that already exists. Called from
// ReconcileVaultPlacement / ReconcilePlacements on the config event that
// changed placements (caller holds o.mu.RLock; role fields are written
// lock-free, matching the dispatch convention).
//
// Reports changed=true when the instance's placement membership actually moved
// — a role flip, or a follower's leader pointer moving to a different node.
// Callers use it to wake the reconciler's stale-pending-ack reconcile on the
// placement event (wakeStalePendingAckReconcile) instead of leaving it to the
// backstop tick.
func (o *Orchestrator) reconcileInstanceRole(sys *system.System, vaultCfg system.VaultConfig, vaultInst *VaultInstance) (leaderResolved, changed bool) {
	leaderNodeID := system.LeaderNodeID(vaultCfg.Placements, sys.Runtime.NodeStorageConfigs)
	if leaderNodeID == "" {
		// Placements resolve to no leader (mid-flap partial state, or a
		// storage config transiently missing). Never flip roles on
		// unresolvable state — that is exactly the race that strands
		// vaults leaderless. Sustained, the caller raises the
		// vault-leaderless alarm.
		return false, false
	}
	followerIDs := system.FollowerNodeIDs(vaultCfg.Placements, sys.Runtime.NodeStorageConfigs)
	isLeader := leaderNodeID == o.localNodeID
	isFollower := slices.Contains(followerIDs, o.localNodeID)
	if !isLeader && !isFollower {
		return true, false // not in placement: instance lifecycle is dispatch-owned
	}
	prevLeaderNodeID := vaultInst.LeaderNodeID
	if isFollower {
		vaultInst.LeaderNodeID = leaderNodeID
	} else {
		vaultInst.LeaderNodeID = ""
	}
	changed = vaultInst.LeaderNodeID != prevLeaderNodeID
	if vaultInst.IsFollower == isFollower {
		return true, changed
	}
	vaultInst.IsFollower = isFollower
	o.rotationLogger.Info("placement reconcile: instance role reconciled",
		"vault", vaultCfg.ID, "name", vaultCfg.Name, "isFollower", isFollower,
		"leader", leaderNodeID)
	return true, true
}

// replicationTargetNodes returns the NodeIDs of a ReplicationTarget slice for
// log lines.
func replicationTargetNodes(targets []system.ReplicationTarget) []string {
	ids := make([]string, 0, len(targets))
	for _, t := range targets {
		ids = append(ids, t.NodeID)
	}
	return ids
}

// refreshFollowerTargets refreshes a leader instance's sealed-chunk replication
// targets from the current placement config. Called from ReconcileVaultPlacement
// / ReconcilePlacements on the placement event (caller holds o.mu.RLock). Logs
// only on change so reconfiguration is auditable without noise, and reports
// whether the target set moved so the caller can wake the reconciler's
// stale-pending-ack reconcile (wakeStalePendingAckReconcile) — a leader whose
// follower set shrank holds pendingDeletes naming the departed node in
// ExpectedFrom, which nothing else unsticks under a stable leader.
func (o *Orchestrator) refreshFollowerTargets(sys *system.System, vaultCfg system.VaultConfig, vaultInst *VaultInstance) (changed bool) {
	newTargets := system.FollowerTargets(vaultCfg.Placements, sys.Runtime.NodeStorageConfigs)
	if !replicationTargetsEqual(vaultInst.FollowerTargets, newTargets) {
		changed = true
		o.rotationLogger.Info("FollowerTargets refreshed",
			"vault", vaultCfg.ID,
			"name", vaultCfg.Name,
			"old", replicationTargetNodes(vaultInst.FollowerTargets),
			"new", replicationTargetNodes(newTargets))
	}
	vaultInst.FollowerTargets = newTargets
	return changed
}
