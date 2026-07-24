package orchestrator

import (
	"context"
	"slices"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

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
		if !o.reconcileInstanceRole(sys, *vaultCfg, vaultInst) {
			leaderless[vaultID] = vaultCfg.Name
		}
		if !vaultInst.IsFollower {
			o.refreshFollowerTargets(sys, *vaultCfg, vaultInst)
		}
	}
	o.mu.RUnlock()

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
	o.mu.RLock()
	if vault := o.vaults[vaultID]; vault != nil && vault.Instance != nil {
		hasInstance = true
		leaderResolved = o.reconcileInstanceRole(sys, *vaultCfg, vault.Instance)
		if !vault.Instance.IsFollower {
			o.refreshFollowerTargets(sys, *vaultCfg, vault.Instance)
		}
	}
	o.mu.RUnlock()

	if hasInstance {
		o.updateLeaderlessAlarm(vaultID, vaultCfg.Name, !leaderResolved)
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
func (o *Orchestrator) reconcileInstanceRole(sys *system.System, vaultCfg system.VaultConfig, vaultInst *VaultInstance) (leaderResolved bool) {
	leaderNodeID := system.LeaderNodeID(vaultCfg.Placements, sys.Runtime.NodeStorageConfigs)
	if leaderNodeID == "" {
		// Placements resolve to no leader (mid-flap partial state, or a
		// storage config transiently missing). Never flip roles on
		// unresolvable state — that is exactly the race that strands
		// vaults leaderless. Sustained, the caller raises the
		// vault-leaderless alarm.
		return false
	}
	followerIDs := system.FollowerNodeIDs(vaultCfg.Placements, sys.Runtime.NodeStorageConfigs)
	isLeader := leaderNodeID == o.localNodeID
	isFollower := slices.Contains(followerIDs, o.localNodeID)
	if !isLeader && !isFollower {
		return true // not in placement: instance lifecycle is dispatch-owned
	}
	if isFollower {
		vaultInst.LeaderNodeID = leaderNodeID
	} else {
		vaultInst.LeaderNodeID = ""
	}
	if vaultInst.IsFollower == isFollower {
		return true
	}
	vaultInst.IsFollower = isFollower
	o.rotationLogger.Info("placement reconcile: instance role reconciled",
		"vault", vaultCfg.ID, "name", vaultCfg.Name, "isFollower", isFollower,
		"leader", leaderNodeID)
	return true
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
// only on change so reconfiguration is auditable without noise.
func (o *Orchestrator) refreshFollowerTargets(sys *system.System, vaultCfg system.VaultConfig, vaultInst *VaultInstance) {
	newTargets := system.FollowerTargets(vaultCfg.Placements, sys.Runtime.NodeStorageConfigs)
	if !replicationTargetsEqual(vaultInst.FollowerTargets, newTargets) {
		o.rotationLogger.Info("FollowerTargets refreshed",
			"vault", vaultCfg.ID,
			"name", vaultCfg.Name,
			"old", replicationTargetNodes(vaultInst.FollowerTargets),
			"new", replicationTargetNodes(newTargets))
	}
	vaultInst.FollowerTargets = newTargets
}
