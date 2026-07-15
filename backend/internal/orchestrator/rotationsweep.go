package orchestrator

import (
	"context"
	"slices"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

const (
	placementSweepJobName  = "placement-reconcile"
	placementSweepSchedule = "*/15 * * * * *" // every 15 seconds
)

// placementSweep is the periodic config-reconcile job. Each tick it refreshes
// every leader instance's sealed-chunk replication targets (FollowerTargets)
// from the current placement config and republishes the pipeline routing table.
// Both are safety nets — the FSM dispatch fan-out also reloads on config change
// for immediate effect — so the sweep only repairs missed or racy updates.
//
// Active-chunk rotation is gone: the pipeline's chunking manager owns chunk
// sealing (event-driven thresholds + scheduler-driven cron via reconcileChunkCron),
// so the legacy chunk-manager rotation/cron path no longer runs here.
func (o *Orchestrator) placementSweep() {
	sys, err := o.loadSystem(context.Background())
	if err != nil {
		o.rotationLogger.Error("placement sweep: failed to load config", "error", err)
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
		// Reconcile the instance ROLE first, every tick, for every
		// instance. Roles used to change only on config dispatch; a raced
		// role update during placement flapping left every node believing
		// it was a follower, and with no further config changes the vault
		// sat leaderless for hours — retention, backfill scheduling, and
		// target refreshes all silently stopped. The sweep is the safety
		// net dispatch never had.
		if !o.reconcileInstanceRole(sys, *vaultCfg, vaultInst) {
			leaderless[vaultID] = vaultCfg.Name
		}
		if !vaultInst.IsFollower {
			o.refreshFollowerTargets(sys, *vaultCfg, vaultInst)
		}
	}
	o.mu.RUnlock()

	// A vault whose placements resolve to no leader is beyond the sweep's
	// self-healing — sustained, that is an operator problem (alarm after
	// the delay-on window; see updateLeaderlessAlarms).
	o.updateLeaderlessAlarms(time.Now(), leaderless)

	// Reconcile the routing table from routes (safety net — dispatch also
	// reloads on config changes for immediate effect).
	o.reconcileFilters(sys)
}

// reconcileFilters republishes the pipeline routing table from config under a
// write lock. Name kept for callsite stability across the Phase 5 refactor.
func (o *Orchestrator) reconcileFilters(sys *system.System) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.reloadRoutesFromConfig(sys); err != nil {
		o.rotationLogger.Warn("placement sweep: routing-table reconciliation failed", "error", err)
	}
}

// replicationTargetsEqual compares two ReplicationTarget slices by (NodeID,
// StorageID) pairs. Order-insensitive. Used to detect FollowerTargets changes
// across placementSweep ticks so the audit log only fires when something
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
// the current placement config — the same computation the config dispatcher
// runs on vault-put events, re-checked every sweep tick so a missed or
// raced dispatch cannot leave a vault leaderless (or doubly-led) forever.
// Membership add/remove stays dispatch-owned; this only corrects the role
// of an instance that already exists. Called each tick by placementSweep
// (caller holds o.mu.RLock; role fields are written lock-free by dispatch
// today, and this follows the same convention).
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
	o.rotationLogger.Info("placement sweep: instance role reconciled",
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
// targets from the current placement config. Called each tick by placementSweep
// (caller holds o.mu.RLock). Logs only on change so reconfiguration is auditable
// without per-tick noise.
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
