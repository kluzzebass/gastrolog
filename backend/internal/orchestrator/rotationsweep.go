package orchestrator

import (
	"context"

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

	o.mu.RLock()
	for vaultID, vault := range o.vaults {
		vaultInst := vault.Instance
		if vaultInst == nil || vaultInst.IsFollower {
			continue
		}
		if vaultCfg := findVaultConfig(cfg.Vaults, vaultID); vaultCfg != nil {
			o.refreshFollowerTargets(sys, *vaultCfg, vaultInst)
		}
	}
	o.mu.RUnlock()

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
