package orchestrator

import (
	"context"
	"gastrolog/internal/glid"

	"gastrolog/internal/chunk"
	"gastrolog/internal/system"
)

const (
	rotationSweepJobName  = "rotation-sweep"
	rotationSweepSchedule = "*/15 * * * * *" // every 15 seconds
)

// rotationSweep is the single scheduled rotation job. Each tick it:
//  1. Loads current config and applies rotation policies to all leader vaults.
//  2. Reconciles cron rotation jobs (add new, remove stale).
//  3. Checks each leader instance's active chunk for time-based rotation triggers.
func (o *Orchestrator) rotationSweep() {
	sys, err := o.loadSystem(context.Background())
	if err != nil {
		o.rotationLogger.Error("rotation sweep: failed to load config", "error", err)
		// Fall through with nil sys — skip policy/cron reconciliation
		// but still check rotation triggers with whatever policies are set.
	}
	var cfg *system.Config
	if sys != nil {
		cfg = &sys.Config
	}

	type sealEvent struct {
		vaultID glid.GLID
		cm      chunk.ChunkManager
		chunkID chunk.ChunkID
	}
	var seals []sealEvent
	activeCronJobs := make(map[string]bool)

	o.mu.RLock()
	for vaultID, vault := range o.vaults {
		var vaultCfg *system.VaultConfig
		if cfg != nil {
			vaultCfg = findVaultConfig(cfg.Vaults, vaultID)
		}

		vaultInst := vault.Instance
		if vaultInst == nil {
			continue
		}
		// gastrolog-2hjfm: every Receiver rotates locally via the
		// FSM-mediated coordinator (gastrolog-3yre7). The legacy
		// "only the placement leader rotates" gate (NeverRotatePolicy
		// on followers) is gone.

		// Apply rotation policy + reconcile cron job + refresh replication targets.
		if cfg != nil && vaultCfg != nil {
			o.applyRotationFromConfig(sys, cfg, *vaultCfg, vaultInst, activeCronJobs)
		}

		// Check for time-based rotation triggers.
		activeBefore := vaultInst.Chunks.Active()
		if trigger := vaultInst.Chunks.CheckRotation(); trigger != nil {
			o.rotationLogger.Debug("rotation triggered",
				"vault", vaultID,
				"name", vault.Name,
				"vault", vaultInst.VaultID,
				"trigger", *trigger,
			)
			if activeBefore != nil {
				seals = append(seals, sealEvent{vaultID: vaultID, cm: vaultInst.Chunks, chunkID: activeBefore.ID})
				// Record the rotation event for the per-instance rate
				// alerter. We do this here (under the read lock) so
				// the count reflects every triggered rotation, not
				// only those whose post-seal pipeline is scheduled.
				o.rotationRates.Record(vaultInst.VaultID, o.now())
			}
		}
	}
	o.mu.RUnlock()

	// Prune cron jobs for vaults that no longer need them.
	if cfg != nil {
		o.cronRotation.pruneExcept(activeCronJobs)
	}

	// Reconcile filters from routes (safety net — dispatch also reloads
	// on config changes for immediate effect).
	if cfg != nil {
		o.reconcileFilters(sys)
	}

	// Schedule compression + index builds outside the outer lock.
	for _, s := range seals {
		o.postSealWork(s.vaultID, s.cm, s.chunkID)
	}
}

// reconcileFilters recompiles the routing table from config under a
// write lock. Name kept for callsite stability across the Phase 5
// refactor — under the hood it now rebuilds a RouteSet rather than a
// per-vault FilterSet.
func (o *Orchestrator) reconcileFilters(sys *system.System) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.reloadRoutesFromConfig(sys); err != nil {
		o.rotationLogger.Warn("rotation sweep: routing-table reconciliation failed", "error", err)
	}
}

// replicationTargetsEqual compares two ReplicationTarget slices by (NodeID,
// StorageID) pairs. Order-insensitive. Used to detect PeerPlacementTargets changes
// across rotationSweep ticks so the audit log only fires when something
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

// applyRotationFromConfig refreshes the leader instance's replication targets
// and rotation policy from the current config. Called each tick by
// rotationSweep. The function proceeds for every leader instance and
// short-circuits per-section based on the data each section needs.
func (o *Orchestrator) applyRotationFromConfig(sys *system.System,
	cfg *system.Config,
	vaultCfg system.VaultConfig,
	vaultInst *VaultInstance,
	activeCronJobs map[string]bool,
) {
	// Refresh replication targets from current system.
	newTargets := system.PeerPlacementTargets(vaultCfg.Placements, sys.Runtime.NodeStorageConfigs)
	// Log only on change so reconfiguration is auditable without per-tick noise.
	if !replicationTargetsEqual(vaultInst.PeerPlacementTargets, newTargets) {
		o.rotationLogger.Info("PeerPlacementTargets refreshed",
			"vault", vaultCfg.ID,
			"name", vaultCfg.Name,
			"old", replicationTargetNodes(vaultInst.PeerPlacementTargets),
			"new", replicationTargetNodes(newTargets))
	}
	vaultInst.PeerPlacementTargets = newTargets

	// Refresh the fan-out Receiving snapshots from current placements +
	// NSCs. Both Manager.fanOutReceiving (deferred AnnounceCreateWithReceiving
	// path) and rotationCoordinator.c.receiving (CmdCreateChunk Raft
	// payload) are otherwise pinned at instance-build time, so an NSC
	// that hadn't replicated yet at build would pin every new chunk's
	// FSM placement.Holding to an incomplete node set forever. See
	// gastrolog-2oav7.
	applyFanOutConfig(vaultInst.Chunks, vaultCfg, vaultCfg.Placements, sys.Runtime.NodeStorageConfigs)
	if vaultInst.RotationCoordinator != nil {
		vaultInst.RotationCoordinator.SetReceiving(
			system.PlacementNodeIDs(vaultCfg.Placements, sys.Runtime.NodeStorageConfigs))
	}

	if vaultCfg.RotationPolicyID == nil {
		return
	}

	policyCfg := findRotationPolicy(cfg.RotationPolicies, *vaultCfg.RotationPolicyID)
	if policyCfg == nil {
		return
	}

	policy, err := policyCfg.ToRotationPolicy()
	if err != nil {
		o.rotationLogger.Warn("rotation sweep: invalid policy",
			"vault", vaultCfg.ID, "error", err)
		return
	}
	if policy != nil {
		vaultInst.Chunks.SetRotationPolicy(policy)
	}

	// Ensure cron job exists with the right schedule.
	if policyCfg.Cron != nil && *policyCfg.Cron != "" {
		jobName := cronJobName(vaultCfg.ID)
		activeCronJobs[jobName] = true
		o.cronRotation.ensure(vaultCfg.ID, vaultCfg.Name, *policyCfg.Cron, vaultInst.Chunks)
	}
}
