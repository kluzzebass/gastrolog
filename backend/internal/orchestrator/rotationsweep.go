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
//  3. Checks each leader inst's active chunk for time-based rotation triggers.
func (o *Orchestrator) rotationSweep() {
	sys, err := o.loadSystem(context.Background())
	if err != nil {
		o.logger.Error("rotation sweep: failed to load config", "error", err)
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

		inst := vault.Instance
		if inst == nil {
			continue
		}
		if inst.IsFollower {
			inst.Chunks.SetRotationPolicy(chunk.NeverRotatePolicy{})
			continue
		}

		// Apply rotation policy + reconcile cron job + refresh replication targets.
		if cfg != nil && vaultCfg != nil {
			o.applyRotationFromConfig(sys, cfg, *vaultCfg, inst, activeCronJobs)
		}

		// Check for time-based rotation triggers.
		activeBefore := inst.Chunks.Active()
		if trigger := inst.Chunks.CheckRotation(); trigger != nil {
			o.logger.Debug("rotation triggered",
				"vault", vaultID,
				"name", vault.Name,
				"vault", inst.VaultID,
				"trigger", *trigger,
			)
			if activeBefore != nil {
				seals = append(seals, sealEvent{vaultID: vaultID, cm: inst.Chunks, chunkID: activeBefore.ID})
				// Record the rotation event for the per-instance rate
				// alerter. We do this here (under the read lock) so
				// the count reflects every triggered rotation, not
				// only those whose post-seal pipeline is scheduled.
				o.rotationRates.Record(inst.VaultID, o.now())
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
		o.logger.Warn("rotation sweep: routing-table reconciliation failed", "error", err)
	}
}

// applyRotationFromConfig refreshes the leader inst's replication targets
// and rotation policy from the current config. Called each tick by
// rotationSweep. The function proceeds for every leader inst and
// short-circuits per-section based on the data each section needs.
func (o *Orchestrator) applyRotationFromConfig(sys *system.System,
	cfg *system.Config,
	vaultCfg system.VaultConfig,
	inst *VaultInstance,
	activeCronJobs map[string]bool,
) {
	// Refresh replication targets from current system.
	inst.FollowerTargets = system.FollowerTargets(vaultCfg.Placements, sys.Runtime.NodeStorageConfigs)

	if vaultCfg.RotationPolicyID == nil {
		return
	}

	policyCfg := findRotationPolicy(cfg.RotationPolicies, *vaultCfg.RotationPolicyID)
	if policyCfg == nil {
		return
	}

	policy, err := policyCfg.ToRotationPolicy()
	if err != nil {
		o.logger.Warn("rotation sweep: invalid policy",
			"vault", vaultCfg.ID, "error", err)
		return
	}
	if policy != nil {
		inst.Chunks.SetRotationPolicy(policy)
	}

	// Ensure cron job exists with the right schedule.
	if policyCfg.Cron != nil && *policyCfg.Cron != "" {
		jobName := cronJobName(vaultCfg.ID, inst.VaultID)
		activeCronJobs[jobName] = true
		o.cronRotation.ensure(vaultCfg.ID, inst.VaultID, vaultCfg.Name, *policyCfg.Cron, inst.Chunks)
	}
}
