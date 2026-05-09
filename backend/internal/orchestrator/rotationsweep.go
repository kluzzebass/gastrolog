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
//  1. Loads current config and applies rotation policies to all leader tiers.
//  2. Reconciles cron rotation jobs (add new, remove stale).
//  3. Checks each leader tier's active chunk for time-based rotation triggers.
//
// This discovery-based approach replaces the per-tier lifecycle management
// (applyTierRotation / reloadTierRotation) — no setup, teardown, or hot-swap.
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

		tier := vault.Instance
		if tier == nil {
			continue
		}
		if tier.IsFollower {
			tier.Chunks.SetRotationPolicy(chunk.NeverRotatePolicy{})
			continue
		}

		// Apply rotation policy + reconcile cron job + refresh replication targets.
		if cfg != nil && vaultCfg != nil {
			o.applyRotationFromConfig(sys, cfg, *vaultCfg, tier, activeCronJobs)
		}

		// Check for time-based rotation triggers.
		activeBefore := tier.Chunks.Active()
		if trigger := tier.Chunks.CheckRotation(); trigger != nil {
			o.logger.Debug("rotation triggered",
				"vault", vaultID,
				"name", vault.Name,
				"vault", tier.VaultID,
				"trigger", *trigger,
			)
			if activeBefore != nil {
				seals = append(seals, sealEvent{vaultID: vaultID, cm: tier.Chunks, chunkID: activeBefore.ID})
				// Record the rotation event for the per-instance rate
				// alerter. We do this here (under the read lock) so
				// the count reflects every triggered rotation, not
				// only those whose post-seal pipeline is scheduled.
				o.rotationRates.Record(tier.VaultID, o.now())
			}
		}
	}
	o.mu.RUnlock()

	// Prune cron jobs for tiers that no longer need them.
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

// applyRotationFromConfig refreshes the leader tier's replication targets
// and rotation policy from the current config. Called each tick by
// rotationSweep.
//
// gastrolog-1ex3b: the tierCfg parameter is no longer consulted — Phase 2/3
// collapsed TierConfig into VaultConfig, so cfg.Tiers is empty and the
// previous `if tierCfg == nil { return }` guard prevented FollowerTargets
// from ever refreshing. Result: vaults built before placements arrived
// kept FollowerTargets=[] forever and never replicated chunks to followers
// despite RF=N. The guard is gone; the function now proceeds for every
// leader tier and short-circuits per-section based on the actual data each
// section needs.
func (o *Orchestrator) applyRotationFromConfig(sys *system.System,
	cfg *system.Config,
	vaultCfg system.VaultConfig,
	tier *VaultInstance,
	activeCronJobs map[string]bool,
) {
	// Refresh replication targets from current system. Reads placements
	// from VaultConfig (mirrored from tier placements via the FSM bridge —
	// gastrolog-257l7).
	tier.FollowerTargets = system.FollowerTargets(vaultCfg.Placements, sys.Runtime.NodeStorageConfigs)

	// Rotation policy is mirrored from TierConfig onto VaultConfig at PutTier
	// time. Reading from vaultCfg keeps this code path stable when TierConfig
	// goes away.
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
		tier.Chunks.SetRotationPolicy(policy)
	}

	// Ensure cron job exists with the right schedule.
	if policyCfg.Cron != nil && *policyCfg.Cron != "" {
		jobName := cronJobName(vaultCfg.ID, tier.VaultID)
		activeCronJobs[jobName] = true
		o.cronRotation.ensure(vaultCfg.ID, tier.VaultID, vaultCfg.Name, *policyCfg.Cron, tier.Chunks)
	}
}
