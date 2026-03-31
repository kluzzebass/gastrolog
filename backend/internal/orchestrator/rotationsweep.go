package orchestrator

import (
	"context"

	"gastrolog/internal/chunk"
	"gastrolog/internal/config"

	"github.com/google/uuid"
)

const (
	rotationSweepJobName  = "rotation-sweep"
	rotationSweepSchedule = "*/15 * * * * *" // every 15 seconds
)

// rotationSweep is the single scheduled rotation job. Each tick it:
//  1. Loads current config and applies rotation policies to all primary tiers.
//  2. Reconciles cron rotation jobs (add new, remove stale).
//  3. Checks each primary tier's active chunk for time-based rotation triggers.
//
// This discovery-based approach replaces the per-tier lifecycle management
// (applyTierRotation / reloadTierRotation) — no setup, teardown, or hot-swap.
func (o *Orchestrator) rotationSweep() {
	cfg, err := o.loadConfig(context.Background())
	if err != nil {
		o.logger.Error("rotation sweep: failed to load config", "error", err)
		// Fall through with nil cfg — skip policy/cron reconciliation
		// but still check rotation triggers with whatever policies are set.
	}

	type sealEvent struct {
		vaultID uuid.UUID
		cm      chunk.ChunkManager
		chunkID chunk.ChunkID
	}
	var seals []sealEvent
	activeCronJobs := make(map[string]bool)

	o.mu.RLock()
	for vaultID, vault := range o.vaults {
		// Find vault config for cron reconciliation.
		var vaultCfg *config.VaultConfig
		if cfg != nil {
			for i := range cfg.Vaults {
				if cfg.Vaults[i].ID == vaultID {
					vaultCfg = &cfg.Vaults[i]
					break
				}
			}
		}

		for _, tier := range vault.Tiers {
			if tier.IsFollower {
				tier.Chunks.SetRotationPolicy(chunk.NeverRotatePolicy{})
				continue
			}

			// Apply rotation policy + reconcile cron job + refresh replication targets.
			if cfg != nil && vaultCfg != nil {
				tierCfg := findTierConfig(cfg.Tiers, tier.TierID)
				o.applyRotationFromConfig(cfg, *vaultCfg, tier, tierCfg, activeCronJobs)
			}

			// Check for time-based rotation triggers.
			activeBefore := tier.Chunks.Active()
			if trigger := tier.Chunks.CheckRotation(); trigger != nil {
				o.logger.Info("background rotation triggered",
					"vault", vaultID,
					"name", vault.Name,
					"tier", tier.TierID,
					"trigger", *trigger,
				)
				if activeBefore != nil {
					seals = append(seals, sealEvent{vaultID: vaultID, cm: tier.Chunks, chunkID: activeBefore.ID})
				}
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
		o.reconcileFilters(cfg)
	}

	// Schedule compression + index builds outside the outer lock.
	for _, s := range seals {
		o.postSealWork(s.vaultID, s.cm, s.chunkID)
	}
}

// reconcileFilters recompiles the filter set from config under a write lock.
func (o *Orchestrator) reconcileFilters(cfg *config.Config) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.reloadFiltersFromRoutes(cfg); err != nil {
		o.logger.Warn("rotation sweep: filter reconciliation failed", "error", err)
	}
}

// applyRotationFromConfig resolves the rotation policy for a leader tier
// from the current config and applies it. Also ensures the cron job exists
// if configured. Called each tick by rotationSweep.
func (o *Orchestrator) applyRotationFromConfig(
	cfg *config.Config,
	vaultCfg config.VaultConfig,
	tier *TierInstance,
	tierCfg *config.TierConfig,
	activeCronJobs map[string]bool,
) {
	if tierCfg == nil {
		return
	}
	// Refresh replication targets from current config.
	tier.FollowerTargets = tierCfg.FollowerTargets(cfg.NodeStorageConfigs)

	if tierCfg.RotationPolicyID == nil {
		return
	}

	policyCfg := findRotationPolicy(cfg.RotationPolicies, *tierCfg.RotationPolicyID)
	if policyCfg == nil {
		return
	}

	policy, err := policyCfg.ToRotationPolicy()
	if err != nil {
		o.logger.Warn("rotation sweep: invalid policy",
			"vault", vaultCfg.ID, "tier", tier.TierID, "error", err)
		return
	}
	if policy != nil {
		tier.Chunks.SetRotationPolicy(policy)
	}

	// Ensure cron job exists with the right schedule.
	if policyCfg.Cron != nil && *policyCfg.Cron != "" {
		jobName := cronJobName(vaultCfg.ID, tier.TierID)
		activeCronJobs[jobName] = true
		o.cronRotation.ensure(vaultCfg.ID, tier.TierID, vaultCfg.Name, *policyCfg.Cron, tier.Chunks)
	}
}
