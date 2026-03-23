package orchestrator

import (
	"context"
	"fmt"

	"gastrolog/internal/config"

	"github.com/google/uuid"
)

// findRotationPolicy finds a RotationPolicyConfig by ID in a slice.
func findRotationPolicy(policies []config.RotationPolicyConfig, id uuid.UUID) *config.RotationPolicyConfig {
	for i := range policies {
		if policies[i].ID == id {
			return &policies[i]
		}
	}
	return nil
}

// findRetentionPolicy finds a RetentionPolicyConfig by ID in a slice.
func findRetentionPolicy(policies []config.RetentionPolicyConfig, id uuid.UUID) *config.RetentionPolicyConfig {
	for i := range policies {
		if policies[i].ID == id {
			return &policies[i]
		}
	}
	return nil
}

// ReloadRotationPolicies loads the full config and resolves rotation policy references
// for all registered vaults, hot-swapping their policies. This is called when a
// rotation policy is created, updated, or deleted.
//
// Vaults that don't reference any policy are left unchanged.
// Vaults referencing a policy that no longer exists get a nil policy (type default).
func (o *Orchestrator) ReloadRotationPolicies(ctx context.Context) error {
	cfg, err := o.loadConfig(ctx)
	if err != nil {
		return fmt.Errorf("load config for rotation policy reload: %w", err)
	}
	if cfg == nil {
		return nil
	}

	o.mu.RLock()
	defer o.mu.RUnlock()

	for _, vaultCfg := range cfg.Vaults {
		vault, ok := o.vaults[vaultCfg.ID]
		if !ok {
			continue // Vault not registered in orchestrator.
		}
		cm := vault.ChunkManager()

		// Resolve rotation policy from the first tier's config.
		if len(vaultCfg.TierIDs) == 0 {
			continue
		}
		tierCfg := findTierConfig(cfg.Tiers, vaultCfg.TierIDs[0])
		if tierCfg == nil || tierCfg.RotationPolicyID == nil {
			continue
		}

		policyCfg := findRotationPolicy(cfg.RotationPolicies, *tierCfg.RotationPolicyID)
		if policyCfg == nil {
			// Policy was deleted — nothing to do; vault keeps its current policy.
			o.logger.Warn("tier references unknown policy", "vault", vaultCfg.ID, "name", vaultCfg.Name, "policy", *tierCfg.RotationPolicyID)
			continue
		}

		policy, err := policyCfg.ToRotationPolicy()
		if err != nil {
			return fmt.Errorf("invalid policy %s for vault %s: %w", *tierCfg.RotationPolicyID, vaultCfg.ID, err)
		}
		if policy != nil {
			vault.ChunkManager().SetRotationPolicy(policy)
			o.logger.Info("vault rotation policy updated", "vault", vaultCfg.ID, "name", vaultCfg.Name, "policy", *tierCfg.RotationPolicyID)
		}

		// Update cron rotation job.
		hasCronJob := o.cronRotation.hasJob(vaultCfg.ID)
		hasCronConfig := policyCfg.Cron != nil && *policyCfg.Cron != ""

		switch {
		case hasCronConfig && hasCronJob:
			// Schedule may have changed — update.
			if err := o.cronRotation.updateJob(vaultCfg.ID, vaultCfg.Name, *policyCfg.Cron, cm); err != nil {
				o.logger.Error("failed to update cron rotation", "vault", vaultCfg.ID, "error", err)
			}
		case hasCronConfig && !hasCronJob:
			// New cron schedule — add.
			if err := o.cronRotation.addJob(vaultCfg.ID, vaultCfg.Name, *policyCfg.Cron, cm); err != nil {
				o.logger.Error("failed to add cron rotation", "vault", vaultCfg.ID, "error", err)
			}
		case !hasCronConfig && hasCronJob:
			// Cron removed — stop.
			o.cronRotation.removeJob(vaultCfg.ID)
		}
	}

	return nil
}

// ReloadRetentionPolicies loads the full config and resolves retention rules
// for all registered vaults. This is called when a retention policy is
// created/updated/deleted or when vault config changes.
//
// Handles three transitions:
//   - Rules added to a vault without a runner → creates runner + scheduler job
//   - Rules changed on a vault with a runner → hot-swaps rules
//   - Rules removed from a vault with a runner → removes runner + scheduler job
func (o *Orchestrator) ReloadRetentionPolicies(ctx context.Context) error {
	cfg, err := o.loadConfig(ctx)
	if err != nil {
		return fmt.Errorf("load config for retention policy reload: %w", err)
	}
	if cfg == nil {
		return nil
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	for _, vaultCfg := range cfg.Vaults {
		vault := o.vaults[vaultCfg.ID]
		if vault == nil {
			continue // Vault not registered locally.
		}

		// Resolve retention rules from the first tier.
		var tierRetentionRules []config.RetentionRule
		if len(vaultCfg.TierIDs) > 0 {
			tierCfg := findTierConfig(cfg.Tiers, vaultCfg.TierIDs[0])
			if tierCfg != nil {
				tierRetentionRules = tierCfg.RetentionRules
			}
		}

		runner, hasRunner := o.retention[vaultCfg.ID]
		hasRules := len(tierRetentionRules) > 0

		switch {
		case hasRules && hasRunner:
			// Update existing runner's rules.
			tierCfg := findTierConfig(cfg.Tiers, vaultCfg.TierIDs[0])
			rules, err := resolveRetentionRulesFromTier(cfg, tierCfg)
			if err != nil {
				o.logger.Warn("failed to resolve retention rules", "vault", vaultCfg.ID, "name", vaultCfg.Name, "error", err)
				continue
			}
			runner.setRules(rules)
			o.logger.Info("vault retention rules updated", "vault", vaultCfg.ID, "name", vaultCfg.Name, "rules", len(rules))

		case hasRules && !hasRunner:
			// Create new runner for vault that gained retention rules.
			tierCfg := findTierConfig(cfg.Tiers, vaultCfg.TierIDs[0])
			rules, err := resolveRetentionRulesFromTier(cfg, tierCfg)
			if err != nil {
				o.logger.Warn("failed to resolve retention rules", "vault", vaultCfg.ID, "name", vaultCfg.Name, "error", err)
				continue
			}
			if len(rules) == 0 {
				continue
			}
			newRunner := &retentionRunner{
				vaultID: vaultCfg.ID,
				cm:      vault.ChunkManager(),
				im:      vault.IndexManager(),
				rules:   rules,
				orch:    o,
				now:     o.now,
				logger:  o.logger,
			}
			o.retention[vaultCfg.ID] = newRunner
			if err := o.scheduler.AddJob(retentionJobName(vaultCfg.ID), defaultRetentionSchedule, newRunner.sweep); err != nil {
				o.logger.Warn("failed to add retention job", "vault", vaultCfg.ID, "error", err)
			}
			o.scheduler.Describe(retentionJobName(vaultCfg.ID), fmt.Sprintf("Retention sweep for '%s'", vaultCfg.Name))
			o.logger.Info("vault retention runner created", "vault", vaultCfg.ID, "name", vaultCfg.Name, "rules", len(rules))

		case !hasRules && hasRunner:
			// Rules removed — tear down the runner.
			o.scheduler.RemoveJob(retentionJobName(vaultCfg.ID))
			delete(o.retention, vaultCfg.ID)
			o.logger.Info("vault retention runner removed", "vault", vaultCfg.ID, "name", vaultCfg.Name)
		}
	}

	return nil
}
