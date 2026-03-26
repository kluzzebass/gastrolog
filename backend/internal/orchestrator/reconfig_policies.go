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
			continue
		}
		for _, tier := range vault.Tiers {
			tierCfg := findTierConfig(cfg.Tiers, tier.TierID)
			if err := o.reloadTierRotation(cfg, vaultCfg, tier, tierCfg); err != nil {
				return err
			}
		}
	}

	return nil
}

// reloadTierRotation reconciles the rotation policy and cron job for a single tier.
func (o *Orchestrator) reloadTierRotation(cfg *config.Config, vaultCfg config.VaultConfig, tier *TierInstance, tierCfg *config.TierConfig) error {
	if tierCfg == nil || tierCfg.RotationPolicyID == nil {
		return nil
	}

	policyCfg := findRotationPolicy(cfg.RotationPolicies, *tierCfg.RotationPolicyID)
	if policyCfg == nil {
		o.logger.Warn("tier references unknown policy", "vault", vaultCfg.ID, "tier", tier.TierID, "policy", *tierCfg.RotationPolicyID)
		return nil
	}

	policy, err := policyCfg.ToRotationPolicy()
	if err != nil {
		return fmt.Errorf("invalid policy %s for tier %s: %w", *tierCfg.RotationPolicyID, tier.TierID, err)
	}
	if policy != nil {
		tier.Chunks.SetRotationPolicy(policy)
		o.logger.Info("tier rotation policy updated", "vault", vaultCfg.ID, "tier", tier.TierID, "policy", *tierCfg.RotationPolicyID)
	}

	// Update cron rotation job.
	hasCronJob := o.cronRotation.hasJob(vaultCfg.ID, tier.TierID)
	hasCronConfig := policyCfg.Cron != nil && *policyCfg.Cron != ""

	switch {
	case hasCronConfig && hasCronJob:
		if err := o.cronRotation.updateJob(vaultCfg.ID, tier.TierID, vaultCfg.Name, *policyCfg.Cron, tier.Chunks); err != nil {
			o.logger.Error("failed to update cron rotation", "vault", vaultCfg.ID, "tier", tier.TierID, "error", err)
		}
	case hasCronConfig && !hasCronJob:
		if err := o.cronRotation.addJob(vaultCfg.ID, tier.TierID, vaultCfg.Name, *policyCfg.Cron, tier.Chunks); err != nil {
			o.logger.Error("failed to add cron rotation", "vault", vaultCfg.ID, "tier", tier.TierID, "error", err)
		}
	case !hasCronConfig && hasCronJob:
		o.cronRotation.removeJob(vaultCfg.ID, tier.TierID)
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

		for _, tier := range vault.Tiers {
			tierCfg := findTierConfig(cfg.Tiers, tier.TierID)
			o.reloadTierRetention(cfg, vaultCfg, tier, tierCfg)
		}
	}

	return nil
}

// reloadTierRetention reconciles the retention runner for a single tier.
// reloadTierRetention reconciles the retention runner for a single tier.
// Secondaries run retention too (to clean up expired replicas), but all
// actions resolve to expire — the primary handles transition/eject.
func (o *Orchestrator) reloadTierRetention(cfg *config.Config, vaultCfg config.VaultConfig, tier *TierInstance, tierCfg *config.TierConfig) {
	key := tier.TierID
	jobName := retentionJobName(tier.TierID)

	var tierRetentionRules []config.RetentionRule
	if tierCfg != nil {
		tierRetentionRules = tierCfg.RetentionRules
	}

	runner, hasRunner := o.retention[key]
	hasRules := len(tierRetentionRules) > 0

	switch {
	case hasRules && hasRunner:
		rules, err := resolveRetentionRulesFromTier(cfg, vaultCfg, tierCfg)
		if err != nil {
			o.logger.Warn("failed to resolve retention rules", "vault", vaultCfg.ID, "tier", tier.TierID, "error", err)
			return
		}
		runner.setRules(rules)
		runner.isSecondary = tier.IsSecondary
		o.logger.Info("tier retention rules updated", "vault", vaultCfg.ID, "tier", tier.TierID, "rules", len(rules), "secondary", tier.IsSecondary)

	case hasRules && !hasRunner:
		rules, err := resolveRetentionRulesFromTier(cfg, vaultCfg, tierCfg)
		if err != nil {
			o.logger.Warn("failed to resolve retention rules", "vault", vaultCfg.ID, "tier", tier.TierID, "error", err)
			return
		}
		if len(rules) == 0 {
			return
		}
		newRunner := &retentionRunner{
			vaultID:     vaultCfg.ID,
			tierID:      tier.TierID,
			cm:          tier.Chunks,
			im:          tier.Indexes,
			rules:       rules,
			orch:        o,
			isSecondary: tier.IsSecondary,
			now:     o.now,
			logger:  o.logger,
		}
		o.retention[key] = newRunner
		if err := o.scheduler.AddJob(jobName, defaultRetentionSchedule, newRunner.sweep); err != nil {
			o.logger.Warn("failed to add retention job", "vault", vaultCfg.ID, "tier", tier.TierID, "error", err)
		}
		o.scheduler.Describe(jobName, fmt.Sprintf("Retention sweep for '%s'", vaultCfg.Name))
		o.logger.Info("tier retention runner created", "vault", vaultCfg.ID, "tier", tier.TierID, "rules", len(rules))

	case !hasRules && hasRunner:
		o.scheduler.RemoveJob(jobName)
		delete(o.retention, key)
		o.logger.Info("tier retention runner removed", "vault", vaultCfg.ID, "tier", tier.TierID)
	}
}
