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
		cm := vault.Chunks
		if vaultCfg.Policy == nil {
			continue // Vault doesn't reference a policy.
		}

		policyCfg := findRotationPolicy(cfg.RotationPolicies, *vaultCfg.Policy)
		if policyCfg == nil {
			// Policy was deleted — nothing to do; vault keeps its current policy.
			// We can't revert to "type default" from here, and the dangling
			// reference will be caught on next restart or vault edit.
			o.logger.Warn("vault references unknown policy", "vault", vaultCfg.ID, "name", vaultCfg.Name, "policy", *vaultCfg.Policy)
			continue
		}

		policy, err := policyCfg.ToRotationPolicy()
		if err != nil {
			return fmt.Errorf("invalid policy %s for vault %s: %w", *vaultCfg.Policy, vaultCfg.ID, err)
		}
		if policy != nil {
			vault.Chunks.SetRotationPolicy(policy)
			o.logger.Info("vault rotation policy updated", "vault", vaultCfg.ID, "name", vaultCfg.Name, "policy", *vaultCfg.Policy)
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
// for all registered vaults, hot-swapping their rules. This is called when a
// retention policy is created, updated, or deleted.
//
// Vaults without rules keep their current state.
func (o *Orchestrator) ReloadRetentionPolicies(ctx context.Context) error {
	cfg, err := o.loadConfig(ctx)
	if err != nil {
		return fmt.Errorf("load config for retention policy reload: %w", err)
	}
	if cfg == nil {
		return nil
	}

	o.mu.RLock()
	defer o.mu.RUnlock()

	for _, vaultCfg := range cfg.Vaults {
		runner, ok := o.retention[vaultCfg.ID]
		if !ok {
			continue // No retention runner for this vault.
		}
		if len(vaultCfg.RetentionRules) == 0 {
			continue // Vault has no rules; keep current.
		}

		rules, err := resolveRetentionRules(cfg, vaultCfg)
		if err != nil {
			o.logger.Warn("failed to resolve retention rules", "vault", vaultCfg.ID, "name", vaultCfg.Name, "error", err)
			continue
		}

		runner.setRules(rules)
		o.logger.Info("vault retention rules updated", "vault", vaultCfg.ID, "name", vaultCfg.Name, "rules", len(rules))
	}

	return nil
}
