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
// for all registered stores, hot-swapping their policies. This is called when a
// rotation policy is created, updated, or deleted.
//
// Stores that don't reference any policy are left unchanged.
// Stores referencing a policy that no longer exists get a nil policy (type default).
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

	for _, storeCfg := range cfg.Stores {
		store, ok := o.stores[storeCfg.ID]
		if !ok {
			continue // Store not registered in orchestrator.
		}
		cm := store.Chunks
		if storeCfg.Policy == nil {
			continue // Store doesn't reference a policy.
		}

		policyCfg := findRotationPolicy(cfg.RotationPolicies, *storeCfg.Policy)
		if policyCfg == nil {
			// Policy was deleted — nothing to do; store keeps its current policy.
			// We can't revert to "type default" from here, and the dangling
			// reference will be caught on next restart or store edit.
			o.logger.Warn("store references unknown policy", "store", storeCfg.ID, "policy", *storeCfg.Policy)
			continue
		}

		policy, err := policyCfg.ToRotationPolicy()
		if err != nil {
			return fmt.Errorf("invalid policy %s for store %s: %w", *storeCfg.Policy, storeCfg.ID, err)
		}
		if policy != nil {
			store.Chunks.SetRotationPolicy(policy)
			o.logger.Info("store rotation policy updated", "store", storeCfg.ID, "policy", *storeCfg.Policy)
		}

		// Update cron rotation job.
		hasCronJob := o.cronRotation.hasJob(storeCfg.ID)
		hasCronConfig := policyCfg.Cron != nil && *policyCfg.Cron != ""

		switch {
		case hasCronConfig && hasCronJob:
			// Schedule may have changed — update.
			if err := o.cronRotation.updateJob(storeCfg.ID, storeCfg.Name, *policyCfg.Cron, cm); err != nil {
				o.logger.Error("failed to update cron rotation", "store", storeCfg.ID, "error", err)
			}
		case hasCronConfig && !hasCronJob:
			// New cron schedule — add.
			if err := o.cronRotation.addJob(storeCfg.ID, storeCfg.Name, *policyCfg.Cron, cm); err != nil {
				o.logger.Error("failed to add cron rotation", "store", storeCfg.ID, "error", err)
			}
		case !hasCronConfig && hasCronJob:
			// Cron removed — stop.
			o.cronRotation.removeJob(storeCfg.ID)
		}
	}

	return nil
}

// ReloadRetentionPolicies loads the full config and resolves retention rules
// for all registered stores, hot-swapping their rules. This is called when a
// retention policy is created, updated, or deleted.
//
// Stores without rules keep their current state.
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

	for _, storeCfg := range cfg.Stores {
		runner, ok := o.retention[storeCfg.ID]
		if !ok {
			continue // No retention runner for this store.
		}
		if len(storeCfg.RetentionRules) == 0 {
			continue // Store has no rules; keep current.
		}

		rules, err := resolveRetentionRules(cfg, storeCfg)
		if err != nil {
			o.logger.Warn("failed to resolve retention rules", "store", storeCfg.ID, "error", err)
			continue
		}

		runner.setRules(rules)
		o.logger.Info("store retention rules updated", "store", storeCfg.ID, "rules", len(rules))
	}

	return nil
}
