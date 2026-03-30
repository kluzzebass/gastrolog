package orchestrator

import (
	"context"
	"fmt"

	"gastrolog/internal/chunk"
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
	if tier.IsSecondary {
		tier.Chunks.SetRotationPolicy(chunk.NeverRotatePolicy{})
		// Don't touch the cron job — it's keyed by (vaultID, tierID) and
		// owned by the primary instance, which shares the same key.
		return nil
	}

	if tierCfg == nil || tierCfg.RotationPolicyID == nil {
		return nil
	}

	policyCfg := findRotationPolicy(cfg.RotationPolicies, *tierCfg.RotationPolicyID)
	if policyCfg == nil {
		o.logger.Warn("tier references unknown rotation policy",
			"vault", vaultCfg.ID, "tier", tier.TierID, "policy", *tierCfg.RotationPolicyID)
		return nil
	}

	policy, err := policyCfg.ToRotationPolicy()
	if err != nil {
		return fmt.Errorf("invalid policy %s for tier %s: %w", *tierCfg.RotationPolicyID, tier.TierID, err)
	}
	if policy != nil {
		tier.Chunks.SetRotationPolicy(policy)
		o.logger.Info("tier rotation policy updated",
			"vault", vaultCfg.ID, "tier", tier.TierID, "policy", *tierCfg.RotationPolicyID)
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

// ReloadRetentionPolicies is a no-op — retained for interface compatibility.
// The single retentionSweepAll job discovers all tier instances and resolves
// rules from the current config each tick. Config changes take effect on the
// next sweep (within 1 minute).
func (o *Orchestrator) ReloadRetentionPolicies(_ context.Context) error {
	return nil
}
