package orchestrator

import (
	"context"
	"fmt"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// findRotationPolicy finds a RotationPolicyConfig by ID in a slice.
func findRotationPolicy(policies []system.RotationPolicyConfig, id glid.GLID) *system.RotationPolicyConfig {
	for i := range policies {
		if policies[i].ID == id {
			return &policies[i]
		}
	}
	return nil
}

// findRetentionPolicy finds a RetentionPolicyConfig by ID in a slice.
func findRetentionPolicy(policies []system.RetentionPolicyConfig, id glid.GLID) *system.RetentionPolicyConfig {
	for i := range policies {
		if policies[i].ID == id {
			return &policies[i]
		}
	}
	return nil
}

// ReloadRotationPolicies hot-swaps the rotation policy on every leader vault
// instance's chunk manager from the current config. Invoked synchronously by
// the FSM dispatcher when a vault's RotationPolicyID changes or when a policy's
// contents are edited, so threshold changes take effect immediately on the
// active chunk rather than waiting up to 15 s for the next rotationSweep tick.
//
// Followers are left alone — the sweep continues to stamp NeverRotatePolicy on
// them. Cron-schedule changes still lag until the next sweep tick (they fire
// on minute boundaries anyway).
func (o *Orchestrator) ReloadRotationPolicies(ctx context.Context) error {
	sys, err := o.loadSystem(ctx)
	if err != nil {
		return fmt.Errorf("load system for rotation policy reload: %w", err)
	}
	if sys == nil {
		return nil
	}
	cfg := &sys.Config

	o.mu.RLock()
	defer o.mu.RUnlock()

	for vaultID, vault := range o.vaults {
		vaultInst := vault.Instance
		if vaultInst == nil {
			continue
		}
		vaultCfg := findVaultConfig(cfg.Vaults, vaultID)
		if vaultCfg == nil || vaultCfg.RotationPolicyID == nil {
			continue
		}
		policyCfg := findRotationPolicy(cfg.RotationPolicies, *vaultCfg.RotationPolicyID)
		if policyCfg == nil {
			continue
		}
		policy, err := policyCfg.ToRotationPolicy()
		if err != nil {
			o.logger.Warn("reload rotation policies: invalid policy",
				"vault", vaultID, "policy", *vaultCfg.RotationPolicyID, "error", err)
			continue
		}
		if policy != nil {
			vaultInst.Chunks.SetRotationPolicy(policy)
		}
	}
	return nil
}

// ReloadRetentionPolicies is a no-op — retained for interface compatibility.
// The single retentionSweepAll job discovers all vault instances and resolves
// rules from the current config each tick. Config changes take effect on the
// next sweep (within 1 minute).
func (o *Orchestrator) ReloadRetentionPolicies(_ context.Context) error {
	return nil
}

// ApplyRotationPolicyForRole applies the user-configured rotation
// policy to a vault instance's chunk manager. Invoked by the
// dispatcher immediately after a placement edit so the policy
// tracks config changes without waiting for the next rotationSweep
// tick (~15 s).
//
// Under fan-out (gastrolog-hshgl) every Receiver runs the same
// rotation policy — the legacy "follower → NeverRotatePolicy"
// branch is gone. Idempotent and safe to call when no role change
// occurred.
func (o *Orchestrator) ApplyRotationPolicyForRole(ctx context.Context, vaultID glid.GLID) error {
	sys, err := o.loadSystem(ctx)
	if err != nil {
		return fmt.Errorf("load system for role-based policy apply: %w", err)
	}
	if sys == nil {
		return nil
	}
	cfg := &sys.Config

	o.mu.RLock()
	defer o.mu.RUnlock()

	vault, ok := o.vaults[vaultID]
	if !ok || vault.Instance == nil {
		return nil
	}
	vaultCfg := findVaultConfig(cfg.Vaults, vaultID)
	if vaultCfg == nil || vaultCfg.RotationPolicyID == nil {
		return nil
	}
	policyCfg := findRotationPolicy(cfg.RotationPolicies, *vaultCfg.RotationPolicyID)
	if policyCfg == nil {
		return nil
	}
	policy, err := policyCfg.ToRotationPolicy()
	if err != nil {
		o.logger.Warn("apply rotation policy for role: invalid policy",
			"vault", vaultID, "policy", *vaultCfg.RotationPolicyID, "error", err)
		return nil
	}
	if policy != nil {
		vault.Instance.Chunks.SetRotationPolicy(policy)
	}
	return nil
}
