package orchestrator

import (
	"context"

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

// ReloadRotationPolicies applies a rotation-policy change to the pipeline.
// Rotation thresholds now drive the pipeline chunking manager's manifest
// rotation policy, captured per vault at registration; a policy edit is applied
// by reconciling the pipeline from config (which re-registers any vault whose
// resolved manifest policy changed and refreshes its cron job). The legacy
// active-chunk rotation path on the per-instance chunk manager is gone.
//
// Invoked synchronously by the FSM dispatcher when a vault's RotationPolicyID
// changes or a policy's contents are edited.
func (o *Orchestrator) ReloadRotationPolicies(ctx context.Context) error {
	return o.ReloadFilters(ctx)
}

// ReloadRetentionPolicies is a no-op — retained for interface compatibility.
// The single retentionSweepAll job discovers all vault instances and resolves
// rules from the current config each tick. Config changes take effect on the
// next sweep (within 1 minute).
func (o *Orchestrator) ReloadRetentionPolicies(_ context.Context) error {
	return nil
}

// ApplyRotationPolicyForRole is a no-op — retained for interface compatibility.
// The pipeline chunking manager reads leadership live on every planner tick and
// captures the manifest rotation policy at registration, so a role transition
// needs no policy stamp on the (write-dead) per-instance chunk manager.
func (o *Orchestrator) ApplyRotationPolicyForRole(_ context.Context, _ glid.GLID) error {
	return nil
}
