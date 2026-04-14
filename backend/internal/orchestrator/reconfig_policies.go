package orchestrator

import (
	"gastrolog/internal/glid"
	"context"

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

// ReloadRotationPolicies is a no-op — retained for interface compatibility.
// The rotationSweep job discovers all tier instances and reconciles rotation
// policies + cron jobs from the current config every 15 seconds.
func (o *Orchestrator) ReloadRotationPolicies(_ context.Context) error {
	return nil
}

// ReloadRetentionPolicies is a no-op — retained for interface compatibility.
// The single retentionSweepAll job discovers all tier instances and resolves
// rules from the current config each tick. Config changes take effect on the
// next sweep (within 1 minute).
func (o *Orchestrator) ReloadRetentionPolicies(_ context.Context) error {
	return nil
}
