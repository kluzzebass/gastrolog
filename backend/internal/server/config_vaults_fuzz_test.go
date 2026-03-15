package server

import (
	"testing"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
)

func FuzzProtoToVaultConfig(f *testing.F) {
	// Seed corpus: (id, name, vtype, policy, action, retentionPolicyID, ejectRouteID)
	f.Add("01961234-5678-7abc-8def-0123456789ab", "my-vault", "file", "", "", "", "")
	f.Add("01961234-5678-7abc-8def-0123456789ab", "my-vault", "file", "01961234-5678-7abc-8def-ffffffffffff", "", "", "")
	f.Add("not-a-uuid", "vault", "file", "", "", "", "")
	f.Add("01961234-5678-7abc-8def-0123456789ab", "vault", "file", "bad-uuid", "", "", "")
	f.Add("01961234-5678-7abc-8def-0123456789ab", "vault", "cloud", "",
		"expire", "01961234-5678-7abc-8def-ffffffffffff", "")
	f.Add("01961234-5678-7abc-8def-0123456789ab", "vault", "file", "",
		"eject", "01961234-5678-7abc-8def-ffffffffffff", "01961234-5678-7abc-8def-aaaaaaaaaaaa")
	f.Add("01961234-5678-7abc-8def-0123456789ab", "vault", "file", "",
		"eject", "01961234-5678-7abc-8def-ffffffffffff", "")
	f.Add("01961234-5678-7abc-8def-0123456789ab", "vault", "file", "",
		"unknown_action", "01961234-5678-7abc-8def-ffffffffffff", "")
	f.Add("", "", "", "", "", "", "")
	f.Add("01961234-5678-7abc-8def-0123456789ab", "vault", "file", "",
		"expire", "not-a-uuid", "")

	f.Fuzz(func(t *testing.T, id, name, vtype, policy, action, retPolicyID, ejectRouteID string) {
		pb := &apiv1.VaultConfig{
			Id:      id,
			Name:    name,
			Type:    vtype,
			Policy:  policy,
			Enabled: true,
			Params:  map[string]string{"dir": "/tmp/test"},
		}

		// Only add a retention rule if there's a policy ID to reference.
		if retPolicyID != "" {
			rule := &apiv1.RetentionRule{
				RetentionPolicyId: retPolicyID,
				Action:            action,
			}
			if ejectRouteID != "" {
				rule.EjectRouteIds = []string{ejectRouteID}
			}
			pb.RetentionRules = []*apiv1.RetentionRule{rule}
		}

		// Must not panic on any input.
		_, _ = protoToVaultConfig(pb)
	})
}

func FuzzValidateRetentionAction(f *testing.F) {
	f.Add("expire", "")
	f.Add("eject", "")
	f.Add("eject", "01961234-5678-7abc-8def-0123456789ab")
	f.Add("", "")
	f.Add("unknown", "")
	f.Add("migrate", "some-id")

	f.Fuzz(func(t *testing.T, action, ejectRouteID string) {
		rule := &apiv1.RetentionRule{
			RetentionPolicyId: "01961234-5678-7abc-8def-0123456789ab",
			Action:            action,
		}
		if ejectRouteID != "" {
			rule.EjectRouteIds = []string{ejectRouteID}
		}
		// Must not panic on any input.
		_ = validateRetentionAction(rule)
	})
}
