package server

import (
	"testing"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
)

func FuzzProtoToVaultConfig(f *testing.F) {
	// Seed corpus: (id, name, enabled, tierID)
	f.Add("01961234-5678-7abc-8def-0123456789ab", "my-vault", true, "")
	f.Add("01961234-5678-7abc-8def-0123456789ab", "my-vault", true, "01961234-5678-7abc-8def-ffffffffffff")
	f.Add("not-a-uuid", "vault", false, "")
	f.Add("01961234-5678-7abc-8def-0123456789ab", "vault", true, "bad-uuid")
	f.Add("", "", false, "")

	f.Fuzz(func(t *testing.T, id, name string, enabled bool, tierID string) {
		pb := &apiv1.VaultConfig{
			Id:      []byte(id),
			Name:    name,
			Enabled: enabled,
		}

		// Must not panic on any input.
		_, _ = protoToVaultConfig(pb)
	})
}
