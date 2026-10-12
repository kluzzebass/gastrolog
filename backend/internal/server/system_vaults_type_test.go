package server_test

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
)

// A vault type the cluster cannot build is refused at PutVault, never
// stored: unset, a retired wire value, or one never defined.
func TestPutVaultRefusesAnUndefinedType(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	for name, vt := range map[string]gastrologv1.VaultType{
		"unspecified":   gastrologv1.VaultType_VAULT_TYPE_UNSPECIFIED,
		"retired value": gastrologv1.VaultType(3),
		"never defined": gastrologv1.VaultType(99),
	} {
		id := glid.New()
		err := putVault(t, client, &gastrologv1.VaultConfig{Id: id.Bytes(), Name: "typeless-" + name, Enabled: true, Type: vt})
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("%s: want InvalidArgument, got %v", name, err)
		}
		if got, _ := store.GetVault(context.Background(), id); got != nil {
			t.Errorf("%s: the refused vault was stored", name)
		}
	}
}
