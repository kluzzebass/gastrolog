package server_test

// Coverage for gastrolog-338j51 / gastrolog-etcjdx: the warm-cache budget is a
// stored expression, resolved at use, scoped to cloud-backed vaults. Unset →
// default; explicit "0" → rejected; non-cloud → left empty.

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

func cloudFileVault(id, cloud glid.GLID, cacheBudget string) *gastrologv1.VaultConfig {
	return &gastrologv1.VaultConfig{
		Id:             id.Bytes(),
		Name:           "cloud-v",
		Enabled:        true,
		Type:           gastrologv1.VaultType_VAULT_TYPE_FILE,
		CloudServiceId: cloud.Bytes(),
		MaxSize:        "1GiB", // avoid the max-size default path
		CacheBudget:    cacheBudget,
	}
}

// An unset cache-budget on a cloud vault is stored as the default expression.
func TestPutVaultDefaultsUnsetCacheBudgetForCloudVault(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()
	id := glid.New()

	if _, err := client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: cloudFileVault(id, glid.New(), ""), // cache-budget unset
	})); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if got := getStoredVault(t, store, id).CacheBudget; got != system.DefaultVaultCacheBudget {
		t.Fatalf("stored cache-budget = %q, want default %q", got, system.DefaultVaultCacheBudget)
	}
}

// An explicit "0" disables the bound — the unbounded state this fixes — so it
// is rejected.
func TestPutVaultRejectsExplicitZeroCacheBudget(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	_, err := client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: cloudFileVault(glid.New(), glid.New(), "0"),
	}))
	if err == nil {
		t.Fatal("expected an error for an explicit cache-budget of 0, got nil")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v: %v", connect.CodeOf(err), err)
	}
}

// A non-cloud vault has no warm cache: cache-budget is neither defaulted nor
// rejected, so an empty value stays empty.
func TestPutVaultLeavesCacheBudgetUnsetForNonCloudVault(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()
	id := glid.New()

	if _, err := client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:      id.Bytes(),
			Name:    "local-v",
			Enabled: true,
			Type:    gastrologv1.VaultType_VAULT_TYPE_FILE,
			MaxSize: "1GiB",
			// no cloud service, no cache-budget
		},
	})); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if got := getStoredVault(t, store, id).CacheBudget; got != "" {
		t.Fatalf("non-cloud cache-budget = %q, want empty (no warm cache to bound)", got)
	}
}
