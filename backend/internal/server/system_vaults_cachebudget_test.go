package server_test

// Coverage for gastrolog-338j51: the warm-cache budget is resolved at the
// PutVault ingress like max-size, but scoped to cloud-backed vaults (only
// they have a warm cache). Unset → default, explicit 0 → rejected, non-cloud
// → untouched.

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

func cloudFileVault(id, cloud glid.GLID, cacheBudget *uint64) *gastrologv1.VaultConfig {
	return &gastrologv1.VaultConfig{
		Id:               id.Bytes(),
		Name:             "cloud-v",
		Enabled:          true,
		Type:             gastrologv1.VaultType_VAULT_TYPE_FILE,
		CloudServiceId:   cloud.Bytes(),
		MaxSizeBytes:     u64(1 << 30), // avoid the max-size default path
		CacheBudgetBytes: cacheBudget,
	}
}

// An unset cache-budget on a cloud vault is stored as the default, not left
// unbounded. Before gastrolog-338j51 the field documented a 1GiB default that
// was never applied.
func TestPutVaultDefaultsUnsetCacheBudgetForCloudVault(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()
	id := glid.New()

	if _, err := client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: cloudFileVault(id, glid.New(), nil), // cache-budget unset
	})); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if got := getStoredVault(t, store, id).CacheBudgetBytes; got != system.DefaultVaultCacheBudgetBytes {
		t.Fatalf("stored cache-budget = %d, want default %d", got, system.DefaultVaultCacheBudgetBytes)
	}
}

// An explicit 0 disables the bound — the unbounded state this fixes — so it
// is rejected.
func TestPutVaultRejectsExplicitZeroCacheBudget(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	_, err := client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: cloudFileVault(glid.New(), glid.New(), u64(0)),
	}))
	if err == nil {
		t.Fatal("expected an error for an explicit cache-budget of 0, got nil")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v: %v", connect.CodeOf(err), err)
	}
}

// A non-cloud vault has no warm cache: cache-budget is neither defaulted nor
// rejected, so a 0 (the common case for a local file vault) is fine.
func TestPutVaultLeavesCacheBudgetUnsetForNonCloudVault(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()
	id := glid.New()

	if _, err := client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:           id.Bytes(),
			Name:         "local-v",
			Enabled:      true,
			Type:         gastrologv1.VaultType_VAULT_TYPE_FILE,
			MaxSizeBytes: u64(1 << 30),
			// no cloud service, no cache-budget
		},
	})); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if got := getStoredVault(t, store, id).CacheBudgetBytes; got != 0 {
		t.Fatalf("non-cloud cache-budget = %d, want 0 (no warm cache to bound)", got)
	}
}
