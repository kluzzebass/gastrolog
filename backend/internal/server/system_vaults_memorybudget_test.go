package server_test

// A memory vault's in-RAM cap is a stored expression resolved at use, scoped
// to memory vaults. Unset → default; explicit "0" → rejected; non-memory →
// left empty.

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// An unset memory-budget on a memory vault is stored as the default.
func TestPutVaultDefaultsUnsetMemoryBudget(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()
	id := glid.New()

	if _, err := client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:      id.Bytes(),
			Name:    "mem-v",
			Enabled: true,
			Type:    gastrologv1.VaultType_VAULT_TYPE_MEMORY,
			// MemoryBudget unset.
		},
	})); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if got := getStoredVault(t, store, id).MemoryBudget; got != system.DefaultVaultMemoryBudget {
		t.Fatalf("stored memory-budget = %q, want default %q", got, system.DefaultVaultMemoryBudget)
	}
}

// An explicit "0" leaves the vault unbounded in RAM, so it is rejected.
func TestPutVaultRejectsExplicitZeroMemoryBudget(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	_, err := client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:           glid.New().Bytes(),
			Name:         "mem-v",
			Enabled:      true,
			Type:         gastrologv1.VaultType_VAULT_TYPE_MEMORY,
			MemoryBudget: "0",
		},
	}))
	if err == nil {
		t.Fatal("expected an error for an explicit memory-budget of 0, got nil")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v: %v", connect.CodeOf(err), err)
	}
}

// A non-memory vault has no in-memory store: memory-budget is left empty.
func TestPutVaultLeavesMemoryBudgetUnsetForFileVault(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()
	id := glid.New()

	if _, err := client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:      id.Bytes(),
			Name:    "file-v",
			Enabled: true,
			Type:    gastrologv1.VaultType_VAULT_TYPE_FILE,
		},
	})); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if got := getStoredVault(t, store, id).MemoryBudget; got != "" {
		t.Fatalf("file-vault memory-budget = %q, want empty", got)
	}
}
