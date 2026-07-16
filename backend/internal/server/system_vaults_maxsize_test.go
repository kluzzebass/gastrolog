package server_test

// Coverage for gastrolog-1epfgb: max-size is resolved at the PutVault
// ingress so an unbounded vault is unrepresentable. An unset budget defaults
// to DefaultVaultMaxSizeBytes; an explicit 0 is rejected; a large explicit
// value is honored; and an update that does not mention max-size preserves
// the stored value rather than silently re-defaulting it.

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

func u64(v uint64) *uint64 { return &v }

func getStoredVault(t *testing.T, store system.Store, id glid.GLID) system.VaultConfig {
	t.Helper()
	v, err := store.GetVault(context.Background(), id)
	if err != nil {
		t.Fatalf("GetVault: %v", err)
	}
	if v == nil {
		t.Fatalf("vault %s not found", id)
	}
	return *v
}

// Unset max-size on create is stored as the default, not left unlimited.
func TestPutVaultDefaultsUnsetMaxSize(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()
	id := glid.New()

	_, err := client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:      id.Bytes(),
			Name:    "v",
			Enabled: true,
			Type:    gastrologv1.VaultType_VAULT_TYPE_FILE,
			// MaxSizeBytes omitted → unset.
		},
	}))
	if err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if got := getStoredVault(t, store, id).MaxSizeBytes; got != system.DefaultVaultMaxSizeBytes {
		t.Fatalf("stored max-size = %d, want default %d", got, system.DefaultVaultMaxSizeBytes)
	}
}

// An explicit 0 is a real error, not a silent accept-nothing.
func TestPutVaultRejectsExplicitZeroMaxSize(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	_, err := client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:           glid.New().Bytes(),
			Name:         "v",
			Enabled:      true,
			Type:         gastrologv1.VaultType_VAULT_TYPE_FILE,
			MaxSizeBytes: u64(0), // present and zero
		},
	}))
	if err == nil {
		t.Fatal("expected an error for an explicit max-size of 0, got nil")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v: %v", connect.CodeOf(err), err)
	}
}

// A large explicit value is the way to say "effectively unlimited".
func TestPutVaultHonorsLargeExplicitMaxSize(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()
	id := glid.New()
	const large = uint64(1) << 50 // 1 PiB

	_, err := client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:           id.Bytes(),
			Name:         "v",
			Enabled:      true,
			Type:         gastrologv1.VaultType_VAULT_TYPE_FILE,
			MaxSizeBytes: u64(large),
		},
	}))
	if err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if got := getStoredVault(t, store, id).MaxSizeBytes; got != large {
		t.Fatalf("stored max-size = %d, want %d", got, large)
	}
}

// An update that omits max-size preserves the stored value; it must not
// re-default a budget the operator previously chose.
func TestPutVaultUpdatePreservesMaxSize(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()
	id := glid.New()
	const chosen = uint64(50) << 30 // 50 GiB

	// Create with an explicit budget.
	_, err := client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:           id.Bytes(),
			Name:         "v",
			Enabled:      true,
			Type:         gastrologv1.VaultType_VAULT_TYPE_FILE,
			MaxSizeBytes: u64(chosen),
		},
	}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Update a different field, omitting max-size entirely.
	_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:      id.Bytes(),
			Name:    "v-renamed",
			Enabled: true,
			Type:    gastrologv1.VaultType_VAULT_TYPE_FILE,
			// MaxSizeBytes omitted → must preserve, not re-default.
		},
	}))
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	stored := getStoredVault(t, store, id)
	if stored.Name != "v-renamed" {
		t.Fatalf("name = %q, want v-renamed (update did not apply)", stored.Name)
	}
	if stored.MaxSizeBytes != chosen {
		t.Fatalf("max-size = %d after an update that omitted it, want preserved %d (a silent re-default)", stored.MaxSizeBytes, chosen)
	}
}
