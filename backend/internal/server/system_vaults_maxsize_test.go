package server_test

// Coverage for gastrolog-1epfgb / gastrolog-etcjdx: max-size is a stored
// expression, resolved at use. The PutVault ingress defaults an unset budget,
// rejects an explicit "0", stores a set value verbatim, and preserves it
// across an update that does not mention it.

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

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

// Unset max-size on create is stored as the default expression, not left empty.
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
			// MaxSize omitted → unset.
		},
	}))
	if err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if got := getStoredVault(t, store, id).MaxSize; got != system.DefaultVaultMaxSize {
		t.Fatalf("stored max-size = %q, want default %q", got, system.DefaultVaultMaxSize)
	}
}

// An explicit "0" is a real error, not a silent accept-nothing.
func TestPutVaultRejectsExplicitZeroMaxSize(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	_, err := client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:      glid.New().Bytes(),
			Name:    "v",
			Enabled: true,
			Type:    gastrologv1.VaultType_VAULT_TYPE_FILE,
			MaxSize: "0",
		},
	}))
	if err == nil {
		t.Fatal("expected an error for an explicit max-size of 0, got nil")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v: %v", connect.CodeOf(err), err)
	}
}

// A set value is stored verbatim — the operator's own expression, echoed back.
func TestPutVaultStoresMaxSizeVerbatim(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()
	id := glid.New()

	_, err := client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:      id.Bytes(),
			Name:    "v",
			Enabled: true,
			Type:    gastrologv1.VaultType_VAULT_TYPE_FILE,
			MaxSize: "100TiB", // effectively unlimited, said explicitly
		},
	}))
	if err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if got := getStoredVault(t, store, id).MaxSize; got != "100TiB" {
		t.Fatalf("stored max-size = %q, want %q verbatim", got, "100TiB")
	}
}

// An unparseable value is rejected at the write boundary, not at use.
func TestPutVaultRejectsUnparseableMaxSize(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	_, err := client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:      glid.New().Bytes(),
			Name:    "v",
			Enabled: true,
			Type:    gastrologv1.VaultType_VAULT_TYPE_FILE,
			MaxSize: "gigabytes-please",
		},
	}))
	if err == nil || connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument for an unparseable max-size, got %v", err)
	}
}

// An update that omits max-size preserves the stored value; it must not
// re-default a budget the operator previously chose.
func TestPutVaultUpdatePreservesMaxSize(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()
	id := glid.New()

	_, err := client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:      id.Bytes(),
			Name:    "v",
			Enabled: true,
			Type:    gastrologv1.VaultType_VAULT_TYPE_FILE,
			MaxSize: "50GiB",
		},
	}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:      id.Bytes(),
			Name:    "v-renamed",
			Enabled: true,
			Type:    gastrologv1.VaultType_VAULT_TYPE_FILE,
			// MaxSize omitted → must preserve, not re-default.
		},
	}))
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	stored := getStoredVault(t, store, id)
	if stored.Name != "v-renamed" {
		t.Fatalf("name = %q, want v-renamed (update did not apply)", stored.Name)
	}
	if stored.MaxSize != "50GiB" {
		t.Fatalf("max-size = %q after an update that omitted it, want preserved %q", stored.MaxSize, "50GiB")
	}
}
