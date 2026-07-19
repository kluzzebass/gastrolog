package server_test

// Coverage for the typeable-defaults directive (policy decision 6): the
// disk-free thresholds accept a percentage of the volume ("10%") alongside an
// absolute size, stored verbatim; percentages are rejected on the size-only
// budget fields (a %-of-volume budget does not compose across vaults); and an
// explicit zero threshold is rejected like the explicit-0 budgets.

import (
	"context"
	"strings"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
)

func diskFreeVault(id glid.GLID, warn, floor string) *gastrologv1.VaultConfig {
	return &gastrologv1.VaultConfig{
		Id:            id.Bytes(),
		Name:          "df-v",
		Enabled:       true,
		Type:          gastrologv1.VaultType_VAULT_TYPE_FILE,
		DiskFreeWarn:  warn,
		DiskFreeFloor: floor,
	}
}

// Percent and size expressions are both accepted and stored VERBATIM — the
// operator's string round-trips through store and export untouched.
func TestPutVaultStoresDiskFreePercentVerbatim(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()

	for _, tc := range []struct{ warn, floor string }{
		{"10%", "3%"},
		{"2.5%", "1.25%"},
		{"10GB", "3GiB"},
		{"15%", "2GiB"}, // mixed forms on one vault
	} {
		id := glid.New()
		cfg := diskFreeVault(id, tc.warn, tc.floor)
		cfg.Name = "df-" + tc.warn + tc.floor
		resp, err := client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{Config: cfg}))
		if err != nil {
			t.Fatalf("PutVault(warn=%q floor=%q): %v", tc.warn, tc.floor, err)
		}
		stored := getStoredVault(t, store, id)
		if stored.DiskFreeWarn != tc.warn || stored.DiskFreeFloor != tc.floor {
			t.Fatalf("stored warn/floor = %q/%q, want %q/%q verbatim",
				stored.DiskFreeWarn, stored.DiskFreeFloor, tc.warn, tc.floor)
		}
		// The RPC echo — what `config export` serializes — is verbatim too.
		for _, v := range resp.Msg.GetSystem().GetVaults() {
			if glid.FromBytes(v.Id) != id {
				continue
			}
			if v.GetDiskFreeWarn() != tc.warn || v.GetDiskFreeFloor() != tc.floor {
				t.Fatalf("echoed warn/floor = %q/%q, want %q/%q verbatim",
					v.GetDiskFreeWarn(), v.GetDiskFreeFloor(), tc.warn, tc.floor)
			}
		}
	}
}

// Unset thresholds stay empty — "inherit the node default" is the empty
// string, not a materialized value, because the default resolves per node.
func TestPutVaultLeavesDiskFreeUnset(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()
	id := glid.New()

	if _, err := client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: diskFreeVault(id, "", ""),
	})); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	stored := getStoredVault(t, store, id)
	if stored.DiskFreeWarn != "" || stored.DiskFreeFloor != "" {
		t.Fatalf("unset thresholds must stay empty, got %q/%q", stored.DiskFreeWarn, stored.DiskFreeFloor)
	}
}

// Nonsense percentages are rejected at write ingress, not at use.
func TestPutVaultRejectsInvalidDiskFreeExpressions(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	for _, bad := range []string{"150%", "-5%", "%", "10%%", "max(10%, 10GiB)"} {
		_, err := client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
			Config: diskFreeVault(glid.New(), bad, ""),
		}))
		if err == nil || connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("disk-free-warn %q: want InvalidArgument, got %v", bad, err)
		}
	}
}

// An explicit zero threshold ("0", "0%") disables the guard for the vault
// and is rejected like the explicit-0 budgets.
func TestPutVaultRejectsZeroDiskFreeThreshold(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	for _, zero := range []string{"0", "0%", "0GB"} {
		_, err := client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
			Config: diskFreeVault(glid.New(), "", zero),
		}))
		if err == nil || connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("disk-free-floor %q: want InvalidArgument, got %v", zero, err)
		}
	}
}

// Percentages are volume-relative and only the disk-free thresholds are
// volume-relative: the budget fields reject them with an error that names
// the reason (shares do not compose across vaults on one volume).
func TestPutVaultRejectsPercentOnBudgetFields(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	for name, cfg := range map[string]*gastrologv1.VaultConfig{
		"memory-budget": {
			Id: glid.New().Bytes(), Name: "pct-mem", Enabled: true,
			Type:         gastrologv1.VaultType_VAULT_TYPE_MEMORY,
			MemoryBudget: "10%",
		},
		"cache-budget": {
			Id: glid.New().Bytes(), Name: "pct-cache", Enabled: true,
			Type:           gastrologv1.VaultType_VAULT_TYPE_FILE,
			CloudServiceId: glid.New().Bytes(),
			CacheBudget:    "10%",
		},
	} {
		_, err := client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{Config: cfg}))
		if err == nil || connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("%s = 10%%: want InvalidArgument, got %v", name, err)
		}
		if !strings.Contains(err.Error(), "percentage") {
			t.Fatalf("%s rejection must explain the percentage rule, got: %v", name, err)
		}
	}
}
