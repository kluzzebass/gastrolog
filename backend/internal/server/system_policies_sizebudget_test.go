package server_test

// Coverage for gastrolog-33ul6h: the per-node disk-claim budget moved off
// VaultConfig.max_size (see gastrolog-1epfgb / gastrolog-etcjdx for its prior
// home, formerly covered by system_vaults_maxsize_test.go) onto
// RetentionPolicyConfig.size_budget. PutRetentionPolicy parse-checks it —
// must parse, must be > 0 when set — mirroring the old PutVault max-size
// validation; unlike the vault field, an absent size_budget is NOT defaulted
// here (no per-policy stamping) — the default floor is applied downstream by
// the disk-guard resolver (orchestrator.resolveVaultSizeBudget) only when NO
// attached policy carries a budget at all.

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

func getStoredRetentionPolicy(t *testing.T, store system.Store, id glid.GLID) system.RetentionPolicyConfig {
	t.Helper()
	p, err := store.GetRetentionPolicy(context.Background(), id)
	if err != nil {
		t.Fatalf("GetRetentionPolicy: %v", err)
	}
	if p == nil {
		t.Fatalf("retention policy %s not found", id)
	}
	return *p
}

func strPtrVal(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}

// An unset size-budget is left nil — no default is stamped onto the policy.
// This is a deliberate divergence from the old vault max-size behavior: the
// creation-default floor now lives in the disk-guard resolver, not on the
// stored config, because it must apply per-vault (across possibly zero
// attached policies), not per-policy.
func TestPutRetentionPolicyLeavesSizeBudgetUnset(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()
	id := glid.New()

	_, err := client.PutRetentionPolicy(ctx, connect.NewRequest(&gastrologv1.PutRetentionPolicyRequest{
		Config: &gastrologv1.RetentionPolicyConfig{
			Id:        id.Bytes(),
			Name:      "budget-unset",
			MaxChunks: 10, // needs at least one condition to pass IsEmpty
			// SizeBudget omitted → unset.
		},
	}))
	if err != nil {
		t.Fatalf("PutRetentionPolicy: %v", err)
	}
	if got := getStoredRetentionPolicy(t, store, id).SizeBudget; got != nil {
		t.Fatalf("stored size-budget = %q, want nil (no default stamped)", strPtrVal(got))
	}
}

// An explicit "0" is a real error, not a silent accept-nothing — it would
// mean "no bound", the unrepresentable state this model exists to prevent.
func TestPutRetentionPolicyRejectsExplicitZeroSizeBudget(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	zero := "0"
	_, err := client.PutRetentionPolicy(ctx, connect.NewRequest(&gastrologv1.PutRetentionPolicyRequest{
		Config: &gastrologv1.RetentionPolicyConfig{
			Id:         glid.New().Bytes(),
			Name:       "budget-zero",
			SizeBudget: &zero,
		},
	}))
	if err == nil {
		t.Fatal("expected an error for an explicit size-budget of 0, got nil")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v: %v", connect.CodeOf(err), err)
	}
}

// A set value is stored verbatim — the operator's own expression, echoed back.
func TestPutRetentionPolicyStoresSizeBudgetVerbatim(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()
	id := glid.New()

	budget := "100TiB" // effectively unlimited, said explicitly
	_, err := client.PutRetentionPolicy(ctx, connect.NewRequest(&gastrologv1.PutRetentionPolicyRequest{
		Config: &gastrologv1.RetentionPolicyConfig{
			Id:         id.Bytes(),
			Name:       "budget-verbatim",
			SizeBudget: &budget,
		},
	}))
	if err != nil {
		t.Fatalf("PutRetentionPolicy: %v", err)
	}
	if got := getStoredRetentionPolicy(t, store, id).SizeBudget; got == nil || *got != "100TiB" {
		t.Fatalf("stored size-budget = %q, want %q verbatim", strPtrVal(got), "100TiB")
	}
}

// Unparseable and percentage expressions are rejected at the write boundary,
// not at use. size_budget is an absolute disk-claim budget, not a
// volume-relative threshold, so a percentage is just an unparseable size —
// no special-cased "percentage" message like the disk-free thresholds.
func TestPutRetentionPolicyRejectsUnparseableSizeBudget(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	for _, bad := range []string{"gigabytes-please", "10%", "-5GB"} {
		_, err := client.PutRetentionPolicy(ctx, connect.NewRequest(&gastrologv1.PutRetentionPolicyRequest{
			Config: &gastrologv1.RetentionPolicyConfig{
				Id:         glid.New().Bytes(),
				Name:       "budget-bad-" + bad,
				SizeBudget: &bad,
			},
		}))
		if err == nil || connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("size-budget %q: want InvalidArgument, got %v", bad, err)
		}
	}
}

// A valid size is accepted alongside a drain trigger.
func TestPutRetentionPolicyAcceptsValidSizeBudget(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()
	id := glid.New()

	budget := "50GB"
	age := "3d"
	_, err := client.PutRetentionPolicy(ctx, connect.NewRequest(&gastrologv1.PutRetentionPolicyRequest{
		Config: &gastrologv1.RetentionPolicyConfig{
			Id:         id.Bytes(),
			Name:       "budget-and-trigger",
			MaxAge:     age,
			SizeBudget: &budget,
		},
	}))
	if err != nil {
		t.Fatalf("PutRetentionPolicy: %v", err)
	}
	stored := getStoredRetentionPolicy(t, store, id)
	if stored.SizeBudget == nil || *stored.SizeBudget != "50GB" {
		t.Fatalf("stored size-budget = %q, want %q", strPtrVal(stored.SizeBudget), "50GB")
	}
	if stored.MaxAge == nil || *stored.MaxAge != "3d" {
		t.Fatalf("stored max-age = %v, want %q", stored.MaxAge, "3d")
	}
}

// A trigger-less policy (no maxAge/maxSize/maxChunks) that sets ONLY
// size_budget is legal and meaningful: it drains nothing, but the bound
// still applies at the disk guard (gastrolog-33ul6h operator decision 1). It
// must not be rejected by the empty-policy check.
func TestPutRetentionPolicyAcceptsSizeBudgetOnlyPolicy(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()
	id := glid.New()

	budget := "50GB"
	_, err := client.PutRetentionPolicy(ctx, connect.NewRequest(&gastrologv1.PutRetentionPolicyRequest{
		Config: &gastrologv1.RetentionPolicyConfig{
			Id:         id.Bytes(),
			Name:       "budget-only",
			SizeBudget: &budget,
			// No MaxAge, MaxSize, or MaxChunks: trigger-less.
		},
	}))
	if err != nil {
		t.Fatalf("PutRetentionPolicy must accept a bound-only (trigger-less) policy: %v", err)
	}
	stored := getStoredRetentionPolicy(t, store, id)
	if stored.SizeBudget == nil || *stored.SizeBudget != "50GB" {
		t.Fatalf("stored size-budget = %q, want %q", strPtrVal(stored.SizeBudget), "50GB")
	}
}
