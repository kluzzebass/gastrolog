package orchestrator

// Coverage for gastrolog-33ul6h: resolveVaultSizeBudget is the config→runtime
// resolver that replaces reading VaultConfig.MaxSize directly. It computes
// the effective per-node disk-claim budget for a file vault from the
// retention policies attached via the vault's RetentionRules: min-wins
// across every attached policy's parsed SizeBudget, with
// system.DefaultVaultMaxSize as the floor when none carries one. The guard's
// own seam (SetVaultGuard / maxSizeBytes) is unchanged — these tests pin
// only the resolver that feeds it a number.

import (
	"context"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

func retentionRuleFor(policyID glid.GLID) system.RetentionRule {
	return system.RetentionRule{RetentionPolicyID: policyID}
}

func policyWithBudget(id glid.GLID, budget string) system.RetentionPolicyConfig {
	b := budget
	return system.RetentionPolicyConfig{ID: id, Name: "p-" + id.String(), SizeBudget: &b}
}

func defaultVaultMaxSizeBytes(t *testing.T) uint64 {
	t.Helper()
	n, err := system.ParseSize(system.DefaultVaultMaxSize)
	if err != nil {
		t.Fatalf("system.DefaultVaultMaxSize %q must parse: %v", system.DefaultVaultMaxSize, err)
	}
	return n
}

// No attached retention rules at all: the vault stays bounded by the
// creation default, not unbounded — the product-defaults invariant survives
// zero operator diligence.
func TestResolveVaultSizeBudgetNoRulesUsesDefault(t *testing.T) {
	vc := system.VaultConfig{ID: glid.New(), Name: "v"}
	got := resolveVaultSizeBudget(vc, nil)
	want := defaultVaultMaxSizeBytes(t)
	if got != want {
		t.Fatalf("resolveVaultSizeBudget with no rules = %d, want default %d", got, want)
	}
}

// A single attached policy with a budget is used directly.
func TestResolveVaultSizeBudgetSinglePolicy(t *testing.T) {
	policyID := glid.New()
	policies := []system.RetentionPolicyConfig{policyWithBudget(policyID, "20GiB")}
	vc := system.VaultConfig{
		ID:             glid.New(),
		RetentionRules: []system.RetentionRule{retentionRuleFor(policyID)},
	}
	got := resolveVaultSizeBudget(vc, policies)
	want, _ := system.ParseSize("20GiB")
	if got != want {
		t.Fatalf("resolveVaultSizeBudget single policy = %d, want %d (20GiB)", got, want)
	}
}

// Multiple attached policies: the effective budget is the MINIMUM across
// every parsed SizeBudget — the operator decision that the refuse bound is
// the tightest constraint among a vault's attached policies.
func TestResolveVaultSizeBudgetMinWinsAcrossPolicies(t *testing.T) {
	loose, tight, mid := glid.New(), glid.New(), glid.New()
	policies := []system.RetentionPolicyConfig{
		policyWithBudget(loose, "500GiB"),
		policyWithBudget(tight, "10GiB"),
		policyWithBudget(mid, "100GiB"),
	}
	vc := system.VaultConfig{
		ID: glid.New(),
		RetentionRules: []system.RetentionRule{
			retentionRuleFor(loose), retentionRuleFor(tight), retentionRuleFor(mid),
		},
	}
	got := resolveVaultSizeBudget(vc, policies)
	want, _ := system.ParseSize("10GiB")
	if got != want {
		t.Fatalf("resolveVaultSizeBudget min-wins = %d, want %d (10GiB, the tightest)", got, want)
	}
}

// A trigger-less policy (no MaxAge/MaxSize/MaxChunks — it drains nothing)
// that carries ONLY a SizeBudget still contributes to the resolved budget: a
// bound-only policy is legal and meaningful (operator decision 1).
func TestResolveVaultSizeBudgetTriggerLessPolicyStillApplies(t *testing.T) {
	policyID := glid.New()
	b := "30GiB"
	policies := []system.RetentionPolicyConfig{
		{ID: policyID, Name: "bound-only", SizeBudget: &b}, // no MaxAge/MaxSize/MaxChunks
	}
	vc := system.VaultConfig{
		ID:             glid.New(),
		RetentionRules: []system.RetentionRule{retentionRuleFor(policyID)},
	}
	got := resolveVaultSizeBudget(vc, policies)
	want, _ := system.ParseSize("30GiB")
	if got != want {
		t.Fatalf("resolveVaultSizeBudget trigger-less bound-only policy = %d, want %d (30GiB)", got, want)
	}
}

// A policy attached but with no SizeBudget set at all contributes nothing —
// falls through to the default floor, same as no rules.
func TestResolveVaultSizeBudgetPolicyWithoutBudgetUsesDefault(t *testing.T) {
	policyID := glid.New()
	policies := []system.RetentionPolicyConfig{
		{ID: policyID, Name: "age-only", MaxAge: strPtr("7d")}, // no SizeBudget
	}
	vc := system.VaultConfig{
		ID:             glid.New(),
		RetentionRules: []system.RetentionRule{retentionRuleFor(policyID)},
	}
	got := resolveVaultSizeBudget(vc, policies)
	want := defaultVaultMaxSizeBytes(t)
	if got != want {
		t.Fatalf("resolveVaultSizeBudget policy without budget = %d, want default %d", got, want)
	}
}

// Defense in depth: an unparseable SizeBudget (should be impossible —
// PutRetentionPolicy validates at write) must never be read as 0/unbounded.
// It is skipped, same as if it were absent; with no other valid budget, the
// resolver falls back to the default floor.
func TestResolveVaultSizeBudgetParseFailureFallsBackToDefault(t *testing.T) {
	policyID := glid.New()
	garbage := "not-a-size"
	policies := []system.RetentionPolicyConfig{
		{ID: policyID, Name: "corrupt", SizeBudget: &garbage},
	}
	vc := system.VaultConfig{
		ID:             glid.New(),
		RetentionRules: []system.RetentionRule{retentionRuleFor(policyID)},
	}
	got := resolveVaultSizeBudget(vc, policies)
	want := defaultVaultMaxSizeBytes(t)
	if got != want {
		t.Fatalf("resolveVaultSizeBudget parse failure = %d, want default fallback %d", got, want)
	}
}

// A parse failure on ONE policy must not poison a valid budget from another
// attached policy — only the unparseable one is skipped.
func TestResolveVaultSizeBudgetParseFailureSkipsOnlyThatPolicy(t *testing.T) {
	good, bad := glid.New(), glid.New()
	garbage := "not-a-size"
	policies := []system.RetentionPolicyConfig{
		policyWithBudget(good, "15GiB"),
		{ID: bad, Name: "corrupt", SizeBudget: &garbage},
	}
	vc := system.VaultConfig{
		ID: glid.New(),
		RetentionRules: []system.RetentionRule{
			retentionRuleFor(good), retentionRuleFor(bad),
		},
	}
	got := resolveVaultSizeBudget(vc, policies)
	want, _ := system.ParseSize("15GiB")
	if got != want {
		t.Fatalf("resolveVaultSizeBudget with one bad policy = %d, want the surviving valid budget %d (15GiB)", got, want)
	}
}

// An unresolvable RetentionPolicyID (rule references a policy that no
// longer exists in the passed slice) contributes nothing — matches the
// "policy without budget" fallback, never a panic or an unbounded result.
func TestResolveVaultSizeBudgetUnknownPolicyIDUsesDefault(t *testing.T) {
	vc := system.VaultConfig{
		ID:             glid.New(),
		RetentionRules: []system.RetentionRule{retentionRuleFor(glid.New())},
	}
	got := resolveVaultSizeBudget(vc, nil)
	want := defaultVaultMaxSizeBytes(t)
	if got != want {
		t.Fatalf("resolveVaultSizeBudget unknown policy id = %d, want default %d", got, want)
	}
}

// TestRefreshVaultDiskGuardsCappedFromPolicyBudgetLifecycle exercises the
// resolver wired into the real guard end to end (refreshVaultDiskGuards +
// evaluateVaults, the same pairing startDiskGuard's scheduler job runs):
// a policy's size_budget caps the vault; raising the budget on the policy
// resumes admission; detaching the policy entirely falls back to the
// creation-default floor, not to unbounded.
func TestRefreshVaultDiskGuardsCappedFromPolicyBudgetLifecycle(t *testing.T) {
	t.Parallel()

	vaultID, policyID := glid.New(), glid.New()
	budget := "10GiB"
	cfg := &system.Config{
		Vaults: []system.VaultConfig{{
			ID:      vaultID,
			Name:    "capped-vault",
			Enabled: true,
			Type:    system.VaultTypeFile,
			RetentionRules: []system.RetentionRule{
				{RetentionPolicyID: policyID},
			},
		}},
		RetentionPolicies: []system.RetentionPolicyConfig{
			{ID: policyID, Name: "budget-policy", SizeBudget: &budget},
		},
	}

	orch := newTestOrch(t, Config{})
	orch.sysLoader = testSystemLoader{cfg: cfg}

	// Footprint fixed above the 10GiB policy budget but below the 1GiB
	// default — the capped/uncapped verdict below can only be explained by
	// the policy budget, not the default floor.
	const footprint = int64(11) << 30 // 11GiB
	orch.diskGuard.vaultFootprint = func(id glid.GLID) int64 {
		if id == vaultID {
			return footprint
		}
		return 0
	}

	ctx := context.Background()
	refresh := func() {
		orch.refreshVaultDiskGuards(ctx)
		orch.diskGuard.evaluateVaults(orch.alerts)
	}

	// 1. Capped state driven from the policy budget: 11GiB footprint over a
	// 10GiB policy budget.
	refresh()
	if !orch.diskGuard.vaultSizeCapped(vaultID) {
		t.Fatal("vault must be capped: 11GiB footprint exceeds the 10GiB policy budget")
	}

	// 2. Budget raised on the policy → admission resumes, same footprint.
	raised := "50GiB"
	cfg.RetentionPolicies[0].SizeBudget = &raised
	refresh()
	if orch.diskGuard.vaultSizeCapped(vaultID) {
		t.Fatal("vault must resume admission once the policy budget is raised above the footprint")
	}

	// 3. Policy detached entirely → the creation default (1GiB) is the
	// floor, NOT unbounded. The fixed 11GiB footprint is far above 1GiB, so
	// the vault must be capped again — if the fallback were "unbounded"
	// instead of the default, it would stay uncapped here.
	cfg.Vaults[0].RetentionRules = nil
	refresh()
	if !orch.diskGuard.vaultSizeCapped(vaultID) {
		t.Fatal("detaching the policy must fall back to the default floor (1GiB), not unbounded — the 11GiB footprint must cap")
	}
}
