package orchestrator

// Coverage for gastrolog-33ul6h: resolveVaultSizeBound is the config→runtime
// resolver that replaces reading VaultConfig.MaxSize directly. It computes
// the effective per-node disk-claim bound for a file vault from the
// retention policies attached via the vault's RetentionRules: min-wins
// across every attached policy's parsed MaxSize, with
// system.DefaultVaultMaxSize as the refuse-only floor when none carries one.
// The guard's own seam (SetVaultGuard / maxSizeBytes) is unchanged — these
// tests pin only the resolver that feeds it a number.
//
// Operator correction (2026-07-19, gastrolog-33ul6h comment c2): the earlier
// shape of this branch split the vault's size story into a MaxSize drain
// trigger and a separate refuse-bound field carried on the same policy.
// That was superseded before merge: MaxSize is now the ONE field and means
// both things at once — it drains AND refuses at the same bound.
// "Bound-only" (a refuse bound with no drain trigger) is no longer a
// concept: a policy that sets only MaxSize is simply a drain policy.

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

func retentionRuleFor(policyID glid.GLID) system.RetentionRule {
	return system.RetentionRule{RetentionPolicyID: policyID}
}

func policyWithBound(id glid.GLID, bound string) system.RetentionPolicyConfig {
	b := bound
	return system.RetentionPolicyConfig{ID: id, Name: "p-" + id.String(), MaxSize: &b}
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
func TestResolveVaultSizeBoundNoRulesUsesDefault(t *testing.T) {
	vc := system.VaultConfig{ID: glid.New(), Name: "v"}
	got := resolveVaultSizeBound(vc, nil)
	want := defaultVaultMaxSizeBytes(t)
	if got != want {
		t.Fatalf("resolveVaultSizeBound with no rules = %d, want default %d", got, want)
	}
}

// A single attached policy with a max_size is used directly.
func TestResolveVaultSizeBoundSinglePolicy(t *testing.T) {
	policyID := glid.New()
	policies := []system.RetentionPolicyConfig{policyWithBound(policyID, "20GiB")}
	vc := system.VaultConfig{
		ID:             glid.New(),
		RetentionRules: []system.RetentionRule{retentionRuleFor(policyID)},
	}
	got := resolveVaultSizeBound(vc, policies)
	want, _ := system.ParseSize("20GiB")
	if got != want {
		t.Fatalf("resolveVaultSizeBound single policy = %d, want %d (20GiB)", got, want)
	}
}

// Multiple attached policies: the effective bound is the MINIMUM across
// every parsed MaxSize — the operator decision that the refuse bound is the
// tightest constraint among a vault's attached policies.
func TestResolveVaultSizeBoundMinWinsAcrossPolicies(t *testing.T) {
	loose, tight, mid := glid.New(), glid.New(), glid.New()
	policies := []system.RetentionPolicyConfig{
		policyWithBound(loose, "500GiB"),
		policyWithBound(tight, "10GiB"),
		policyWithBound(mid, "100GiB"),
	}
	vc := system.VaultConfig{
		ID: glid.New(),
		RetentionRules: []system.RetentionRule{
			retentionRuleFor(loose), retentionRuleFor(tight), retentionRuleFor(mid),
		},
	}
	got := resolveVaultSizeBound(vc, policies)
	want, _ := system.ParseSize("10GiB")
	if got != want {
		t.Fatalf("resolveVaultSizeBound min-wins = %d, want %d (10GiB, the tightest)", got, want)
	}
}

// A policy attached but with no MaxSize set at all contributes nothing —
// falls through to the default floor, same as no rules.
func TestResolveVaultSizeBoundPolicyWithoutMaxSizeUsesDefault(t *testing.T) {
	policyID := glid.New()
	policies := []system.RetentionPolicyConfig{
		{ID: policyID, Name: "age-only", MaxAge: strPtr("7d")}, // no MaxSize
	}
	vc := system.VaultConfig{
		ID:             glid.New(),
		RetentionRules: []system.RetentionRule{retentionRuleFor(policyID)},
	}
	got := resolveVaultSizeBound(vc, policies)
	want := defaultVaultMaxSizeBytes(t)
	if got != want {
		t.Fatalf("resolveVaultSizeBound policy without max_size = %d, want default %d", got, want)
	}
}

// Defense in depth: an unparseable MaxSize (should be impossible —
// PutRetentionPolicy validates at write) must never be read as 0/unbounded.
// It is skipped, same as if it were absent; with no other valid bound, the
// resolver falls back to the default floor.
func TestResolveVaultSizeBoundParseFailureFallsBackToDefault(t *testing.T) {
	policyID := glid.New()
	garbage := "not-a-size"
	policies := []system.RetentionPolicyConfig{
		{ID: policyID, Name: "corrupt", MaxSize: &garbage},
	}
	vc := system.VaultConfig{
		ID:             glid.New(),
		RetentionRules: []system.RetentionRule{retentionRuleFor(policyID)},
	}
	got := resolveVaultSizeBound(vc, policies)
	want := defaultVaultMaxSizeBytes(t)
	if got != want {
		t.Fatalf("resolveVaultSizeBound parse failure = %d, want default fallback %d", got, want)
	}
}

// A parse failure on ONE policy must not poison a valid bound from another
// attached policy — only the unparseable one is skipped.
func TestResolveVaultSizeBoundParseFailureSkipsOnlyThatPolicy(t *testing.T) {
	good, bad := glid.New(), glid.New()
	garbage := "not-a-size"
	policies := []system.RetentionPolicyConfig{
		policyWithBound(good, "15GiB"),
		{ID: bad, Name: "corrupt", MaxSize: &garbage},
	}
	vc := system.VaultConfig{
		ID: glid.New(),
		RetentionRules: []system.RetentionRule{
			retentionRuleFor(good), retentionRuleFor(bad),
		},
	}
	got := resolveVaultSizeBound(vc, policies)
	want, _ := system.ParseSize("15GiB")
	if got != want {
		t.Fatalf("resolveVaultSizeBound with one bad policy = %d, want the surviving valid bound %d (15GiB)", got, want)
	}
}

// An unresolvable RetentionPolicyID (rule references a policy that no
// longer exists in the passed slice) contributes nothing — matches the
// "policy without max_size" fallback, never a panic or an unbounded result.
func TestResolveVaultSizeBoundUnknownPolicyIDUsesDefault(t *testing.T) {
	vc := system.VaultConfig{
		ID:             glid.New(),
		RetentionRules: []system.RetentionRule{retentionRuleFor(glid.New())},
	}
	got := resolveVaultSizeBound(vc, nil)
	want := defaultVaultMaxSizeBytes(t)
	if got != want {
		t.Fatalf("resolveVaultSizeBound unknown policy id = %d, want default %d", got, want)
	}
}

// TestRefreshVaultDiskGuardsCappedFromPolicyMaxSizeLifecycle exercises the
// resolver wired into the real guard end to end (refreshVaultDiskGuards +
// evaluateVaults, the same pairing startDiskGuard's scheduler job runs): a
// policy's max_size caps the vault; raising it on the policy resumes
// admission; detaching the policy entirely falls back to the
// creation-default floor, not to unbounded.
func TestRefreshVaultDiskGuardsCappedFromPolicyMaxSizeLifecycle(t *testing.T) {
	t.Parallel()

	vaultID, policyID := glid.New(), glid.New()
	bound := "10GiB"
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
			{ID: policyID, Name: "bound-policy", MaxSize: &bound},
		},
	}

	orch := newTestOrch(t, Config{})
	orch.sysLoader = testSystemLoader{cfg: cfg}

	// Footprint fixed above the 10GiB policy bound but below the 1GiB
	// default — the capped/uncapped verdict below can only be explained by
	// the policy bound, not the default floor.
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

	// 1. Capped state driven from the policy bound: 11GiB footprint over a
	// 10GiB policy bound.
	refresh()
	if !orch.diskGuard.vaultSizeCapped(vaultID) {
		t.Fatal("vault must be capped: 11GiB footprint exceeds the 10GiB policy bound")
	}

	// 2. Bound raised on the policy → admission resumes, same footprint.
	raised := "50GiB"
	cfg.RetentionPolicies[0].MaxSize = &raised
	refresh()
	if orch.diskGuard.vaultSizeCapped(vaultID) {
		t.Fatal("vault must resume admission once the policy bound is raised above the footprint")
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

// TestRefreshVaultDiskGuardsLogsOnBoundChangeOnly pins gastrolog-33ul6h
// finding 4: an effective-bound CHANGE (the resolved max-size differs from
// what was previously registered for this vault) logs exactly once at INFO,
// naming old, new, and the source ("policy <name/id>" or "default floor").
// First observation (nothing registered yet) is not a transition and must
// not log; re-resolving the SAME bound on every subsequent tick (the
// steady-state case — refreshVaultDiskGuards runs on every 15s guard tick)
// must never log again either.
func TestRefreshVaultDiskGuardsLogsOnBoundChangeOnly(t *testing.T) {
	t.Parallel()

	vaultID, policyID := glid.New(), glid.New()
	bound := "10GiB"
	cfg := &system.Config{
		Vaults: []system.VaultConfig{{
			ID:      vaultID,
			Name:    "bound-vault",
			Enabled: true,
			Type:    system.VaultTypeFile,
			RetentionRules: []system.RetentionRule{
				{RetentionPolicyID: policyID},
			},
		}},
		RetentionPolicies: []system.RetentionPolicyConfig{
			{ID: policyID, Name: "bound-policy", MaxSize: &bound},
		},
	}

	logSink := &syncBuffer{}
	orch := newTestOrch(t, Config{Logger: slog.New(slog.NewTextHandler(logSink, nil))})
	orch.sysLoader = testSystemLoader{cfg: cfg}
	ctx := context.Background()

	const changeMsg = "vault size budget changed"

	// 1. First observation: no prior entry, so no transition to log.
	orch.refreshVaultDiskGuards(ctx)
	if strings.Contains(logSink.String(), changeMsg) {
		t.Fatalf("first observation must not log a bound change:\n%s", logSink.String())
	}

	// 2. Steady state: same policy, same resolved bound, repeated ticks.
	orch.refreshVaultDiskGuards(ctx)
	orch.refreshVaultDiskGuards(ctx)
	if strings.Contains(logSink.String(), changeMsg) {
		t.Fatalf("re-resolving an unchanged bound must never log:\n%s", logSink.String())
	}

	// 3. Raise the policy's bound: a real transition.
	raised := "50GiB"
	cfg.RetentionPolicies[0].MaxSize = &raised
	orch.refreshVaultDiskGuards(ctx)
	if got := strings.Count(logSink.String(), changeMsg); got != 1 {
		t.Fatalf("a bound change must log exactly once, got %d:\n%s", got, logSink.String())
	}
	if !strings.Contains(logSink.String(), "policy bound-policy") {
		t.Errorf("the log must name the source policy:\n%s", logSink.String())
	}

	// 4. Steady state again after the change: no additional log line.
	orch.refreshVaultDiskGuards(ctx)
	orch.refreshVaultDiskGuards(ctx)
	if got := strings.Count(logSink.String(), changeMsg); got != 1 {
		t.Fatalf("steady state after the change must not add another log line, got %d:\n%s", got, logSink.String())
	}

	// 5. Detach the policy entirely: falls back to the default floor — a
	// second real transition, source now "default floor".
	cfg.Vaults[0].RetentionRules = nil
	orch.refreshVaultDiskGuards(ctx)
	if got := strings.Count(logSink.String(), changeMsg); got != 2 {
		t.Fatalf("falling back to the default floor is a real change, want 2 total log lines, got %d:\n%s", got, logSink.String())
	}
	if !strings.Contains(logSink.String(), "default floor") {
		t.Errorf("the log must name the fallback source:\n%s", logSink.String())
	}
}
