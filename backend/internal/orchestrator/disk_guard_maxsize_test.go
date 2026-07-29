package orchestrator

// Coverage for gastrolog-33ul6h: resolveVaultSizeBound is the config→runtime
// resolver that replaces reading VaultConfig.MaxSize directly. It computes
// the effective per-node disk-claim bound for a file vault from the
// retention policies attached via the vault's RetentionRules: min-wins
// across every attached policy's parsed MaxSize, and NO bound when none
// carries one — there is no per-vault default (gastrolog-vl2p98).
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

// policyWithBound builds a refuse-eligible ("hard") size-bound policy —
// every caller in this file is testing the REFUSE bound resolution itself
// (min-wins, default floor, parse-failure handling), which only makes
// sense against a policy that actually contributes to it. Explicit
// Refuse:true here since refuse now defaults off (gastrolog-5yfaqj
// operator decision) — the OLD default-true assumption these tests were
// written under no longer holds, so the fixture states its intent
// directly rather than relying on an unset flag.
func policyWithBound(id glid.GLID, bound string) system.RetentionPolicyConfig {
	b := bound
	return system.RetentionPolicyConfig{ID: id, Name: "p-" + id.String(), MaxSize: &b, Refuse: new(true)}
}

// No attached retention rules at all: NO refuse bound. The volume-level
// storage thresholds (FileStorage.DiskFreeWarn / DiskFreeFloor) are what
// protect the node; a per-vault byte default used to apply here and refused
// admission, which made an unconfigured vault stricter than any configured one
// (gastrolog-vl2p98).
func TestResolveVaultSizeBoundNoRulesIsUnbounded(t *testing.T) {
	vc := system.VaultConfig{ID: glid.New(), Name: "v"}
	if got := resolveVaultSizeBound(vc, nil); got != 0 {
		t.Fatalf("resolveVaultSizeBound with no rules = %d, want 0 (no refuse bound)", got)
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

// policyWithBoundRefuse is policyWithBound plus an explicit Refuse value —
// gastrolog-5yfaqj: the tightest attached MaxSize must not win the
// instantaneous refuse bound unless its OWN policy has refuse=true.
func policyWithBoundRefuse(id glid.GLID, bound string, refuse bool) system.RetentionPolicyConfig {
	p := policyWithBound(id, bound)
	p.Refuse = &refuse
	return p
}

// TestResolveVaultSizeBoundRefuseFalseExcludedFromRefuseBound pins review
// fix C3 (refuse=false doesn't gate size): the spec example. A soft
// (refuse=false) 10GB policy plus a hard (refuse=true) 50GB policy on the
// same vault refuses at 50GB — the hard policy's OWN bound — even though
// 10GB is tighter. Drain is unaffected (each rule keeps draining its own
// bound independently; this resolver only feeds the guard's instantaneous
// refuse threshold).
func TestResolveVaultSizeBoundRefuseFalseExcludedFromRefuseBound(t *testing.T) {
	soft, hard := glid.New(), glid.New()
	policies := []system.RetentionPolicyConfig{
		policyWithBoundRefuse(soft, "10GB", false),
		policyWithBoundRefuse(hard, "50GB", true),
	}
	vc := system.VaultConfig{
		ID: glid.New(),
		RetentionRules: []system.RetentionRule{
			retentionRuleFor(soft), retentionRuleFor(hard),
		},
	}
	got := resolveVaultSizeBound(vc, policies)
	want, _ := system.ParseSize("50GB")
	if got != want {
		t.Fatalf("resolveVaultSizeBound(soft 10GB + hard 50GB) = %d, want %d (50GB — the tighter soft bound must not contribute to refusal)", got, want)
	}
}

// TestResolveVaultSizeBoundAllSoftMeansNoRefuseBoundNoFloor pins the
// corner case: every attached policy that states a size bound is soft
// (refuse=false). No refuse bound applies — and the refuse-only
// creation-default floor must NOT re-engage either, since that would
// silently override the operator's explicit opt-out on every stating
// policy with a bound they never asked for.
func TestResolveVaultSizeBoundAllSoftMeansNoRefuseBoundNoFloor(t *testing.T) {
	soft := glid.New()
	policies := []system.RetentionPolicyConfig{
		policyWithBoundRefuse(soft, "10GB", false),
	}
	vc := system.VaultConfig{
		ID:             glid.New(),
		RetentionRules: []system.RetentionRule{retentionRuleFor(soft)},
	}
	got, source := resolveVaultSizeBoundSource(vc, policies)
	if got != 0 {
		t.Fatalf("resolveVaultSizeBound(soft-only) = %d, want 0 (no refuse bound)", got)
	}
	if strings.Contains(source, "default floor") {
		t.Fatalf("source = %q must NOT be the default floor — the operator explicitly opted the stating policy out of refusal", source)
	}
}

// TestUnsetRefusePolicyWithMaxSizeDrainsNotRefusesNoFloor is the operator's
// default-flip pin (gastrolog-5yfaqj): a policy that states max_size but
// leaves Refuse UNSET (nil, not explicit false) must:
//  1. still DRAIN — ToRetentionPolicy builds the same SizeRetentionPolicy
//     it always has, since drain never reads Refuse at all;
//  2. NOT contribute to the guard's instantaneous refuse bound — nil now
//     reads as false (RefuseEnabled()), so this policy is excluded from
//     attachedSizeBound's winner search exactly like an explicit
//     refuse=false policy;
//  3. NOT re-engage the refuse-only creation-default floor either — the
//     floor applies only when NO policy STATES a size at all, and this
//     one does state one, just without opting into refusal.
//
// This is the direct behavior change from the flip: before, an unset
// Refuse on a max_size policy silently refused (nil read as true); now it
// silently drains only, and an operator who wants the old behavior must
// set refuse=true explicitly.
func TestUnsetRefusePolicyWithMaxSizeDrainsNotRefusesNoFloor(t *testing.T) {
	t.Parallel()
	policyID := glid.New()
	bound := "10GB"
	// Refuse deliberately left nil — NOT policyWithBound (which sets
	// Refuse:true) and NOT policyWithBoundRefuse(..., false) either: this
	// pins the true zero-value/unset case, not an explicit opt-out.
	policy := system.RetentionPolicyConfig{ID: policyID, Name: "unset-refuse", MaxSize: &bound}
	if policy.Refuse != nil {
		t.Fatal("fixture setup: Refuse must be nil (unset), not explicitly set")
	}
	if policy.RefuseEnabled() {
		t.Fatal("fixture setup: an unset Refuse must read as false after the default flip")
	}

	// 1. Drain: ToRetentionPolicy must still build a usable drain policy —
	// unaffected by Refuse, which it never reads.
	drainPolicy, err := policy.ToRetentionPolicy()
	if err != nil {
		t.Fatalf("ToRetentionPolicy: %v", err)
	}
	if drainPolicy == nil {
		t.Fatal("a policy with max_size set must still produce a drain trigger, unset Refuse or not")
	}

	// 2 & 3. No refuse bound, no floor re-engagement.
	vc := system.VaultConfig{
		ID:             glid.New(),
		RetentionRules: []system.RetentionRule{retentionRuleFor(policyID)},
	}
	got, source := resolveVaultSizeBoundSource(vc, []system.RetentionPolicyConfig{policy})
	if got != 0 {
		t.Fatalf("resolveVaultSizeBound(unset-refuse max_size) = %d, want 0 (drains only, no refuse bound)", got)
	}
	if strings.Contains(source, "default floor") {
		t.Fatalf("source = %q must NOT be the default floor — the policy DOES state a size, just without opting into refusal", source)
	}
}

// A policy attached but with no MaxSize contributes nothing to the SIZE bound.
// This is the shape the dev cluster ran into: an age-only policy (max_age, no
// max_size) is a complete, working retention configuration, and it must not
// produce a size refuse bound the operator never asked for.
func TestResolveVaultSizeBoundPolicyWithoutMaxSizeIsUnbounded(t *testing.T) {
	policyID := glid.New()
	policies := []system.RetentionPolicyConfig{
		{ID: policyID, Name: "age-only", MaxAge: strPtr("7d")}, // no MaxSize
	}
	vc := system.VaultConfig{
		ID:             glid.New(),
		RetentionRules: []system.RetentionRule{retentionRuleFor(policyID)},
	}
	if got := resolveVaultSizeBound(vc, policies); got != 0 {
		t.Fatalf("resolveVaultSizeBound with an age-only policy = %d, want 0 (no size bound)", got)
	}
}

// An unparseable MaxSize (should be impossible — PutRetentionPolicy validates
// at write) is skipped, same as if it were absent. With no other valid bound
// the result is no bound: with the floor gone there is nothing to fall back TO,
// and inventing one here would resurrect exactly the behaviour vl2p98 removed.
func TestResolveVaultSizeBoundParseFailureIsUnbounded(t *testing.T) {
	policyID := glid.New()
	garbage := "not-a-size"
	policies := []system.RetentionPolicyConfig{
		{ID: policyID, Name: "corrupt", MaxSize: &garbage},
	}
	vc := system.VaultConfig{
		ID:             glid.New(),
		RetentionRules: []system.RetentionRule{retentionRuleFor(policyID)},
	}
	if got := resolveVaultSizeBound(vc, policies); got != 0 {
		t.Fatalf("resolveVaultSizeBound with an unparseable max_size = %d, want 0", got)
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

// An unresolvable RetentionPolicyID (rule references a policy absent from the
// passed slice) contributes nothing, and must not panic.
func TestResolveVaultSizeBoundUnknownPolicyIDIsUnbounded(t *testing.T) {
	vc := system.VaultConfig{
		ID:             glid.New(),
		RetentionRules: []system.RetentionRule{retentionRuleFor(glid.New())},
	}
	if got := resolveVaultSizeBound(vc, nil); got != 0 {
		t.Fatalf("resolveVaultSizeBound with an unknown policy id = %d, want 0", got)
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
			// Refuse:true explicit — refuse now defaults off
			// (gastrolog-5yfaqj); this test is about the size-cap
			// lifecycle itself, so the fixture states its intent.
			{ID: policyID, Name: "bound-policy", MaxSize: &bound, Refuse: new(true)},
		},
	}

	orch := newTestOrch(t, Config{})
	orch.setSystemLoader(testSystemLoader{cfg: cfg})

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

	// 3. Policy detached entirely → NO size bound (gastrolog-vl2p98). The
	// 11GiB footprint must NOT cap: with no stated bound the vault is
	// bounded by its volume's free-space thresholds, not by a per-vault
	// default. This assertion is the inverse of what it was — the old floor
	// capped here, which is precisely the behaviour that refused a live
	// cluster's ingestion on an axis its operator never configured.
	cfg.Vaults[0].RetentionRules = nil
	refresh()
	if orch.diskGuard.vaultSizeCapped(vaultID) {
		t.Fatal("detaching every policy must leave the vault unbounded on size, not fall back to a per-vault default")
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
			// Refuse:true explicit — refuse now defaults off
			// (gastrolog-5yfaqj); without it this policy is not a
			// refuse-eligible "winner" at all and the bound resolves to
			// the soft-only no-refuse-bound path instead of the
			// policy-sourced bound this test pins the log text for.
			{ID: policyID, Name: "bound-policy", MaxSize: &bound, Refuse: new(true)},
		},
	}

	logSink := &syncBuffer{}
	orch := newTestOrch(t, Config{Logger: slog.New(slog.NewTextHandler(logSink, nil))})
	orch.setSystemLoader(testSystemLoader{cfg: cfg})
	ctx := context.Background()

	const changeMsg = "vault max-size bound changed"

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

	// 5. Detach the policy entirely. The bound resolves to 0 and this vault
	// has no local storage placement, so there is nothing left for this node
	// to guard: refreshVaultDiskGuards skips it and retainVaultGuards retires
	// the entry (clearing any standing cap/alarm with it).
	//
	// No additional log line, and that is right rather than a gap — the line
	// exists to flag a change in ADMISSION behaviour on this node, and a vault
	// this node neither bounds nor stores has none. Before gastrolog-vl2p98
	// this branch was unreachable: the floor meant maxSize was never 0, so
	// every vault always registered.
	cfg.Vaults[0].RetentionRules = nil
	orch.refreshVaultDiskGuards(ctx)
	if got := strings.Count(logSink.String(), changeMsg); got != 1 {
		t.Fatalf("retiring an unbounded, unplaced vault must not log a bound change, got %d:\n%s", got, logSink.String())
	}
	if strings.Contains(logSink.String(), "default floor") {
		t.Errorf("no log may still mention a default floor:\n%s", logSink.String())
	}
	if _, existed := orch.diskGuard.currentMaxSizeBytes(vaultID); existed {
		t.Error("a vault with no bound and no local storage must not stay registered with the disk guard")
	}
}

// The dev-cluster incident, pinned end to end (gastrolog-vl2p98).
//
// first-vault attached one policy stating only max_age: 3m — a complete,
// working, age-based retention configuration. Because no attached policy stated
// a SIZE, a 1GiB creation floor engaged and refused admission at 25.8 GiB used,
// on all four nodes, on an axis the operator never configured and could not see
// in their config.
//
// The guard must register no cap for this vault at all.
func TestAgeOnlyPolicyNeverProducesASizeRefuseBound(t *testing.T) {
	t.Parallel()
	policyID := glid.New()
	age := "3m"
	policies := []system.RetentionPolicyConfig{
		{ID: policyID, Name: "3m-retain", MaxAge: &age},
	}
	vc := system.VaultConfig{
		ID:             glid.New(),
		Name:           "first-vault",
		Type:           system.VaultTypeFile,
		RetentionRules: []system.RetentionRule{retentionRuleFor(policyID)},
	}

	bound, source := resolveVaultSizeBoundSource(vc, policies)
	if bound != 0 {
		t.Fatalf("an age-only policy produced a %d-byte size refuse bound (source %q); "+
			"the operator configured retention by age and must not be refused on size", bound, source)
	}
	if strings.Contains(strings.ToLower(source), "floor") {
		t.Errorf("bound source = %q; no floor may remain in the resolution", source)
	}
}
