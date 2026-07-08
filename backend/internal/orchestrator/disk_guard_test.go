package orchestrator

import (
	"errors"
	"testing"

	"gastrolog/internal/glid"
)

// fakeSampler returns a controllable free/total per path.
type fakeSampler struct {
	free  map[string]uint64
	total uint64
}

func (f *fakeSampler) sample(path string) (uint64, uint64, error) {
	if v, ok := f.free[path]; ok {
		return v, f.total, nil
	}
	return 0, 0, errors.New("no such volume")
}

func newGuardFixture(total uint64, free map[string]uint64) (*diskGuard, *fakeSampler) {
	fs := &fakeSampler{free: free, total: total}
	g := newDiskGuard([]string{"a", "b"})
	g.sample = fs.sample
	return g, fs
}

const gib = uint64(1) << 30

// TestDiskGuardLifecycle walks the full arc the incident needed: quiet when
// healthy, alarm below warn, protect below floor, hysteretic exit on both.
func TestDiskGuardLifecycle(t *testing.T) {
	t.Parallel()
	total := 400 * gib // warn at 40GiB (10%), floor at 12GiB (3%)
	fs := map[string]uint64{"a": 200 * gib, "b": 200 * gib}
	g, sampler := newGuardFixture(total, fs)
	spy := &alertSpy{}

	g.evaluate(spy)
	if spy.active() != 0 || g.protect.Load() {
		t.Fatal("healthy volume must raise nothing")
	}

	// One path (the worst) crosses warn: alarm, no protect.
	sampler.free["b"] = 30 * gib
	g.evaluate(spy)
	if spy.active() != 1 {
		t.Fatal("below warn threshold must raise the disk-space alarm")
	}
	if g.protect.Load() {
		t.Fatal("warn is not the floor: admission must stay open")
	}

	// Crossing the floor: both tiers engage, alarm text escalates.
	sampler.free["b"] = 10 * gib
	g.evaluate(spy)
	if !g.protect.Load() {
		t.Fatal("below the floor the node must stop accepting work")
	}
	if !g.deferWrites.Load() {
		t.Fatal("below the floor the drain tier must pause too")
	}

	// Just above the floor: hysteresis holds both tiers on.
	sampler.free["b"] = 13 * gib
	g.evaluate(spy)
	if !g.protect.Load() || !g.deferWrites.Load() {
		t.Fatal("protect must not flap at the boundary")
	}

	// Clear of the FLOOR band but still under WARN: staged release — the
	// drain tier resumes so the pipeline can seal backlog, but the consumer
	// tier (admission) stays held so a burst can't re-cross the floor.
	sampler.free["b"] = 20 * gib
	g.evaluate(spy)
	if g.deferWrites.Load() {
		t.Fatal("drain tier must resume once clear of the floor band")
	}
	if !g.protect.Load() {
		t.Fatal("admission must stay held until free clears the WARN band (ratchet headroom)")
	}
	if spy.active() != 1 {
		t.Fatal("still under warn: alarm must stand")
	}

	// Above the floor band but still inside the WARN band: admission STILL
	// held — the whole point of the wide asymmetric deadband.
	sampler.free["b"] = 45 * gib // warn=40GiB, admission resumes above 50GiB
	g.evaluate(spy)
	if !g.protect.Load() {
		t.Fatal("admission must remain held inside the WARN band")
	}

	// Recovery past the WARN hysteresis band: admission resumes AND alarm clears.
	sampler.free["b"] = 60 * gib
	g.evaluate(spy)
	if g.protect.Load() {
		t.Fatal("admission must resume once free clears the WARN band")
	}
	if spy.active() != 0 {
		t.Fatal("alarm must clear with hysteresis above warn")
	}
}

// TestDiskGuardWorstPathGoverns pins min-across-paths: one starved volume
// trips the guard even when the other is fine.
func TestDiskGuardWorstPathGoverns(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"a": 300 * gib, "b": 5 * gib})
	g.evaluate(nil)
	if !g.protect.Load() {
		t.Fatal("the worst volume governs")
	}
}

// TestDiskGuardUnsampleablePathsAreInert: sampling errors (unmounted,
// permissions) must not trip protect on garbage.
func TestDiskGuardUnsampleablePathsAreInert(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{})
	g.evaluate(nil)
	if g.protect.Load() {
		t.Fatal("no trustworthy sample, no protect")
	}
}

// TestDiskGuardTinyVolumeThresholds pins the clamp: on a 10GB quota volume
// the absolute byte minimums must not exceed their share ceilings — without
// the clamp the warn threshold (10GiB) exceeded the whole volume and the
// alarm latched on from boot, unclearable.
func TestDiskGuardTinyVolumeThresholds(t *testing.T) {
	t.Parallel()
	total := 10 * gib
	g, sampler := newGuardFixture(total, map[string]uint64{"a": 9 * gib, "b": 9 * gib})
	spy := &alertSpy{}

	g.evaluate(spy)
	if spy.active() != 0 || g.protect.Load() {
		t.Fatalf("near-empty tiny volume must be quiet (warn=%d floor=%d)",
			g.warnThreshold(total), g.floorThreshold(total))
	}
	if w := g.warnThreshold(total); w > total/4 {
		t.Fatalf("warn threshold %d exceeds 25%% of a %d volume", w, total)
	}

	// Fill toward the clamped floor (10% of 10GiB = 1GiB): protect trips.
	sampler.free["b"] = gib / 2
	g.evaluate(spy)
	if !g.protect.Load() {
		t.Fatal("tiny volume below its clamped floor must protect")
	}
	if spy.active() != 1 {
		t.Fatal("alarm must accompany protect")
	}
}

// TestDiskAdmissionGate pins the orchestrator-facing contract.
func TestDiskAdmissionGate(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"a": 5 * gib, "b": 5 * gib})
	o := &Orchestrator{diskGuard: g}
	if err := o.diskAdmissionGate(); err != nil {
		t.Fatalf("gate must admit before evaluation: %v", err)
	}
	g.evaluate(nil)
	if err := o.diskAdmissionGate(); !errors.Is(err, ErrDiskProtect) {
		t.Fatalf("gate must reject under protect, got %v", err)
	}
	// No guard configured: always admit.
	if err := (&Orchestrator{}).diskAdmissionGate(); err != nil {
		t.Fatalf("guardless orchestrator must admit: %v", err)
	}
}

// has reports whether an alert with the given ID is currently raised.
func (s *alertSpy) has(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.set[id]
	return ok
}

// TestDiskGuardStagedReleaseInvariant sweeps free space up from a floor
// breach and pins the ratchet-avoidance invariant: at NO recovery level is
// ingest admission open while the drain tier is still paused. Admission always
// resumes at or after the drain tier, and only above the WARN band — the wide
// asymmetric deadband that denies the release burst room to re-cross the floor.
func TestDiskGuardStagedReleaseInvariant(t *testing.T) {
	t.Parallel()
	total := 400 * gib // warn 40GiB, floor 12GiB; admission resumes above 50GiB
	g, sampler := newGuardFixture(total, map[string]uint64{"a": 200 * gib, "b": 200 * gib})

	// Breach the floor so both tiers engage.
	sampler.free["b"] = 5 * gib
	g.evaluate(nil)
	if !g.protect.Load() || !g.deferWrites.Load() {
		t.Fatal("floor breach must engage both tiers")
	}

	// Walk recovery upward in 1GiB steps through the whole band.
	sawDrainResumeWhileAdmissionHeld := false
	for free := uint64(6); free <= 70; free++ {
		sampler.free["b"] = free * gib
		g.evaluate(nil)
		admissionOpen := !g.protect.Load()
		drainOpen := !g.deferWrites.Load()

		// The invariant: admission open implies drain already open.
		if admissionOpen && !drainOpen {
			t.Fatalf("at %dGiB free admission reopened while the drain tier was still paused", free)
		}
		if drainOpen && !admissionOpen {
			sawDrainResumeWhileAdmissionHeld = true
		}
		// Admission must never reopen inside the WARN band (<= 50GiB).
		if admissionOpen && free*gib <= clearAbove(g.warnThreshold(total)) {
			t.Fatalf("at %dGiB free admission reopened at or below the WARN resume band", free)
		}
	}
	if !sawDrainResumeWhileAdmissionHeld {
		t.Fatal("staged release never observed: drain tier should reopen while admission is still held")
	}
	// Fully recovered: both open.
	if g.protect.Load() || g.deferWrites.Load() {
		t.Fatal("well above the WARN band both tiers must be open")
	}
}

// TestDiskDeferWritesGate pins the orchestrator accessor the supervisor's
// drain gate reads.
func TestDiskDeferWritesGate(t *testing.T) {
	t.Parallel()
	g, sampler := newGuardFixture(400*gib, map[string]uint64{"a": 5 * gib, "b": 5 * gib})
	o := &Orchestrator{diskGuard: g}
	if o.diskDeferWrites() {
		t.Fatal("drain gate must be open before evaluation")
	}
	g.evaluate(nil) // below floor
	if !o.diskDeferWrites() {
		t.Fatal("below the floor the drain gate must be closed")
	}
	// Recover past the floor band but stay under WARN: drain reopens, admission holds.
	sampler.free["a"], sampler.free["b"] = 20*gib, 20*gib
	g.evaluate(nil)
	if o.diskDeferWrites() {
		t.Fatal("drain gate must reopen once clear of the floor band")
	}
	if !o.diskProtectActive() {
		t.Fatal("admission must still be held inside the WARN band")
	}
	// Guardless orchestrator never defers.
	if (&Orchestrator{}).diskDeferWrites() {
		t.Fatal("guardless orchestrator must not defer writes")
	}
}

// TestVaultDiskGuardLifecycle pins the per-vault arc: a starved vault volume
// trips ONLY that vault's protect and a vault-scoped alarm, while a vault on
// a healthy volume stays open. Exits are hysteretic like the node guard.
func TestVaultDiskGuardLifecycle(t *testing.T) {
	t.Parallel()
	total := 400 * gib
	fs := map[string]uint64{"volA": 200 * gib, "volB": 200 * gib}
	g, sampler := newGuardFixture(total, fs)
	spy := &alertSpy{}

	vaultA, vaultB := glid.New(), glid.New()
	g.SetVaultGuard(vaultA, "hot", []string{"volA"}, 0, 0, 0)
	g.SetVaultGuard(vaultB, "cold", []string{"volB"}, 0, 0, 0)

	g.evaluateVaults(spy)
	if g.vaultProtectActive(vaultA) || g.vaultProtectActive(vaultB) || spy.active() != 0 {
		t.Fatal("healthy volumes must raise nothing")
	}

	// vaultA's volume crosses the floor (node-default 3%% = 12GiB): protect
	// and alarm for vaultA only.
	sampler.free["volA"] = 10 * gib
	g.evaluateVaults(spy)
	if !g.vaultProtectActive(vaultA) {
		t.Fatal("starved vault volume must protect that vault")
	}
	if g.vaultProtectActive(vaultB) {
		t.Fatal("vaultB's healthy volume must keep it open")
	}
	if !spy.has("disk-space:" + vaultA.String()) {
		t.Fatal("vault alarm must be scoped to the starved vault's ID")
	}
	if spy.has("disk-space:" + vaultB.String()) {
		t.Fatal("healthy vault must not alarm")
	}

	// Just above the floor: hysteresis holds protect.
	sampler.free["volA"] = 13 * gib
	g.evaluateVaults(spy)
	if !g.vaultProtectActive(vaultA) {
		t.Fatal("vault protect must not flap at the boundary")
	}

	// Clear of both bands: protect releases, then the alarm clears.
	sampler.free["volA"] = 60 * gib
	g.evaluateVaults(spy)
	if g.vaultProtectActive(vaultA) {
		t.Fatal("vault protect must release once clear of the floor band")
	}
	if spy.active() != 0 {
		t.Fatal("vault alarm must clear with hysteresis above warn")
	}
}

// TestVaultDiskGuardConfigOverridesThresholds pins the per-vault override:
// explicit warn/floor bytes replace the node-default fractions entirely.
func TestVaultDiskGuardConfigOverridesThresholds(t *testing.T) {
	t.Parallel()
	total := 400 * gib
	g, sampler := newGuardFixture(total, map[string]uint64{"volA": 30 * gib})
	spy := &alertSpy{}
	vaultA := glid.New()

	// Node default floor would be 12GiB; this vault demands 50GiB free.
	g.SetVaultGuard(vaultA, "greedy", []string{"volA"}, 100*gib, 50*gib, 0)
	g.evaluateVaults(spy)
	if !g.vaultProtectActive(vaultA) {
		t.Fatal("30GiB free is below the vault's 50GiB floor override")
	}

	// A modest override in the other direction: 30GiB free clears a 1GiB floor.
	sampler.free["volA"] = 30 * gib
	g.SetVaultGuard(vaultA, "modest", []string{"volA"}, 2*gib, gib, 0)
	g.evaluateVaults(spy)
	if g.vaultProtectActive(vaultA) {
		t.Fatal("30GiB free must clear a 1GiB floor override (with hysteresis)")
	}
}

// TestVaultDiskGuardRetain pins the discovery-refresh prune: entries not in
// the keep set fall out, clearing their protect verdict with them.
func TestVaultDiskGuardRetain(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": gib})
	vaultA, vaultB := glid.New(), glid.New()
	g.SetVaultGuard(vaultA, "a", []string{"volA"}, 0, 0, 0)
	g.SetVaultGuard(vaultB, "b", []string{"volA"}, 0, 0, 0)
	g.evaluateVaults(nil)
	if !g.vaultProtectActive(vaultA) || !g.vaultProtectActive(vaultB) {
		t.Fatal("both vaults share the starved volume")
	}
	g.retainVaultGuards(map[glid.GLID]bool{vaultB: true}, nil)
	if g.vaultProtectActive(vaultA) {
		t.Fatal("pruned vault must no longer report protect")
	}
	if !g.vaultProtectActive(vaultB) {
		t.Fatal("retained vault must keep its verdict")
	}
}

// TestVaultDiskGuardRetainClearsAlarm pins the prune-side alarm contract: a
// vault dropped from the guard set takes its standing alarm with it —
// nothing else would ever clear an alert for an entry no longer evaluated.
func TestVaultDiskGuardRetainClearsAlarm(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": gib})
	spy := &alertSpy{}
	vaultA := glid.New()
	g.SetVaultGuard(vaultA, "a", []string{"volA"}, 0, 0, 0)
	g.evaluateVaults(spy)
	if !spy.has("disk-space:" + vaultA.String()) {
		t.Fatal("starved vault must alarm before the prune")
	}
	g.retainVaultGuards(map[glid.GLID]bool{}, spy)
	if spy.active() != 0 {
		t.Fatal("pruning an alarmed vault must clear its alert")
	}
}

// TestVaultAdmissionGate pins the orchestrator-facing contract, including the
// peer-broadcast half: a vault protected on ANY live node is refused here.
func TestVaultAdmissionGate(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": gib, "volB": 200 * gib})
	vaultA, vaultB := glid.New(), glid.New()
	g.SetVaultGuard(vaultA, "a", []string{"volA"}, 0, 0, 0)
	g.SetVaultGuard(vaultB, "b", []string{"volB"}, 0, 0, 0)
	g.evaluateVaults(nil)

	o := &Orchestrator{diskGuard: g}
	if err := o.vaultAdmissionGate(vaultA); !errors.Is(err, ErrVaultDiskProtect) {
		t.Fatalf("locally protected vault must be refused, got %v", err)
	}
	if err := o.vaultAdmissionGate(vaultB); err != nil {
		t.Fatalf("healthy vault must be admitted: %v", err)
	}

	// Remote protect (another node's broadcast) refuses vaultB here too.
	o.SetRemoteVaultDiskProtected(func(id glid.GLID) bool { return id == vaultB })
	if err := o.vaultAdmissionGate(vaultB); !errors.Is(err, ErrVaultDiskProtect) {
		t.Fatalf("remotely protected vault must be refused, got %v", err)
	}

	// Broadcast side: only locally protected vaults are published.
	prot := o.DiskProtectedVaults()
	if len(prot) != 1 || prot[0] != vaultA {
		t.Fatalf("DiskProtectedVaults = %v, want [%s]", prot, vaultA)
	}

	// No guard, no remote fn: always admit.
	if err := (&Orchestrator{}).vaultAdmissionGate(vaultA); err != nil {
		t.Fatalf("guardless orchestrator must admit: %v", err)
	}
}

// TestVaultMaxSizeCap pins cap-and-refuse: at the budget the vault refuses
// admission and alarms at Error severity; approaching it alarms at Warning;
// draining below the budget resumes admission immediately (retention frees
// whole chunks — that chunkiness is the natural deadband), and the alarm
// clears with hysteresis below the approach threshold.
func TestVaultMaxSizeCap(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": 200 * gib})
	spy := &alertSpy{}
	vaultA, vaultB := glid.New(), glid.New()

	footprint := map[glid.GLID]int64{vaultA: int64(gib), vaultB: int64(gib)}
	g.vaultFootprint = func(id glid.GLID) int64 { return footprint[id] }
	g.SetVaultGuard(vaultA, "capped", []string{"volA"}, 0, 0, 10*gib)
	g.SetVaultGuard(vaultB, "roomy", []string{"volA"}, 0, 0, 100*gib)

	g.evaluateVaults(spy)
	if g.vaultSizeCapped(vaultA) || spy.active() != 0 {
		t.Fatal("well under budget must be quiet")
	}

	// Approach (>= 90% of 10GiB): Warning alarm, no cap.
	footprint[vaultA] = int64(9*gib + gib/2)
	g.evaluateVaults(spy)
	if g.vaultSizeCapped(vaultA) {
		t.Fatal("approach is not the cap: admission must stay open")
	}
	if !spy.has("vault-max-size:" + vaultA.String()) {
		t.Fatal("approaching the budget must raise the vault-max-size alarm")
	}

	// At the budget: capped, sibling unaffected.
	footprint[vaultA] = int64(10 * gib)
	g.evaluateVaults(spy)
	if !g.vaultSizeCapped(vaultA) {
		t.Fatal("at the budget the vault must refuse admission")
	}
	if g.vaultSizeCapped(vaultB) {
		t.Fatal("sibling vault under its own budget must be unaffected")
	}

	// One chunk drains: cap releases at once, alarm stands (still >= 90%).
	footprint[vaultA] = int64(9*gib + gib/2)
	g.evaluateVaults(spy)
	if g.vaultSizeCapped(vaultA) {
		t.Fatal("below the budget admission must resume")
	}
	if !spy.has("vault-max-size:" + vaultA.String()) {
		t.Fatal("approach alarm must stand until the hysteresis band clears")
	}

	// Well below approach - 10%: alarm clears.
	footprint[vaultA] = int64(7 * gib)
	g.evaluateVaults(spy)
	if spy.has("vault-max-size:" + vaultA.String()) {
		t.Fatal("alarm must clear once clear of the approach band")
	}
}

// TestVaultMaxSizeBudgetOnlyEntry pins the origin-node case: a vault with a
// budget but NO local placement (nothing to statfs) is still evaluated —
// origin segment backlog claims local disk everywhere records are accepted.
func TestVaultMaxSizeBudgetOnlyEntry(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{})
	vaultA := glid.New()
	g.vaultFootprint = func(glid.GLID) int64 { return int64(20 * gib) }
	g.SetVaultGuard(vaultA, "originy", nil, 0, 0, 10*gib)
	g.evaluateVaults(nil)
	if !g.vaultSizeCapped(vaultA) {
		t.Fatal("budget must be enforced even with no sampleable volume paths")
	}
}

// TestVaultAdmissionGateMaxSize pins gate ordering and the remote half for
// the size cap, mirroring TestVaultAdmissionGate for disk protect.
func TestVaultAdmissionGateMaxSize(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": 200 * gib})
	vaultA, vaultB := glid.New(), glid.New()
	g.vaultFootprint = func(id glid.GLID) int64 {
		if id == vaultA {
			return int64(11 * gib)
		}
		return int64(gib)
	}
	g.SetVaultGuard(vaultA, "a", []string{"volA"}, 0, 0, 10*gib)
	g.SetVaultGuard(vaultB, "b", []string{"volA"}, 0, 0, 10*gib)
	g.evaluateVaults(nil)

	o := &Orchestrator{diskGuard: g}
	if err := o.vaultAdmissionGate(vaultA); !errors.Is(err, ErrVaultMaxSize) {
		t.Fatalf("capped vault must be refused with ErrVaultMaxSize, got %v", err)
	}
	if err := o.vaultAdmissionGate(vaultB); err != nil {
		t.Fatalf("vault under budget must be admitted: %v", err)
	}

	// Remote cap (another node's broadcast) refuses vaultB here too.
	o.SetRemoteVaultSizeCapped(func(id glid.GLID) bool { return id == vaultB })
	if err := o.vaultAdmissionGate(vaultB); !errors.Is(err, ErrVaultMaxSize) {
		t.Fatalf("remotely capped vault must be refused, got %v", err)
	}

	// Broadcast side: only locally capped vaults are published.
	capped := o.SizeCappedVaults()
	if len(capped) != 1 || capped[0] != vaultA {
		t.Fatalf("SizeCappedVaults = %v, want [%s]", capped, vaultA)
	}
}
