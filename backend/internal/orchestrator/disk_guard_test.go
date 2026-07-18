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

	// Crossing the floor: both gates engage, alarm text escalates.
	sampler.free["b"] = 10 * gib
	g.evaluate(spy)
	if !g.protect.Load() {
		t.Fatal("below the floor the node must stop accepting work")
	}
	if !g.deferWrites.Load() {
		t.Fatal("below the floor the drain gate must pause too")
	}

	// Just above the floor: hysteresis holds both gates on.
	sampler.free["b"] = 13 * gib
	g.evaluate(spy)
	if !g.protect.Load() || !g.deferWrites.Load() {
		t.Fatal("protect must not flap at the boundary")
	}

	// Clear of the FLOOR band but still under WARN: staged release — the
	// drain gate resumes so the pipeline can seal backlog, but the consumer
	// gate (admission) stays held so a burst can't re-cross the floor.
	sampler.free["b"] = 20 * gib
	g.evaluate(spy)
	if g.deferWrites.Load() {
		t.Fatal("drain gate must resume once clear of the floor band")
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

// TestDiskGuardTinyVolumeThresholds pins that the percentage defaults scale
// with the volume: a 10GiB test volume carries a 1GiB warn / ~0.3GiB floor,
// never a threshold larger than the volume itself (the failure the old
// absolute byte minimums had before their clamps, and the whole curve, were
// replaced by the typeable "10%"/"3%" defaults).
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
	if w := g.warnThreshold(total); w != total/10 {
		t.Fatalf("warn threshold %d, want 10%% of a %d volume", w, total)
	}
	if f := g.floorThreshold(total); f != 3*total/100 {
		t.Fatalf("floor threshold %d, want 3%% of a %d volume", f, total)
	}

	// Fill below the scaled floor (3% of 10GiB ≈ 0.3GiB): protect trips.
	sampler.free["b"] = gib / 4
	g.evaluate(spy)
	if !g.protect.Load() {
		t.Fatal("tiny volume below its scaled floor must protect")
	}
	if spy.active() != 1 {
		t.Fatal("alarm must accompany protect")
	}
}

// TestDiskGuardDefaultsAreTypeable pins the typeable-defaults contract: the
// node defaults are the expressions "10%"/"3%" — values an operator could
// enter in a disk-free field — resolved against the volume through the same
// shared resolver an explicit value uses.
func TestDiskGuardDefaultsAreTypeable(t *testing.T) {
	t.Parallel()
	g := newDiskGuard(nil)
	if g.warnExpr != defaultDiskFreeWarn || g.floorExpr != defaultDiskFreeFloor {
		t.Fatalf("node expressions = %q/%q, want the typeable defaults %q/%q",
			g.warnExpr, g.floorExpr, defaultDiskFreeWarn, defaultDiskFreeFloor)
	}
	total := 400 * gib
	if w := g.warnThreshold(total); w != 40*gib {
		t.Fatalf("warn threshold on 400GiB = %d, want 40GiB (10%%)", w)
	}
	if f := g.floorThreshold(total); f != 12*gib {
		t.Fatalf("floor threshold on 400GiB = %d, want 12GiB (3%%)", f)
	}
}

// TestDiskGuardEnvOverride pins the .env operator channel: the node-level
// expressions accept the same size-or-percent vocabulary as the config
// fields, and a malformed value is ignored in favor of the defaults.
func TestDiskGuardEnvOverride(t *testing.T) { //nolint:paralleltest // t.Setenv
	t.Setenv("GLOG_DISK_FREE_WARN", "20%")
	t.Setenv("GLOG_DISK_FREE_FLOOR", "5GiB")
	g := newDiskGuard(nil)
	total := 100 * gib
	if w := g.warnThreshold(total); w != 20*gib {
		t.Fatalf("env warn 20%% of 100GiB = %d, want 20GiB", w)
	}
	if f := g.floorThreshold(total); f != 5*gib {
		t.Fatalf("env floor 5GiB = %d, want 5GiB regardless of volume", f)
	}

	t.Setenv("GLOG_DISK_FREE_WARN", "max(10%, 10GiB)") // not typeable ⇒ not parseable
	g = newDiskGuard(nil)
	if w := g.warnThreshold(total); w != 10*gib {
		t.Fatalf("malformed env override must fall back to the default 10%%: got %d", w)
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
// ingest admission open while the drain gate is still paused. Admission always
// resumes at or after the drain gate, and only above the WARN band — the wide
// asymmetric deadband that denies the release burst room to re-cross the floor.
func TestDiskGuardStagedReleaseInvariant(t *testing.T) {
	t.Parallel()
	total := 400 * gib // warn 40GiB, floor 12GiB; admission resumes above 50GiB
	g, sampler := newGuardFixture(total, map[string]uint64{"a": 200 * gib, "b": 200 * gib})

	// Breach the floor so both gates engage.
	sampler.free["b"] = 5 * gib
	g.evaluate(nil)
	if !g.protect.Load() || !g.deferWrites.Load() {
		t.Fatal("floor breach must engage both gates")
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
			t.Fatalf("at %dGiB free admission reopened while the drain gate was still paused", free)
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
		t.Fatal("staged release never observed: drain gate should reopen while admission is still held")
	}
	// Fully recovered: both open.
	if g.protect.Load() || g.deferWrites.Load() {
		t.Fatal("well above the WARN band both gates must be open")
	}
}

// TestPrimeDiskGuardClosesBootWindow pins the boot blind-window fix: a node
// starting on an already-full volume must be in protect BEFORE admission
// opens, from the synchronous prime pass — not only after the first 15s tick.
func TestPrimeDiskGuardClosesBootWindow(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"a": 5 * gib, "b": 5 * gib})
	o := &Orchestrator{diskGuard: g}

	// Before any pass: admission open (nothing sampled yet).
	if o.diskProtectActive() {
		t.Fatal("no pass yet: admission should be open")
	}
	// Prime (what Start does before pipeline.Start): full volume must close it.
	o.primeDiskGuard()
	if !o.diskProtectActive() {
		t.Fatal("prime must engage protect on a full volume before admission opens")
	}
	if !o.diskDeferWrites() {
		t.Fatal("prime must also pause the drain gate below the floor")
	}
	// Guardless / pathless orchestrators prime to a no-op without panicking.
	(&Orchestrator{}).primeDiskGuard()
	(&Orchestrator{diskGuard: newDiskGuard(nil)}).primeDiskGuard()
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
	g.SetVaultGuard(vaultA, "hot", []string{"volA"}, "", "", 0)
	g.SetVaultGuard(vaultB, "cold", []string{"volB"}, "", "", 0)

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
	if !spy.has("disk-space-exhausted:" + vaultA.String()) {
		t.Fatal("vault alarm must be scoped to the starved vault's ID")
	}
	if spy.has("disk-space-exhausted:" + vaultB.String()) {
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
// an explicit expression — absolute size or percentage of the volume —
// replaces the node-default expressions entirely.
func TestVaultDiskGuardConfigOverridesThresholds(t *testing.T) {
	t.Parallel()
	total := 400 * gib
	g, sampler := newGuardFixture(total, map[string]uint64{"volA": 30 * gib})
	spy := &alertSpy{}
	vaultA := glid.New()

	// Node default floor would be 12GiB; this vault demands 50GiB free.
	g.SetVaultGuard(vaultA, "greedy", []string{"volA"}, "100GiB", "50GiB", 0)
	g.evaluateVaults(spy)
	if !g.vaultProtectActive(vaultA) {
		t.Fatal("30GiB free is below the vault's 50GiB floor override")
	}

	// A modest override in the other direction: 30GiB free clears a 1GiB floor.
	sampler.free["volA"] = 30 * gib
	g.SetVaultGuard(vaultA, "modest", []string{"volA"}, "2GiB", "1GiB", 0)
	g.evaluateVaults(spy)
	if g.vaultProtectActive(vaultA) {
		t.Fatal("30GiB free must clear a 1GiB floor override (with hysteresis)")
	}

	// A percentage override resolves against the vault's own volume: 20% of
	// 400GiB = 80GiB floor, so 30GiB free is a breach the node default
	// (3% = 12GiB) would not see.
	g.SetVaultGuard(vaultA, "percenty", []string{"volA"}, "25%", "20%", 0)
	g.evaluateVaults(spy)
	if !g.vaultProtectActive(vaultA) {
		t.Fatal(`30GiB free is below the vault's "20%" floor override`)
	}
	// And a small percentage clears again: floor 1% = 4GiB, resume above the
	// 2% warn band (with hysteresis) — 30GiB is well clear.
	g.SetVaultGuard(vaultA, "percenty", []string{"volA"}, "2%", "1%", 0)
	g.evaluateVaults(spy)
	if g.vaultProtectActive(vaultA) {
		t.Fatal(`30GiB free must clear a "1%" floor override`)
	}
}

// TestVaultDiskGuardRetain pins the discovery-refresh prune: entries not in
// the keep set fall out, clearing their protect verdict with them.
func TestVaultDiskGuardRetain(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": gib})
	vaultA, vaultB := glid.New(), glid.New()
	g.SetVaultGuard(vaultA, "a", []string{"volA"}, "", "", 0)
	g.SetVaultGuard(vaultB, "b", []string{"volA"}, "", "", 0)
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
	g.SetVaultGuard(vaultA, "a", []string{"volA"}, "", "", 0)
	g.evaluateVaults(spy)
	if !spy.has("disk-space-exhausted:" + vaultA.String()) {
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
	g.SetVaultGuard(vaultA, "a", []string{"volA"}, "", "", 0)
	g.SetVaultGuard(vaultB, "b", []string{"volB"}, "", "", 0)
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
	g.SetVaultGuard(vaultA, "capped", []string{"volA"}, "", "", 10*gib)
	g.SetVaultGuard(vaultB, "roomy", []string{"volA"}, "", "", 100*gib)

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
	if !spy.has("vault-max-size-approaching:" + vaultA.String()) {
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
	if !spy.has("vault-max-size-approaching:" + vaultA.String()) {
		t.Fatal("approach alarm must stand until the hysteresis band clears")
	}

	// Well below approach - 10%: alarm clears.
	footprint[vaultA] = int64(7 * gib)
	g.evaluateVaults(spy)
	if spy.has("vault-max-size-approaching:" + vaultA.String()) {
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
	g.SetVaultGuard(vaultA, "originy", nil, "", "", 10*gib)
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
	g.SetVaultGuard(vaultA, "a", []string{"volA"}, "", "", 10*gib)
	g.SetVaultGuard(vaultB, "b", []string{"volA"}, "", "", 10*gib)
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

// TestVaultBacklogBudget pins the backlog operating bound (the R2 half of the
// backpressure design): a vault whose unreleased registry bytes reach the
// cluster-global budget refuses admission at Error severity; approaching it
// alarms at Warning; chunking draining below the budget resumes admission
// immediately (releases free whole segments — natural deadband), and the alarm
// clears with hysteresis below the approach threshold.
func TestVaultBacklogBudget(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": 200 * gib})
	spy := &alertSpy{}
	vaultA, vaultB := glid.New(), glid.New()

	backlog := map[glid.GLID]int64{vaultA: int64(gib), vaultB: int64(gib)}
	g.vaultBacklogBytes = func(id glid.GLID) int64 { return backlog[id] }
	g.backlogBudget.Store(10 * gib)
	g.SetVaultGuard(vaultA, "busy", []string{"volA"}, "", "", 0)
	g.SetVaultGuard(vaultB, "calm", []string{"volA"}, "", "", 0)

	g.evaluateVaults(spy)
	if g.vaultBacklogCapped(vaultA) || spy.active() != 0 {
		t.Fatal("well under budget must be quiet")
	}

	// Approach (>= 90% of 10GiB): Warning alarm, no cap.
	backlog[vaultA] = int64(9*gib + gib/2)
	g.evaluateVaults(spy)
	if g.vaultBacklogCapped(vaultA) {
		t.Fatal("approach is not the cap: admission must stay open")
	}
	if !spy.has("pipeline-backlog-approaching:" + vaultA.String()) {
		t.Fatal("approaching the budget must raise the pipeline-backlog alarm")
	}

	// At the budget: capped, sibling unaffected.
	backlog[vaultA] = int64(10 * gib)
	g.evaluateVaults(spy)
	if !g.vaultBacklogCapped(vaultA) {
		t.Fatal("at the budget the vault must refuse admission")
	}
	if g.vaultBacklogCapped(vaultB) {
		t.Fatal("sibling vault under budget must be unaffected")
	}

	// Chunking releases segments: cap releases at once, alarm stands (>= 90%).
	backlog[vaultA] = int64(9*gib + gib/2)
	g.evaluateVaults(spy)
	if g.vaultBacklogCapped(vaultA) {
		t.Fatal("below the budget admission must resume")
	}
	if !spy.has("pipeline-backlog-approaching:" + vaultA.String()) {
		t.Fatal("approach alarm must stand until the hysteresis band clears")
	}

	// Well below approach - 10%: alarm clears.
	backlog[vaultA] = int64(7 * gib)
	g.evaluateVaults(spy)
	if spy.has("pipeline-backlog-approaching:" + vaultA.String()) {
		t.Fatal("alarm must clear once clear of the approach band")
	}
}

// TestVaultBacklogBudgetDisabled pins the 0 = unbounded contract, including
// the operator turning the budget OFF while a vault is capped: the standing
// cap and alarm must release on the next pass, or admission would be refused
// forever under a bound that no longer exists.
func TestVaultBacklogBudgetDisabled(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": 200 * gib})
	spy := &alertSpy{}
	vaultA := glid.New()
	g.vaultBacklogBytes = func(glid.GLID) int64 { return int64(50 * gib) }
	g.SetVaultGuard(vaultA, "busy", []string{"volA"}, "", "", 0)

	// Budget unset: enormous backlog, no cap, no alarm.
	g.evaluateVaults(spy)
	if g.vaultBacklogCapped(vaultA) || spy.active() != 0 {
		t.Fatal("without a budget the backlog bound must be inert")
	}

	// Operator sets a budget: caps on the next pass.
	g.backlogBudget.Store(10 * gib)
	g.evaluateVaults(spy)
	if !g.vaultBacklogCapped(vaultA) || !spy.has("pipeline-backlog-capped:"+vaultA.String()) {
		t.Fatal("setting a budget below the backlog must cap and alarm")
	}

	// Operator disables it again: cap and alarm release.
	g.backlogBudget.Store(0)
	g.evaluateVaults(spy)
	if g.vaultBacklogCapped(vaultA) {
		t.Fatal("disabling the budget must release the standing cap")
	}
	if spy.has("pipeline-backlog-approaching:" + vaultA.String()) {
		t.Fatal("disabling the budget must clear the standing alarm")
	}
}

// TestVaultAdmissionGateBacklog pins the gate: a backlog-capped vault is
// refused with ErrVaultBacklogBudget while siblings keep ingesting. No remote
// half — the registry measure is FSM-replicated, so the local verdict IS the
// cluster verdict.
func TestVaultAdmissionGateBacklog(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": 200 * gib})
	vaultA, vaultB := glid.New(), glid.New()
	g.vaultBacklogBytes = func(id glid.GLID) int64 {
		if id == vaultA {
			return int64(11 * gib)
		}
		return int64(gib)
	}
	g.backlogBudget.Store(10 * gib)
	g.SetVaultGuard(vaultA, "a", []string{"volA"}, "", "", 0)
	g.SetVaultGuard(vaultB, "b", []string{"volA"}, "", "", 0)
	g.evaluateVaults(nil)

	o := &Orchestrator{diskGuard: g}
	if err := o.vaultAdmissionGate(vaultA); !errors.Is(err, ErrVaultBacklogBudget) {
		t.Fatalf("backlog-capped vault must be refused with ErrVaultBacklogBudget, got %v", err)
	}
	if err := o.vaultAdmissionGate(vaultB); err != nil {
		t.Fatalf("vault under budget must be admitted: %v", err)
	}
}
