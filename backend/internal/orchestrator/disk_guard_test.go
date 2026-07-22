package orchestrator

import (
	"errors"
	"strings"
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
	// drain gate resumes so the pipeline can seal backlog, but the admission
	// gate stays held so a burst can't re-cross the floor.
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

// TestDiskGuardIgnoresEnv pins the removal of the env-var channel
// (gastrolog-2mrfdw): node thresholds come from the typeable defaults and
// the config store only — a stray GLOG_DISK_FREE_* in the environment must
// have no effect.
func TestDiskGuardIgnoresEnv(t *testing.T) { //nolint:paralleltest // t.Setenv
	t.Setenv("GLOG_DISK_FREE_WARN", "20%")
	t.Setenv("GLOG_DISK_FREE_FLOOR", "5GiB")
	g := newDiskGuard(nil)
	total := 400 * gib
	if w := g.warnThreshold(total); w != 40*gib {
		t.Fatalf("warn must stay the 10%% default despite env: got %d, want 40GiB", w)
	}
	if f := g.floorThreshold(total); f != 12*gib {
		t.Fatalf("floor must stay the 3%% default despite env: got %d, want 12GiB", f)
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

// TestStorageDiskGuardLifecycle pins the per-storage arc (gastrolog-9akebz):
// a starved storage trips protect/alarm ONCE for that storage, and every
// vault placed there inherits the derived STORAGE_DISK_PROTECT signal via
// vaultStorageProtected, while a vault on a healthy sibling storage stays
// open. Exits are hysteretic like the node guard.
func TestStorageDiskGuardLifecycle(t *testing.T) {
	t.Parallel()
	total := 400 * gib
	fs := map[string]uint64{"volA": 200 * gib, "volB": 200 * gib}
	g, sampler := newGuardFixture(total, fs)
	spy := &alertSpy{}

	vaultA, vaultB := glid.New(), glid.New()
	g.SetStorageGuard("storA", "storage-a", "node-1", "volA", "", "")
	g.SetStorageGuard("storB", "storage-b", "node-1", "volB", "", "")
	g.SetVaultGuard(vaultA, "hot", []string{"storA"}, 0, "", "")
	g.SetVaultGuard(vaultB, "cold", []string{"storB"}, 0, "", "")

	g.evaluateStorages(spy)
	if g.vaultStorageProtected(vaultA) || g.vaultStorageProtected(vaultB) || spy.active() != 0 {
		t.Fatal("healthy storages must raise nothing")
	}

	// storA crosses the floor (node-default 3%% = 12GiB): protect and alarm
	// for storA — and therefore vaultA, its only placed vault — only.
	sampler.free["volA"] = 10 * gib
	g.evaluateStorages(spy)
	if !g.vaultStorageProtected(vaultA) {
		t.Fatal("starved storage must protect every vault placed there")
	}
	if g.vaultStorageProtected(vaultB) {
		t.Fatal("vaultB's healthy storage must keep it open")
	}
	if !spy.has("disk-space-exhausted:storA") {
		t.Fatal("storage alarm must be scoped to the starved storage's ID")
	}
	if spy.has("disk-space-exhausted:storB") {
		t.Fatal("healthy storage must not alarm")
	}

	// Just above the floor: hysteresis holds protect.
	sampler.free["volA"] = 13 * gib
	g.evaluateStorages(spy)
	if !g.vaultStorageProtected(vaultA) {
		t.Fatal("storage protect must not flap at the boundary")
	}

	// Clear of both bands: protect releases, then the alarm clears.
	sampler.free["volA"] = 60 * gib
	g.evaluateStorages(spy)
	if g.vaultStorageProtected(vaultA) {
		t.Fatal("storage protect must release once clear of the floor band")
	}
	if spy.active() != 0 {
		t.Fatal("storage alarm must clear with hysteresis above warn")
	}
}

// TestStorageDiskGuardSharedStorage pins the ONE-EVALUATION-PER-STORAGE
// requirement (gastrolog-9akebz core shape): several vaults placed on the
// SAME storage share exactly ONE statfs sample and ONE alarm, and ALL of
// them refuse once that storage is below floor — not one duplicated
// statfs/alarm per vault, the old per-vault model's modeling error.
func TestStorageDiskGuardSharedStorage(t *testing.T) {
	t.Parallel()
	total := 400 * gib
	g, _ := newGuardFixture(total, map[string]uint64{"volA": 10 * gib})
	sampleCount := 0
	underlying := g.sample
	g.sample = func(path string) (uint64, uint64, error) {
		sampleCount++
		return underlying(path)
	}
	spy := &alertSpy{}

	vaultA, vaultB, vaultC := glid.New(), glid.New(), glid.New()
	g.SetStorageGuard("storA", "shared", "node-1", "volA", "", "")
	g.SetVaultGuard(vaultA, "a", []string{"storA"}, 0, "", "")
	g.SetVaultGuard(vaultB, "b", []string{"storA"}, 0, "", "")
	g.SetVaultGuard(vaultC, "c", []string{"storA"}, 0, "", "")

	g.evaluateStorages(spy)

	if sampleCount != 1 {
		t.Fatalf("one storage must be statfs'd ONCE regardless of vault count, got %d samples", sampleCount)
	}
	if !g.vaultStorageProtected(vaultA) || !g.vaultStorageProtected(vaultB) || !g.vaultStorageProtected(vaultC) {
		t.Fatal("every vault placed on the below-floor storage must be storage-protected")
	}
	if spy.active() != 1 {
		t.Fatalf("one below-floor storage must raise exactly ONE alarm, got %d active", spy.active())
	}
}

// TestStorageDiskGuardInheritsNodeDefaults pins the empty-expression
// contract for a storage entity: unset DiskFreeWarn/DiskFreeFloor ("" from
// FileStorage, same as an operator who never touched those fields)
// resolves against the node-level defaults (10%/3%), exactly like the node
// guard itself — a storage is never left with a silent 0 threshold just
// because the operator didn't configure it (gastrolog-9akebz).
func TestStorageDiskGuardInheritsNodeDefaults(t *testing.T) {
	t.Parallel()
	total := 400 * gib // node defaults resolve to warn=40GiB, floor=12GiB
	g, sampler := newGuardFixture(total, map[string]uint64{"volA": 200 * gib})
	spy := &alertSpy{}
	vaultA := glid.New()
	g.SetStorageGuard("storA", "inherits", "node-1", "volA", "", "")
	g.SetVaultGuard(vaultA, "a", []string{"storA"}, 0, "", "")

	g.evaluateStorages(spy)
	if g.vaultStorageProtected(vaultA) || spy.active() != 0 {
		t.Fatal("well above the inherited 12GiB floor must be quiet")
	}

	// Cross the INHERITED floor (3% of 400GiB = 12GiB) — never configured
	// on the storage itself.
	sampler.free["volA"] = 10 * gib
	g.evaluateStorages(spy)
	if !g.vaultStorageProtected(vaultA) {
		t.Fatal("crossing the inherited node-default floor must protect")
	}
	if !spy.has("disk-space-exhausted:storA") {
		t.Fatal("the alarm must fire off the inherited threshold too")
	}
}

// TestStorageDiskGuardRestartReDerives pins restart safety: a freshly
// constructed guard (simulating a node restart — no carried-over protect
// state, no alarmRaised history) that discovers a storage ALREADY below its
// floor must protect on the very first evaluation, deriving purely from the
// current sample and config — never from state that only existed in the
// pre-restart process (gastrolog-9akebz; mirrors TestPrimeDiskGuardClosesBootWindow's
// node-level boot-window contract for the storage/vault dimension).
func TestStorageDiskGuardRestartReDerives(t *testing.T) {
	t.Parallel()
	total := 400 * gib
	vaultA := glid.New()

	// "Before restart": nothing constructed yet — there is no prior guard
	// instance to carry state over from, which is the point.
	g, _ := newGuardFixture(total, map[string]uint64{"volA": 5 * gib}) // already below the 12GiB floor
	spy := &alertSpy{}

	// "After restart": discovery (refreshVaultDiskGuards' equivalent)
	// registers the storage and vault fresh, exactly as it would from
	// replicated config with no memory of any prior process.
	g.SetStorageGuard("storA", "restarted", "node-1", "volA", "", "")
	g.SetVaultGuard(vaultA, "a", []string{"storA"}, 0, "", "")

	g.evaluateStorages(spy)
	if !g.vaultStorageProtected(vaultA) {
		t.Fatal("a storage already below floor at restart must protect on the FIRST evaluation")
	}
	if !spy.has("disk-space-exhausted:storA") {
		t.Fatal("the alarm must fire on the first post-restart evaluation too")
	}
}

// TestStorageDiskGuardConfigOverridesThresholds pins the per-storage
// override: an explicit expression — absolute size or percentage of the
// volume — replaces the node-default expressions entirely.
func TestStorageDiskGuardConfigOverridesThresholds(t *testing.T) {
	t.Parallel()
	total := 400 * gib
	g, sampler := newGuardFixture(total, map[string]uint64{"volA": 30 * gib})
	spy := &alertSpy{}
	vaultA := glid.New()
	g.SetVaultGuard(vaultA, "greedy", []string{"storA"}, 0, "", "")

	// Node default floor would be 12GiB; this storage demands 50GiB free.
	g.SetStorageGuard("storA", "greedy-storage", "node-1", "volA", "100GiB", "50GiB")
	g.evaluateStorages(spy)
	if !g.vaultStorageProtected(vaultA) {
		t.Fatal("30GiB free is below the storage's 50GiB floor override")
	}

	// A modest override in the other direction: 30GiB free clears a 1GiB floor.
	sampler.free["volA"] = 30 * gib
	g.SetStorageGuard("storA", "modest-storage", "node-1", "volA", "2GiB", "1GiB")
	g.evaluateStorages(spy)
	if g.vaultStorageProtected(vaultA) {
		t.Fatal("30GiB free must clear a 1GiB floor override (with hysteresis)")
	}

	// A percentage override resolves against the storage's own volume: 20%
	// of 400GiB = 80GiB floor, so 30GiB free is a breach the node default
	// (3% = 12GiB) would not see.
	g.SetStorageGuard("storA", "percenty-storage", "node-1", "volA", "25%", "20%")
	g.evaluateStorages(spy)
	if !g.vaultStorageProtected(vaultA) {
		t.Fatal(`30GiB free is below the storage's "20%" floor override`)
	}
	// And a small percentage clears again: floor 1% = 4GiB, resume above the
	// 2% warn band (with hysteresis) — 30GiB is well clear.
	g.SetStorageGuard("storA", "percenty-storage", "node-1", "volA", "2%", "1%")
	g.evaluateStorages(spy)
	if g.vaultStorageProtected(vaultA) {
		t.Fatal(`30GiB free must clear a "1%" floor override`)
	}
}

// TestStorageDiskGuardRetain pins the discovery-refresh no-strand contract
// for a REMOVED storage (gastrolog-9akebz retainVaultGuards precedent): a
// storage entry dropped from the keep set falls out, releasing every
// vault's derived protect flag with it — a vault sharing the pruned storage
// with a still-tracked storage/vault must not stay stranded in protect.
func TestStorageDiskGuardRetain(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": gib, "volB": gib})
	vaultA, vaultB := glid.New(), glid.New()
	g.SetStorageGuard("storA", "a", "node-1", "volA", "", "")
	g.SetStorageGuard("storB", "b", "node-1", "volB", "", "")
	g.SetVaultGuard(vaultA, "a", []string{"storA"}, 0, "", "")
	g.SetVaultGuard(vaultB, "b", []string{"storB"}, 0, "", "")
	g.evaluateStorages(nil)
	if !g.vaultStorageProtected(vaultA) || !g.vaultStorageProtected(vaultB) {
		t.Fatal("both starved storages must protect their vault")
	}

	// storA is removed from config (or no longer local): retainStorageGuards
	// prunes it, and vaultA's derived flag must release even though vaultA's
	// guard entry still lists storA in its storageIDs — the storage entry
	// itself is simply gone, so the lookup finds nothing to protect on.
	g.retainStorageGuards(map[string]bool{"storB": true}, nil)
	if g.vaultStorageProtected(vaultA) {
		t.Fatal("a vault referencing a pruned storage must not stay stranded in protect")
	}
	if !g.vaultStorageProtected(vaultB) {
		t.Fatal("retained storage must keep its verdict")
	}
}

// TestStorageDiskGuardRetainClearsAlarm pins the prune-side alarm contract: a
// storage dropped from the guard set takes its standing alarm with it —
// nothing else would ever clear an alert for an entry no longer evaluated.
func TestStorageDiskGuardRetainClearsAlarm(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": gib})
	spy := &alertSpy{}
	g.SetStorageGuard("storA", "a", "node-1", "volA", "", "")
	g.evaluateStorages(spy)
	if !spy.has("disk-space-exhausted:storA") {
		t.Fatal("starved storage must alarm before the prune")
	}
	g.retainStorageGuards(map[string]bool{}, spy)
	if spy.active() != 0 {
		t.Fatal("pruning an alarmed storage must clear its alert")
	}
}

// TestVaultDiskGuardRetainDropsStorageLinkage pins the no-strand contract
// for a vault's PLACEMENT MOVING off a storage (or the vault itself being
// removed): retainVaultGuards drops the vault's storageIDs linkage, so it
// reports unprotected even while the storage it used to reference is still
// starved — the vault no longer claims that storage as its own, so nothing
// should keep refusing on its behalf. A sibling vault still placed there
// stays protected.
func TestVaultDiskGuardRetainDropsStorageLinkage(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": gib})
	vaultA, vaultB := glid.New(), glid.New()
	g.SetStorageGuard("storA", "a", "node-1", "volA", "", "")
	g.SetVaultGuard(vaultA, "a", []string{"storA"}, 0, "", "")
	g.SetVaultGuard(vaultB, "b", []string{"storA"}, 0, "", "")
	g.evaluateStorages(nil)
	if !g.vaultStorageProtected(vaultA) || !g.vaultStorageProtected(vaultB) {
		t.Fatal("both vaults share the starved storage")
	}

	// vaultA's placement moves elsewhere (or vaultA is removed): its guard
	// entry is pruned, dropping the storageIDs linkage.
	g.retainVaultGuards(map[glid.GLID]bool{vaultB: true}, nil)
	if g.vaultStorageProtected(vaultA) {
		t.Fatal("a vault no longer linked to the storage must not report protect")
	}
	if !g.vaultStorageProtected(vaultB) {
		t.Fatal("a sibling still placed on the starved storage must keep its verdict")
	}
}

// TestStorageSnapshotsReportsEffectiveThresholdsAndInheritance pins the
// snapshot contract for gastrolog-3cobq4: an unset expression reports
// *_inherited-equivalent (empty WarnExpr/FloorExpr the caller renders as
// "inherited") alongside the RESOLVED bytes value — never leaving the
// caller to resolve the expression itself (operator directive, 9akebz: no
// client-side derivation of thresholds).
func TestStorageSnapshotsReportsEffectiveThresholdsAndInheritance(t *testing.T) {
	t.Parallel()
	total := 400 * gib // node defaults: warn=40GiB (10%), floor=12GiB (3%)
	g, _ := newGuardFixture(total, map[string]uint64{"volA": 200 * gib})
	g.SetStorageGuard("storA", "inherits", "node-1", "volA", "", "")
	g.evaluateStorages(nil)

	snaps := g.storageSnapshots()
	if len(snaps) != 1 {
		t.Fatalf("want 1 snapshot, got %d", len(snaps))
	}
	s := snaps[0]
	if s.WarnExpr != "" || s.FloorExpr != "" {
		t.Fatalf("unset expressions must stay empty (caller renders as inherited), got warn=%q floor=%q", s.WarnExpr, s.FloorExpr)
	}
	if s.WarnBytes != 40*gib {
		t.Fatalf("effective warn bytes = %d, want 40GiB (10%% of 400GiB)", s.WarnBytes)
	}
	if s.FloorBytes != 12*gib {
		t.Fatalf("effective floor bytes = %d, want 12GiB (3%% of 400GiB)", s.FloorBytes)
	}
	if s.FreeBytes != 200*gib || s.TotalBytes != total {
		t.Fatalf("free/total = %d/%d, want %d/%d", s.FreeBytes, s.TotalBytes, 200*gib, total)
	}
	if s.WarnVerdict || s.ProtectVerdict {
		t.Fatal("well above both thresholds must report neither verdict")
	}
	if s.SampledAt.IsZero() {
		t.Fatal("a successful sample must record a sampled-at instant")
	}
}

// TestStorageSnapshotsReportsExplicitThresholds pins the explicit-override
// half: a storage with its own configured expressions reports them
// verbatim, not the node defaults.
func TestStorageSnapshotsReportsExplicitThresholds(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": 200 * gib})
	g.SetStorageGuard("storA", "explicit", "node-1", "volA", "20%", "10%")
	g.evaluateStorages(nil)

	snaps := g.storageSnapshots()
	s := snaps[0]
	if s.WarnExpr != "20%" || s.FloorExpr != "10%" {
		t.Fatalf("explicit expressions must round-trip verbatim, got warn=%q floor=%q", s.WarnExpr, s.FloorExpr)
	}
	if s.WarnBytes != 80*gib || s.FloorBytes != 40*gib {
		t.Fatalf("effective bytes = %d/%d, want 80GiB/40GiB", s.WarnBytes, s.FloorBytes)
	}
}

// TestStorageSnapshotsReportsWarnAndProtectVerdicts pins the three-state
// badge grammar (gastrolog-3cobq4): ok when healthy, warn-only inside the
// warn band, protect once below the floor — WarnVerdict and ProtectVerdict
// are never both true (protect supersedes the warn badge, same as the
// alarm pair's low/exhausted split).
func TestStorageSnapshotsReportsWarnAndProtectVerdicts(t *testing.T) {
	t.Parallel()
	total := 400 * gib // warn=40GiB, floor=12GiB
	g, sampler := newGuardFixture(total, map[string]uint64{"volA": 200 * gib})
	spy := &alertSpy{}
	g.SetStorageGuard("storA", "a", "node-1", "volA", "", "")

	g.evaluateStorages(spy)
	s := g.storageSnapshots()[0]
	if s.WarnVerdict || s.ProtectVerdict {
		t.Fatal("healthy storage must report neither verdict")
	}

	sampler.free["volA"] = 30 * gib // below warn (40GiB), above floor (12GiB)
	g.evaluateStorages(spy)
	s = g.storageSnapshots()[0]
	if !s.WarnVerdict || s.ProtectVerdict {
		t.Fatalf("inside the warn band must report warn only, got warn=%v protect=%v", s.WarnVerdict, s.ProtectVerdict)
	}

	sampler.free["volA"] = 5 * gib // below floor
	g.evaluateStorages(spy)
	s = g.storageSnapshots()[0]
	if s.WarnVerdict || !s.ProtectVerdict {
		t.Fatalf("below floor must report protect only, got warn=%v protect=%v", s.WarnVerdict, s.ProtectVerdict)
	}
}

// TestStorageSnapshotsIncludesPlacedVaults pins the placements-on-storage
// contract: SetStorageMeta's vault IDs surface in the snapshot, for the
// storage inspector's cross-link to each placed vault's card.
func TestStorageSnapshotsIncludesPlacedVaults(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": 200 * gib})
	vaultA, vaultB := glid.New(), glid.New()
	g.SetStorageGuard("storA", "shared", "node-1", "volA", "", "")
	g.SetStorageMeta("storA", 2, []glid.GLID{vaultA, vaultB})
	g.evaluateStorages(nil)

	s := g.storageSnapshots()[0]
	if s.StorageClass != 2 {
		t.Fatalf("storage class = %d, want 2", s.StorageClass)
	}
	if len(s.PlacedVaultIDs) != 2 {
		t.Fatalf("placed vaults = %v, want [%s %s]", s.PlacedVaultIDs, vaultA, vaultB)
	}
}

// TestSetStorageMetaNoopForUnknownStorage pins the no-op contract: calling
// SetStorageMeta before the storage is registered (or after it's been
// pruned) must not panic and must not fabricate an entry.
func TestSetStorageMetaNoopForUnknownStorage(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, nil)
	g.SetStorageMeta("unknown", 1, []glid.GLID{glid.New()})
	if len(g.storageSnapshots()) != 0 {
		t.Fatal("SetStorageMeta must not create an entry for an unregistered storage")
	}
}

// TestStorageSnapshotsOmitRemovedStorage pins the no-strand contract at the
// snapshot layer: a storage pruned by retainStorageGuards is gone from
// storageSnapshots() — the inspector's card must disappear, not linger with
// stale state.
func TestStorageSnapshotsOmitRemovedStorage(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": gib})
	g.SetStorageGuard("storA", "a", "node-1", "volA", "", "")
	g.evaluateStorages(nil)
	if len(g.storageSnapshots()) != 1 {
		t.Fatal("precondition: storage must be present before removal")
	}

	g.retainStorageGuards(map[string]bool{}, nil)
	if len(g.storageSnapshots()) != 0 {
		t.Fatal("a pruned storage must be gone from storageSnapshots — no strand")
	}
}

// TestVaultAdmissionGate pins the orchestrator-facing contract, including the
// peer-broadcast half: a vault whose storage is protected on ANY live node
// is refused here.
func TestVaultAdmissionGate(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": gib, "volB": 200 * gib})
	vaultA, vaultB := glid.New(), glid.New()
	g.SetStorageGuard("storA", "a", "node-1", "volA", "", "")
	g.SetStorageGuard("storB", "b", "node-1", "volB", "", "")
	g.SetVaultGuard(vaultA, "a", []string{"storA"}, 0, "", "")
	g.SetVaultGuard(vaultB, "b", []string{"storB"}, 0, "", "")
	g.evaluateStorages(nil)

	o := &Orchestrator{diskGuard: g}
	if err := o.vaultAdmissionGate(vaultA); !errors.Is(err, ErrStorageDiskProtect) {
		t.Fatalf("vault on a locally protected storage must be refused, got %v", err)
	}
	if err := o.vaultAdmissionGate(vaultB); err != nil {
		t.Fatalf("healthy vault must be admitted: %v", err)
	}

	// Remote protect (another node's broadcast) refuses vaultB here too.
	o.SetRemoteVaultStorageProtected(func(id glid.GLID) bool { return id == vaultB })
	if err := o.vaultAdmissionGate(vaultB); !errors.Is(err, ErrStorageDiskProtect) {
		t.Fatalf("remotely protected vault must be refused, got %v", err)
	}

	// Broadcast side: only locally protected vaults are published.
	prot := o.StorageProtectedVaults()
	if len(prot) != 1 || prot[0] != vaultA {
		t.Fatalf("StorageProtectedVaults = %v, want [%s]", prot, vaultA)
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
	g.SetVaultGuard(vaultA, "capped", []string{"volA"}, 10*gib, "", "")
	g.SetVaultGuard(vaultB, "roomy", []string{"volA"}, 100*gib, "", "")

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

// TestVaultMaxSizeEntryWithoutLocalPaths pins the origin-node case: a vault
// with a max-size bound but NO local placement (nothing to statfs) is still
// evaluated — origin segment backlog claims local disk everywhere records
// are accepted.
func TestVaultMaxSizeEntryWithoutLocalPaths(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{})
	vaultA := glid.New()
	g.vaultFootprint = func(glid.GLID) int64 { return int64(20 * gib) }
	g.SetVaultGuard(vaultA, "originy", nil, 10*gib, "", "")
	g.evaluateVaults(nil)
	if !g.vaultSizeCapped(vaultA) {
		t.Fatal("the bound must be enforced even with no sampleable volume paths")
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
	g.SetVaultGuard(vaultA, "a", []string{"volA"}, 10*gib, "", "")
	g.SetVaultGuard(vaultB, "b", []string{"volA"}, 10*gib, "", "")
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
	g.SetVaultGuard(vaultA, "busy", []string{"volA"}, 0, "", "")
	g.SetVaultGuard(vaultB, "calm", []string{"volA"}, 0, "", "")

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
	g.SetVaultGuard(vaultA, "busy", []string{"volA"}, 0, "", "")

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
	g.SetVaultGuard(vaultA, "a", []string{"volA"}, 0, "", "")
	g.SetVaultGuard(vaultB, "b", []string{"volA"}, 0, "", "")
	g.evaluateVaults(nil)

	o := &Orchestrator{diskGuard: g}
	if err := o.vaultAdmissionGate(vaultA); !errors.Is(err, ErrVaultBacklogBudget) {
		t.Fatalf("backlog-capped vault must be refused with ErrVaultBacklogBudget, got %v", err)
	}
	if err := o.vaultAdmissionGate(vaultB); err != nil {
		t.Fatalf("vault under budget must be admitted: %v", err)
	}
}

// causesEqual compares a VaultAdmissionCauses result against an expected
// ordered set, ignoring nothing — order is part of the contract (gate-check
// order: disk protect, max-size bound, backlog budget).
func causesEqual(got []VaultAdmissionCause, want ...VaultAdmissionCause) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestVaultAdmissionCausesEmpty pins the healthy case: a vault admitting
// normally reports zero causes, and so does a guardless orchestrator (same
// contract vaultAdmissionGate has for "no guard, always admit").
func TestVaultAdmissionCausesEmpty(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": 200 * gib})
	vaultA := glid.New()
	g.SetVaultGuard(vaultA, "healthy", []string{"volA"}, 0, "", "")
	g.evaluateVaults(nil)

	o := &Orchestrator{diskGuard: g}
	if got := o.VaultAdmissionCauses(vaultA); len(got) != 0 {
		t.Fatalf("healthy vault must report no causes, got %v", got)
	}
	if got := (&Orchestrator{}).VaultAdmissionCauses(vaultA); len(got) != 0 {
		t.Fatalf("guardless orchestrator must report no causes, got %v", got)
	}
}

// TestVaultAdmissionCausesEachGate pins one cause per gate, local half:
// disk protect alone, max-size alone, backlog alone — each reporting
// exactly its own cause and nothing else.
func TestVaultAdmissionCausesEachGate(t *testing.T) {
	t.Parallel()

	t.Run("storage disk protect", func(t *testing.T) {
		t.Parallel()
		g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": gib})
		vaultA := glid.New()
		g.SetStorageGuard("storA", "my-storage", "node-1", "volA", "", "")
		g.SetVaultGuard(vaultA, "a", []string{"storA"}, 0, "", "")
		g.evaluateStorages(nil)

		o := &Orchestrator{diskGuard: g}
		if got := o.VaultAdmissionCauses(vaultA); !causesEqual(got, VaultAdmissionCauseStorageDiskProtect) {
			t.Fatalf("VaultAdmissionCauses = %v, want [StorageDiskProtect]", got)
		}
		// Detail names the storage and its free-vs-floor numbers when
		// locally sampled — facts before speculation (gastrolog-9akebz).
		details := o.VaultAdmissionCauseDetails(vaultA)
		if len(details) != 1 || !strings.Contains(details[0].Detail, "my-storage") {
			t.Fatalf("detail must name the protecting storage, got %+v", details)
		}
	})

	t.Run("max-size bound", func(t *testing.T) {
		t.Parallel()
		g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": 200 * gib})
		vaultA := glid.New()
		g.vaultFootprint = func(glid.GLID) int64 { return int64(11 * gib) }
		g.SetVaultGuard(vaultA, "a", []string{"volA"}, 10*gib, "", "")
		g.evaluateVaults(nil)

		o := &Orchestrator{diskGuard: g}
		if got := o.VaultAdmissionCauses(vaultA); !causesEqual(got, VaultAdmissionCauseMaxSizeBound) {
			t.Fatalf("VaultAdmissionCauses = %v, want [MaxSizeBound]", got)
		}
		details := o.VaultAdmissionCauseDetails(vaultA)
		if len(details) != 1 || !strings.Contains(details[0].Detail, "max-size bound") {
			t.Fatalf("detail must name the max-size bound, got %+v", details)
		}
	})

	t.Run("backlog budget", func(t *testing.T) {
		t.Parallel()
		g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": 200 * gib})
		vaultA := glid.New()
		g.vaultBacklogBytes = func(glid.GLID) int64 { return int64(11 * gib) }
		g.backlogBudget.Store(10 * gib)
		g.SetVaultGuard(vaultA, "a", []string{"volA"}, 0, "", "")
		g.evaluateVaults(nil)

		o := &Orchestrator{diskGuard: g}
		if got := o.VaultAdmissionCauses(vaultA); !causesEqual(got, VaultAdmissionCauseBacklogBudget) {
			t.Fatalf("VaultAdmissionCauses = %v, want [BacklogBudget]", got)
		}
	})
}

// TestVaultAdmissionCausesRemote pins the peer-broadcast half: a vault
// reported protected/capped only by a live peer's broadcast (no local guard
// state) reports the same cause the local half would.
func TestVaultAdmissionCausesRemote(t *testing.T) {
	t.Parallel()

	t.Run("storage disk protect", func(t *testing.T) {
		t.Parallel()
		vaultA := glid.New()
		o := &Orchestrator{}
		o.SetRemoteVaultStorageProtected(func(id glid.GLID) bool { return id == vaultA })
		o.SetRemoteVaultStorageProtectedNodes(func(id glid.GLID) []string {
			if id == vaultA {
				return []string{"node-b"}
			}
			return nil
		})
		if got := o.VaultAdmissionCauses(vaultA); !causesEqual(got, VaultAdmissionCauseStorageDiskProtect) {
			t.Fatalf("VaultAdmissionCauses = %v, want [StorageDiskProtect]", got)
		}
		// No local sample to attach numbers to: the detail names WHO
		// reported it instead (gastrolog-9akebz).
		details := o.VaultAdmissionCauseDetails(vaultA)
		if len(details) != 1 || !strings.Contains(details[0].Detail, "node-b") {
			t.Fatalf("remote detail must name the reporting node, got %+v", details)
		}
	})

	t.Run("max-size bound", func(t *testing.T) {
		t.Parallel()
		vaultA := glid.New()
		o := &Orchestrator{}
		o.SetRemoteVaultSizeCapped(func(id glid.GLID) bool { return id == vaultA })
		if got := o.VaultAdmissionCauses(vaultA); !causesEqual(got, VaultAdmissionCauseMaxSizeBound) {
			t.Fatalf("VaultAdmissionCauses = %v, want [MaxSizeBound]", got)
		}
	})
}

// TestVaultAdmissionCausesCombination pins the ALL-CAUSES-AT-ONCE case: a
// vault simultaneously with a protected storage, at its max-size bound, and
// at its backlog budget reports all three, in gate-check order — this is
// the "no drift" contract: vaultAdmissionGate returns causes[0] (storage
// disk protect) for this exact vault, and the RPC field must show the full
// set, not just the one the gate acts on.
func TestVaultAdmissionCausesCombination(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": gib})
	vaultA := glid.New()
	g.vaultFootprint = func(glid.GLID) int64 { return int64(11 * gib) }
	g.vaultBacklogBytes = func(glid.GLID) int64 { return int64(11 * gib) }
	g.backlogBudget.Store(10 * gib)
	g.SetStorageGuard("storA", "combo-storage", "node-1", "volA", "", "")
	g.SetVaultGuard(vaultA, "a", []string{"storA"}, 10*gib, "", "")
	g.evaluateStorages(nil)
	g.evaluateVaults(nil)

	o := &Orchestrator{diskGuard: g}
	got := o.VaultAdmissionCauses(vaultA)
	want := []VaultAdmissionCause{
		VaultAdmissionCauseStorageDiskProtect,
		VaultAdmissionCauseMaxSizeBound,
		VaultAdmissionCauseBacklogBudget,
	}
	if !causesEqual(got, want...) {
		t.Fatalf("VaultAdmissionCauses = %v, want %v (gate-check order)", got, want)
	}

	// The gate takes causes[0]: storage disk protect wins even though all
	// three fired.
	if err := o.vaultAdmissionGate(vaultA); !errors.Is(err, ErrStorageDiskProtect) {
		t.Fatalf("gate must return the FIRST cause (storage disk protect), got %v", err)
	}
}
