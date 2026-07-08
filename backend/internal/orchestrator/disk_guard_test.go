package orchestrator

import (
	"errors"
	"testing"
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

	// Crossing the floor: protect engages, alarm text escalates.
	sampler.free["b"] = 10 * gib
	g.evaluate(spy)
	if !g.protect.Load() {
		t.Fatal("below the floor the node must stop accepting work")
	}

	// Just above the floor: hysteresis holds protect on.
	sampler.free["b"] = 13 * gib
	g.evaluate(spy)
	if !g.protect.Load() {
		t.Fatal("protect must not flap at the boundary")
	}

	// Clear of the floor band: protect exits, alarm persists (still < warn).
	sampler.free["b"] = 20 * gib
	g.evaluate(spy)
	if g.protect.Load() {
		t.Fatal("protect must release once clear of the floor band")
	}
	if spy.active() != 1 {
		t.Fatal("still under warn: alarm must stand")
	}

	// Recovery past warn hysteresis: alarm clears.
	sampler.free["b"] = 60 * gib
	g.evaluate(spy)
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
