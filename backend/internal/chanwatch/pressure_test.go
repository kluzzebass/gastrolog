package chanwatch

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeProbe is a mutable channel-stat probe for tests.
type fakeProbe struct {
	mu sync.Mutex
	l  int
	c  int
}

func (f *fakeProbe) set(length, capacity int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.l = length
	f.c = capacity
}

func (f *fakeProbe) probe() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.l, f.c
}

func TestPressureGateClassifyHysteresis(t *testing.T) {
	t.Parallel()
	g := NewPressureGate(DefaultThresholds())

	// Start normal.
	if got := g.classify(PressureNormal, 0.10); got != PressureNormal {
		t.Errorf("0.10 from normal: got %v, want normal", got)
	}
	// Cross elevated.
	if got := g.classify(PressureNormal, 0.80); got != PressureElevated {
		t.Errorf("0.80 from normal: got %v, want elevated", got)
	}
	// Cross critical.
	if got := g.classify(PressureElevated, 0.95); got != PressureCritical {
		t.Errorf("0.95 from elevated: got %v, want critical", got)
	}
	// Hysteresis: 0.70 from elevated stays elevated (above NormalAt=0.50).
	if got := g.classify(PressureElevated, 0.70); got != PressureElevated {
		t.Errorf("0.70 from elevated: got %v, want elevated (hysteresis)", got)
	}
	// Hysteresis: 0.70 from critical stays critical (above NormalAt=0.50).
	if got := g.classify(PressureCritical, 0.70); got != PressureCritical {
		t.Errorf("0.70 from critical: got %v, want critical (hysteresis)", got)
	}
	// Drop below NormalAt returns to normal.
	if got := g.classify(PressureElevated, 0.49); got != PressureNormal {
		t.Errorf("0.49 from elevated: got %v, want normal", got)
	}
	if got := g.classify(PressureCritical, 0.49); got != PressureNormal {
		t.Errorf("0.49 from critical: got %v, want normal", got)
	}
}

func TestPressureGateAggregatesMaxAcrossProbes(t *testing.T) {
	t.Parallel()
	g := NewPressureGate(DefaultThresholds())
	a := &fakeProbe{}
	b := &fakeProbe{}
	g.AddProbe("a", a.probe)
	g.AddProbe("b", b.probe)

	// Both normal → gate normal.
	a.set(10, 100)
	b.set(20, 100)
	g.tick()
	if g.Level() != PressureNormal {
		t.Errorf("both normal: got %v", g.Level())
	}

	// One elevated, one normal → gate elevated.
	a.set(80, 100)
	b.set(20, 100)
	g.tick()
	if g.Level() != PressureElevated {
		t.Errorf("one elevated: got %v", g.Level())
	}

	// One elevated, one critical → gate critical.
	b.set(95, 100)
	g.tick()
	if g.Level() != PressureCritical {
		t.Errorf("one critical: got %v", g.Level())
	}

	// Both drop below NormalAt → gate normal.
	a.set(10, 100)
	b.set(10, 100)
	g.tick()
	if g.Level() != PressureNormal {
		t.Errorf("both below normal: got %v", g.Level())
	}
}

func TestPressureGateOnChangeFiresOnTransition(t *testing.T) {
	t.Parallel()
	g := NewPressureGate(DefaultThresholds())
	p := &fakeProbe{}
	g.AddProbe("p", p.probe)

	var transitions []PressureTransition
	var mu sync.Mutex
	g.AddOnChange(func(tr PressureTransition) {
		mu.Lock()
		defer mu.Unlock()
		transitions = append(transitions, tr)
	})

	p.set(10, 100)
	g.tick()
	p.set(80, 100)
	g.tick()
	p.set(95, 100)
	g.tick()
	p.set(10, 100)
	g.tick()

	mu.Lock()
	defer mu.Unlock()
	if len(transitions) != 3 {
		t.Fatalf("expected 3 transitions, got %d: %v", len(transitions), transitions)
	}
	expected := []PressureLevel{PressureElevated, PressureCritical, PressureNormal}
	for i, want := range expected {
		if transitions[i].To != want {
			t.Errorf("transition[%d].To = %v, want %v", i, transitions[i].To, want)
		}
	}
}

func TestPressureGateOnChangeNotFiredForNoChange(t *testing.T) {
	t.Parallel()
	g := NewPressureGate(DefaultThresholds())
	p := &fakeProbe{}
	g.AddProbe("p", p.probe)

	var count atomic.Int32
	g.AddOnChange(func(PressureTransition) { count.Add(1) })

	p.set(10, 100)
	g.tick()
	g.tick()
	g.tick()
	if count.Load() != 0 {
		t.Errorf("expected 0 transitions for stable normal, got %d", count.Load())
	}
}

func TestPressureGateWaitBlocksAndReleases(t *testing.T) {
	t.Parallel()
	g := NewPressureGate(DefaultThresholds())
	p := &fakeProbe{}
	g.AddProbe("p", p.probe)

	// Start critical.
	p.set(100, 100)
	g.tick()
	if g.Level() != PressureCritical {
		t.Fatalf("setup: expected critical, got %v", g.Level())
	}

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- g.Wait(context.Background())
	}()

	// Wait is blocked. Verify by checking nothing came through.
	select {
	case <-waitDone:
		t.Fatal("Wait returned while critical — should block")
	case <-time.After(50 * time.Millisecond):
	}

	// Drop pressure.
	p.set(10, 100)
	g.tick()

	select {
	case err := <-waitDone:
		if err != nil {
			t.Errorf("Wait returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Wait did not release after pressure dropped")
	}
}

func TestPressureGateWaitReturnsWhenAlreadyNormal(t *testing.T) {
	t.Parallel()
	g := NewPressureGate(DefaultThresholds())
	// Gate is in PressureNormal from construction.
	if err := g.Wait(context.Background()); err != nil {
		t.Errorf("Wait on normal gate: got %v, want nil", err)
	}
}

func TestPressureGateWaitRespectsContextCancellation(t *testing.T) {
	t.Parallel()
	g := NewPressureGate(DefaultThresholds())
	p := &fakeProbe{}
	g.AddProbe("p", p.probe)
	p.set(100, 100)
	g.tick()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := g.Wait(ctx); err == nil {
		t.Error("expected ctx error, got nil")
	}
}

func TestPressureGateZeroCapacityProbesIgnored(t *testing.T) {
	t.Parallel()
	g := NewPressureGate(DefaultThresholds())
	zero := &fakeProbe{} // l=0, c=0
	g.AddProbe("zero", zero.probe)
	g.tick()
	if g.Level() != PressureNormal {
		t.Errorf("zero-capacity probe should not change level, got %v", g.Level())
	}
}
