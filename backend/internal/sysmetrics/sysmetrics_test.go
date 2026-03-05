package sysmetrics

import (
	"math"
	"sync"
	"testing"
	"time"
)

// fakeClock returns a clock function that advances by step on each call.
// The first call (used by newCPUTracker to seed lastWall) returns start.
// Safe for concurrent use.
func fakeClock(start time.Time, step time.Duration) func() time.Time {
	var mu sync.Mutex
	t := start.Add(-step) // pre-offset so first call returns start
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		t = t.Add(step)
		return t
	}
}

// fakeRusage returns a rusage function that advances user and sys by the
// given deltas on each call. The first call returns (0, 0).
// Safe for concurrent use.
func fakeRusage(userStep, sysStep time.Duration) func() (time.Duration, time.Duration) {
	var mu sync.Mutex
	var user, sys time.Duration
	first := true
	return func() (time.Duration, time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		if first {
			first = false
			return user, sys
		}
		user += userStep
		sys += sysStep
		return user, sys
	}
}

func TestCPUBasic(t *testing.T) {
	// 1s wall, 300ms user + 200ms sys = 500ms CPU → 50%.
	clock := fakeClock(time.Unix(0, 0), time.Second)
	rusage := fakeRusage(300*time.Millisecond, 200*time.Millisecond)

	tr := newCPUTracker(clock, rusage)
	pct := tr.percent()

	if math.Abs(pct-50.0) > 0.01 {
		t.Fatalf("expected ~50%%, got %.2f%%", pct)
	}
}

func TestCPUClockRegression(t *testing.T) {
	start := time.Unix(1000, 0)
	calls := 0
	clock := func() time.Time {
		calls++
		switch calls {
		case 1: // seed
			return start
		case 2: // first percent() → normal
			return start.Add(time.Second)
		default: // second percent() → regression
			return start // earlier than lastWall
		}
	}

	rusage := fakeRusage(100*time.Millisecond, 0)
	tr := newCPUTracker(clock, rusage)

	first := tr.percent()
	if first < 0 {
		t.Fatalf("first call should be non-negative, got %.2f", first)
	}

	second := tr.percent()
	if second != first {
		t.Fatalf("clock regression should return cached value %.2f, got %.2f", first, second)
	}
}

func TestCPUZeroDelta(t *testing.T) {
	// Clock advances but no CPU time consumed → 0%.
	clock := fakeClock(time.Unix(0, 0), time.Second)
	rusage := fakeRusage(0, 0)

	tr := newCPUTracker(clock, rusage)
	pct := tr.percent()

	if pct != 0 {
		t.Fatalf("expected 0%%, got %.2f%%", pct)
	}
}

func TestCPUMultiCore(t *testing.T) {
	// 1s wall, 1200ms user + 800ms sys = 2000ms CPU → 200%.
	clock := fakeClock(time.Unix(0, 0), time.Second)
	rusage := fakeRusage(1200*time.Millisecond, 800*time.Millisecond)

	tr := newCPUTracker(clock, rusage)
	pct := tr.percent()

	if math.Abs(pct-200.0) > 0.01 {
		t.Fatalf("expected ~200%%, got %.2f%%", pct)
	}
}

func TestCPUConcurrent(t *testing.T) {
	clock := fakeClock(time.Unix(0, 0), time.Millisecond)
	rusage := fakeRusage(100*time.Microsecond, 50*time.Microsecond)

	tr := newCPUTracker(clock, rusage)

	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			for range 100 {
				p := tr.percent()
				if math.IsNaN(p) || math.IsInf(p, 0) {
					t.Errorf("got NaN or Inf")
				}
			}
		})
	}
	wg.Wait()
}

func TestMemoryReasonable(t *testing.T) {
	m := Memory()

	if m.Inuse <= 0 {
		t.Errorf("Inuse should be > 0, got %d", m.Inuse)
	}
	if m.Sys <= 0 {
		t.Errorf("Sys should be > 0, got %d", m.Sys)
	}
	if m.RSS < 0 {
		t.Errorf("RSS should be >= 0, got %d", m.RSS)
	}
}

func TestCPUPercentPackageLevel(t *testing.T) {
	pct := CPUPercent()
	if pct < 0 {
		t.Errorf("CPUPercent should be non-negative, got %.2f", pct)
	}
}
