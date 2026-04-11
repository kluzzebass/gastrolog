package orchestrator

import (
	"sync"
	"testing"
	"time"
)

// baseTime is a fixed reference time used by all RateWindow tests so the
// bucket indices are deterministic regardless of when the test runs.
var baseTime = time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)

func TestRateWindowEmptyReturnsZero(t *testing.T) {
	t.Parallel()
	w := NewRateWindow(30 * time.Second)
	if got := w.Rate(baseTime); got != 0 {
		t.Errorf("empty Rate: got %v, want 0", got)
	}
	if got := w.Count(baseTime); got != 0 {
		t.Errorf("empty Count: got %v, want 0", got)
	}
}

func TestRateWindowSingleEvent(t *testing.T) {
	t.Parallel()
	w := NewRateWindow(30 * time.Second)
	w.Record(baseTime)
	// One event over a 30s window = 1/30 per second.
	if got := w.Rate(baseTime); got != 1.0/30.0 {
		t.Errorf("single event Rate: got %v, want %v", got, 1.0/30.0)
	}
	if got := w.Count(baseTime); got != 1 {
		t.Errorf("single event Count: got %v, want 1", got)
	}
}

func TestRateWindowMultipleEventsSameBucket(t *testing.T) {
	t.Parallel()
	w := NewRateWindow(30 * time.Second)
	for range 10 {
		w.Record(baseTime)
	}
	if got := w.Count(baseTime); got != 10 {
		t.Errorf("Count: got %v, want 10", got)
	}
	if got := w.Rate(baseTime); got != 10.0/30.0 {
		t.Errorf("Rate: got %v, want %v", got, 10.0/30.0)
	}
}

func TestRateWindowEventsSpreadAcrossBuckets(t *testing.T) {
	t.Parallel()
	w := NewRateWindow(30 * time.Second)
	// 1 event per second for 5 seconds.
	for i := range 5 {
		w.Record(baseTime.Add(time.Duration(i) * time.Second))
	}
	now := baseTime.Add(4 * time.Second)
	if got := w.Count(now); got != 5 {
		t.Errorf("spread Count: got %v, want 5", got)
	}
	if got := w.Rate(now); got != 5.0/30.0 {
		t.Errorf("spread Rate: got %v, want %v", got, 5.0/30.0)
	}
}

func TestRateWindowOldEventsFallOff(t *testing.T) {
	t.Parallel()
	w := NewRateWindow(30 * time.Second)

	// 5 events at t=0, then we observe at t=60 — all events should have
	// fallen out of the 30s window.
	for range 5 {
		w.Record(baseTime)
	}
	future := baseTime.Add(60 * time.Second)
	if got := w.Count(future); got != 0 {
		t.Errorf("after window Count: got %v, want 0", got)
	}
	if got := w.Rate(future); got != 0 {
		t.Errorf("after window Rate: got %v, want 0", got)
	}
}

func TestRateWindowBucketReuseResetsStaleSlot(t *testing.T) {
	t.Parallel()
	w := NewRateWindow(30 * time.Second)

	// Record 5 events at t=0. Bucket index = 0 % 30 = 0.
	for range 5 {
		w.Record(baseTime)
	}
	// Record 3 events at t=30. Bucket index = 30 % 30 = 0 — same slot,
	// must be reset to 3 (not 5+3).
	later := baseTime.Add(30 * time.Second)
	for range 3 {
		w.Record(later)
	}
	if got := w.Count(later); got != 3 {
		t.Errorf("bucket reuse: got %v, want 3", got)
	}
}

func TestRateWindowGradualWindowExpiry(t *testing.T) {
	t.Parallel()
	w := NewRateWindow(10 * time.Second)

	// Record 1 event each second from t=0 to t=9 inclusive: 10 events
	// in a 10s window = 1.0/sec rate.
	for i := range 10 {
		w.Record(baseTime.Add(time.Duration(i) * time.Second))
	}
	t9 := baseTime.Add(9 * time.Second)
	if got := w.Rate(t9); got != 1.0 {
		t.Errorf("at t=9: got %v, want 1.0", got)
	}

	// At t=10 the t=0 bucket is now outside the window — should drop to 0.9.
	t10 := baseTime.Add(10 * time.Second)
	if got := w.Rate(t10); got != 0.9 {
		t.Errorf("at t=10 (one bucket dropped): got %v, want 0.9", got)
	}

	// At t=14: only events from t=5..9 remain in the window — 5 events / 10s = 0.5.
	t14 := baseTime.Add(14 * time.Second)
	if got := w.Rate(t14); got != 0.5 {
		t.Errorf("at t=14: got %v, want 0.5", got)
	}
}

func TestRateWindowConcurrentAccess(t *testing.T) {
	t.Parallel()
	w := NewRateWindow(30 * time.Second)

	const goroutines = 16
	const eventsPer = 1000

	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for range eventsPer {
				w.Record(baseTime)
			}
		})
	}
	// Simultaneously hammer Rate and Count from another goroutine.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = w.Rate(baseTime)
				_ = w.Count(baseTime)
			}
		}
	}()
	wg.Wait()
	close(stop)

	if got := w.Count(baseTime); got != goroutines*eventsPer {
		t.Errorf("concurrent Count: got %v, want %v", got, goroutines*eventsPer)
	}
}

func TestRateWindowPanicsOnSubSecondWindow(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for sub-second window")
		}
	}()
	NewRateWindow(500 * time.Millisecond)
}
