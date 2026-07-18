package orchestrator

// gastrolog-5ct2av: a destination that passes its admission gate but stops
// draining must not hang the retention sweep. The watchdog aborts a fan-out
// that makes no progress for a full stall window. Tested through injected
// ticks and the pure stalled() predicate — never by racing real timers.

import (
	"errors"
	"testing"
	"time"
)

func TestProgressWatchStalledSemantics(t *testing.T) {
	t.Parallel()
	w := &progressWatch{}
	// No progress since construction: stalled.
	if !w.stalled() {
		t.Fatal("no progress observed: stalled() must be true")
	}
	w.bump()
	if w.stalled() {
		t.Fatal("progress since last check: stalled() must be false")
	}
	// The bump was consumed by the previous check; no new progress.
	if !w.stalled() {
		t.Fatal("no progress since last check: stalled() must be true again")
	}
}

func TestRunStallMonitorAbortsOnStall(t *testing.T) {
	t.Parallel()
	w := &progressWatch{}
	tick := make(chan time.Time)
	done := make(chan struct{})
	defer close(done)

	aborted := make(chan error, 1)
	go runStallMonitor(done, tick, w, func(cause error) { aborted <- cause })

	// First tick with progress: no abort.
	w.bump()
	tick <- time.Time{}
	select {
	case cause := <-aborted:
		t.Fatalf("progress tick must not abort, got %v", cause)
	default:
	}

	// Second tick without progress: abort with the stall sentinel.
	tick <- time.Time{}
	select {
	case cause := <-aborted:
		if !errors.Is(cause, errRetentionFanOutStalled) {
			t.Fatalf("want errRetentionFanOutStalled, got %v", cause)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stalled tick must abort")
	}
}

func TestRunStallMonitorStopsOnDone(t *testing.T) {
	t.Parallel()
	w := &progressWatch{}
	tick := make(chan time.Time)
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		runStallMonitor(done, tick, w, func(error) { t.Error("must not abort after done") })
		close(stopped)
	}()
	close(done)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("monitor must return when done closes")
	}
}
