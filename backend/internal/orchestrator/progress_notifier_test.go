package orchestrator

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// runThrottleWithCounter wires the progress notifier's throttle loop
// against a stand-in NotifyChunkChange that just increments a counter,
// so tests can assert exactly how many fan-outs happen for a given
// signal pattern. Returns the running orchestrator (with a noop
// Stop), the trigger, the counter, and a cancel func that stops the
// loop and waits for it to exit.
func newThrottleHarness(t *testing.T, window time.Duration) (*progressNotifier, *atomic.Int64, func()) {
	t.Helper()
	p := newProgressNotifier()
	var fired atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runProgressThrottleLoop(ctx, p, window, func() { fired.Add(1) })
	}()
	stop := func() {
		cancel()
		<-done
	}
	return p, &fired, stop
}

// runProgressThrottleLoop is a copy of (*Orchestrator).runProgressNotifier's
// inner loop, parameterized on the fire callback so tests don't need
// to construct a full Orchestrator. The production wrapper just calls
// this with o.NotifyChunkChange.
func runProgressThrottleLoop(ctx context.Context, p *progressNotifier, window time.Duration, fire func()) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.trigger:
		}
		fire()
		timer := time.NewTimer(window)
		moreCame := false
	windowLoop:
		for {
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
				break windowLoop
			case <-p.trigger:
				moreCame = true
			}
		}
		if moreCame {
			fire()
		}
	}
}

// TestProgressNotifier_Idle pins the central guarantee: with no
// Signal() calls, the throttle goroutine never fires. Idle clusters
// pay zero CPU.
func TestProgressNotifier_Idle(t *testing.T) {
	t.Parallel()
	_, fired, stop := newThrottleHarness(t, 50*time.Millisecond)
	defer stop()

	time.Sleep(150 * time.Millisecond)

	if got := fired.Load(); got != 0 {
		t.Errorf("fired = %d, want 0 (idle should never fire)", got)
	}
}

// TestProgressNotifier_LeadingEdge pins that the very first Signal
// after quiet fires immediately, before the throttle window even
// starts collecting.
func TestProgressNotifier_LeadingEdge(t *testing.T) {
	t.Parallel()
	p, fired, stop := newThrottleHarness(t, 200*time.Millisecond)
	defer stop()

	p.Signal()
	// Leading-edge fire should land within a few ms.
	deadline := time.Now().Add(50 * time.Millisecond)
	for time.Now().Before(deadline) {
		if fired.Load() == 1 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Errorf("leading-edge fire did not happen within 50ms (fired=%d)", fired.Load())
}

// TestProgressNotifier_BurstCoalesces pins that many signals during a
// single window collapse to two fires (leading + trailing) regardless
// of burst rate.
func TestProgressNotifier_BurstCoalesces(t *testing.T) {
	t.Parallel()
	p, fired, stop := newThrottleHarness(t, 100*time.Millisecond)
	defer stop()

	// Hammer the trigger for ~50ms (mid-window).
	for range 1000 {
		p.Signal()
	}
	time.Sleep(50 * time.Millisecond)
	for range 1000 {
		p.Signal()
	}
	// Wait for the window to end and trailing fire to happen.
	time.Sleep(200 * time.Millisecond)

	if got := fired.Load(); got != 2 {
		t.Errorf("fired = %d, want 2 (1 leading + 1 trailing for one burst)", got)
	}
}

// TestProgressNotifier_TrailingThenQuietGoesIdle pins that after a
// burst ends, the next signal kicks off a fresh leading-edge fire
// (i.e. the throttle correctly resets on quiet).
func TestProgressNotifier_TrailingThenQuietGoesIdle(t *testing.T) {
	t.Parallel()
	p, fired, stop := newThrottleHarness(t, 50*time.Millisecond)
	defer stop()

	// First burst: 1 leading + 1 trailing.
	p.Signal()
	time.Sleep(20 * time.Millisecond)
	p.Signal()
	// Wait past the window plus a margin to guarantee throttle is back
	// to quiet wait.
	time.Sleep(150 * time.Millisecond)

	if got := fired.Load(); got != 2 {
		t.Fatalf("after first burst, fired = %d, want 2", got)
	}

	// Second burst: should fire fresh leading edge.
	p.Signal()
	time.Sleep(20 * time.Millisecond)

	if got := fired.Load(); got != 3 {
		t.Errorf("after second burst leading edge, fired = %d, want 3", got)
	}
}

// TestProgressNotifier_NoTrailingForSingleSignal pins that a single
// Signal during a quiet period fires once (leading) and not again
// (no trailing) — the trailing fire is conditional on more activity
// during the window.
func TestProgressNotifier_NoTrailingForSingleSignal(t *testing.T) {
	t.Parallel()
	p, fired, stop := newThrottleHarness(t, 50*time.Millisecond)
	defer stop()

	p.Signal()
	time.Sleep(150 * time.Millisecond)

	if got := fired.Load(); got != 1 {
		t.Errorf("fired = %d, want 1 (single signal: leading only, no trailing)", got)
	}
}

// TestProgressNotifier_SignalNilSafe pins that calling Signal on a
// nil receiver is safe — the production paths use the cheap
// `o.progressTrigger.Signal()` form, and a nil notifier (test
// orchestrator without lifecycle.Start running) must not panic.
func TestProgressNotifier_SignalNilSafe(_ *testing.T) {
	var p *progressNotifier
	p.Signal()
}
