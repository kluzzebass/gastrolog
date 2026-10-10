package orchestrator

import (
	"context"
	"fmt"
	"testing"

	"gastrolog/internal/waittest"
)

const (
	leadingFire  = "leading"
	trailingFire = "trailing"
)

// throttleHarness runs the progress notifier's throttle loop against a fire
// callback that records which edge fired, with the throttle window closed by
// the test instead of a timer, so every assertion is about the coalescing
// logic and none about the scheduler's cadence.
type throttleHarness struct {
	p         *progressNotifier
	fires     chan string
	windowEnd chan struct{}
	stop      func()
}

func newThrottleHarness(t *testing.T) *throttleHarness {
	t.Helper()
	h := &throttleHarness{
		p:         newProgressNotifier(),
		fires:     make(chan string, 16),
		windowEnd: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runProgressThrottleLoop(ctx, h.p, h.windowEnd, func(edge string) { h.fires <- edge })
	}()
	h.stop = func() {
		cancel()
		<-done
	}
	t.Cleanup(h.stop)
	return h
}

// runProgressThrottleLoop is (*Orchestrator).runProgressNotifier's loop with
// the window timer replaced by windowEnd and the fan-out by fire.
func runProgressThrottleLoop(ctx context.Context, p *progressNotifier, windowEnd <-chan struct{}, fire func(edge string)) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.trigger:
		}
		fire(leadingFire)
		moreCame := false
	windowLoop:
		for {
			select {
			case <-ctx.Done():
				return
			case <-windowEnd:
				break windowLoop
			case <-p.trigger:
				moreCame = true
			}
		}
		if moreCame {
			fire(trailingFire)
		}
	}
}

func (h *throttleHarness) progress() string {
	return fmt.Sprintf("pending-tokens=%d fires-queued=%d", len(h.p.trigger), len(h.fires))
}

// expectFire waits for the next fire and requires it to be edge.
func (h *throttleHarness) expectFire(t *testing.T, edge string) {
	t.Helper()
	if got := waittest.Recv(t, edge+" fire", h.fires, h.progress); got != edge {
		t.Fatalf("fire = %q, want %q", got, edge)
	}
}

// closeWindow waits for the loop to consume every pending signal, then ends
// the open window. The send completes only once the loop is inside the window.
func (h *throttleHarness) closeWindow(t *testing.T) {
	t.Helper()
	waittest.Progress(t, "throttle loop consumes pending signals", func() (string, bool) {
		return h.progress(), len(h.p.trigger) == 0
	})
	h.windowEnd <- struct{}{}
}

// requireNoMoreFires stops the loop and requires that it fired nothing
// beyond what the test already received. Every fire precedes the loop's
// exit, so after stop nothing can still arrive.
func (h *throttleHarness) requireNoMoreFires(t *testing.T) {
	t.Helper()
	h.stop()
	select {
	case edge := <-h.fires:
		t.Fatalf("unexpected %s fire", edge)
	default:
	}
}

// TestProgressNotifier_Idle pins the central guarantee: with no
// Signal() calls, the throttle goroutine never fires. Idle clusters
// pay zero CPU.
func TestProgressNotifier_Idle(t *testing.T) {
	t.Parallel()
	h := newThrottleHarness(t)
	h.requireNoMoreFires(t)
}

// TestProgressNotifier_LeadingEdge pins that the very first Signal
// after quiet fires immediately, before the throttle window even
// starts collecting.
func TestProgressNotifier_LeadingEdge(t *testing.T) {
	t.Parallel()
	h := newThrottleHarness(t)

	h.p.Signal()
	h.expectFire(t, leadingFire)
}

// TestProgressNotifier_BurstCoalesces pins that many signals during a
// single window collapse to two fires (leading + trailing) regardless
// of burst rate.
func TestProgressNotifier_BurstCoalesces(t *testing.T) {
	t.Parallel()
	h := newThrottleHarness(t)

	for range 1000 {
		h.p.Signal()
	}
	h.expectFire(t, leadingFire)
	for range 1000 {
		h.p.Signal()
	}
	h.closeWindow(t)
	h.expectFire(t, trailingFire)
	h.requireNoMoreFires(t)
}

// TestProgressNotifier_TrailingThenQuietGoesIdle pins that after a
// burst ends, the next signal kicks off a fresh leading-edge fire
// (i.e. the throttle correctly resets on quiet).
func TestProgressNotifier_TrailingThenQuietGoesIdle(t *testing.T) {
	t.Parallel()
	h := newThrottleHarness(t)

	// First burst: 1 leading + 1 trailing.
	h.p.Signal()
	h.expectFire(t, leadingFire)
	h.p.Signal()
	h.closeWindow(t)
	h.expectFire(t, trailingFire)

	// Second burst: should fire fresh leading edge.
	h.p.Signal()
	h.expectFire(t, leadingFire)
}

// TestProgressNotifier_NoTrailingForSingleSignal pins that a single
// Signal during a quiet period fires once (leading) and not again
// (no trailing) — the trailing fire is conditional on more activity
// during the window.
func TestProgressNotifier_NoTrailingForSingleSignal(t *testing.T) {
	t.Parallel()
	h := newThrottleHarness(t)

	h.p.Signal()
	h.expectFire(t, leadingFire)
	h.closeWindow(t)
	h.requireNoMoreFires(t)
}

// TestProgressNotifier_SignalNilSafe pins that calling Signal on a
// nil receiver is safe — the production paths use the cheap
// `o.progressTrigger.Signal()` form, and a nil notifier (test
// orchestrator without lifecycle.Start running) must not panic.
func TestProgressNotifier_SignalNilSafe(_ *testing.T) {
	var p *progressNotifier
	p.Signal()
}
