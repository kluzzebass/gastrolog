package waittest

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// StallWindow is how long the observed progress of an awaited condition may
// stand still before the wait fails. It bounds a wedge, not the work: every
// observed change restarts it, so a slow machine that keeps advancing the
// condition never reaches it.
const StallWindow = Failsafe

// Backstop bounds a wait whose progress keeps changing without the condition
// ever holding — a livelock rather than a stall. Steady progress toward the
// condition finishes far inside it.
const Backstop = 2 * time.Minute

// stallObservations is the minimum number of consecutive unchanged
// observations a stall must span. A poller starved alongside the system it
// watches sees the window elapse without having looked; that is not evidence
// the system stopped.
const stallObservations = 50

const pollInterval = 2 * time.Millisecond

type limits struct {
	stall        time.Duration
	backstop     time.Duration
	observations int
	interval     time.Duration
}

var defaultLimits = limits{
	stall:        StallWindow,
	backstop:     Backstop,
	observations: stallObservations,
	interval:     pollInterval,
}

type point struct {
	elapsed time.Duration
	state   string
}

// tracker folds observations of a progress snapshot and decides when the wait
// has stalled.
type tracker struct {
	lim        limits
	start      time.Time
	lastChange time.Time
	state      string
	unchanged  int
	trajectory []point
}

func newTracker(lim limits, initial string) *tracker {
	now := time.Now()
	return &tracker{
		lim:        lim,
		start:      now,
		lastChange: now,
		state:      initial,
		trajectory: []point{{0, initial}},
	}
}

// observe folds one snapshot and returns a non-empty reason once the wait
// must fail.
func (k *tracker) observe(state string) string {
	now := time.Now()
	if state != k.state {
		k.state = state
		k.lastChange = now
		k.unchanged = 0
		k.trajectory = append(k.trajectory, point{now.Sub(k.start), state})
	} else {
		k.unchanged++
	}
	if still := now.Sub(k.lastChange); still >= k.lim.stall && k.unchanged >= k.lim.observations {
		return fmt.Sprintf("progress stalled: no change for %s across %d observations (stall window %s)",
			still.Round(time.Millisecond), k.unchanged, k.lim.stall)
	}
	if now.Sub(k.start) >= k.lim.backstop {
		return fmt.Sprintf("progress kept changing without the condition holding for %s (backstop; livelock?)", k.lim.backstop)
	}
	return ""
}

func (k *tracker) failure(what, reason string) error {
	const headKeep, tailKeep = 8, 24
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %s after %s\nprogress trajectory (%d changes):\n",
		what, reason, time.Since(k.start).Round(time.Millisecond), len(k.trajectory)-1)
	write := func(p point) {
		fmt.Fprintf(&b, "  +%-10s %s\n", p.elapsed.Round(time.Millisecond), p.state)
	}
	points := k.trajectory
	if len(points) <= headKeep+tailKeep {
		for _, p := range points {
			write(p)
		}
		return fmt.Errorf("%s", b.String())
	}
	for _, p := range points[:headKeep] {
		write(p)
	}
	fmt.Fprintf(&b, "  ... %d changes elided ...\n", len(points)-headKeep-tailKeep)
	for _, p := range points[len(points)-tailKeep:] {
		write(p)
	}
	return fmt.Errorf("%s", b.String())
}

// Progress polls sample until it reports done. sample returns a snapshot of
// the progress toward the condition — stage counters, sizes, states, anything
// that moves while the system works on it — and whether the condition holds.
// The wait fails when that snapshot stops changing for StallWindow, or keeps
// changing past Backstop without the condition holding, and reports every
// observed change. sample runs on the calling goroutine and may call t.Fatal.
func Progress(t testing.TB, what string, sample func() (progress string, done bool)) {
	t.Helper()
	if err := pollProgress(defaultLimits, what, sample); err != nil {
		t.Fatal(err)
	}
}

func pollProgress(lim limits, what string, sample func() (string, bool)) error {
	state, done := sample()
	if done {
		return nil
	}
	k := newTracker(lim, state)
	for {
		time.Sleep(lim.interval)
		state, done = sample()
		if done {
			return nil
		}
		if reason := k.observe(state); reason != "" {
			return k.failure(what, reason)
		}
	}
}

// Recv waits for a value from ch, the event that signals the condition, and
// returns it. progress, when non-nil, snapshots the work that leads up to the
// event; the wait fails when that snapshot stops changing for StallWindow, as
// in Progress. Without a progress measure the event is the only evidence of
// life, and the wait fails once StallWindow passes without it.
func Recv[T any](t testing.TB, what string, ch <-chan T, progress func() string) T {
	t.Helper()
	v, err := recvEvent(defaultLimits, what, ch, progress)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func recvEvent[T any](lim limits, what string, ch <-chan T, progress func() string) (T, error) {
	sample := func() string {
		if progress == nil {
			return "(no progress measure; waiting on the event alone)"
		}
		return progress()
	}
	select {
	case v := <-ch:
		return v, nil
	default:
	}
	k := newTracker(lim, sample())
	tick := time.NewTicker(lim.interval)
	defer tick.Stop()
	for {
		select {
		case v := <-ch:
			return v, nil
		case <-tick.C:
			if reason := k.observe(sample()); reason != "" {
				var zero T
				return zero, k.failure(what, reason)
			}
		}
	}
}
