package alert

// Alarm state tests: an alarm is standing or it is not. Acknowledgment,
// shelving and rate self-monitoring were built here and removed on operator
// verdict (management machinery presumes the alarm volume the epic exists
// to eliminate); what remains is the suppression substrate plus release.
// Every test drives the collector's injected clock — there is not a single
// sleep here and never should be.

import (
	"testing"
	"time"
)

// standingByID returns the Standing() snapshot keyed by alarm ID.
func standingByID(c *Collector) map[string]*Alarm {
	m := make(map[string]*Alarm)
	for _, a := range c.Standing() {
		m[a.ID] = a
	}
	return m
}

func mustStanding(t *testing.T, c *Collector, id string) *Alarm {
	t.Helper()
	a := standingByID(c)[id]
	if a == nil {
		t.Fatalf("alarm %q not standing", id)
	}
	return a
}

func mustGone(t *testing.T, c *Collector, id string) {
	t.Helper()
	if standingByID(c)[id] != nil {
		t.Fatalf("alarm %q still standing, want gone", id)
	}
}

// TestLifecycle_StandingReleasesOnClear: standing → (condition resolves) →
// gone, full stop. There is no retained cleared state.
func TestLifecycle_StandingReleasesOnClear(t *testing.T) {
	clock := newSuppressionClock()
	c := NewWithClock(clock.Now)

	c.Raise("disk-space-exhausted", "storage-1", "volume full")
	mustStanding(t, c, "disk-space-exhausted:storage-1")
	if got := c.Count(); got != 1 {
		t.Fatalf("Count = %d, want 1", got)
	}

	c.Clear("disk-space-exhausted", "storage-1")
	mustGone(t, c, "disk-space-exhausted:storage-1")
	if got := c.Count(); got != 0 {
		t.Fatalf("Count = %d after clear, want 0", got)
	}
}

// TestLifecycle_ReRaiseAfterReleaseIsFresh: a condition returning after its
// alarm released is simply a new alarm — fresh FirstSeen, fresh delay-on
// window, nothing carried over.
func TestLifecycle_ReRaiseAfterReleaseIsFresh(t *testing.T) {
	clock := newSuppressionClock()
	c := NewWithClock(clock.Now)
	window := leaderlessWindow(t)

	c.Raise(leaderlessType, "v", "no leader")
	clock.Advance(window + time.Second)
	mustStanding(t, c, leaderlessType+":v")
	c.Clear(leaderlessType, "v")
	mustGone(t, c, leaderlessType+":v")

	// The condition returns much later: the full delay-on window applies
	// again, and the annunciated alarm's FirstSeen is the new condition
	// start.
	clock.Advance(time.Hour)
	freshStart := clock.Now()
	c.Raise(leaderlessType, "v", "no leader again")
	mustGone(t, c, leaderlessType+":v") // pending its window, not standing
	clock.Advance(window + time.Second)
	a := mustStanding(t, c, leaderlessType+":v")
	if !a.FirstSeen.Equal(freshStart) {
		t.Fatalf("FirstSeen = %v, want the new condition start %v", a.FirstSeen, freshStart)
	}
}

// TestLifecycle_LatchedStandsUntilRestart pins the exact lifecycle of
// orchestrator-lock-leak: latched is plain STICKY. The condition clearing
// does not release it, time does not release it — there is no release path,
// and that is intentional (the response to a software fault is report +
// restart). A restart (a fresh collector) is what clears it: the fault is
// simply gone unless re-detected.
func TestLifecycle_LatchedStandsUntilRestart(t *testing.T) {
	clock := newSuppressionClock()
	c := NewWithClock(clock.Now)

	c.Raise("orchestrator-lock-leak", "", "read hold stuck for 2m")
	mustStanding(t, c, "orchestrator-lock-leak")

	// The condition "clears" (it cannot, for a leaked hold, but the state
	// machine must hold even if a Clear arrives): the latch keeps it
	// standing.
	c.Clear("orchestrator-lock-leak", "")
	mustStanding(t, c, "orchestrator-lock-leak")
	if got := c.Count(); got != 1 {
		t.Fatalf("Count = %d, want 1 — a latched alarm stands", got)
	}

	// Days pass. Still standing: no release path.
	clock.Advance(72 * time.Hour)
	mustStanding(t, c, "orchestrator-lock-leak")

	// Restart: a fresh collector holds nothing. Nothing persists across
	// restart — a re-detected condition would simply be a standing alarm
	// again.
	c2 := NewWithClock(clock.Now)
	mustGone(t, c2, "orchestrator-lock-leak")
}

// TestLifecycle_DelayOffResolutionUsesWindowCloseInstant: an alarm with a
// delay-off window releases when the window closes; the verdict is
// identical whether the next read runs immediately or much later.
func TestLifecycle_DelayOffResolutionUsesWindowCloseInstant(t *testing.T) {
	clock := newSuppressionClock()
	c := NewWithClock(clock.Now)

	delayedRaise(c, "flappy", "up", 0, 5*time.Minute, false)
	c.Clear("flappy", "")
	// Inside the delay-off window the alarm is still standing.
	clock.Advance(2 * time.Minute)
	mustStanding(t, c, "flappy")
	// Long after the window closed: released — regardless of the read
	// arriving hours late.
	clock.Advance(6 * time.Hour)
	mustGone(t, c, "flappy")
}
