package alert

// Alarm state tests: the two states — active and shelved — layered on the
// suppression entry. Acknowledgment and its journal were built here and
// removed on operator verdict (alarms are state with suppression; nothing
// persists across restart). Every test drives the collector's injected
// clock; shelve expiry is a time construct settled lazily, so there is not
// a single sleep here and never should be.

import (
	"errors"
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

func mustState(t *testing.T, c *Collector, id string, want AlarmState) *Alarm {
	t.Helper()
	a := standingByID(c)[id]
	if a == nil {
		t.Fatalf("alarm %q not standing; want state %v", id, want)
	}
	if a.State != want {
		t.Fatalf("alarm %q state = %v, want %v", id, a.State, want)
	}
	return a
}

func mustGone(t *testing.T, c *Collector, id string) {
	t.Helper()
	if a := standingByID(c)[id]; a != nil {
		t.Fatalf("alarm %q still standing in state %v, want gone", id, a.State)
	}
}

// TestLifecycle_ActiveReleasesOnClear: active → (condition resolves) →
// gone, full stop. There is no retained cleared state.
func TestLifecycle_ActiveReleasesOnClear(t *testing.T) {
	clock := newSuppressionClock()
	c := NewWithClock(clock.Now)

	c.Raise("disk-space-exhausted", "vault-1", "volume full")
	mustState(t, c, "disk-space-exhausted:vault-1", StateActive)
	if got := c.Count(); got != 1 {
		t.Fatalf("Count = %d, want 1", got)
	}

	c.Clear("disk-space-exhausted", "vault-1")
	mustGone(t, c, "disk-space-exhausted:vault-1")
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
	mustState(t, c, leaderlessType+":v", StateActive)
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
	a := mustState(t, c, leaderlessType+":v", StateActive)
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
	a := mustState(t, c, "orchestrator-lock-leak", StateActive)
	if a.Shelveable {
		t.Fatal("lock-leak must be unshelveable on the snapshot")
	}

	// The condition "clears" (it cannot, for a leaked hold, but the state
	// machine must hold even if a Clear arrives): the latch keeps it
	// standing in the active list.
	c.Clear("orchestrator-lock-leak", "")
	mustState(t, c, "orchestrator-lock-leak", StateActive)
	if got := c.Count(); got != 1 {
		t.Fatalf("Count = %d, want 1 — a latched alarm stands", got)
	}

	// Days pass. Still standing: no release path.
	clock.Advance(72 * time.Hour)
	mustState(t, c, "orchestrator-lock-leak", StateActive)

	// Restart: a fresh collector holds nothing. Nothing persists across
	// restart — a re-detected condition would simply be an active alarm
	// again.
	c2 := NewWithClock(clock.Now)
	mustGone(t, c2, "orchestrator-lock-leak")
}

// TestLifecycle_ShelveMandatoryExpiry: zero and negative durations are
// rejected at the collector boundary — there are no permanent shelves.
func TestLifecycle_ShelveMandatoryExpiry(t *testing.T) {
	c := NewWithClock(newSuppressionClock().Now)
	c.Raise("disk-space-exhausted", "vault-1", "volume full")

	for _, d := range []time.Duration{0, -time.Hour} {
		if _, err := c.Shelve("disk-space-exhausted:vault-1", d, "alice"); !errors.Is(err, ErrShelveExpiryRequired) {
			t.Fatalf("Shelve(%v) error = %v, want ErrShelveExpiryRequired", d, err)
		}
	}
	mustState(t, c, "disk-space-exhausted:vault-1", StateActive)
}

// TestLifecycle_ShelveRefusal: types where deferral is meaningless refuse
// shelve with a reason. The catalog sweep verdict is pinned here: exactly
// the software-fault class plus alarm-flood refuse; every process alarm
// allows shelving.
func TestLifecycle_ShelveRefusal(t *testing.T) {
	clock := newSuppressionClock()
	c := NewWithClock(clock.Now)

	c.Raise("orchestrator-lock-leak", "", "read hold stuck")
	if _, err := c.Shelve("orchestrator-lock-leak", time.Hour, "alice"); !errors.Is(err, ErrNotShelveable) {
		t.Fatalf("Shelve(lock-leak) error = %v, want ErrNotShelveable", err)
	}
	mustState(t, c, "orchestrator-lock-leak", StateActive)

	c.Raise(FloodTypeID, "", "12 activations in 10m")
	if _, err := c.Shelve(FloodTypeID, time.Hour, "alice"); !errors.Is(err, ErrNotShelveable) {
		t.Fatalf("Shelve(alarm-flood) error = %v, want ErrNotShelveable", err)
	}

	// Catalog sweep: software faults and alarm-flood are the only refusals.
	for _, typ := range Types() {
		wantRefuse := typ.SoftwareFault || typ.IDPrefix == FloodTypeID
		if typ.Shelveable() == wantRefuse {
			t.Errorf("catalog %s: Shelveable() = %v, want %v", typ.IDPrefix, typ.Shelveable(), !wantRefuse)
		}
	}
	// The unregistered-type fallback is a software fault and must refuse too.
	if unregisteredAlarmType("mystery").Shelveable() {
		t.Error("unregistered-type fallback must be unshelveable")
	}
}

// TestLifecycle_ShelveAndExpiry: shelved alarms leave the active list but
// stay visible; expiry with the condition still true returns the alarm to
// ACTIVE — settled lazily on the next read, like every suppression window.
func TestLifecycle_ShelveAndExpiry(t *testing.T) {
	clock := newSuppressionClock()
	c := NewWithClock(clock.Now)

	c.Raise("disk-space-exhausted", "vault-1", "volume full")
	until, err := c.Shelve("disk-space-exhausted:vault-1", time.Hour, "alice")
	if err != nil {
		t.Fatalf("Shelve: %v", err)
	}
	if want := clock.Now().Add(time.Hour); !until.Equal(want) {
		t.Fatalf("shelved until %v, want %v", until, want)
	}

	a := mustState(t, c, "disk-space-exhausted:vault-1", StateShelved)
	if !a.ShelvedUntil.Equal(until) {
		t.Fatalf("snapshot ShelvedUntil = %v, want %v", a.ShelvedUntil, until)
	}
	if got := c.Count(); got != 0 {
		t.Fatalf("Count = %d, want 0 — shelved alarms leave the active list", got)
	}

	// Inside the window it stays shelved.
	clock.Advance(30 * time.Minute)
	mustState(t, c, "disk-space-exhausted:vault-1", StateShelved)

	// Expiry with the condition still true: back to ACTIVE, no residue.
	clock.Advance(31 * time.Minute)
	a = mustState(t, c, "disk-space-exhausted:vault-1", StateActive)
	if !a.ShelvedUntil.IsZero() {
		t.Fatalf("expired shelve left residue: %+v", a)
	}
	if got := c.Count(); got != 1 {
		t.Fatalf("Count = %d after expiry, want 1", got)
	}
}

// TestLifecycle_ShelveDoesNotSurviveRestart: shelve state is in-memory
// only. A fresh collector (the in-process restart) knows nothing of the
// shelve; the re-detected condition is simply an active alarm again. Loud
// is safe.
func TestLifecycle_ShelveDoesNotSurviveRestart(t *testing.T) {
	clock := newSuppressionClock()
	c1 := NewWithClock(clock.Now)

	c1.Raise("disk-space-exhausted", "vault-1", "volume full")
	if _, err := c1.Shelve("disk-space-exhausted:vault-1", 24*time.Hour, "alice"); err != nil {
		t.Fatalf("Shelve: %v", err)
	}
	mustState(t, c1, "disk-space-exhausted:vault-1", StateShelved)

	// Restart well inside the shelve window: the raiser re-detects the
	// standing condition; the alarm annunciates plain ACTIVE.
	clock.Advance(time.Minute)
	c2 := NewWithClock(clock.Now)
	c2.Raise("disk-space-exhausted", "vault-1", "volume full")
	a := mustState(t, c2, "disk-space-exhausted:vault-1", StateActive)
	if !a.ShelvedUntil.IsZero() {
		t.Fatalf("shelve resurrected across restart: %+v", a)
	}
}

// TestLifecycle_ConditionClearsWhileShelved: the operator deferred the
// alarm and the condition resolved inside the window — the alarm releases
// entirely, regardless of when the next read runs.
func TestLifecycle_ConditionClearsWhileShelved(t *testing.T) {
	clock := newSuppressionClock()
	c := NewWithClock(clock.Now)

	c.Raise("disk-space-exhausted", "vault-1", "volume full")
	if _, err := c.Shelve("disk-space-exhausted:vault-1", time.Hour, "alice"); err != nil {
		t.Fatalf("Shelve: %v", err)
	}
	clock.Advance(10 * time.Minute)
	c.Clear("disk-space-exhausted", "vault-1")
	mustGone(t, c, "disk-space-exhausted:vault-1")

	// Same story when the release is only observed LONG after the shelve
	// would have expired: lazy settling must not change outcomes.
	c.Raise("disk-space-exhausted", "vault-2", "volume full")
	if _, err := c.Shelve("disk-space-exhausted:vault-2", time.Hour, "alice"); err != nil {
		t.Fatalf("Shelve: %v", err)
	}
	clock.Advance(10 * time.Minute)
	c.Clear("disk-space-exhausted", "vault-2")
	clock.Advance(24 * time.Hour) // read long after the shelve's nominal expiry
	mustGone(t, c, "disk-space-exhausted:vault-2")
}

// TestLifecycle_Unshelve: ends the shelve early, returning the alarm to
// ACTIVE; unshelving a non-shelved alarm errors.
func TestLifecycle_Unshelve(t *testing.T) {
	clock := newSuppressionClock()
	c := NewWithClock(clock.Now)

	c.Raise("disk-space-exhausted", "vault-1", "volume full")
	if err := c.Unshelve("disk-space-exhausted:vault-1"); !errors.Is(err, ErrNotShelved) {
		t.Fatalf("Unshelve(not shelved) error = %v, want ErrNotShelved", err)
	}
	if _, err := c.Shelve("disk-space-exhausted:vault-1", time.Hour, "alice"); err != nil {
		t.Fatalf("Shelve: %v", err)
	}
	mustState(t, c, "disk-space-exhausted:vault-1", StateShelved)
	if err := c.Unshelve("disk-space-exhausted:vault-1"); err != nil {
		t.Fatalf("Unshelve: %v", err)
	}
	mustState(t, c, "disk-space-exhausted:vault-1", StateActive)
}

// TestLifecycle_UnknownAndPendingAlarms: shelve operations on unknown IDs
// and on conditions still inside their delay-on window (not yet
// annunciated — not standing) return ErrUnknownAlarm.
func TestLifecycle_UnknownAndPendingAlarms(t *testing.T) {
	clock := newSuppressionClock()
	c := NewWithClock(clock.Now)

	if _, err := c.Shelve("no-such-alarm", time.Hour, "alice"); !errors.Is(err, ErrUnknownAlarm) {
		t.Fatalf("Shelve(unknown) error = %v, want ErrUnknownAlarm", err)
	}
	if err := c.Unshelve("no-such-alarm"); !errors.Is(err, ErrUnknownAlarm) {
		t.Fatalf("Unshelve(unknown) error = %v, want ErrUnknownAlarm", err)
	}

	// A condition inside its delay-on window has not annunciated: there is
	// nothing standing to shelve.
	c.Raise(leaderlessType, "v", "no leader")
	if _, err := c.Shelve(leaderlessType+":v", time.Hour, "alice"); !errors.Is(err, ErrUnknownAlarm) {
		t.Fatalf("Shelve(pending) error = %v, want ErrUnknownAlarm", err)
	}

	// An alarm that fully released is unknown again.
	c.Raise("disk-space-exhausted", "vault-1", "full")
	c.Clear("disk-space-exhausted", "vault-1")
	if _, err := c.Shelve("disk-space-exhausted:vault-1", time.Hour, "alice"); !errors.Is(err, ErrUnknownAlarm) {
		t.Fatalf("Shelve(released) error = %v, want ErrUnknownAlarm", err)
	}
}

// TestLifecycle_ShelveIsIdempotent: re-shelving refreshes the expiry
// instead of erroring — the cross-node fan-out retries after partial
// failures.
func TestLifecycle_ShelveIsIdempotent(t *testing.T) {
	clock := newSuppressionClock()
	c := NewWithClock(clock.Now)

	c.Raise("disk-space-exhausted", "vault-1", "volume full")
	if _, err := c.Shelve("disk-space-exhausted:vault-1", time.Hour, "alice"); err != nil {
		t.Fatalf("first Shelve: %v", err)
	}
	clock.Advance(time.Minute)
	until, err := c.Shelve("disk-space-exhausted:vault-1", 2*time.Hour, "bob")
	if err != nil {
		t.Fatalf("second Shelve: %v", err)
	}
	a := mustState(t, c, "disk-space-exhausted:vault-1", StateShelved)
	if !a.ShelvedUntil.Equal(until) || !until.Equal(clock.Now().Add(2*time.Hour)) {
		t.Fatalf("re-shelve must refresh the expiry: %v, want %v", a.ShelvedUntil, until)
	}
}

// TestLifecycle_DelayOffResolutionUsesWindowCloseInstant: an alarm with a
// delay-off window releases when the window closes; the verdict is
// identical whether the next read runs immediately or much later.
func TestLifecycle_DelayOffResolutionUsesWindowCloseInstant(t *testing.T) {
	clock := newSuppressionClock()
	c := NewWithClock(clock.Now)

	delayedRaise(c, "flappy", "up", 0, 5*time.Minute, false)
	c.Clear("flappy", "")
	// Inside the delay-off window the alarm is still active.
	clock.Advance(2 * time.Minute)
	mustState(t, c, "flappy", StateActive)
	// Long after the window closed: released — regardless of the read
	// arriving hours late.
	clock.Advance(6 * time.Hour)
	mustGone(t, c, "flappy")
}
