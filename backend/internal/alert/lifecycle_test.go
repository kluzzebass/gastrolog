package alert

// Alarm lifecycle tests (gastrolog-1z5gg4, EEMUA 191 principles 5 & 6):
// the four states — active-unacked, active-acked, cleared-unacked,
// shelved — layered on the suppression entry. Every test drives the
// collector's injected clock; shelve expiry is a time construct settled
// lazily, so there is not a single sleep here and never should be.

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

// TestLifecycle_AckActiveAlarm: active-unacked → (ack) → active-acked with
// who/when recorded → (condition resolves) → gone, silently.
func TestLifecycle_AckActiveAlarm(t *testing.T) {
	clock := newSuppressionClock()
	c := NewWithClock(clock.Now)

	c.Raise("disk-space-exhausted", "vault-1", "volume full")
	a := mustState(t, c, "disk-space-exhausted:vault-1", StateActiveUnacked)
	if a.Occurrences != 1 {
		t.Fatalf("occurrences = %d, want 1", a.Occurrences)
	}

	if err := c.Ack("disk-space-exhausted:vault-1", "alice"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	a = mustState(t, c, "disk-space-exhausted:vault-1", StateActiveAcked)
	if a.AckedBy != "alice" || !a.AckedAt.Equal(clock.Now()) {
		t.Fatalf("ack record = %q @ %v, want alice @ %v", a.AckedBy, a.AckedAt, clock.Now())
	}
	// Acked alarms stay in the active list (Active) — the condition stands.
	if got := c.Count(); got != 1 {
		t.Fatalf("Count = %d, want 1", got)
	}

	// Condition resolves: acked alarms release silently, no cleared-unacked.
	c.Clear("disk-space-exhausted", "vault-1")
	mustGone(t, c, "disk-space-exhausted:vault-1")
}

// TestLifecycle_ClearedUnackedRetention: an unacked alarm whose condition
// resolves is retained as cleared-unacked — out of the active list but
// visible — until an operator acks it.
func TestLifecycle_ClearedUnackedRetention(t *testing.T) {
	clock := newSuppressionClock()
	c := NewWithClock(clock.Now)

	c.Raise("disk-space-exhausted", "vault-1", "volume full")
	c.Clear("disk-space-exhausted", "vault-1")

	a := mustState(t, c, "disk-space-exhausted:vault-1", StateClearedUnacked)
	if a.Occurrences != 1 {
		t.Fatalf("occurrences = %d, want 1", a.Occurrences)
	}
	if got := c.Count(); got != 0 {
		t.Fatalf("Count = %d, want 0 — cleared-unacked must not block the active list", got)
	}
	if c.Active() != nil {
		t.Fatal("Active() must exclude cleared-unacked alarms")
	}

	// Ack is what it was waiting for.
	if err := c.Ack("disk-space-exhausted:vault-1", "alice"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	mustGone(t, c, "disk-space-exhausted:vault-1")
}

// TestLifecycle_NewOccurrenceOnClearedEntry: a condition returning on a
// retained cleared-unacked entry is a NEW occurrence — the delay-on window
// runs again, the occurrence count increments, FirstSeen resets to the new
// condition start, and any previous acknowledgment does not carry over.
func TestLifecycle_NewOccurrenceOnClearedEntry(t *testing.T) {
	clock := newSuppressionClock()
	c := NewWithClock(clock.Now)
	window := leaderlessWindow(t)

	c.Raise(leaderlessType, "v", "no leader")
	clock.Advance(window + time.Second)
	mustState(t, c, leaderlessType+":v", StateActiveUnacked)
	c.Clear(leaderlessType, "v")
	mustState(t, c, leaderlessType+":v", StateClearedUnacked)
	firstOccurrenceStart := standingByID(c)[leaderlessType+":v"].FirstSeen

	// Condition returns: the entry stays cleared-unacked while the new
	// occurrence sits inside its delay-on window — chattering suppression
	// applies to the new occurrence exactly as to a fresh alarm.
	clock.Advance(time.Hour)
	c.Raise(leaderlessType, "v", "no leader again")
	mustState(t, c, leaderlessType+":v", StateClearedUnacked)

	clock.Advance(window + time.Second)
	a := mustState(t, c, leaderlessType+":v", StateActiveUnacked)
	if a.Occurrences != 2 {
		t.Fatalf("occurrences = %d, want 2", a.Occurrences)
	}
	if !a.FirstSeen.After(firstOccurrenceStart) {
		t.Fatalf("FirstSeen must reset to the new occurrence's condition start; got %v (old %v)",
			a.FirstSeen, firstOccurrenceStart)
	}
	if a.AckedBy != "" {
		t.Fatal("acknowledgment must not carry over to a new occurrence")
	}
}

// TestLifecycle_LatchedClearViaAck completes the phase-3 interim: a latched
// alarm whose condition resolves stands active-unacked until acknowledged,
// and the ack releases it (resolution first, ack second).
func TestLifecycle_LatchedClearViaAck(t *testing.T) {
	clock := newSuppressionClock()
	c := NewWithClock(clock.Now)

	delayedRaise(c, "latched-alarm", "condition up", 0, 0, true)
	mustState(t, c, "latched-alarm", StateActiveUnacked)

	c.Clear("latched-alarm", "")
	// Latched: stays standing in the ACTIVE list, not cleared-unacked.
	mustState(t, c, "latched-alarm", StateActiveUnacked)
	if got := c.Count(); got != 1 {
		t.Fatalf("Count = %d, want 1 — a latched alarm stands until acked", got)
	}
	clock.Advance(24 * time.Hour)
	mustState(t, c, "latched-alarm", StateActiveUnacked)

	if err := c.Ack("latched-alarm", "alice"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	mustGone(t, c, "latched-alarm")
}

// TestLifecycle_LatchedAckThenClear is the other order: ack while the
// condition still stands (active-acked), then the condition resolves and
// the latch is satisfied — released on the Clear.
func TestLifecycle_LatchedAckThenClear(t *testing.T) {
	clock := newSuppressionClock()
	c := NewWithClock(clock.Now)

	delayedRaise(c, "latched-alarm", "condition up", 0, 0, true)
	if err := c.Ack("latched-alarm", "alice"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	mustState(t, c, "latched-alarm", StateActiveAcked)

	c.Clear("latched-alarm", "")
	mustGone(t, c, "latched-alarm")
}

// TestLifecycle_LockLeakAckIsTheOnlyRelease pins the exact lifecycle of
// orchestrator-lock-leak: the raiser never calls Clear (a leaked hold
// cannot be observed releasing), so the path is active-unacked →
// active-acked → (restart) gone. It never reaches cleared-unacked on its
// own, no matter how much time passes.
func TestLifecycle_LockLeakAckIsTheOnlyRelease(t *testing.T) {
	clock := newSuppressionClock()
	c := NewWithClock(clock.Now)

	c.Raise("orchestrator-lock-leak", "", "read hold stuck for 2m")
	a := mustState(t, c, "orchestrator-lock-leak", StateActiveUnacked)
	if a.Shelveable {
		t.Fatal("lock-leak must be unshelveable on the snapshot")
	}

	if err := c.Ack("orchestrator-lock-leak", "alice"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	mustState(t, c, "orchestrator-lock-leak", StateActiveAcked)

	// Days pass; no Clear ever arrives. The fault stands acked — visible,
	// attributed, never cleared-unacked — until the process restarts.
	clock.Advance(72 * time.Hour)
	mustState(t, c, "orchestrator-lock-leak", StateActiveAcked)
	if got := c.Count(); got != 1 {
		t.Fatalf("Count = %d, want 1", got)
	}
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
	mustState(t, c, "disk-space-exhausted:vault-1", StateActiveUnacked)
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
	mustState(t, c, "orchestrator-lock-leak", StateActiveUnacked)

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
// active-unacked (any acknowledgment resets with the shelve) — settled
// lazily on the next read, like every suppression window.
func TestLifecycle_ShelveAndExpiry(t *testing.T) {
	clock := newSuppressionClock()
	c := NewWithClock(clock.Now)

	c.Raise("disk-space-exhausted", "vault-1", "volume full")
	if err := c.Ack("disk-space-exhausted:vault-1", "alice"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
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

	// Expiry with the condition still true: back to active-unacked. The
	// pre-shelve ack does not survive — the deferral window is over and the
	// condition demands fresh attention.
	clock.Advance(31 * time.Minute)
	a = mustState(t, c, "disk-space-exhausted:vault-1", StateActiveUnacked)
	if a.AckedBy != "" || !a.ShelvedUntil.IsZero() {
		t.Fatalf("expired shelve left residue: %+v", a)
	}
	if got := c.Count(); got != 1 {
		t.Fatalf("Count = %d after expiry, want 1", got)
	}
}

// TestLifecycle_ConditionClearsWhileShelved: the operator deferred the
// alarm and the condition resolved inside the window — the shelve covered
// the awareness function, so the alarm releases entirely (no cleared-
// unacked comeback), regardless of when the next read runs.
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
	// would have expired: the verdict is taken at the resolution instant,
	// not the read instant (lazy settling must not change outcomes).
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
// active-unacked; unshelving a non-shelved alarm errors.
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
	mustState(t, c, "disk-space-exhausted:vault-1", StateActiveUnacked)
}

// TestLifecycle_UnknownAndPendingAlarms: lifecycle operations on unknown
// IDs and on conditions still inside their delay-on window (not yet
// annunciated — not standing) return ErrUnknownAlarm.
func TestLifecycle_UnknownAndPendingAlarms(t *testing.T) {
	clock := newSuppressionClock()
	c := NewWithClock(clock.Now)

	if err := c.Ack("no-such-alarm", "alice"); !errors.Is(err, ErrUnknownAlarm) {
		t.Fatalf("Ack(unknown) error = %v, want ErrUnknownAlarm", err)
	}
	if _, err := c.Shelve("no-such-alarm", time.Hour, "alice"); !errors.Is(err, ErrUnknownAlarm) {
		t.Fatalf("Shelve(unknown) error = %v, want ErrUnknownAlarm", err)
	}
	if err := c.Unshelve("no-such-alarm"); !errors.Is(err, ErrUnknownAlarm) {
		t.Fatalf("Unshelve(unknown) error = %v, want ErrUnknownAlarm", err)
	}

	// A condition inside its delay-on window has not annunciated: there is
	// nothing standing to ack.
	c.Raise(leaderlessType, "v", "no leader")
	if err := c.Ack(leaderlessType+":v", "alice"); !errors.Is(err, ErrUnknownAlarm) {
		t.Fatalf("Ack(pending) error = %v, want ErrUnknownAlarm", err)
	}

	// An alarm that fully released is unknown again: raise, ack, clear.
	c.Raise("disk-space-exhausted", "vault-1", "full")
	if err := c.Ack("disk-space-exhausted:vault-1", "alice"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	c.Clear("disk-space-exhausted", "vault-1")
	if err := c.Ack("disk-space-exhausted:vault-1", "alice"); !errors.Is(err, ErrUnknownAlarm) {
		t.Fatalf("Ack(released) error = %v, want ErrUnknownAlarm", err)
	}
}

// TestLifecycle_ShelveClearedAlarmRefused: shelving a cleared-unacked alarm
// is refused — there is nothing standing to defer; ack is the operation
// that releases it.
func TestLifecycle_ShelveClearedAlarmRefused(t *testing.T) {
	clock := newSuppressionClock()
	c := NewWithClock(clock.Now)

	c.Raise("disk-space-exhausted", "vault-1", "volume full")
	c.Clear("disk-space-exhausted", "vault-1")
	mustState(t, c, "disk-space-exhausted:vault-1", StateClearedUnacked)
	if _, err := c.Shelve("disk-space-exhausted:vault-1", time.Hour, "alice"); !errors.Is(err, ErrAlarmCleared) {
		t.Fatalf("Shelve(cleared) error = %v, want ErrAlarmCleared", err)
	}
}

// TestLifecycle_AckIsIdempotent: re-acking refreshes who/when instead of
// erroring — the cross-node fan-out retries after partial failures.
func TestLifecycle_AckIsIdempotent(t *testing.T) {
	clock := newSuppressionClock()
	c := NewWithClock(clock.Now)

	c.Raise("disk-space-exhausted", "vault-1", "volume full")
	if err := c.Ack("disk-space-exhausted:vault-1", "alice"); err != nil {
		t.Fatalf("first Ack: %v", err)
	}
	clock.Advance(time.Minute)
	if err := c.Ack("disk-space-exhausted:vault-1", "bob"); err != nil {
		t.Fatalf("second Ack: %v", err)
	}
	a := mustState(t, c, "disk-space-exhausted:vault-1", StateActiveAcked)
	if a.AckedBy != "bob" {
		t.Fatalf("acked_by = %q, want bob (refreshed)", a.AckedBy)
	}
}

// TestLifecycle_DelayOffResolutionUsesWindowCloseInstant: an acked alarm
// with a delay-off window releases when the window closes; the verdict is
// identical whether the next read runs immediately or much later.
func TestLifecycle_DelayOffResolutionUsesWindowCloseInstant(t *testing.T) {
	clock := newSuppressionClock()
	c := NewWithClock(clock.Now)

	delayedRaise(c, "flappy", "up", 0, 5*time.Minute, false)
	if err := c.Ack("flappy", "alice"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	c.Clear("flappy", "")
	// Inside the delay-off window the alarm is still active (acked).
	clock.Advance(2 * time.Minute)
	mustState(t, c, "flappy", StateActiveAcked)
	// Long after the window closed: released (acked at close), not
	// cleared-unacked — regardless of the read arriving hours late.
	clock.Advance(6 * time.Hour)
	mustGone(t, c, "flappy")

	// The unacked twin resolves to cleared-unacked at the same instant.
	delayedRaise(c, "flappy2", "up", 0, 5*time.Minute, false)
	c.Clear("flappy2", "")
	clock.Advance(6 * time.Hour)
	mustState(t, c, "flappy2", StateClearedUnacked)
}
