package alert

// Chattering-suppression tests (gastrolog-4wvxqh, EEMUA 191 principle 3).
// Every test drives the collector's injected clock — suppression state is a
// pure function of Raise/Clear calls and that clock, so there is not a
// single sleep here and never should be.
//
// Catalog-driven paths use real catalog types (vault-leaderless for
// DelayOn, orchestrator-lock-leak for Latching). The catalog declares no
// DelayOff today, so the delay-off mechanism is exercised through the
// internal raise() the catalog path lands on — same state machine, one
// hop below TypeByID.

import (
	"sync"
	"testing"
	"time"
)

// suppressionClock is a deterministic clock for collector tests.
type suppressionClock struct {
	mu  sync.Mutex
	now time.Time
}

func newSuppressionClock() *suppressionClock {
	return &suppressionClock{now: time.Date(2026, 7, 1, 9, 0, 0, 0, time.UTC)}
}

func (c *suppressionClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *suppressionClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// delayedRaise raises a synthetic alarm through the internal state machine
// with explicit suppression parameters — the hook for delay-off and
// latching combinations the catalog does not currently declare.
func delayedRaise(c *Collector, id, detail string, delayOn, delayOff time.Duration, latching bool) {
	c.raise(Alarm{
		ID:     id,
		TypeID: id,
		Source: "test",
		Detail: detail,
	}, delayOn, delayOff, latching)
}

const leaderlessType = "vault-leaderless" // catalog: DelayOn 60s, no DelayOff, not latching

func leaderlessWindow(t *testing.T) time.Duration {
	t.Helper()
	typ, ok := TypeByID(leaderlessType)
	if !ok || typ.DelayOn <= 0 {
		t.Fatalf("catalog: %s must declare a DelayOn", leaderlessType)
	}
	return typ.DelayOn
}

func TestDelayOnFlappingConditionNeverActivates(t *testing.T) {
	clk := newSuppressionClock()
	c := NewWithClock(clk.Now)
	window := leaderlessWindow(t)

	// Three raise/clear cycles, each shorter than the window: classic
	// chattering. Nothing may ever annunciate.
	for i := 0; i < 3; i++ {
		c.Raise(leaderlessType, "v1", "flap")
		clk.Advance(window / 2)
		if got := c.Count(); got != 0 {
			t.Fatalf("cycle %d: alarm active inside the delay-on window (count=%d)", i, got)
		}
		c.Clear(leaderlessType, "v1")
		clk.Advance(window / 2)
		if got := c.Count(); got != 0 {
			t.Fatalf("cycle %d: cleared flap left an alarm standing (count=%d)", i, got)
		}
	}
	// Even the SUM of the flapping exceeded the window; only continuous
	// persistence counts, so still nothing.
	if c.Active() != nil {
		t.Fatal("flapping below DelayOn must never activate")
	}
}

func TestDelayOnPersistingConditionActivatesOnceWithConditionStartFirstSeen(t *testing.T) {
	clk := newSuppressionClock()
	c := NewWithClock(clk.Now)
	window := leaderlessWindow(t)
	conditionStart := clk.Now()

	c.Raise(leaderlessType, "v1", "no leader")
	if c.Count() != 0 {
		t.Fatal("active before the delay-on window elapsed")
	}

	// Lazy evaluation: no re-raise — the persisting condition activates on
	// the next read after the window.
	clk.Advance(window + time.Second)
	alarms := c.Active()
	if len(alarms) != 1 {
		t.Fatalf("persisting condition must activate; active=%d", len(alarms))
	}
	// FirstSeen is the CONDITION start, not the activation instant: the
	// window suppresses annunciation, not the condition's history.
	if !alarms[0].FirstSeen.Equal(conditionStart) {
		t.Errorf("FirstSeen = %v, want condition start %v", alarms[0].FirstSeen, conditionStart)
	}

	// Re-raising while active refreshes detail but preserves the occurrence.
	c.Raise(leaderlessType, "v1", "still no leader")
	alarms = c.Active()
	if len(alarms) != 1 {
		t.Fatalf("re-raise must not duplicate; active=%d", len(alarms))
	}
	if !alarms[0].FirstSeen.Equal(conditionStart) {
		t.Error("FirstSeen changed on re-raise of an active alarm")
	}
	if alarms[0].Detail != "still no leader" {
		t.Errorf("detail = %q", alarms[0].Detail)
	}
}

func TestDelayOnActivatesViaReRaiseToo(t *testing.T) {
	clk := newSuppressionClock()
	c := NewWithClock(clk.Now)
	window := leaderlessWindow(t)

	// Sweep-style raiser: re-raises every 15s, no reads in between. The
	// re-raise itself settles the window once it has elapsed.
	c.Raise(leaderlessType, "v1", "tick")
	for elapsed := time.Duration(0); elapsed < window; elapsed += 15 * time.Second {
		clk.Advance(15 * time.Second)
		c.Raise(leaderlessType, "v1", "tick")
	}
	if c.Count() != 1 {
		t.Fatal("condition re-raised past its window must be active")
	}
}

func TestDelayOnClearAfterWindowWithoutReadStillCounts(t *testing.T) {
	clk := newSuppressionClock()
	c := NewWithClock(clk.Now)

	// Non-latching, no delay-off: raise, persist past the window with no
	// reads, then clear — the occurrence happened but nothing remains.
	delayedRaise(c, "won", "persisted unseen", time.Minute, 0, false)
	clk.Advance(2 * time.Minute)
	c.Clear("won", "")
	if c.Count() != 0 {
		t.Fatal("cleared non-latching alarm must be gone")
	}

	// Latching with delay-on: the same shape must LATCH — the condition
	// outlived the window even though no read observed it in between.
	delayedRaise(c, "latch-won", "persisted unseen", time.Minute, 0, true)
	clk.Advance(2 * time.Minute)
	c.Clear("latch-won", "")
	if c.Count() != 1 {
		t.Fatal("latched alarm whose condition outlived delay-on must survive the clear")
	}
}

func TestDelayOffReRaiseInsideWindowIsOneContinuousOccurrence(t *testing.T) {
	clk := newSuppressionClock()
	c := NewWithClock(clk.Now)
	conditionStart := clk.Now()

	delayedRaise(c, "doff", "up", 0, time.Minute, false)
	if c.Count() != 1 {
		t.Fatal("zero delay-on must activate immediately")
	}

	// Condition clears, then returns inside the delay-off window — the
	// alarm must stay active the whole time, same occurrence.
	c.Clear("doff", "")
	if c.Count() != 1 {
		t.Fatal("active alarm must remain active inside its delay-off window")
	}
	clk.Advance(30 * time.Second)
	if c.Count() != 1 {
		t.Fatal("alarm dropped before the delay-off window elapsed")
	}
	delayedRaise(c, "doff", "up again", 0, time.Minute, false)
	alarms := c.Active()
	if len(alarms) != 1 {
		t.Fatalf("re-raise inside delay-off must be the same occurrence; active=%d", len(alarms))
	}
	if !alarms[0].FirstSeen.Equal(conditionStart) {
		t.Error("FirstSeen changed across a clear/re-raise inside the delay-off window — that is a phantom re-occurrence")
	}

	// And the resumed condition keeps the alarm active indefinitely.
	clk.Advance(10 * time.Minute)
	if c.Count() != 1 {
		t.Fatal("resumed condition must keep the alarm active")
	}
}

func TestDelayOffExpiresAfterConditionStaysClear(t *testing.T) {
	clk := newSuppressionClock()
	c := NewWithClock(clk.Now)

	delayedRaise(c, "doff", "up", 0, time.Minute, false)
	c.Clear("doff", "")
	clk.Advance(time.Minute + time.Second)
	if c.Count() != 0 {
		t.Fatal("alarm must auto-clear once the condition stays clear past delay-off")
	}

	// A raise AFTER the window expired is a fresh occurrence with a fresh
	// FirstSeen.
	freshStart := clk.Now()
	delayedRaise(c, "doff", "up later", 0, time.Minute, false)
	alarms := c.Active()
	if len(alarms) != 1 {
		t.Fatalf("fresh occurrence must activate; active=%d", len(alarms))
	}
	if !alarms[0].FirstSeen.Equal(freshStart) {
		t.Errorf("FirstSeen = %v, want fresh start %v", alarms[0].FirstSeen, freshStart)
	}
}

func TestDelayOffRepeatClearKeepsFirstClearInstant(t *testing.T) {
	clk := newSuppressionClock()
	c := NewWithClock(clk.Now)

	delayedRaise(c, "doff", "up", 0, time.Minute, false)
	c.Clear("doff", "")
	clk.Advance(45 * time.Second)
	// A second clear must NOT restart the window: the condition has been
	// down since the first clear.
	c.Clear("doff", "")
	clk.Advance(20 * time.Second) // 65s since the first clear, 20s since the second
	if c.Count() != 0 {
		t.Fatal("delay-off window must measure from the first clear, not be restarted by repeats")
	}
}

func TestLatchingAlarmSurvivesConditionClear(t *testing.T) {
	clk := newSuppressionClock()
	c := NewWithClock(clk.Now)

	// orchestrator-lock-leak: catalog-declared latching software fault.
	c.Raise("orchestrator-lock-leak", "", "read hold stuck for 2m")
	if c.Count() != 1 {
		t.Fatal("lock-leak must activate immediately (zero delay-on)")
	}
	c.Clear("orchestrator-lock-leak", "")
	if c.Count() != 1 {
		t.Fatal("latched alarm must survive its condition clearing")
	}
	// Time does not clear it either — a latched alarm has no release path;
	// it stands until process restart.
	clk.Advance(24 * time.Hour)
	alarms := c.Active()
	if len(alarms) != 1 {
		t.Fatal("latched alarm must stand until process restart")
	}
	if !alarms[0].SoftwareFault {
		t.Error("lock-leak must remain a software fault through suppression")
	}

	// The condition returning and clearing again changes nothing.
	c.Raise("orchestrator-lock-leak", "", "write wait stuck for 3m")
	c.Clear("orchestrator-lock-leak", "")
	if c.Count() != 1 {
		t.Fatal("latched alarm must survive repeat raise/clear cycles")
	}
}

func TestZeroDelayTypesBehaveExactlyAsBefore(t *testing.T) {
	clk := newSuppressionClock()
	c := NewWithClock(clk.Now)

	// cloud-store has no suppression: raise → visible, clear → gone,
	// with no clock movement at all.
	c.Raise("cloud-store", "v1", "unreachable")
	if c.Count() != 1 {
		t.Fatal("zero-delay type must activate on raise")
	}
	c.Clear("cloud-store", "v1")
	if c.Count() != 0 {
		t.Fatal("zero-delay type must clear immediately")
	}
}

func TestUnknownTypeRaiseIsImmediateDespiteSuppression(t *testing.T) {
	clk := newSuppressionClock()
	c := NewWithClock(clk.Now)

	// The software-fault path for uncataloged raises must not gain a
	// window: it surfaces on the very next read, no clock movement.
	c.Raise("no-such-type", "x", "the condition detail")
	alarms := c.Active()
	if len(alarms) != 1 {
		t.Fatalf("unregistered raise must surface immediately; active=%d", len(alarms))
	}
	if !alarms[0].SoftwareFault || alarms[0].Detail != "the condition detail" {
		t.Fatalf("unregistered fallback mangled: %+v", alarms[0])
	}
}

func TestPendingConditionIsInvisibleToActiveAndCount(t *testing.T) {
	clk := newSuppressionClock()
	c := NewWithClock(clk.Now)
	window := leaderlessWindow(t)

	c.Raise(leaderlessType, "v1", "no leader")
	clk.Advance(window / 2)
	if c.Active() != nil {
		t.Fatal("pending condition leaked into Active()")
	}
	if c.Count() != 0 {
		t.Fatal("pending condition counted")
	}
	// …and both reads settle: once the window elapses they surface it.
	clk.Advance(window)
	if c.Count() != 1 {
		t.Fatal("Count must settle pending conditions")
	}
}

func TestSuppressionConcurrency(t *testing.T) {
	clk := newSuppressionClock()
	c := NewWithClock(clk.Now)
	done := make(chan struct{})
	for i := 0; i < 4; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 200; j++ {
				c.Raise(leaderlessType, "shared", "flap")
				clk.Advance(time.Millisecond)
				c.Active()
				c.Clear(leaderlessType, "shared")
				c.Count()
			}
		}()
	}
	for i := 0; i < 4; i++ {
		<-done
	}
}
