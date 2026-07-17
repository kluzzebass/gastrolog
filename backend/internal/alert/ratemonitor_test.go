package alert

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// testClock is a deterministic, manually advanced clock. The rolling window
// is a time construct: tests advance the clock, never sleep.
type testClock struct {
	t time.Time
}

func newTestClock() *testClock {
	return &testClock{t: time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)}
}

func (c *testClock) now() time.Time          { return c.t }
func (c *testClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// newMonitoredCollector wires a collector to a rate monitor exactly as
// app wiring does: activation hook in, flood raises back through the sink.
func newMonitoredCollector(clock *testClock) (*Collector, *RateMonitor) {
	c := New()
	m := NewRateMonitor(c, clock.now)
	c.SetOnActivate(m.Observe)
	return c, m
}

func floodAlarm(c *Collector) *Alarm {
	for _, a := range c.Active() {
		if a.TypeID == FloodTypeID {
			return a
		}
	}
	return nil
}

// raiseDistinct raises n distinct alarm instances of a cataloged type.
func raiseDistinct(c *Collector, prefix string, n int) {
	for i := 0; i < n; i++ {
		c.Raise("node-unreachable", fmt.Sprintf("%s-%d", prefix, i), "peer gone")
	}
}

func TestRateGaugeRollsAcrossWindowBoundary(t *testing.T) {
	clock := newTestClock()
	c, m := newMonitoredCollector(clock)

	raiseDistinct(c, "a", 5)
	clock.advance(5 * time.Minute)
	raiseDistinct(c, "b", 5)

	if got := m.Rate(); got != 10 {
		t.Fatalf("rate = %d, want 10", got)
	}
	// 9m59s after the first batch: still inside the window.
	clock.advance(4*time.Minute + 59*time.Second)
	if got := m.Rate(); got != 10 {
		t.Fatalf("rate at 9m59s = %d, want 10", got)
	}
	// 10m after the first batch: the first 5 age out.
	clock.advance(1 * time.Second)
	if got := m.Rate(); got != 5 {
		t.Fatalf("rate at 10m = %d, want 5", got)
	}
	// 10m after the second batch: empty window.
	clock.advance(5 * time.Minute)
	if got := m.Rate(); got != 0 {
		t.Fatalf("rate at 15m = %d, want 0", got)
	}
}

func TestRefreshOfActiveAlarmIsNotAnActivation(t *testing.T) {
	clock := newTestClock()
	c, m := newMonitoredCollector(clock)

	for i := 0; i < 100; i++ {
		c.Raise("node-unreachable", "same-node", "peer gone")
	}
	if got := m.Rate(); got != 1 {
		t.Fatalf("rate = %d after 100 refreshes of one alarm, want 1", got)
	}
}

func TestExactlyOneFloodAlarmRegardlessOfOvershoot(t *testing.T) {
	clock := newTestClock()
	c, m := newMonitoredCollector(clock)

	raiseDistinct(c, "n", 500) // 50x over the default threshold

	floods := 0
	for _, a := range c.Active() {
		if a.TypeID == FloodTypeID {
			floods++
		}
	}
	if floods != 1 {
		t.Fatalf("flood alarms = %d, want exactly 1", floods)
	}
	f := floodAlarm(c)
	if f.Priority != High {
		t.Fatalf("flood priority = %v, want High", f.Priority)
	}
	if !strings.Contains(f.Detail, "500 alarms") {
		t.Fatalf("flood detail %q does not carry the observed rate", f.Detail)
	}
	if !strings.Contains(f.Detail, fmt.Sprintf("threshold %d", DefaultFloodThreshold)) {
		t.Fatalf("flood detail %q does not carry the threshold", f.Detail)
	}
	if got := m.Rate(); got != 500 {
		t.Fatalf("rate = %d, want 500 (the flood alarm itself must not count)", got)
	}
}

func TestNoFloodAtOrUnderThreshold(t *testing.T) {
	clock := newTestClock()
	c, _ := newMonitoredCollector(clock)

	raiseDistinct(c, "n", DefaultFloodThreshold) // exactly at threshold
	if f := floodAlarm(c); f != nil {
		t.Fatalf("flood raised at threshold; must raise only over it")
	}
	raiseDistinct(c, "x", 1) // one over
	if f := floodAlarm(c); f == nil {
		t.Fatalf("flood not raised one over threshold")
	}
}

func TestFloodClearsAfterFullCleanWindow(t *testing.T) {
	clock := newTestClock()
	c, m := newMonitoredCollector(clock)

	raiseDistinct(c, "n", 11) // over the default threshold of 10, all at t0
	if floodAlarm(c) == nil {
		t.Fatal("flood not raised")
	}

	// The count decays to <= threshold at t0+10m (activations expire);
	// the flood clears a full window later, at t0+20m — not before.
	clock.advance(19*time.Minute + 59*time.Second)
	m.Evaluate()
	if floodAlarm(c) == nil {
		t.Fatal("flood cleared before a full under-threshold window elapsed")
	}
	clock.advance(1 * time.Second)
	m.Evaluate()
	if f := floodAlarm(c); f != nil {
		t.Fatalf("flood still active after a full clean window: %+v", f)
	}
}

func TestFloodDoesNotClearWhileRateStaysOver(t *testing.T) {
	clock := newTestClock()
	c, m := newMonitoredCollector(clock)

	// Sustained flood: 11 fresh activations every 5 minutes.
	for batch := 0; batch < 6; batch++ {
		raiseDistinct(c, fmt.Sprintf("b%d", batch), 11)
		clock.advance(5 * time.Minute)
		m.Evaluate()
		if floodAlarm(c) == nil {
			t.Fatalf("flood cleared mid-upset at batch %d", batch)
		}
	}
}

func TestRefloodAfterClear(t *testing.T) {
	clock := newTestClock()
	c, m := newMonitoredCollector(clock)

	raiseDistinct(c, "first", 11)
	clock.advance(20 * time.Minute)
	m.Evaluate()
	if floodAlarm(c) != nil {
		t.Fatal("flood did not clear")
	}

	raiseDistinct(c, "second", 11)
	if floodAlarm(c) == nil {
		t.Fatal("flood did not re-raise after clearing")
	}
}

func TestThresholdChangeTakesEffect(t *testing.T) {
	clock := newTestClock()
	c, m := newMonitoredCollector(clock)

	// Raised threshold: 50 activations stay under 100.
	m.SetThreshold(100)
	raiseDistinct(c, "n", 50)
	if floodAlarm(c) != nil {
		t.Fatal("flood raised under a raised threshold")
	}

	// Lowering the threshold floods on the next evaluation, no new raise
	// needed — this is how a settings change reaches a hot window.
	m.SetThreshold(10)
	m.Evaluate()
	if floodAlarm(c) == nil {
		t.Fatal("flood not raised after threshold lowered below the current rate")
	}

	// Zero (the stored "unset" value) selects the default.
	m.SetThreshold(0)
	if got := m.Threshold(); got != DefaultFloodThreshold {
		t.Fatalf("threshold after SetThreshold(0) = %d, want default %d", got, DefaultFloodThreshold)
	}
}

func TestPerNodeIsolation(t *testing.T) {
	clock := newTestClock()
	// Two nodes: two collectors, two monitors — the collector is per-node.
	cA, _ := newMonitoredCollector(clock)
	cB, mB := newMonitoredCollector(clock)

	raiseDistinct(cA, "n", 50) // node A floods
	raiseDistinct(cB, "n", 2)  // node B stays calm

	if floodAlarm(cA) == nil {
		t.Fatal("node A flood not raised")
	}
	if f := floodAlarm(cB); f != nil {
		t.Fatalf("node B flooded from node A's alarms: %+v", f)
	}
	if got := mB.Rate(); got != 2 {
		t.Fatalf("node B rate = %d, want 2", got)
	}
}

func TestOperatorAlarmsCountTowardRate(t *testing.T) {
	clock := newTestClock()
	c, m := newMonitoredCollector(clock)

	for i := 0; i < 3; i++ {
		c.RaiseOperator(OperatorAlarm{
			TypeID:      "retention-rate",
			InstanceKey: fmt.Sprintf("vault-%d", i),
			Priority:    Low,
			Source:      "ratealerter",
			Detail:      "rate over threshold",
		})
	}
	if got := m.Rate(); got != 3 {
		t.Fatalf("rate = %d, want 3 operator-alarm activations", got)
	}
}

func TestFloodDetailRefreshesWithRate(t *testing.T) {
	clock := newTestClock()
	c, _ := newMonitoredCollector(clock)

	raiseDistinct(c, "n", 11)
	first := floodAlarm(c)
	raiseDistinct(c, "more", 9)
	second := floodAlarm(c)
	if !strings.Contains(second.Detail, "20 alarms") {
		t.Fatalf("flood detail %q not refreshed to the current rate", second.Detail)
	}
	if !second.FirstSeen.Equal(first.FirstSeen) {
		t.Fatal("flood refresh reset FirstSeen — it must stay one alarm, not a new one")
	}
}
