package alert

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// journalOn wires a fresh clock-driven collector to a fresh event journal
// and returns both plus the clock advance function. Zero sleeps: every
// window elapses by advancing the shared clock.
func journalOn(t *testing.T, capacity int) (*Collector, *EventJournal, func(time.Duration)) {
	t.Helper()
	var mu sync.Mutex
	now := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		mu.Lock()
		now = now.Add(d)
		mu.Unlock()
	}
	c := NewWithClock(clock)
	j := NewEventJournalWithClock(capacity, clock)
	c.SetOnEvent(j.Record)
	return c, j, advance
}

// eventsAfterSeed strips the node-started seed so lifecycle assertions
// count only transition entries.
func eventsAfterSeed(t *testing.T, j *EventJournal) []Event {
	t.Helper()
	evs := j.Events()
	if len(evs) == 0 || evs[0].Type != EventNodeStarted {
		// The seed may have aged out of a full ring; only assert when the
		// journal still starts at seq 1.
		if len(evs) > 0 && evs[0].Seq == 1 {
			t.Fatalf("journal does not begin with node-started: %+v", evs[0])
		}
		return evs
	}
	return evs[1:]
}

// TestEventJournalSeededWithNodeStarted: the restart decision is visible —
// a fresh journal's first entry is node-started with the boot instant, so
// an empty history reads "journal begins here", never "nothing happened".
func TestEventJournalSeededWithNodeStarted(t *testing.T) {
	boot := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	j := NewEventJournalWithClock(16, func() time.Time { return boot })
	evs := j.Events()
	if len(evs) != 1 {
		t.Fatalf("fresh journal has %d entries, want 1 (the node-started seed)", len(evs))
	}
	e := evs[0]
	if e.Type != EventNodeStarted || e.Source != "node" || !e.Time.Equal(boot) || e.Seq != 1 {
		t.Fatalf("node-started seed wrong: %+v", e)
	}
}

// TestEventJournalRingDropsOldestAtCapacity fills the ring past capacity
// and proves the bound: size stays at capacity, the oldest entries are
// gone, the newest are present, and sequence numbers keep counting across
// the drops.
func TestEventJournalRingDropsOldestAtCapacity(t *testing.T) {
	const capacity = 64
	j := NewEventJournalWithClock(capacity, func() time.Time {
		return time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	})
	const total = capacity * 3 // way past capacity, wraps twice
	for i := range total {
		j.Record(Event{Type: "test-event", Detail: fmt.Sprintf("entry %d", i)})
	}
	if got := j.Len(); got != capacity {
		t.Fatalf("journal len = %d, want stable capacity %d", got, capacity)
	}
	evs := j.Events()
	if len(evs) != capacity {
		t.Fatalf("Events() returned %d, want %d", len(evs), capacity)
	}
	// total+1 records went in (seed + total). The survivors are the newest
	// `capacity` of them, oldest first.
	wantFirstSeq := uint64(total + 1 - capacity + 1)
	if evs[0].Seq != wantFirstSeq {
		t.Fatalf("oldest surviving seq = %d, want %d (oldest dropped)", evs[0].Seq, wantFirstSeq)
	}
	if evs[0].Type == EventNodeStarted {
		t.Fatal("node-started seed survived a full wrap — oldest not dropped")
	}
	if got := evs[capacity-1]; got.Seq != uint64(total+1) || got.Detail != fmt.Sprintf("entry %d", total-1) {
		t.Fatalf("newest entry wrong: %+v", got)
	}
	// Strictly consecutive: nothing reordered, nothing duplicated.
	for i := 1; i < len(evs); i++ {
		if evs[i].Seq != evs[i-1].Seq+1 {
			t.Fatalf("seq gap between %d and %d", evs[i-1].Seq, evs[i].Seq)
		}
	}
}

// TestLifecycleTransitionsProduceExactlyOneEventEach drives the acceptance
// sequence raise → ack → clear → (re-raise) → ack and counts entries per
// transition: exactly one alarm-raised, one alarm-acked, one alarm-cleared,
// one alarm-raised (new occurrence), one alarm-acked — no more, no fewer,
// even with reads settling state in between.
func TestLifecycleTransitionsProduceExactlyOneEventEach(t *testing.T) {
	c, j, _ := journalOn(t, 128)

	// wal-reserve: zero delay-on, zero delay-off, non-latching catalog row.
	c.Raise("wal-reserve", "cluster-ctl", "reservation below floor")
	c.Standing() // reads must not double-journal
	c.Standing()
	if err := c.Ack("wal-reserve:cluster-ctl", "op"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	c.Clear("wal-reserve", "cluster-ctl") // acked → released
	c.Standing()

	// Second occurrence: raise again, retained cleared-unacked, ack releases.
	c.Raise("wal-reserve", "cluster-ctl", "reservation below floor again")
	c.Clear("wal-reserve", "cluster-ctl") // unacked → retained cleared-unacked
	c.Standing()
	if err := c.Ack("wal-reserve:cluster-ctl", "op"); err != nil {
		t.Fatalf("Ack of cleared-unacked: %v", err)
	}
	c.Standing()

	evs := eventsAfterSeed(t, j)
	want := []struct {
		typ, detailSub string
	}{
		{EventAlarmRaised, "reservation below floor"},
		{EventAlarmAcked, "acknowledged"},
		{EventAlarmCleared, "released"},
		{EventAlarmRaised, "reservation below floor again"},
		{EventAlarmCleared, "retained"},
		{EventAlarmAcked, "released"},
	}
	if len(evs) != len(want) {
		t.Fatalf("journal has %d lifecycle entries, want %d: %+v", len(evs), len(want), evs)
	}
	for i, w := range want {
		if evs[i].Type != w.typ {
			t.Fatalf("entry %d type = %s, want %s (%+v)", i, evs[i].Type, w.typ, evs[i])
		}
		if evs[i].AlarmID != "wal-reserve:cluster-ctl" {
			t.Fatalf("entry %d alarm ID = %q", i, evs[i].AlarmID)
		}
	}
	// Operator identity travels on the ack entries.
	if evs[1].By != "op" || evs[5].By != "op" {
		t.Fatalf("ack entries missing operator identity: %+v %+v", evs[1], evs[5])
	}
}

// TestShelveLifecycleEvents covers shelve, early unshelve, re-shelve and
// lapsed expiry — each exactly one entry, the expiry attributed to the
// system (no By), the shelves carrying the operator.
func TestShelveLifecycleEvents(t *testing.T) {
	c, j, advance := journalOn(t, 128)

	c.Raise("disk-space-exhausted", "vault1", "disk protect engaged")
	if _, err := c.Shelve("disk-space-exhausted:vault1", time.Hour, "op"); err != nil {
		t.Fatalf("Shelve: %v", err)
	}
	if err := c.Unshelve("disk-space-exhausted:vault1"); err != nil {
		t.Fatalf("Unshelve: %v", err)
	}
	if _, err := c.Shelve("disk-space-exhausted:vault1", time.Hour, "op"); err != nil {
		t.Fatalf("re-Shelve: %v", err)
	}
	advance(2 * time.Hour)
	c.Standing() // lazy settle runs the expiry

	evs := eventsAfterSeed(t, j)
	wantTypes := []string{
		EventAlarmRaised,
		EventAlarmShelved,
		EventAlarmUnshelved,
		EventAlarmShelved,
		EventAlarmShelveExpired,
	}
	if len(evs) != len(wantTypes) {
		t.Fatalf("journal has %d entries, want %d: %+v", len(evs), len(wantTypes), evs)
	}
	for i, w := range wantTypes {
		if evs[i].Type != w {
			t.Fatalf("entry %d type = %s, want %s", i, evs[i].Type, w)
		}
	}
	if evs[1].By != "op" {
		t.Fatalf("shelve entry missing operator: %+v", evs[1])
	}
	if evs[4].By != "" {
		t.Fatalf("shelve-expired is a system transition, must carry no operator: %+v", evs[4])
	}
}

// TestSuppressedConditionJournalsNothing: a condition that dies inside its
// delay-on window never annunciated — journaling it would reintroduce the
// chattering the window suppresses.
func TestSuppressedConditionJournalsNothing(t *testing.T) {
	c, j, advance := journalOn(t, 128)

	lt, ok := TypeByID("vault-leaderless")
	if !ok || lt.DelayOn <= 0 {
		t.Fatal("vault-leaderless must carry a catalog DelayOn")
	}
	c.Raise("vault-leaderless", "vault1", "no leader (flap)")
	advance(lt.DelayOn / 2)
	c.Clear("vault-leaderless", "vault1")
	advance(time.Hour)
	c.Standing()

	if evs := eventsAfterSeed(t, j); len(evs) != 0 {
		t.Fatalf("suppressed flap journaled %d entries: %+v", len(evs), evs)
	}
}

// TestDelayedAnnunciationJournalsOnce: a condition outliving its delay-on
// window annunciates on the settling read and journals exactly one
// alarm-raised entry stamped with the collector clock.
func TestDelayedAnnunciationJournalsOnce(t *testing.T) {
	c, j, advance := journalOn(t, 128)

	lt, _ := TypeByID("vault-leaderless")
	c.Raise("vault-leaderless", "vault1", "no leader (stuck)")
	advance(lt.DelayOn + time.Second)
	c.Standing()
	c.Standing()

	evs := eventsAfterSeed(t, j)
	if len(evs) != 1 || evs[0].Type != EventAlarmRaised {
		t.Fatalf("want exactly one alarm-raised, got %+v", evs)
	}
}

// TestLatchedAlarmEvents: condition resolves before ack → one cleared entry
// (latched standing), then the releasing ack → one acked entry.
func TestLatchedAlarmEvents(t *testing.T) {
	c, j, _ := journalOn(t, 128)

	// Find a latching catalog type so the test tracks the real catalog.
	var latchingID string
	for _, at := range Types() {
		if at.Latching && at.DelayOn == 0 {
			latchingID = at.IDPrefix
			break
		}
	}
	if latchingID == "" {
		t.Skip("no zero-delay latching type in the catalog")
	}
	c.Raise(latchingID, "inst", "latched condition")
	c.Clear(latchingID, "inst")
	if err := c.Ack(latchingID+":inst", "op"); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	evs := eventsAfterSeed(t, j)
	wantTypes := []string{EventAlarmRaised, EventAlarmCleared, EventAlarmAcked}
	if len(evs) != len(wantTypes) {
		t.Fatalf("journal has %d entries, want %d: %+v", len(evs), len(wantTypes), evs)
	}
	for i, w := range wantTypes {
		if evs[i].Type != w {
			t.Fatalf("entry %d type = %s, want %s", i, evs[i].Type, w)
		}
	}
}

// TestEventJournalConcurrentRecord hammers Record from many goroutines and
// asserts the ring stays bounded and internally consistent.
func TestEventJournalConcurrentRecord(t *testing.T) {
	const capacity = 32
	j := NewEventJournal(capacity)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 200 {
				j.Record(Event{Type: "test-event", Detail: fmt.Sprintf("g%d-%d", g, i)})
			}
		})
	}
	wg.Wait()
	evs := j.Events()
	if len(evs) != capacity {
		t.Fatalf("len = %d, want %d", len(evs), capacity)
	}
	for i := 1; i < len(evs); i++ {
		if evs[i].Seq != evs[i-1].Seq+1 {
			t.Fatalf("seq gap after concurrent records: %d → %d", evs[i-1].Seq, evs[i].Seq)
		}
	}
	if evs[len(evs)-1].Seq != uint64(1+8*200) {
		t.Fatalf("final seq = %d, want %d", evs[len(evs)-1].Seq, 1+8*200)
	}
}
