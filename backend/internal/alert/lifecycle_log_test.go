package alert

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// The log stream IS the event record (gastrolog-1m3e0d): every alarm
// lifecycle transition edge logs exactly one structured slog line from the
// collector, and the self ingester captures those lines into a vault. These
// tests pin that contract the way the removed event-journal tests pinned
// ring entries: one line per transition, none for suppressed flaps, on an
// injected clock with zero sleeps.

// logSpy is a slog.Handler that records every line with its level and
// attributes, so tests assert on structure, not formatting.
type logSpy struct {
	mu      sync.Mutex
	records []spyLine
}

type spyLine struct {
	level slog.Level
	msg   string
	attrs map[string]string
}

func (s *logSpy) Enabled(context.Context, slog.Level) bool { return true }

func (s *logSpy) Handle(_ context.Context, r slog.Record) error {
	attrs := make(map[string]string)
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.String()
		return true
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, spyLine{level: r.Level, msg: r.Message, attrs: attrs})
	return nil
}

func (s *logSpy) WithAttrs([]slog.Attr) slog.Handler { return s }
func (s *logSpy) WithGroup(string) slog.Handler      { return s }

func (s *logSpy) lines() []spyLine {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]spyLine(nil), s.records...)
}

// spyDefaultLogger swaps the process default logger for a spy for the
// duration of the test. The collector logs through the slog default.
func spyDefaultLogger(t *testing.T) *logSpy {
	t.Helper()
	spy := &logSpy{}
	prev := slog.Default()
	slog.SetDefault(slog.New(spy))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return spy
}

// TestLifecycleTransitionsLogExactlyOneLineEach drives the full lifecycle —
// raise → clear (released) → re-raise → clear (released) — and asserts
// exactly one slog line per transition edge, in order. Reads settle state
// in between and must add nothing.
func TestLifecycleTransitionsLogExactlyOneLineEach(t *testing.T) {
	spy := spyDefaultLogger(t)
	clk := newSuppressionClock()
	c := NewWithClock(clk.Now)

	// wal-reserve: zero delay-on, zero delay-off, non-latching.
	c.Raise("wal-reserve", "cluster-ctl", "reservation below floor")
	c.Standing() // reads must not double-log
	c.Standing()
	c.Clear("wal-reserve", "cluster-ctl") // released
	c.Standing()

	// Second firing: same edges, nothing carried over.
	c.Raise("wal-reserve", "cluster-ctl", "reservation below floor again")
	clk.Advance(2 * time.Hour)
	c.Standing() // reads settle, never log
	c.Clear("wal-reserve", "cluster-ctl") // released
	c.Standing()

	want := []struct {
		level slog.Level
		msg   string
	}{
		{slog.LevelWarn, "alarm raised"},
		{slog.LevelInfo, "alarm cleared — condition resolved"},
		{slog.LevelWarn, "alarm raised"},
		{slog.LevelInfo, "alarm cleared — condition resolved"},
	}
	got := spy.lines()
	if len(got) != len(want) {
		t.Fatalf("logged %d lines, want %d:\n%+v", len(got), len(want), got)
	}
	for i, w := range want {
		g := got[i]
		if g.msg != w.msg {
			t.Fatalf("line %d = %q, want %q", i, g.msg, w.msg)
		}
		if g.level != w.level {
			t.Errorf("line %d (%q) level = %v, want %v", i, g.msg, g.level, w.level)
		}
		if id := g.attrs["id"]; id != "wal-reserve:cluster-ctl" {
			t.Errorf("line %d (%q) id = %q, want the full alarm ID", i, g.msg, id)
		}
	}
}

// TestSuppressedConditionLogsNothing: a condition that dies inside its
// delay-on window never annunciated — logging it would reintroduce the
// chattering the window suppresses.
func TestSuppressedConditionLogsNothing(t *testing.T) {
	spy := spyDefaultLogger(t)
	clk := newSuppressionClock()
	c := NewWithClock(clk.Now)

	lt, ok := TypeByID("vault-leaderless")
	if !ok || lt.DelayOn <= 0 {
		t.Fatal("vault-leaderless must carry a catalog DelayOn")
	}
	c.Raise("vault-leaderless", "vault1", "no leader (flap)")
	clk.Advance(lt.DelayOn / 2)
	c.Clear("vault-leaderless", "vault1")
	clk.Advance(time.Hour)
	c.Standing()

	if lines := spy.lines(); len(lines) != 0 {
		t.Fatalf("suppressed flap logged %d lines: %+v", len(lines), lines)
	}
}

// TestDelayedAnnunciationLogsOnce: a condition outliving its delay-on window
// annunciates on the settling read and logs exactly one activation line.
func TestDelayedAnnunciationLogsOnce(t *testing.T) {
	spy := spyDefaultLogger(t)
	clk := newSuppressionClock()
	c := NewWithClock(clk.Now)

	lt, _ := TypeByID("vault-leaderless")
	c.Raise("vault-leaderless", "vault1", "no leader (stuck)")
	clk.Advance(lt.DelayOn + time.Second)
	c.Standing()
	c.Standing()

	lines := spy.lines()
	if len(lines) != 1 || lines[0].msg != "alarm active — condition persisted past its delay-on window" {
		t.Fatalf("want exactly one activation line, got %+v", lines)
	}
	if lines[0].level != slog.LevelWarn {
		t.Errorf("activation line level = %v, want Warn", lines[0].level)
	}
}

// TestLatchedAlarmLogs: the condition resolving on a latched alarm logs one
// latched-standing line — and nothing more, because there is no release
// path. Repeat reads add nothing.
func TestLatchedAlarmLogs(t *testing.T) {
	spy := spyDefaultLogger(t)
	clk := newSuppressionClock()
	c := NewWithClock(clk.Now)

	// Find a zero-delay latching catalog type so the test tracks the catalog.
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
	clk.Advance(24 * time.Hour)
	c.Standing()
	c.Standing()

	wantMsgs := []string{
		"alarm raised",
		"alarm condition cleared but the alarm is latched — standing until process restart",
	}
	lines := spy.lines()
	if len(lines) != len(wantMsgs) {
		t.Fatalf("logged %d lines, want %d: %+v", len(lines), len(wantMsgs), lines)
	}
	for i, w := range wantMsgs {
		if lines[i].msg != w {
			t.Fatalf("line %d = %q, want %q", i, lines[i].msg, w)
		}
	}
}
