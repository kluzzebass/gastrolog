package cluster

// Coverage for gastrolog-1io54g: Raft liveness fields in NodeStats, and for
// gastrolog-5nvb4y: neither election storm nor WAL latency may raise an
// alarm — both are diagnostics with no operator action, carried as stats
// plus transition-edge logs (EEMUA 191 actionability test).

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/alert"
)

type stubRaftLiveness struct {
	count, totalNanos, maxNanos uint64
	elections, losses, failedHB uint64
}

func (s *stubRaftLiveness) WALAppendTotals() (uint64, uint64) { return s.count, s.totalNanos }
func (s *stubRaftLiveness) TakeWALAppendMax() uint64 {
	m := s.maxNanos
	s.maxNanos = 0
	return m
}
func (s *stubRaftLiveness) RaftLiveness() (uint64, uint64, uint64) {
	return s.elections, s.losses, s.failedHB
}

func TestStatsCollector_RaftLivenessFieldsAndAlerts(t *testing.T) {
	t.Parallel()
	live := &stubRaftLiveness{count: 100, totalNanos: uint64(2 * time.Second), maxNanos: uint64(1500 * time.Millisecond)}
	alerts := alert.New()
	collector := NewStatsCollector(StatsCollectorConfig{
		RaftLiveness: live,
		Alerts:       alerts,
		NodeID:       "node-a",
		NodeNameFn:   func() string { return "node-a" },
	})

	t0 := time.Now()
	stats := collector.CollectLocalTick(t0)
	if stats.RaftWalAppendsTotal != 100 {
		t.Fatalf("appends total = %d, want 100", stats.RaftWalAppendsTotal)
	}
	// First tick: max is consumed and surfaced even before windows warm up.
	if stats.RaftWalAppendMaxMs != 1500 {
		t.Fatalf("wal max = %vms, want 1500", stats.RaftWalAppendMaxMs)
	}
	// WAL append latency is a diagnostic, not an alarm: the max ships in
	// stats and the degraded transition is logged, but nothing reaches the
	// alarm list (gastrolog-5nvb4y).
	if alertActive(alerts, "raft-wal-latency") {
		t.Fatal("wal latency must not raise an alarm; it is log + stats only")
	}
	if !collectorWalDegraded(collector) {
		t.Fatal("wal degraded hysteresis state should be active at 1500ms max")
	}

	// Snapshot between ticks must NOT consume the max.
	live.maxNanos = uint64(999 * time.Second) // would be eaten by a buggy snapshot
	snap := collector.CollectLocalSnapshot()
	if snap.RaftWalAppendMaxMs != 1500 {
		t.Fatalf("snapshot wal max = %vms, want cached 1500", snap.RaftWalAppendMaxMs)
	}
	live.maxNanos = 0

	// Tick 2, 60s later: +6 elections in a minute → storm alert; WAL calm.
	live.count = 200
	live.totalNanos += uint64(200 * time.Millisecond) // 100 appends × 2ms avg
	live.elections = 6
	stats = collector.CollectLocalTick(t0.Add(60 * time.Second))
	if got := stats.RaftElectionsPerMin; got < 5.9 || got > 6.1 {
		t.Fatalf("elections/min = %v, want ~6", got)
	}
	if got := stats.RaftWalAppendAvgMs; got < 1.9 || got > 2.1 {
		t.Fatalf("wal avg = %vms, want ~2ms", got)
	}
	// Election churn is a diagnostic, not an alarm (EEMUA 191 actionability
	// test, gastrolog-29380r): the rate ships in stats, transitions are
	// logged, and no alert may appear.
	if alertActive(alerts, "raft-liveness-elections") {
		t.Fatal("election churn must not raise an alarm; it is log + stats only")
	}
	if !collectorStormActive(collector) {
		t.Fatal("storm hysteresis state should be active at 6/min")
	}
	if collectorWalDegraded(collector) {
		t.Fatal("wal degraded state should clear once max drops below the calm threshold")
	}

	// Tick 3: calm — storm state clears with hysteresis.
	stats = collector.CollectLocalTick(t0.Add(120 * time.Second))
	if stats.RaftElectionsPerMin != 0 {
		t.Fatalf("elections/min = %v, want 0", stats.RaftElectionsPerMin)
	}
	if collectorStormActive(collector) {
		t.Fatal("storm hysteresis state should clear when calm")
	}
}

func collectorStormActive(c *StatsCollector) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.electionStormActive
}

func collectorWalDegraded(c *StatsCollector) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.walLatencyDegradedActive
}

// logSpy captures emitted records so the transition-edge tests can assert
// one line per edge rather than one per tick.
type logSpy struct {
	mu       sync.Mutex
	messages []string
}

func (s *logSpy) Enabled(context.Context, slog.Level) bool { return true }

func (s *logSpy) Handle(_ context.Context, r slog.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, r.Message)
	return nil
}

func (s *logSpy) WithAttrs([]slog.Attr) slog.Handler { return s }
func (s *logSpy) WithGroup(string) slog.Handler      { return s }

func (s *logSpy) count(substr string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, m := range s.messages {
		if strings.Contains(m, substr) {
			n++
		}
	}
	return n
}

// A demoted diagnostic logs once entering the condition and once on
// recovery — never once per tick, which is the chattering the razor demotion
// exists to avoid (gastrolog-5nvb4y). No alert collector is wired here on
// purpose: before this change the whole block sat behind a `cfg.Alerts ==
// nil` guard, which silently disabled the logging on any node without a
// collector.
func TestStatsCollector_WalLatencyLogsOnTransitionEdgesOnly(t *testing.T) {
	t.Parallel()
	spy := &logSpy{}
	live := &stubRaftLiveness{}
	collector := NewStatsCollector(StatsCollectorConfig{
		RaftLiveness: live,
		Logger:       slog.New(spy),
		NodeID:       "node-a",
		NodeNameFn:   func() string { return "node-a" },
	})
	t0 := time.Now()

	// Tick 1: over the degraded threshold — one line.
	live.maxNanos = uint64(1500 * time.Millisecond)
	collector.CollectLocalTick(t0)
	if got := spy.count("WAL append latency degraded"); got != 1 {
		t.Fatalf("degraded logs = %d, want exactly 1 on the transition edge", got)
	}
	if !collectorWalDegraded(collector) {
		t.Fatal("wal degraded state should be active at 1500ms max")
	}

	// Tick 2: still degraded — a sustained condition must not repeat.
	live.maxNanos = uint64(1600 * time.Millisecond)
	collector.CollectLocalTick(t0.Add(60 * time.Second))
	if got := spy.count("WAL append latency degraded"); got != 1 {
		t.Fatalf("degraded logs = %d after a second degraded tick, want still 1", got)
	}

	// Tick 3: inside the hysteresis band (250–1000ms) — neither edge fires.
	live.maxNanos = uint64(500 * time.Millisecond)
	collector.CollectLocalTick(t0.Add(120 * time.Second))
	if got := spy.count("back to normal"); got != 0 {
		t.Fatalf("recovery logs = %d inside the hysteresis band, want 0", got)
	}
	if !collectorWalDegraded(collector) {
		t.Fatal("hysteresis: state must hold degraded between the thresholds")
	}

	// Tick 4: below the calm threshold — one recovery line.
	live.maxNanos = uint64(10 * time.Millisecond)
	collector.CollectLocalTick(t0.Add(180 * time.Second))
	if got := spy.count("back to normal"); got != 1 {
		t.Fatalf("recovery logs = %d, want exactly 1 on the recovery edge", got)
	}
	if collectorWalDegraded(collector) {
		t.Fatal("wal degraded state should clear once calm")
	}
}

func alertActive(c *alert.Collector, id string) bool {
	for _, a := range c.Standing() {
		if a.ID == id {
			return true
		}
	}
	return false
}
