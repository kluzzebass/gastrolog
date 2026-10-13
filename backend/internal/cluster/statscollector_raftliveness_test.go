package cluster

// Coverage: Raft liveness fields in NodeStats, and that neither election
// storm nor WAL latency may raise an alarm — both are diagnostics with no
// operator action, carried as stats plus transition-edge logs (EEMUA 191
// actionability test).

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
	// alarm list.
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
	// test): the rate ships in stats, transitions are logged, and no alert
	// may appear.
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
// exists to avoid. No alert collector is wired here on
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

func newElectionCollector(live *stubRaftLiveness, spy *logSpy) *StatsCollector {
	return NewStatsCollector(StatsCollectorConfig{
		RaftLiveness: live,
		Logger:       slog.New(spy),
		NodeID:       "node-a",
		NodeNameFn:   func() string { return "node-a" },
	})
}

// One election seen by one short stats tick is one election in the window,
// not a per-tick rate extrapolated to a minute: it neither reads as a storm
// nor logs one.
func TestStatsCollector_SingleElectionInOneTickIsNotAStorm(t *testing.T) {
	t.Parallel()
	spy := &logSpy{}
	live := &stubRaftLiveness{elections: 4}
	collector := newElectionCollector(live, spy)
	t0 := time.Now()
	tick := 5 * time.Second

	collector.CollectLocalTick(t0)
	live.elections++
	stats := collector.CollectLocalTick(t0.Add(tick))
	if stats.RaftElectionsPerMin != 1 {
		t.Fatalf("elections/min = %v, want 1 (one election in the last minute)", stats.RaftElectionsPerMin)
	}
	if stats.RaftElectionsTotal != 5 {
		t.Fatalf("elections total = %d, want 5", stats.RaftElectionsTotal)
	}
	if collectorStormActive(collector) {
		t.Fatal("one election must not read as a storm")
	}
	if got := spy.count("Raft election storm"); got != 0 {
		t.Fatalf("storm logs = %d, want 0", got)
	}

	// Between-tick snapshots report the same window.
	if snap := collector.CollectLocalSnapshot(); snap.RaftElectionsPerMin != 1 {
		t.Fatalf("snapshot elections/min = %v, want 1", snap.RaftElectionsPerMin)
	}
}

// Elections spread across ticks accumulate in the window: three inside one
// minute is a storm even though no single tick saw more than one. The storm
// clears once the window slides past them, one line per edge.
func TestStatsCollector_ElectionStormOverSlidingWindow(t *testing.T) {
	t.Parallel()
	spy := &logSpy{}
	live := &stubRaftLiveness{}
	collector := newElectionCollector(live, spy)
	t0 := time.Now()
	tick := 5 * time.Second

	collector.CollectLocalTick(t0)
	at := t0
	for range 12 { // one minute of ticks, an election every fourth tick
		at = at.Add(tick)
		if at.Sub(t0)%(4*tick) == 0 {
			live.elections++
		}
		collector.CollectLocalTick(at)
	}
	if live.elections != 3 {
		t.Fatalf("test drove %d elections, want 3", live.elections)
	}
	if !collectorStormActive(collector) {
		t.Fatal("three elections within a minute must read as a storm")
	}
	if got := spy.count("Raft election storm"); got != 1 {
		t.Fatalf("storm logs = %d, want exactly 1 on the transition edge", got)
	}

	// Quiet ticks: the window slides past the elections one by one; the
	// storm holds through the hysteresis band and clears at zero.
	for range 12 {
		at = at.Add(tick)
		collector.CollectLocalTick(at)
	}
	stats := collector.CollectLocalTick(at.Add(tick))
	if stats.RaftElectionsPerMin != 0 {
		t.Fatalf("elections/min = %v, want 0 once the window holds no elections", stats.RaftElectionsPerMin)
	}
	if collectorStormActive(collector) {
		t.Fatal("storm must clear once the window is quiet")
	}
	if got := spy.count("Raft election storm"); got != 1 {
		t.Fatalf("storm logs = %d, want still 1", got)
	}
	if got := spy.count("back to calm"); got != 1 {
		t.Fatalf("calm logs = %d, want exactly 1 on the recovery edge", got)
	}
}
