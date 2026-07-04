package cluster

// Coverage for gastrolog-1io54g: Raft liveness fields in NodeStats and the
// election-storm / WAL-latency alerts maintained by the collector tick.

import (
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
	if alertActive(alerts, "raft-wal-latency") == false {
		t.Fatal("wal-latency alert should be set at 1500ms max")
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
	if !alertActive(alerts, "raft-liveness-elections") {
		t.Fatal("election-storm alert should be set at 6/min")
	}
	if alertActive(alerts, "raft-wal-latency") {
		t.Fatal("wal-latency alert should clear once max drops below the clear threshold")
	}

	// Tick 3: calm — storm alert clears with hysteresis.
	stats = collector.CollectLocalTick(t0.Add(120 * time.Second))
	if stats.RaftElectionsPerMin != 0 {
		t.Fatalf("elections/min = %v, want 0", stats.RaftElectionsPerMin)
	}
	if alertActive(alerts, "raft-liveness-elections") {
		t.Fatal("election-storm alert should clear when calm")
	}
}

func alertActive(c *alert.Collector, id string) bool {
	for _, a := range c.Active() {
		if a.ID == id {
			return true
		}
	}
	return false
}
