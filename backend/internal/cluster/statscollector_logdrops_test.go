package cluster

// Coverage for gastrolog-3phtqv: the diagnostic-log drop count is a metric,
// not an alarm. It reaches NodeStats as a plain counter read, and the
// collector tolerates a node with no capture handler wired.
//
// This replaces the self-ingester's drop-monitor tests, which covered an
// alert state machine (raise on delta, clear after a dwell window) that no
// longer exists: there is nothing for an operator to do about a full capture
// channel, and a log line about dropped logs feeds the channel that is
// dropping them.

import (
	"testing"
	"time"

	"gastrolog/internal/alert"
)

type stubLogDrops struct{ n int64 }

func (s *stubLogDrops) DroppedCount() int64 { return s.n }

func TestStatsCollector_LogDropsSurfaceAsAMetric(t *testing.T) {
	t.Parallel()
	drops := &stubLogDrops{}
	collector := NewStatsCollector(StatsCollectorConfig{
		LogDrops:   drops,
		NodeID:     "node-a",
		NodeNameFn: func() string { return "node-a" },
	})
	t0 := time.Now()

	stats := collector.CollectLocalTick(t0)
	if stats.SelfIngesterDropsTotal != 0 {
		t.Fatalf("drops = %d, want 0 before anything is discarded", stats.SelfIngesterDropsTotal)
	}
	if len(stats.Alerts) != 0 {
		t.Fatalf("alerts = %d, want 0: drops must never raise an alarm", len(stats.Alerts))
	}

	// The counter is cumulative and read straight through — no thresholds,
	// no dwell window, no state machine.
	drops.n = 4200
	stats = collector.CollectLocalTick(t0.Add(5 * time.Second))
	if stats.SelfIngesterDropsTotal != 4200 {
		t.Fatalf("drops = %d, want 4200", stats.SelfIngesterDropsTotal)
	}
	if len(stats.Alerts) != 0 {
		t.Fatalf("alerts = %d, want 0 even while drops climb", len(stats.Alerts))
	}

	// A snapshot between ticks reports the same counter; nothing consumes it.
	if snap := collector.CollectLocalSnapshot(); snap.SelfIngesterDropsTotal != 4200 {
		t.Fatalf("snapshot drops = %d, want 4200", snap.SelfIngesterDropsTotal)
	}
}

// A node with capture disabled wires no provider. The guard has to be a nil
// interface, not a typed-nil *logging.CaptureHandler: the latter reads as
// non-nil and DroppedCount would dereference a nil receiver on every tick.
func TestStatsCollector_LogDropsAbsentIsNotAPanic(t *testing.T) {
	t.Parallel()
	collector := NewStatsCollector(StatsCollectorConfig{
		NodeID:     "node-a",
		NodeNameFn: func() string { return "node-a" },
	})
	stats := collector.CollectLocalTick(time.Now())
	if stats.SelfIngesterDropsTotal != 0 {
		t.Fatalf("drops = %d, want 0 with no capture handler wired", stats.SelfIngesterDropsTotal)
	}
}

type stubPressureStats struct {
	stubStatsProvider
	level string
}

func (s *stubPressureStats) IngestQueueCapacity() int    { return 1024 }
func (s *stubPressureStats) IngestPressureLevel() string { return s.level }

// Ingest pressure ships as a level for the health surfaces and raises no
// alarm: a throttled pipeline is already handled — the throttle is the
// response — so there is nothing for an operator to do (gastrolog-3phtqv).
func TestStatsCollector_IngestPressureIsAMetricNotAnAlarm(t *testing.T) {
	t.Parallel()
	stats0 := &stubPressureStats{level: "normal"}
	alerts := alert.New()
	collector := NewStatsCollector(StatsCollectorConfig{
		Stats:      stats0,
		Alerts:     alerts,
		NodeID:     "node-a",
		NodeNameFn: func() string { return "node-a" },
	})
	t0 := time.Now()

	for _, level := range []string{"normal", "elevated", "critical", "normal"} {
		stats0.level = level
		stats := collector.CollectLocalTick(t0)
		if stats.IngestPressureLevel != level {
			t.Fatalf("pressure = %q, want %q", stats.IngestPressureLevel, level)
		}
		if alertActive(alerts, "ingest-pressure") {
			t.Fatalf("ingest-pressure raised an alarm at level %q; throttling is the response, not a call to action", level)
		}
		t0 = t0.Add(5 * time.Second)
	}
}
