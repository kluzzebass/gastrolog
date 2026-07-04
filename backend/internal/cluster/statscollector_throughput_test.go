package cluster

// Coverage for gastrolog-4eh5ns: per-vault segmentation append rates and
// node-level routing rates from the collector's rolling windows, plus the
// cross-peer rate aggregation GetRouteStats uses for cluster totals.

import (
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
)

// stubStatsProvider implements StatsProvider with mutable counters.
type stubStatsProvider struct {
	appendStats []StatsVaultAppendSnapshot
	route       StatsRouteSnapshot
}

func (s *stubStatsProvider) IngestQueueDepth() int    { return 0 }
func (s *stubStatsProvider) IngestQueueCapacity() int { return 0 }
func (s *stubStatsProvider) VaultSnapshots() []StatsVaultSnapshot {
	out := make([]StatsVaultSnapshot, len(s.appendStats))
	for i, as := range s.appendStats {
		out[i] = StatsVaultSnapshot{ID: as.VaultID, Name: "v"}
	}
	return out
}
func (s *stubStatsProvider) IngesterIDs() []string { return nil }
func (s *stubStatsProvider) IngesterStats(string) (string, int64, int64, int64, bool) {
	return "", 0, 0, 0, false
}
func (s *stubStatsProvider) RouteStats() StatsRouteSnapshot { return s.route }
func (s *stubStatsProvider) VaultAppendStats() []StatsVaultAppendSnapshot {
	return s.appendStats
}
func (s *stubStatsProvider) PipelineDiskSnapshots() []StatsVaultPipelineDiskSnapshot { return nil }
func (s *stubStatsProvider) LocalStorageBytes() int64                                { return 0 }

func TestStatsCollector_ThroughputRates(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	provider := &stubStatsProvider{
		appendStats: []StatsVaultAppendSnapshot{{
			VaultID:         vaultID,
			RecordsAppended: 1000,
			BytesAppended:   10000,
			RecordsDurable:  900,
			QueueDepth:      3,
			QueueCap:        64,
		}},
		route: StatsRouteSnapshot{Ingested: 5000, Routed: 4000},
	}
	collector := NewStatsCollector(StatsCollectorConfig{
		Stats:      provider,
		NodeID:     "node-a",
		NodeNameFn: func() string { return "node-a" },
	})

	t0 := time.Now()
	stats := collector.CollectLocalTick(t0)
	// First tick initializes the windows: rates zero, totals pass through.
	v := findVaultStats(t, stats, vaultID)
	if v.AppendRecords.GetInstantPerSec() != 0 || stats.RouteIngested.GetInstantPerSec() != 0 {
		t.Fatalf("first tick rates = %v/%v, want 0 (window init)",
			v.AppendRecords.GetInstantPerSec(), stats.RouteIngested.GetInstantPerSec())
	}
	if v.AppendRecordsTotal != 1000 || v.AppendBytesTotal != 10000 {
		t.Fatalf("totals = %d/%d, want 1000/10000", v.AppendRecordsTotal, v.AppendBytesTotal)
	}
	if v.AppendQueueDepth != 3 || v.AppendQueueCapacity != 64 {
		t.Fatalf("queue = %d/%d, want 3/64", v.AppendQueueDepth, v.AppendQueueCapacity)
	}

	// 2s later: +300 records, +3000 bytes, +200 durable; route +100/+50.
	provider.appendStats[0].RecordsAppended = 1300
	provider.appendStats[0].BytesAppended = 13000
	provider.appendStats[0].RecordsDurable = 1100
	provider.route.Ingested = 5100
	provider.route.Routed = 4050

	stats = collector.CollectLocalTick(t0.Add(2 * time.Second))
	v = findVaultStats(t, stats, vaultID)
	assertRate(t, "append_records instant", v.AppendRecords.GetInstantPerSec(), 150)
	assertRate(t, "append_bytes instant", v.AppendBytes.GetInstantPerSec(), 1500)
	assertRate(t, "append_durable instant", v.AppendDurable.GetInstantPerSec(), 100)
	assertRate(t, "route_ingested instant", stats.RouteIngested.GetInstantPerSec(), 50)
	assertRate(t, "route_routed instant", stats.RouteRouted.GetInstantPerSec(), 25)
	if len(stats.RouteIngested.Spark) != 1 {
		t.Fatalf("route spark len = %d, want 1 after one stepped tick", len(stats.RouteIngested.Spark))
	}
}

// TestStatsCollector_TrailingAverages: 30s/60s averages come from counter
// deltas over the sample ring, not means of instant samples — a burst then
// silence averages down smoothly.
func TestStatsCollector_TrailingAverages(t *testing.T) {
	t.Parallel()
	provider := &stubStatsProvider{route: StatsRouteSnapshot{Ingested: 0, Routed: 0}}
	collector := NewStatsCollector(StatsCollectorConfig{
		Stats:      provider,
		NodeID:     "node-a",
		NodeNameFn: func() string { return "node-a" },
	})

	// 13 ticks, 5s apart: +1000 ingested per tick for the first 6 intervals,
	// then flat. At t=60s: instant 0; last 30s = 0; last 60s spans the burst.
	t0 := time.Now()
	var stats *gastrologv1.NodeStats
	for i := 0; i <= 12; i++ {
		if i > 0 && i <= 6 {
			provider.route.Ingested += 1000 * 5 // 1000 rec/s for 5s
		}
		stats = collector.CollectLocalTick(t0.Add(time.Duration(i) * 5 * time.Second))
	}
	// t=60s. Burst delivered 30000 records between t0 and t=30s.
	assertRate(t, "instant after silence", stats.RouteIngested.GetInstantPerSec()+1, 1) // 0
	assertRate(t, "30s avg after silence", stats.RouteIngested.GetAvg_30SPerSec()+1, 1) // 0
	// 60s span covers the whole burst: 30000 records / 60s = 500/s.
	assertRate(t, "60s avg spans burst", stats.RouteIngested.GetAvg_60SPerSec(), 500)
}

func findVaultStats(t *testing.T, stats *gastrologv1.NodeStats, vaultID glid.GLID) *gastrologv1.VaultStats {
	t.Helper()
	want := string(vaultID.ToProto())
	for _, v := range stats.Vaults {
		if string(v.Id) == want {
			return v
		}
	}
	t.Fatalf("vault %s missing from NodeStats.Vaults", vaultID)
	return nil
}

func assertRate(t *testing.T, name string, got, want float64) {
	t.Helper()
	if got < want*0.99 || got > want*1.01 {
		t.Fatalf("%s = %v, want ~%v", name, got, want)
	}
}

func TestPeerState_AggregateRouteRates(t *testing.T) {
	t.Parallel()
	ps := NewPeerState(time.Minute)
	ps.Update("node-b", &gastrologv1.NodeStats{
		RouteIngested: &gastrologv1.ThroughputRate{InstantPerSec: 100, Avg_30SPerSec: 90, Avg_60SPerSec: 85},
		RouteRouted:   &gastrologv1.ThroughputRate{InstantPerSec: 80},
	}, time.Now())
	ps.Update("node-c", &gastrologv1.NodeStats{
		RouteIngested: &gastrologv1.ThroughputRate{InstantPerSec: 25, Avg_30SPerSec: 10, Avg_60SPerSec: 5},
		RouteRouted:   &gastrologv1.ThroughputRate{InstantPerSec: 20},
	}, time.Now())
	// Expired entries must not count.
	ps.Update("node-dead", &gastrologv1.NodeStats{
		RouteIngested: &gastrologv1.ThroughputRate{InstantPerSec: 999},
	}, time.Now().Add(-2*time.Minute))

	in, routed := ps.AggregateRouteRates()
	assertRate(t, "aggregate ingested instant", in.InstantPerSec, 125)
	assertRate(t, "aggregate ingested 30s", in.Avg_30SPerSec, 100)
	assertRate(t, "aggregate ingested 60s", in.Avg_60SPerSec, 90)
	assertRate(t, "aggregate routed instant", routed.InstantPerSec, 100)
}

// TestStatsCollector_ClusterRouteRates: the cluster-total series (including
// the spark the route panel renders) is windowed server-side over SUMMED
// cluster counters — never accumulated client-side (gastrolog-4eh5ns).
func TestStatsCollector_ClusterRouteRates(t *testing.T) {
	t.Parallel()
	var clusterIngested, clusterRouted int64 = 10000, 8000
	collector := NewStatsCollector(StatsCollectorConfig{
		Stats:      &stubStatsProvider{},
		NodeID:     "node-a",
		NodeNameFn: func() string { return "node-a" },
		ClusterRouteTotals: func() (int64, int64) {
			return clusterIngested, clusterRouted
		},
	})

	t0 := time.Now()
	_ = collector.CollectLocalTick(t0)
	in, _ := collector.ClusterRouteRates()
	if in.InstantPerSec != 0 || len(in.Spark) != 0 {
		t.Fatalf("first tick = %v/%d sparks, want 0/none (window init)", in.InstantPerSec, len(in.Spark))
	}

	clusterIngested += 500 // +500 over 5s → 100/s
	clusterRouted += 250
	_ = collector.CollectLocalTick(t0.Add(5 * time.Second))
	in, routed := collector.ClusterRouteRates()
	assertRate(t, "cluster ingested instant", in.InstantPerSec, 100)
	assertRate(t, "cluster routed instant", routed.InstantPerSec, 50)
	if len(in.Spark) != 1 {
		t.Fatalf("spark len = %d, want 1 (server-side history)", len(in.Spark))
	}

	// A peer expiring (summed counter drops) re-anchors instead of going negative.
	clusterIngested -= 5000
	_ = collector.CollectLocalTick(t0.Add(10 * time.Second))
	in, _ = collector.ClusterRouteRates()
	if in.InstantPerSec != 0 {
		t.Fatalf("post-drop instant = %v, want 0 (reset guard)", in.InstantPerSec)
	}
}
