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
	appendStats   []StatsVaultAppendSnapshot
	route         StatsRouteSnapshot
	diskProtected []glid.GLID
	sizeCapped    []glid.GLID
}

func (s *stubStatsProvider) IngestQueueDepth() int    { return 0 }
func (s *stubStatsProvider) IngestQueueCapacity() int { return 0 }
func (s *stubStatsProvider) IngestPressureLevel() string { return "normal" }
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
func (s *stubStatsProvider) DiskProtectedVaults() []glid.GLID                        { return s.diskProtected }
func (s *stubStatsProvider) SizeCappedVaults() []glid.GLID                           { return s.sizeCapped }

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
		route: StatsRouteSnapshot{Routed: 5000, Matched: 4000},
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
	if v.AppendRecords.GetInstantPerSec() != 0 || stats.RouteRouted.GetInstantPerSec() != 0 {
		t.Fatalf("first tick rates = %v/%v, want 0 (window init)",
			v.AppendRecords.GetInstantPerSec(), stats.RouteRouted.GetInstantPerSec())
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
	provider.route.Routed = 5100
	provider.route.Matched = 4050

	stats = collector.CollectLocalTick(t0.Add(2 * time.Second))
	v = findVaultStats(t, stats, vaultID)
	assertRate(t, "append_records instant", v.AppendRecords.GetInstantPerSec(), 150)
	assertRate(t, "append_bytes instant", v.AppendBytes.GetInstantPerSec(), 1500)
	assertRate(t, "append_durable instant", v.AppendDurable.GetInstantPerSec(), 100)
	assertRate(t, "route_routed instant", stats.RouteRouted.GetInstantPerSec(), 50)
	assertRate(t, "route_matched instant", stats.RouteMatched.GetInstantPerSec(), 25)
	if len(stats.RouteRouted.Spark) != 1 {
		t.Fatalf("route spark len = %d, want 1 after one stepped tick", len(stats.RouteRouted.Spark))
	}
}

// TestStatsCollector_EwmaAverages: sustained rates are Unix-load-style
// EWMAs — one float per horizon updated with e^(-dt/tau) decay per tick, no
// history buffer. Constant input converges to the input; silence decays
// exponentially with the 15m horizon barely moving.
func TestStatsCollector_EwmaAverages(t *testing.T) {
	t.Parallel()
	provider := &stubStatsProvider{route: StatsRouteSnapshot{Routed: 0, Matched: 0}}
	collector := NewStatsCollector(StatsCollectorConfig{
		Stats:      provider,
		NodeID:     "node-a",
		NodeNameFn: func() string { return "node-a" },
	})

	// 60 ticks, 5s apart, constant 1000 rec/s: the 1m EWMA converges to
	// ~1000 (5 minutes = 5 tau), the 15m EWMA reaches 1-e^(-300/900) ≈ 28%.
	t0 := time.Now()
	var stats *gastrologv1.NodeStats
	tick := 0
	step := func() {
		tick++
		provider.route.Routed += 1000 * 5
		stats = collector.CollectLocalTick(t0.Add(time.Duration(tick) * 5 * time.Second))
	}
	_ = collector.CollectLocalTick(t0) // window init
	for range 60 {
		step()
	}
	if got := stats.RouteRouted.GetAvg_1MPerSec(); got < 980 || got > 1000 {
		t.Fatalf("1m EWMA after 5 tau of constant input = %v, want ~1000", got)
	}
	got15 := stats.RouteRouted.GetAvg_15MPerSec()
	if got15 < 250 || got15 > 320 {
		t.Fatalf("15m EWMA after 300s = %v, want ~283 (1-e^(-1/3))", got15)
	}

	// Silence: one 60s-later tick with no new records. The 1m EWMA decays
	// by e^(-60/60) ≈ 0.368; instant drops straight to 0.
	before := stats.RouteRouted.GetAvg_1MPerSec()
	stats = collector.CollectLocalTick(t0.Add(time.Duration(tick)*5*time.Second + 60*time.Second))
	if got := stats.RouteRouted.GetInstantPerSec(); got != 0 {
		t.Fatalf("instant after silence = %v, want 0", got)
	}
	want := before * 0.3679
	if got := stats.RouteRouted.GetAvg_1MPerSec(); got < want*0.98 || got > want*1.02 {
		t.Fatalf("1m EWMA after 60s silence = %v, want ~%.1f (e^-1 decay)", got, want)
	}
}

// TestStatsCollector_StageCounters (gastrolog-4r784a): the discrete pipeline
// stage-count milestones pass through as cumulative totals, and the four
// rate-bearing milestones (segments completed/published, chunks built/sealed)
// get windowed rates from the same rolling-window mechanism as append/seal.
func TestStatsCollector_StageCounters(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	provider := &stubStatsProvider{
		appendStats: []StatsVaultAppendSnapshot{{
			VaultID:            vaultID,
			SegmentsCompleted:  10,
			SegmentsPublished:  8,
			SegmentsReleased:   3,
			ChunksPlanned:      4,
			ChunksBuilt:        2,
			ChunksSealed:       2,
			HeadPurges:         5,
			GLCBPullsAttempted: 7,
			GLCBPullsFailed:    1,
			RetentionDeletes:   6,
		}},
	}
	collector := NewStatsCollector(StatsCollectorConfig{
		Stats:      provider,
		NodeID:     "node-a",
		NodeNameFn: func() string { return "node-a" },
	})

	t0 := time.Now()
	stats := collector.CollectLocalTick(t0)
	v := findVaultStats(t, stats, vaultID)
	// Totals pass through on the first tick; rates initialize to zero.
	if v.SegmentsCompletedTotal != 10 || v.SegmentsPublishedTotal != 8 || v.SegmentsReleasedTotal != 3 {
		t.Fatalf("segment totals = %d/%d/%d, want 10/8/3",
			v.SegmentsCompletedTotal, v.SegmentsPublishedTotal, v.SegmentsReleasedTotal)
	}
	if v.ChunksPlannedTotal != 4 || v.ChunksBuiltTotal != 2 || v.ChunksSealedTotal != 2 {
		t.Fatalf("chunk totals = %d/%d/%d, want 4/2/2",
			v.ChunksPlannedTotal, v.ChunksBuiltTotal, v.ChunksSealedTotal)
	}
	if v.HeadPurgesTotal != 5 || v.GlcbPullsAttemptedTotal != 7 || v.GlcbPullsFailedTotal != 1 || v.RetentionDeletesTotal != 6 {
		t.Fatalf("recovery totals = %d/%d/%d/%d, want 5/7/1/6",
			v.HeadPurgesTotal, v.GlcbPullsAttemptedTotal, v.GlcbPullsFailedTotal, v.RetentionDeletesTotal)
	}
	if v.SegmentsCompletedRate.GetInstantPerSec() != 0 || v.ChunksBuiltRate.GetInstantPerSec() != 0 {
		t.Fatalf("first-tick rates non-zero: completed=%v built=%v",
			v.SegmentsCompletedRate.GetInstantPerSec(), v.ChunksBuiltRate.GetInstantPerSec())
	}

	// 2s later: +20 completed, +16 published, +4 built, +6 sealed.
	provider.appendStats[0].SegmentsCompleted = 30
	provider.appendStats[0].SegmentsPublished = 24
	provider.appendStats[0].ChunksBuilt = 6
	provider.appendStats[0].ChunksSealed = 8
	stats = collector.CollectLocalTick(t0.Add(2 * time.Second))
	v = findVaultStats(t, stats, vaultID)
	assertRate(t, "segments_completed instant", v.SegmentsCompletedRate.GetInstantPerSec(), 10)
	assertRate(t, "segments_published instant", v.SegmentsPublishedRate.GetInstantPerSec(), 8)
	assertRate(t, "chunks_built instant", v.ChunksBuiltRate.GetInstantPerSec(), 2)
	assertRate(t, "chunks_sealed instant", v.ChunksSealedRate.GetInstantPerSec(), 3)
	if v.SegmentsCompletedTotal != 30 {
		t.Fatalf("completed total after step = %d, want 30", v.SegmentsCompletedTotal)
	}
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
		RouteRouted:  &gastrologv1.ThroughputRate{InstantPerSec: 100, Avg_1MPerSec: 90, Avg_15MPerSec: 85},
		RouteMatched: &gastrologv1.ThroughputRate{InstantPerSec: 80},
	}, time.Now())
	ps.Update("node-c", &gastrologv1.NodeStats{
		RouteRouted:  &gastrologv1.ThroughputRate{InstantPerSec: 25, Avg_1MPerSec: 10, Avg_15MPerSec: 5},
		RouteMatched: &gastrologv1.ThroughputRate{InstantPerSec: 20},
	}, time.Now())
	// Expired entries must not count.
	ps.Update("node-dead", &gastrologv1.NodeStats{
		RouteRouted: &gastrologv1.ThroughputRate{InstantPerSec: 999},
	}, time.Now().Add(-2*time.Minute))

	routed, matched := ps.AggregateRouteRates()
	assertRate(t, "aggregate routed instant", routed.InstantPerSec, 125)
	assertRate(t, "aggregate routed 1m", routed.Avg_1MPerSec, 100)
	assertRate(t, "aggregate routed 15m", routed.Avg_15MPerSec, 90)
	assertRate(t, "aggregate matched instant", matched.InstantPerSec, 100)
}

// TestStatsCollector_ClusterRouteRates: the cluster-total series (including
// the spark the route panel renders) is windowed server-side over SUMMED
// cluster counters — never accumulated client-side (gastrolog-4eh5ns).
func TestStatsCollector_ClusterRouteRates(t *testing.T) {
	t.Parallel()
	var clusterRouted, clusterMatched int64 = 10000, 8000
	collector := NewStatsCollector(StatsCollectorConfig{
		Stats:      &stubStatsProvider{},
		NodeID:     "node-a",
		NodeNameFn: func() string { return "node-a" },
		ClusterRouteTotals: func() (int64, int64, string) {
			return clusterRouted, clusterMatched, "self"
		},
	})

	t0 := time.Now()
	_ = collector.CollectLocalTick(t0)
	routed0, _ := collector.ClusterRouteRates()
	if routed0.InstantPerSec != 0 || len(routed0.Spark) != 0 {
		t.Fatalf("first tick = %v/%d sparks, want 0/none (window init)", routed0.InstantPerSec, len(routed0.Spark))
	}

	clusterRouted += 500 // +500 over 5s → 100/s
	clusterMatched += 250
	_ = collector.CollectLocalTick(t0.Add(5 * time.Second))
	routed1, matched1 := collector.ClusterRouteRates()
	assertRate(t, "cluster routed instant", routed1.InstantPerSec, 100)
	assertRate(t, "cluster matched instant", matched1.InstantPerSec, 50)
	if len(routed1.Spark) != 1 {
		t.Fatalf("spark len = %d, want 1 (server-side history)", len(routed1.Spark))
	}

	// A peer expiring (summed counter drops) re-anchors instead of going negative.
	clusterRouted -= 5000
	_ = collector.CollectLocalTick(t0.Add(10 * time.Second))
	routed2, _ := collector.ClusterRouteRates()
	if routed2.InstantPerSec != 0 {
		t.Fatalf("post-drop instant = %v, want 0 (reset guard)", routed2.InstantPerSec)
	}
}
