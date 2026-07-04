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
	if v.AppendRecordsPerSec != 0 || stats.RouteIngestedPerSec != 0 {
		t.Fatalf("first tick rates = %v/%v, want 0 (window init)", v.AppendRecordsPerSec, stats.RouteIngestedPerSec)
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
	assertRate(t, "append_records_per_sec", v.AppendRecordsPerSec, 150)
	assertRate(t, "append_bytes_per_sec", v.AppendBytesPerSec, 1500)
	assertRate(t, "append_durable_per_sec", v.AppendDurablePerSec, 100)
	assertRate(t, "route_ingested_per_sec", stats.RouteIngestedPerSec, 50)
	assertRate(t, "route_routed_per_sec", stats.RouteRoutedPerSec, 25)
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
	ps.Update("node-b", &gastrologv1.NodeStats{RouteIngestedPerSec: 100, RouteRoutedPerSec: 80}, time.Now())
	ps.Update("node-c", &gastrologv1.NodeStats{RouteIngestedPerSec: 25, RouteRoutedPerSec: 20}, time.Now())
	// Expired entries must not count.
	ps.Update("node-dead", &gastrologv1.NodeStats{RouteIngestedPerSec: 999, RouteRoutedPerSec: 999}, time.Now().Add(-2*time.Minute))

	in, routed := ps.AggregateRouteRates()
	assertRate(t, "aggregate ingested", in, 125)
	assertRate(t, "aggregate routed", routed, 100)
}
