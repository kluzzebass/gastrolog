package server

// Regression test for gastrolog-4eh5ns: the WatchSystemStatus stream builder
// (buildRouteStats) is what actually feeds the UI's route-stats cache — the
// stream continuously overwrites the cache, so a response built WITHOUT the
// throughput rate fields pinned the route inspector at 0/s even while the
// GetRouteStats RPC returned correct rates.

import (
	"path/filepath"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/orchestrator"
	sysmem "gastrolog/internal/system/memory"
)

type stubStreamPeerRates struct{ in, routed float64 }

func (s *stubStreamPeerRates) AggregateRouteStats() (int64, int64, int64, bool, []*gastrologv1.VaultRouteStats, []*gastrologv1.PerRouteStats) {
	return 0, 0, 0, false, nil, nil
}

func (s *stubStreamPeerRates) AggregateRouteRates() (*gastrologv1.ThroughputRate, *gastrologv1.ThroughputRate) {
	return &gastrologv1.ThroughputRate{InstantPerSec: s.in}, &gastrologv1.ThroughputRate{InstantPerSec: s.routed}
}

func TestBuildRouteStatsIncludesThroughputRates(t *testing.T) {
	t.Parallel()
	cfgStore := sysmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{
		SystemLoader: cfgStore,
		SegmentsDir:  filepath.Join(t.TempDir(), "segments"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(orch.Close)

	localStats := func() *gastrologv1.NodeStats {
		return &gastrologv1.NodeStats{
			RouteIngested: &gastrologv1.ThroughputRate{InstantPerSec: 40},
			RouteRouted:   &gastrologv1.ThroughputRate{InstantPerSec: 30},
		}
	}
	srv := NewLifecycleServer(orch, nil, nil, cfgStore, "node-a", "", nil, localStats, nil)
	srv.SetPeerRouteStats(&stubStreamPeerRates{in: 10, routed: 5})

	resp := srv.buildRouteStats()
	if got := resp.IngestedRate.GetInstantPerSec(); got != 50 {
		t.Fatalf("stream ingested instant = %v, want 50 (40 local + 10 peers)", got)
	}
	if got := resp.RoutedRate.GetInstantPerSec(); got != 35 {
		t.Fatalf("stream routed instant = %v, want 35 (30 local + 5 peers)", got)
	}
}
