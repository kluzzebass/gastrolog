package server_test

// Coverage for gastrolog-4eh5ns: GetRouteStats cluster-total throughput rates
// = local node's rolling-window rates (from the stats collector snapshot) +
// the sum of live peers' broadcast rates.

import (
	"context"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
	sysmem "gastrolog/internal/system/memory"
)

type stubPeerRouteRates struct{ in, routed float64 }

func (s *stubPeerRouteRates) AggregateRouteStats() (int64, int64, int64, bool, []*gastrologv1.VaultRouteStats, []*gastrologv1.PerRouteStats) {
	return 0, 0, 0, false, nil, nil
}

func (s *stubPeerRouteRates) AggregateRouteRates() (float64, float64) {
	return s.in, s.routed
}

func TestGetRouteStatsSumsLocalAndPeerRates(t *testing.T) {
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

	srv := server.NewSystemServer(server.SystemServerConfig{
		Orch:           orch,
		CfgStore:       cfgStore,
		PeerRouteStats: &stubPeerRouteRates{in: 75, routed: 60},
		LocalStats: func() *gastrologv1.NodeStats {
			return &gastrologv1.NodeStats{RouteIngestedPerSec: 25, RouteRoutedPerSec: 15}
		},
	})

	resp, err := srv.GetRouteStats(context.Background(), connect.NewRequest(&gastrologv1.GetRouteStatsRequest{}))
	if err != nil {
		t.Fatalf("GetRouteStats: %v", err)
	}
	if got := resp.Msg.IngestedPerSec; got != 100 {
		t.Fatalf("IngestedPerSec = %v, want 100 (25 local + 75 peers)", got)
	}
	if got := resp.Msg.RoutedPerSec; got != 75 {
		t.Fatalf("RoutedPerSec = %v, want 75 (15 local + 60 peers)", got)
	}
}
