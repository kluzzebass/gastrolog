package cluster

import (
	"testing"
	"time"
)

type stubPeerConnSnapshots struct {
	snaps []PeerConnSnapshot
}

func (s *stubPeerConnSnapshots) Snapshot() []PeerConnSnapshot {
	return s.snaps
}

func (s *stubPeerConnSnapshots) ResetPurposeWindows() {}

func TestStatsCollector_PeerTrafficTotals_SumLanes(t *testing.T) {
	provider := &stubPeerConnSnapshots{
		snaps: []PeerConnSnapshot{
			{PeerNodeID: "node-b", Lane: "service", PoolIndex: 0, BytesSent: 100, BytesRecv: 50},
			{PeerNodeID: "node-b", Lane: "raft", GroupID: "vault/x/ctl", BytesSent: 200, BytesRecv: 80},
			{PeerNodeID: "node-c", Lane: "service", PoolIndex: 0, BytesSent: 10, BytesRecv: 5},
		},
	}
	collector := NewStatsCollector(StatsCollectorConfig{
		PeerConns:  provider,
		NodeID:     "node-a",
		Interval:   5 * time.Second,
		NodeNameFn: func() string { return "node-a" },
	})

	stats := collector.CollectLocalTick(time.Now())
	if len(stats.PeerConnections) != 3 {
		t.Fatalf("peer_connections: got %d want 3", len(stats.PeerConnections))
	}
	if len(stats.PeerTrafficTotals) != 2 {
		t.Fatalf("peer_traffic_totals: got %d want 2", len(stats.PeerTrafficTotals))
	}
	if stats.PeerTrafficTotals[0].Peer != "node-b" || stats.PeerTrafficTotals[1].Peer != "node-c" {
		t.Fatalf("sort order: %+v", stats.PeerTrafficTotals)
	}
	if stats.PeerTrafficTotals[0].BytesSent != 300 || stats.PeerTrafficTotals[0].BytesReceived != 130 {
		t.Errorf("node-b totals sent=%d recv=%d, want 300/130",
			stats.PeerTrafficTotals[0].BytesSent, stats.PeerTrafficTotals[0].BytesReceived)
	}
}

func TestStatsCollector_PeerTrafficTotals_Delete(t *testing.T) {
	provider := &stubPeerConnSnapshots{
		snaps: []PeerConnSnapshot{
			{PeerNodeID: "node-b", Lane: "service", BytesSent: 1, BytesRecv: 1},
		},
	}
	collector := NewStatsCollector(StatsCollectorConfig{
		PeerConns:  provider,
		NodeID:     "node-a",
		NodeNameFn: func() string { return "node-a" },
	})
	_ = collector.CollectLocalTick(time.Now())
	collector.Delete("node-b")
	collector.mu.Lock()
	for k := range collector.rates {
		if p, ok := rateSeriesPeerID(k); ok && p == "node-b" {
			t.Fatalf("expected node-b series cleared, still have %q", k)
		}
	}
	collector.mu.Unlock()
}
