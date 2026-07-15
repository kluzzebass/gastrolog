package cluster

import (
	"io"
	"net"
	"testing"
	"time"
)

func TestStatsCollector_PurposeWindowsPublishedThroughLocalSnapshot(t *testing.T) {
	t.Parallel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lis.Close() })

	mgr := NewStaticPeerConns("local", func(id string) (string, bool) {
		if id == "node-x" {
			return lis.Addr().String(), true
		}
		return "", false
	})

	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, conn)
			_ = conn.Close()
		}
	}()

	h, err := mgr.AcquireService("node-x", PurposeSearch)
	if err != nil {
		t.Fatal(err)
	}
	h.Release()

	collector := NewStatsCollector(StatsCollectorConfig{
		PeerConns:  mgr,
		NodeID:     "local",
		Interval:   5 * time.Second,
		NodeNameFn: func() string { return "local" },
	})

	tick := collector.CollectLocalTick(time.Now())
	if len(tick.PeerConnections) != 1 {
		t.Fatalf("tick peer conns: got %d want 1", len(tick.PeerConnections))
	}
	if got := tick.PeerConnections[0].PurposesWindow; len(got) != 1 || got[0] != PurposeSearch {
		t.Fatalf("tick purposes_window: %#v", got)
	}

	snap := collector.CollectLocalSnapshot()
	if len(snap.PeerConnections) != 1 {
		t.Fatalf("snapshot peer conns: got %d want 1", len(snap.PeerConnections))
	}
	if got := snap.PeerConnections[0].PurposesWindow; len(got) != 1 || got[0] != PurposeSearch {
		t.Fatalf("snapshot purposes_window after reset: %#v", got)
	}

	// Live manager window was reset; a new acquire should not appear until the next tick.
	h2, err := mgr.AcquireService("node-x", PurposeChunkApply)
	if err != nil {
		t.Fatal(err)
	}
	h2.Release()

	snap2 := collector.CollectLocalSnapshot()
	if got := snap2.PeerConnections[0].PurposesWindow; len(got) != 1 || got[0] != PurposeSearch {
		t.Fatalf("snapshot should still show last published window: %#v", got)
	}

	tick2 := collector.CollectLocalTick(time.Now())
	if got := tick2.PeerConnections[0].PurposesWindow; len(got) != 1 || got[0] != PurposeChunkApply {
		t.Fatalf("next tick purposes_window: %#v", got)
	}
}
