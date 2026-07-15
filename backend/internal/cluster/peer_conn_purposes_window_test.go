package cluster

import (
	"io"
	"net"
	"testing"
)

func TestPeerConnManager_PurposesWindowAccumulatesUntilSnapshot(t *testing.T) {
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
			_ = conn.Close()
		}
	}()

	h1, err := mgr.AcquireService("node-x", PurposeSearch)
	if err != nil {
		t.Fatalf("AcquireService search: %v", err)
	}
	h1.Release()

	snaps := mgr.Snapshot()
	if len(snaps) != 1 {
		t.Fatalf("snapshot len: got %d want 1", len(snaps))
	}
	if len(snaps[0].PurposesWindow) != 1 || snaps[0].PurposesWindow[0] != PurposeSearch {
		t.Fatalf("first window: %#v", snaps[0].PurposesWindow)
	}
	if len(snaps[0].Purposes) != 0 {
		t.Fatalf("purposes at snapshot should be empty after release: %#v", snaps[0].Purposes)
	}

	h2, err := mgr.AcquireService("node-x", PurposeChunkApply)
	if err != nil {
		t.Fatalf("AcquireService chunk-apply: %v", err)
	}
	h2.Release()

	snaps = mgr.Snapshot()
	got := snaps[0].PurposesWindow
	if len(got) != 2 || got[0] != PurposeChunkApply || got[1] != PurposeSearch {
		t.Fatalf("accumulated window after second acquire: %#v", got)
	}

	h3, err := mgr.AcquireService("node-x", PurposeSearch)
	if err != nil {
		t.Fatalf("AcquireService search again: %v", err)
	}
	h3.Release()
	h4, err := mgr.AcquireService("node-x", PurposeBroadcast)
	if err != nil {
		t.Fatalf("AcquireService broadcast: %v", err)
	}
	h4.Release()

	snaps = mgr.Snapshot()
	got = snaps[0].PurposesWindow
	if len(got) != 3 || got[0] != PurposeBroadcast || got[1] != PurposeChunkApply || got[2] != PurposeSearch {
		t.Fatalf("combined window: %#v", got)
	}

	// Read-only snapshots do not clear; reset happens on stats broadcast tick.
	snaps = mgr.Snapshot()
	if len(snaps[0].PurposesWindow) != 3 {
		t.Fatalf("peek snapshot should retain window: %#v", snaps[0].PurposesWindow)
	}
	mgr.ResetPurposeWindows()
	snaps = mgr.Snapshot()
	if len(snaps[0].PurposesWindow) != 0 {
		t.Fatalf("cleared window after reset: %#v", snaps[0].PurposesWindow)
	}
}

func TestPeerConnManager_PurposesWindowPerPoolSlot(t *testing.T) {
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
	mgr.SetServicePoolMaxPerPeer(2)

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

	hold1, err := mgr.AcquireService("node-x", PurposeSearch)
	if err != nil {
		t.Fatal(err)
	}
	hold2, err := mgr.AcquireService("node-x", PurposeChunkApply)
	if err != nil {
		t.Fatal(err)
	}
	hold1.Release()
	hold2.Release()

	snaps := mgr.Snapshot()
	if len(snaps) != 2 {
		t.Fatalf("pool slots: got %d want 2", len(snaps))
	}
	byPool := map[int][]string{}
	for _, s := range snaps {
		byPool[s.PoolIndex] = s.PurposesWindow
	}
	if byPool[0][0] != PurposeSearch {
		t.Fatalf("pool 0: %#v", byPool[0])
	}
	if byPool[1][0] != PurposeChunkApply {
		t.Fatalf("pool 1: %#v", byPool[1])
	}
}
