package raftgroup

// DestroyGroup and DecommissionGroup are a contract pair: destroy stops a
// group whose durable state must survive (a vault leaving this node while it
// lives on elsewhere), decommission retires a group deleted everywhere. The
// difference is only observable across a WAL reopen — which is exactly where
// getting it wrong either resurrects a deleted group or blanks a live one's
// vote.

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/multiraft"
	"gastrolog/internal/raftwal"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func TestDecommissionForgetsWALStateDestroyKeepsIt(t *testing.T) {
	if testing.Short() {
		t.Skip("spins up real raft groups and reopens the WAL; -short skips")
	}
	// Not parallel — Raft instances + gRPC servers need clean sequential lifecycle.

	baseDir := t.TempDir()
	const nodeAddr = "decom-node"

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	tp := multiraft.New(
		hraft.ServerAddress(nodeAddr),
		func(s string) []byte { return []byte(s) },
		func(b []byte) string { return string(b) },
	)
	pool := multiraft.NewSimpleDialerPeerPool(map[string]func() (net.Conn, error){
		nodeAddr: func() (net.Conn, error) { return lis.Dial() },
	})
	tp.SetPeerConnPool(pool)
	tp.Register(srv)
	go func() { _ = srv.Serve(lis) }()

	walPath := filepath.Join(baseDir, "wal")
	wal1, err := raftwal.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}

	mgr := NewGroupManager(GroupManagerConfig{
		Transport: tp,
		NodeID:    "node-1",
		BaseDir:   baseDir,
		WAL:       wal1,
	})
	seed := []hraft.Server{{ID: hraft.ServerID("node-1"), Address: hraft.ServerAddress(nodeAddr)}}

	for _, name := range []string{"retired", "reassigned"} {
		g, err := mgr.CreateGroup(GroupConfig{GroupID: name, FSM: &counterFSM{}, SeedMembers: seed})
		if err != nil {
			t.Fatalf("CreateGroup %s: %v", name, err)
		}
		waitForLeader(t, g, 5*time.Second)
		for range 5 {
			if err := g.Raft.Apply([]byte("x"), 5*time.Second).Error(); err != nil {
				t.Fatalf("Apply on %s: %v", name, err)
			}
		}
	}

	if err := mgr.DecommissionGroup("retired"); err != nil {
		t.Fatalf("DecommissionGroup: %v", err)
	}
	if err := mgr.DestroyGroup("reassigned"); err != nil {
		t.Fatalf("DestroyGroup: %v", err)
	}
	// Decommission is idempotent — reached again by a reconcile sweep or a
	// replayed delete notification with the group already gone.
	if err := mgr.DecommissionGroup("retired"); err != nil {
		t.Fatalf("repeat DecommissionGroup: %v", err)
	}

	mgr.Shutdown()
	_ = wal1.Close()
	pool.Close()
	srv.Stop()
	_ = tp.Close()

	// The reopened WAL is what a restarted node recovers from.
	wal2, err := raftwal.Open(walPath)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer func() { _ = wal2.Close() }()

	if last, err := wal2.GroupStore("retired").LastIndex(); err != nil || last != 0 {
		t.Errorf("decommissioned group state survived reopen: LastIndex=%d err=%v, want 0", last, err)
	}
	if term, err := wal2.GroupStore("retired").GetUint64([]byte("CurrentTerm")); err == nil && term != 0 {
		t.Errorf("decommissioned group kept CurrentTerm=%d, want gone", term)
	}
	if last, err := wal2.GroupStore("reassigned").LastIndex(); err != nil || last == 0 {
		t.Errorf("destroyed group lost its log across reopen: LastIndex=%d err=%v, want > 0", last, err)
	}
	if term, err := wal2.GroupStore("reassigned").GetUint64([]byte("CurrentTerm")); err != nil || term == 0 {
		t.Errorf("destroyed group lost CurrentTerm across reopen: term=%d err=%v, want > 0", term, err)
	}
}
