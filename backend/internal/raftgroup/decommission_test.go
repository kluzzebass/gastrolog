package raftgroup

// DestroyGroup and DecommissionGroup are a contract pair: destroy stops a
// group whose durable state must survive (a vault leaving this node while it
// lives on elsewhere), decommission retires a group deleted everywhere. The
// difference is only observable across a WAL reopen — which is exactly where
// getting it wrong either resurrects a deleted group or blanks a live one's
// vote.

import (
	"errors"
	"io/fs"
	"maps"
	"net"
	"os"
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

// Decommission removes the retired group's directory and the parents it
// leaves empty; a sibling group whose ID shares the retired one's prefix
// keeps every file, and a destroyed group keeps its directory for recovery.
func TestDecommissionRemovesGroupDirKeepsSiblings(t *testing.T) {
	if testing.Short() {
		t.Skip("spins up real raft groups; -short skips")
	}

	baseDir := t.TempDir()
	const nodeAddr = "decom-dir-node"

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
	t.Cleanup(func() {
		pool.Close()
		srv.Stop()
		_ = tp.Close()
	})

	wal, err := raftwal.Open(filepath.Join(baseDir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	groupsDir := filepath.Join(baseDir, "groups")
	mgr := NewGroupManager(GroupManagerConfig{
		Transport: tp,
		NodeID:    "node-1",
		BaseDir:   groupsDir,
		WAL:       wal,
	})
	t.Cleanup(mgr.Shutdown)
	seed := []hraft.Server{{ID: hraft.ServerID("node-1"), Address: hraft.ServerAddress(nodeAddr)}}

	const retired, sibling, reassigned = "nest/a1/ctl", "nest/a1b/ctl", "nest/a2/ctl"
	for _, name := range []string{retired, sibling, reassigned} {
		g, err := mgr.CreateGroup(GroupConfig{GroupID: name, FSM: &counterFSM{}, SeedMembers: seed})
		if err != nil {
			t.Fatalf("CreateGroup %s: %v", name, err)
		}
		waitForLeader(t, g, 5*time.Second)
		if err := g.Raft.Apply([]byte("x"), 5*time.Second).Error(); err != nil {
			t.Fatalf("Apply on %s: %v", name, err)
		}
		if err := g.Raft.Snapshot().Error(); err != nil {
			t.Fatalf("Snapshot on %s: %v", name, err)
		}
	}
	siblingBefore := treeFiles(t, filepath.Join(groupsDir, sibling))
	if len(siblingBefore) == 0 {
		t.Fatalf("sibling group %s has no files to protect", sibling)
	}

	if err := mgr.DecommissionGroup(retired); err != nil {
		t.Fatalf("DecommissionGroup: %v", err)
	}
	if err := mgr.DestroyGroup(reassigned); err != nil {
		t.Fatalf("DestroyGroup: %v", err)
	}

	if _, err := os.Stat(filepath.Join(groupsDir, "nest", "a1")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("decommissioned group's directory survives: stat err=%v", err)
	}
	if got := treeFiles(t, filepath.Join(groupsDir, sibling)); !maps.Equal(got, siblingBefore) {
		t.Errorf("sibling group's files changed: before %v, after %v", siblingBefore, got)
	}
	if len(treeFiles(t, filepath.Join(groupsDir, reassigned))) == 0 {
		t.Errorf("destroyed group lost its directory")
	}

	// Decommissioning a group that is not running here still removes its
	// directory: the node may hold it from before a restart.
	stale := filepath.Join(groupsDir, "nest", "a3", "ctl", "snapshots")
	if err := os.MkdirAll(stale, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := mgr.DecommissionGroup("nest/a3/ctl"); err != nil {
		t.Fatalf("DecommissionGroup of a stopped group: %v", err)
	}
	if _, err := os.Stat(filepath.Join(groupsDir, "nest", "a3")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("stopped group's directory survives decommission: stat err=%v", err)
	}
	if _, err := os.Stat(groupsDir); err != nil {
		t.Errorf("base directory removed: %v", err)
	}

	if err := mgr.DecommissionGroup("../escape"); err == nil {
		t.Errorf("DecommissionGroup accepted a group ID outside the base directory")
	}
}

// treeFiles maps every regular file under root to its size.
func treeFiles(t *testing.T, root string) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			out[path] = info.Size()
		}
		return nil
	})
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("walk %s: %v", root, err)
	}
	return out
}
