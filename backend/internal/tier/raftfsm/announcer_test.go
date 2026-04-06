package raftfsm

import (
	"context"
	"net"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/multiraft"
	"gastrolog/internal/raftgroup"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1 << 20

// TestAnnouncerReplicatesMetadata verifies the full loop:
// file.Manager (with Announcer) → Raft.Apply → FSM on all nodes.
func TestAnnouncerReplicatesMetadata(t *testing.T) {
	// Not parallel — Raft instances need clean sequential lifecycle.

	const nodeCount = 3
	nodeIDs := []string{"node-1", "node-2", "node-3"}

	// Set up transport + gRPC for each node.
	type testNode struct {
		transport *multiraft.Transport[string]
		server    *grpc.Server
		lis       *bufconn.Listener
		manager   *raftgroup.GroupManager
		fsm       *FSM
	}
	nodes := make([]testNode, nodeCount)

	for i := range nodeCount {
		lis := bufconn.Listen(bufSize)
		srv := grpc.NewServer()
		tp := multiraft.New(
			hraft.ServerAddress(nodeIDs[i]),
			[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
			func(s string) []byte { return []byte(s) },
			func(b []byte) string { return string(b) },
		)
		tp.Register(srv)
		go func() { _ = srv.Serve(lis) }()
		nodes[i] = testNode{transport: tp, server: srv, lis: lis}
	}

	// Wire bufconn dialers.
	dialers := make(map[string]func() (net.Conn, error))
	for i, n := range nodes {
		l := n.lis
		dialers[nodeIDs[i]] = func() (net.Conn, error) { return l.Dial() }
	}
	for i := range nodes {
		nodes[i].transport.SetDialOptions([]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(_ context.Context, addr string) (net.Conn, error) {
				return dialers[addr]()
			}),
		})
	}

	// Create group managers and a 3-node Raft group with FSM.
	members := make([]hraft.Server, nodeCount)
	for i := range nodeCount {
		members[i] = hraft.Server{
			ID:      hraft.ServerID(nodeIDs[i]),
			Address: hraft.ServerAddress(nodeIDs[i]),
		}
	}

	for i := range nodeCount {
		nodes[i].manager = raftgroup.NewGroupManager(raftgroup.GroupManagerConfig{
			Transport: nodes[i].transport,
			NodeID:    nodeIDs[i],
			BaseDir:   t.TempDir(),
		})
		nodes[i].fsm = New()
		_, err := nodes[i].manager.CreateGroup(raftgroup.GroupConfig{
			GroupID:   "tier-test",
			FSM:       nodes[i].fsm,
			Bootstrap: i == 0,
			Members:   members,
		})
		if err != nil {
			t.Fatalf("node %d CreateGroup: %v", i, err)
		}
	}

	t.Cleanup(func() {
		for _, n := range nodes {
			n.manager.Shutdown()
			n.server.Stop()
			_ = n.transport.Close()
		}
	})

	// Wait for leader.
	var leaderGroup *raftgroup.Group
	for _, n := range nodes {
		g := n.manager.GetGroup("tier-test")
		waitForLeader(t, g, 5*time.Second)
		if g.Raft.State() == hraft.Leader {
			leaderGroup = g
		}
	}
	if leaderGroup == nil {
		t.Fatal("no leader found")
	}

	// Create a file.Manager with a Announcer on the leader node.
	announcer := NewAnnouncer(leaderGroup.Raft, 5*time.Second, nil)
	dir := t.TempDir()
	mgr, err := chunkfile.NewManager(chunkfile.Config{
		Dir:       dir,
		Now:       time.Now,
		Announcer: announcer,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	// Append records and seal — this triggers AnnounceCreate + AnnounceSeal.
	for range 10 {
		rec := chunk.Record{
			SourceTS: time.Now(),
			IngestTS: time.Now(),
			Raw:      []byte("test-record"),
			Attrs:    chunk.Attributes{"key": "val"},
		}
		if _, _, err := mgr.Append(rec); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := mgr.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	// Get the chunk ID.
	metas, err := mgr.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(metas) == 0 {
		t.Fatal("expected at least one chunk")
	}
	chunkID := metas[0].ID

	// Wait for full replication: chunk must be sealed on all nodes.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		allSealed := true
		for _, n := range nodes {
			e := n.fsm.Get(chunkID)
			if e == nil || !e.Sealed {
				allSealed = false
				break
			}
		}
		if allSealed {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Verify all nodes see the chunk metadata.
	for i, n := range nodes {
		entry := n.fsm.Get(chunkID)
		if entry == nil {
			t.Fatalf("node %d: chunk not found in FSM", i)
		}
		if !entry.Sealed {
			t.Errorf("node %d: expected sealed", i)
		}
		if entry.RecordCount != 10 {
			t.Errorf("node %d: RecordCount got %d, want 10", i, entry.RecordCount)
		}
	}

	// Delete the chunk — triggers AnnounceDelete.
	if err := mgr.Delete(chunkID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Wait for delete to replicate.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		allGone := true
		for _, n := range nodes {
			if n.fsm.Get(chunkID) != nil {
				allGone = false
				break
			}
		}
		if allGone {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	for i, n := range nodes {
		if n.fsm.Get(chunkID) != nil {
			t.Errorf("node %d: chunk should be deleted from FSM", i)
		}
	}
}

func waitForLeader(t *testing.T, g *raftgroup.Group, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if addr, _ := g.Raft.LeaderWithID(); addr != "" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timed out waiting for leader")
}
