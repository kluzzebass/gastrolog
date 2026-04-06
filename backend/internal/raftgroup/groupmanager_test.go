package raftgroup

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/multiraft"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1 << 20

// counterFSM is a minimal FSM that counts applied commands.
type counterFSM struct {
	count atomic.Int64
}

func (f *counterFSM) Apply(log *hraft.Log) any {
	f.count.Add(1)
	return nil
}

func (f *counterFSM) Snapshot() (hraft.FSMSnapshot, error) {
	return &counterSnapshot{count: f.count.Load()}, nil
}

func (f *counterFSM) Restore(rc io.ReadCloser) error {
	defer func() { _ = rc.Close() }()
	var buf [8]byte
	if _, err := io.ReadFull(rc, buf[:]); err != nil {
		return err
	}
	n := int64(buf[0])<<56 | int64(buf[1])<<48 | int64(buf[2])<<40 | int64(buf[3])<<32 |
		int64(buf[4])<<24 | int64(buf[5])<<16 | int64(buf[6])<<8 | int64(buf[7])
	f.count.Store(n)
	return nil
}

type counterSnapshot struct{ count int64 }

func (s *counterSnapshot) Persist(sink hraft.SnapshotSink) error {
	n := s.count
	buf := [8]byte{
		byte(n >> 56), byte(n >> 48), byte(n >> 40), byte(n >> 32),
		byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n),
	}
	if _, err := sink.Write(buf[:]); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *counterSnapshot) Release() {}

// managerTestNode holds a transport + gRPC server for one test node.
type managerTestNode struct {
	manager   *GroupManager
	transport *multiraft.Transport[string]
	server    *grpc.Server
	lis       *bufconn.Listener
}

func makeManagerCluster(t *testing.T, nodeIDs []string) []*managerTestNode {
	t.Helper()
	n := len(nodeIDs)
	nodes := make([]*managerTestNode, n)

	for i := range n {
		lis := bufconn.Listen(bufSize)
		srv := grpc.NewServer()
		// bufconn listeners all return "bufconn" as their address, so use
		// the node ID as the Raft address to ensure uniqueness.
		tp := multiraft.New(
			hraft.ServerAddress(nodeIDs[i]),
			[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
			func(s string) []byte { return []byte(s) },
			func(b []byte) string { return string(b) },
		)
		tp.Register(srv)
		go func() { _ = srv.Serve(lis) }()

		mgr := NewGroupManager(GroupManagerConfig{
			Transport: tp,
			NodeID:    nodeIDs[i],
			BaseDir:   t.TempDir(),
		})
		nodes[i] = &managerTestNode{manager: mgr, transport: tp, server: srv, lis: lis}
	}

	// Wire up bufconn dialers — keyed by node ID (= Raft address).
	dialers := make(map[string]func() (net.Conn, error))
	for i, node := range nodes {
		_ = node // suppress unused
		l := nodes[i].lis
		dialers[nodeIDs[i]] = func() (net.Conn, error) { return l.Dial() }
	}
	for _, node := range nodes {
		node.transport.SetDialOptions([]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(_ context.Context, addr string) (net.Conn, error) {
				return dialers[addr]()
			}),
		})
	}

	t.Cleanup(func() {
		for _, node := range nodes {
			node.manager.Shutdown()
			node.server.Stop()
			_ = node.transport.Close()
		}
	})

	return nodes
}

// waitForLeader polls until the group has a leader.
func waitForLeader(t *testing.T, g *Group, timeout time.Duration) {
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

func TestCreateGroupSingleNode(t *testing.T) {
	// Not parallel — Raft instances + gRPC servers need clean sequential lifecycle.
	nodes := makeManagerCluster(t, []string{"node-1"})

	fsm := &counterFSM{}
	g, err := nodes[0].manager.CreateGroup(GroupConfig{
		GroupID:   "test",
		FSM:       fsm,
		Bootstrap: true,
	})
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}

	waitForLeader(t, g, 5*time.Second)

	// Apply a command.
	f := g.Raft.Apply([]byte("hello"), 5*time.Second)
	if err := f.Error(); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if fsm.count.Load() != 1 {
		t.Errorf("FSM count: got %d, want 1", fsm.count.Load())
	}
}

func TestCreateGroupThreeNode(t *testing.T) {
	// Not parallel — Raft instances + gRPC servers need clean sequential lifecycle.
	nodes := makeManagerCluster(t, []string{"node-1", "node-2", "node-3"})

	members := make([]hraft.Server, len(nodes))
	for i, n := range nodes {
		members[i] = hraft.Server{
			ID:      hraft.ServerID(n.manager.nodeID),
			Address: n.transport.LocalAddr(),
		}
	}

	fsms := make([]*counterFSM, len(nodes))
	groups := make([]*Group, len(nodes))
	for i, n := range nodes {
		fsms[i] = &counterFSM{}
		g, err := n.manager.CreateGroup(GroupConfig{
			GroupID:   "replicated",
			FSM:       fsms[i],
			Bootstrap: i == 0,
			Members:   members,
		})
		if err != nil {
			t.Fatalf("node %d CreateGroup: %v", i, err)
		}
		groups[i] = g
	}

	waitForLeader(t, groups[0], 5*time.Second)

	// Find the leader and apply a command.
	var leader *Group
	for _, g := range groups {
		if g.Raft.State() == hraft.Leader {
			leader = g
			break
		}
	}
	if leader == nil {
		t.Fatal("no leader found")
	}

	f := leader.Raft.Apply([]byte("replicated-cmd"), 5*time.Second)
	if err := f.Error(); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// Wait for replication to all nodes.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		allApplied := true
		for _, fsm := range fsms {
			if fsm.count.Load() < 1 {
				allApplied = false
				break
			}
		}
		if allApplied {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	for i, fsm := range fsms {
		if fsm.count.Load() < 1 {
			t.Errorf("node %d FSM count: got %d, want >= 1", i, fsm.count.Load())
		}
	}
}

func TestMultipleGroupsSameNode(t *testing.T) {
	// Not parallel — Raft instances + gRPC servers need clean sequential lifecycle.
	nodes := makeManagerCluster(t, []string{"node-1"})

	fsmA := &counterFSM{}
	fsmB := &counterFSM{}

	gA, err := nodes[0].manager.CreateGroup(GroupConfig{GroupID: "group-a", FSM: fsmA, Bootstrap: true})
	if err != nil {
		t.Fatalf("CreateGroup A: %v", err)
	}
	gB, err := nodes[0].manager.CreateGroup(GroupConfig{GroupID: "group-b", FSM: fsmB, Bootstrap: true})
	if err != nil {
		t.Fatalf("CreateGroup B: %v", err)
	}

	waitForLeader(t, gA, 5*time.Second)
	waitForLeader(t, gB, 5*time.Second)

	// Apply to group A only.
	for range 5 {
		if err := gA.Raft.Apply([]byte("a"), 5*time.Second).Error(); err != nil {
			t.Fatal(err)
		}
	}
	// Apply to group B only.
	for range 3 {
		if err := gB.Raft.Apply([]byte("b"), 5*time.Second).Error(); err != nil {
			t.Fatal(err)
		}
	}

	if fsmA.count.Load() != 5 {
		t.Errorf("group-a count: got %d, want 5", fsmA.count.Load())
	}
	if fsmB.count.Load() != 3 {
		t.Errorf("group-b count: got %d, want 3", fsmB.count.Load())
	}
}

func TestDestroyGroup(t *testing.T) {
	// Not parallel — Raft instances + gRPC servers need clean sequential lifecycle.
	nodes := makeManagerCluster(t, []string{"node-1"})

	fsm := &counterFSM{}
	_, err := nodes[0].manager.CreateGroup(GroupConfig{GroupID: "ephemeral", FSM: fsm, Bootstrap: true})
	if err != nil {
		t.Fatal(err)
	}

	if err := nodes[0].manager.DestroyGroup("ephemeral"); err != nil {
		t.Fatalf("DestroyGroup: %v", err)
	}

	if g := nodes[0].manager.GetGroup("ephemeral"); g != nil {
		t.Error("group should be nil after destroy")
	}

	ids := nodes[0].manager.Groups()
	for _, id := range ids {
		if id == "ephemeral" {
			t.Error("destroyed group should not appear in Groups()")
		}
	}
}

func TestDuplicateGroupReturnsError(t *testing.T) {
	// Not parallel — Raft instances + gRPC servers need clean sequential lifecycle.
	nodes := makeManagerCluster(t, []string{"node-1"})

	_, err := nodes[0].manager.CreateGroup(GroupConfig{GroupID: "dup", FSM: &counterFSM{}, Bootstrap: true})
	if err != nil {
		t.Fatal(err)
	}

	_, err = nodes[0].manager.CreateGroup(GroupConfig{GroupID: "dup", FSM: &counterFSM{}, Bootstrap: true})
	if err == nil {
		t.Fatal("expected error for duplicate group")
	}
}

func TestVoterNonvoterAutoEnforcement(t *testing.T) {
	// Not parallel — Raft instances + gRPC servers need clean sequential lifecycle.
	nodes := makeManagerCluster(t, []string{"node-1", "node-2", "node-3"})

	fsm1 := &counterFSM{}
	g1, err := nodes[0].manager.CreateGroup(GroupConfig{GroupID: "voter-test", FSM: fsm1, Bootstrap: true})
	if err != nil {
		t.Fatal(err)
	}
	waitForLeader(t, g1, 5*time.Second)

	// Add node-2 — 2-member group → should be nonvoter.
	err = nodes[0].manager.AddMember("voter-test",
		hraft.ServerID("node-2"), nodes[1].transport.LocalAddr())
	if err != nil {
		t.Fatalf("AddMember node-2: %v", err)
	}

	future := g1.Raft.GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatal(err)
	}
	for _, srv := range future.Configuration().Servers {
		if string(srv.ID) == "node-2" && srv.Suffrage != hraft.Nonvoter {
			t.Errorf("node-2 should be Nonvoter in 2-member group, got %v", srv.Suffrage)
		}
	}

	// Add node-3 — 3-member group → should be voter.
	err = nodes[0].manager.AddMember("voter-test",
		hraft.ServerID("node-3"), nodes[2].transport.LocalAddr())
	if err != nil {
		t.Fatalf("AddMember node-3: %v", err)
	}

	future = g1.Raft.GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatal(err)
	}
	for _, srv := range future.Configuration().Servers {
		if string(srv.ID) == "node-3" && srv.Suffrage != hraft.Voter {
			t.Errorf("node-3 should be Voter in 3-member group, got %v", srv.Suffrage)
		}
	}
}

func TestGroupRecoveryAfterRestart(t *testing.T) {
	// Not parallel — Raft instances + gRPC servers need clean sequential lifecycle.

	// Use a persistent temp dir for the group so we can restart.
	groupDir := t.TempDir()

	// Use a stable address so the Raft log's server address matches after restart.
	const stableAddr = "recovery-node"

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	tp := multiraft.New(
		hraft.ServerAddress(stableAddr),
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		func(s string) []byte { return []byte(s) },
		func(b []byte) string { return string(b) },
	)
	tp.SetDialOptions([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) { return lis.Dial() }),
	})
	tp.Register(srv)
	go func() { _ = srv.Serve(lis) }()

	mgr := NewGroupManager(GroupManagerConfig{
		Transport: tp,
		NodeID:    "node-1",
		BaseDir:   groupDir,
	})

	// Create group and apply some commands.
	fsm1 := &counterFSM{}
	g, err := mgr.CreateGroup(GroupConfig{GroupID: "persistent", FSM: fsm1, Bootstrap: true})
	if err != nil {
		t.Fatal(err)
	}
	waitForLeader(t, g, 5*time.Second)

	for range 10 {
		if err := g.Raft.Apply([]byte("x"), 5*time.Second).Error(); err != nil {
			t.Fatal(err)
		}
	}
	if fsm1.count.Load() != 10 {
		t.Fatalf("before restart: count = %d, want 10", fsm1.count.Load())
	}

	// Shutdown.
	mgr.Shutdown()
	srv.Stop()
	_ = tp.Close()

	// Restart with fresh transport + server but same baseDir and stableAddr.
	lis2 := bufconn.Listen(bufSize)
	srv2 := grpc.NewServer()
	tp2 := multiraft.New(
		hraft.ServerAddress(stableAddr),
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		func(s string) []byte { return []byte(s) },
		func(b []byte) string { return string(b) },
	)
	tp2.SetDialOptions([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) { return lis2.Dial() }),
	})
	tp2.Register(srv2)
	go func() { _ = srv2.Serve(lis2) }()

	mgr2 := NewGroupManager(GroupManagerConfig{
		Transport: tp2,
		NodeID:    "node-1",
		BaseDir:   groupDir,
	})

	fsm2 := &counterFSM{}
	g2, err := mgr2.CreateGroup(GroupConfig{GroupID: "persistent", FSM: fsm2, Bootstrap: true})
	if err != nil {
		t.Fatal(err)
	}
	waitForLeader(t, g2, 5*time.Second)

	// FSM should have recovered from snapshot.
	if fsm2.count.Load() != 10 {
		t.Errorf("after restart: count = %d, want 10", fsm2.count.Load())
	}

	// Apply more commands to verify it's fully operational.
	if err := g2.Raft.Apply([]byte("y"), 5*time.Second).Error(); err != nil {
		t.Fatal(err)
	}
	if fsm2.count.Load() != 11 {
		t.Errorf("after new apply: count = %d, want 11", fsm2.count.Load())
	}

	mgr2.Shutdown()
	srv2.Stop()
	_ = tp2.Close()
}
