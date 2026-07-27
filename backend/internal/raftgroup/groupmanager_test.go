package raftgroup

import (
	"io"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/multiraft"
	"gastrolog/internal/raftwal"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
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
	pool      *multiraft.DialerPeerPool
	server    *grpc.Server
	lis       *bufconn.Listener
}

func testWAL(t *testing.T, dir string) *raftwal.WAL {
	t.Helper()
	w, err := raftwal.Open(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
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
			func(s string) []byte { return []byte(s) },
			func(b []byte) string { return string(b) },
		)
		tp.Register(srv)
		go func() { _ = srv.Serve(lis) }()

		baseDir := t.TempDir()
		mgr := NewGroupManager(GroupManagerConfig{
			Transport: tp,
			NodeID:    nodeIDs[i],
			BaseDir:   baseDir,
			WAL:       testWAL(t, baseDir),
		})
		nodes[i] = &managerTestNode{manager: mgr, transport: tp, server: srv, lis: lis}
	}

	dialers := make(map[string]func() (net.Conn, error))
	for i, node := range nodes {
		l := node.lis
		dialers[nodeIDs[i]] = func() (net.Conn, error) { return l.Dial() }
	}
	for _, node := range nodes {
		pool := multiraft.NewSimpleDialerPeerPool(dialers)
		node.transport.SetPeerConnPool(pool)
		node.pool = pool
	}

	t.Cleanup(func() {
		for _, node := range nodes {
			if node.pool != nil {
				node.pool.Close()
			}
			node.manager.Shutdown()
			node.server.Stop()
			_ = node.transport.Close()
		}
	})

	return nodes
}

// selfSeed returns a single-element SeedMembers list for the given test node.
// Used by single-node tests that want to start a group containing only the
// local node.
func selfSeed(n *managerTestNode) []hraft.Server {
	return []hraft.Server{{
		ID:      hraft.ServerID(n.manager.nodeID),
		Address: n.transport.LocalAddr(),
	}}
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
	if testing.Short() {
		t.Skip("spins up a real raft group and waits out election timing; -short skips")
	}
	// Not parallel — Raft instances + gRPC servers need clean sequential lifecycle.
	nodes := makeManagerCluster(t, []string{"node-1"})

	fsm := &counterFSM{}
	g, err := nodes[0].manager.CreateGroup(GroupConfig{
		GroupID:     "test",
		FSM:         fsm,
		SeedMembers: selfSeed(nodes[0]),
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
	if testing.Short() {
		t.Skip("spins up a real 3-node raft group and waits out election + replication timing; -short skips")
	}
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
		// Symmetric seeding: every node passes the same member list. Raft
		// elects a leader through normal election. No node has a special role.
		g, err := n.manager.CreateGroup(GroupConfig{
			GroupID:     "replicated",
			FSM:         fsms[i],
			SeedMembers: members,
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
	if testing.Short() {
		t.Skip("spins up two real raft groups and waits out election timing; -short skips")
	}
	// Not parallel — Raft instances + gRPC servers need clean sequential lifecycle.
	nodes := makeManagerCluster(t, []string{"node-1"})

	fsmA := &counterFSM{}
	fsmB := &counterFSM{}

	gA, err := nodes[0].manager.CreateGroup(GroupConfig{GroupID: "group-a", FSM: fsmA, SeedMembers: selfSeed(nodes[0])})
	if err != nil {
		t.Fatalf("CreateGroup A: %v", err)
	}
	gB, err := nodes[0].manager.CreateGroup(GroupConfig{GroupID: "group-b", FSM: fsmB, SeedMembers: selfSeed(nodes[0])})
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
	_, err := nodes[0].manager.CreateGroup(GroupConfig{GroupID: "ephemeral", FSM: fsm, SeedMembers: selfSeed(nodes[0])})
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

	_, err := nodes[0].manager.CreateGroup(GroupConfig{GroupID: "dup", FSM: &counterFSM{}, SeedMembers: selfSeed(nodes[0])})
	if err != nil {
		t.Fatal(err)
	}

	_, err = nodes[0].manager.CreateGroup(GroupConfig{GroupID: "dup", FSM: &counterFSM{}, SeedMembers: selfSeed(nodes[0])})
	if err == nil {
		t.Fatal("expected error for duplicate group")
	}
}

func TestVoterNonvoterAutoEnforcement(t *testing.T) {
	if testing.Short() {
		t.Skip("spins up a real 3-node raft group and waits out membership-change convergence; -short skips")
	}
	// Not parallel — Raft instances + gRPC servers need clean sequential lifecycle.
	nodes := makeManagerCluster(t, []string{"node-1", "node-2", "node-3"})

	fsm1 := &counterFSM{}
	g1, err := nodes[0].manager.CreateGroup(GroupConfig{GroupID: "voter-test", FSM: fsm1, SeedMembers: selfSeed(nodes[0])})
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
	if testing.Short() {
		t.Skip("spins up a real raft group twice (restart) and waits out election timing; -short skips")
	}
	// Not parallel — Raft instances + gRPC servers need clean sequential lifecycle.

	// Use a persistent temp dir for the group so we can restart.
	groupDir := t.TempDir()

	// Use a stable address so the Raft log's server address matches after restart.
	const stableAddr = "recovery-node"

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	tp := multiraft.New(
		hraft.ServerAddress(stableAddr),
		func(s string) []byte { return []byte(s) },
		func(b []byte) string { return string(b) },
	)
	pool := multiraft.NewSimpleDialerPeerPool(map[string]func() (net.Conn, error){
		stableAddr: func() (net.Conn, error) { return lis.Dial() },
	})
	tp.SetPeerConnPool(pool)
	tp.Register(srv)
	go func() { _ = srv.Serve(lis) }()

	wal1, err := raftwal.Open(filepath.Join(groupDir, "wal"))
	if err != nil {
		t.Fatal(err)
	}

	mgr := NewGroupManager(GroupManagerConfig{
		Transport: tp,
		NodeID:    "node-1",
		BaseDir:   groupDir,
		WAL:       wal1,
	})

	// Create group and apply some commands.
	fsm1 := &counterFSM{}
	g, err := mgr.CreateGroup(GroupConfig{
		GroupID:     "persistent",
		FSM:         fsm1,
		SeedMembers: []hraft.Server{{ID: hraft.ServerID("node-1"), Address: hraft.ServerAddress(stableAddr)}},
	})
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
	_ = wal1.Close()
	pool.Close()
	srv.Stop()
	_ = tp.Close()

	// Restart with fresh transport + server but same baseDir and stableAddr.
	lis2 := bufconn.Listen(bufSize)
	srv2 := grpc.NewServer()
	tp2 := multiraft.New(
		hraft.ServerAddress(stableAddr),
		func(s string) []byte { return []byte(s) },
		func(b []byte) string { return string(b) },
	)
	pool2 := multiraft.NewSimpleDialerPeerPool(map[string]func() (net.Conn, error){
		stableAddr: func() (net.Conn, error) { return lis2.Dial() },
	})
	tp2.SetPeerConnPool(pool2)
	tp2.Register(srv2)
	go func() { _ = srv2.Serve(lis2) }()

	wal2, err := raftwal.Open(filepath.Join(groupDir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = wal2.Close() }()

	mgr2 := NewGroupManager(GroupManagerConfig{
		Transport: tp2,
		NodeID:    "node-1",
		BaseDir:   groupDir,
		WAL:       wal2,
	})

	fsm2 := &counterFSM{}
	// On restart, SeedMembers is ignored because the existing log already
	// contains a configuration. Pass it anyway to keep parity with the
	// initial start above.
	g2, err := mgr2.CreateGroup(GroupConfig{
		GroupID:     "persistent",
		FSM:         fsm2,
		SeedMembers: []hraft.Server{{ID: hraft.ServerID("node-1"), Address: hraft.ServerAddress(stableAddr)}},
	})
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
	pool2.Close()
	srv2.Stop()
	_ = tp2.Close()
}

// TestCreateGroupLateJoinerKeepsGroupWhenPeersAlreadyElected pins the
// seedGroup race a late joiner loses (gastrolog-4yzpcj).
//
// Every node of a group seeds symmetrically, but a node that starts late
// races its own bootstrap against peers that have already begun electing.
// The instant NewRaft registers the instance with the transport, an inbound
// RequestVote can stamp a term on it — leaving it with a term but still no
// configuration, which is precisely the state hraft refuses to bootstrap
// (ErrCantBootstrap). That refusal is correct and harmless: the node is a
// valid follower and the leader replicates the configuration to it.
//
// Treating it as a CreateGroup failure was not harmless. The instance was
// shut down and discarded, and every retry started from state that was by
// then definitely non-empty, so the node could never join that group again
// — a voter silently and permanently lost to a startup race.
//
// The term is stamped directly on the group's stable store here rather than
// won by racing real peers: that IS the state the race produces, and pinning
// it deterministically beats reproducing a timing window.
func TestCreateGroupLateJoinerKeepsGroupWhenPeersAlreadyElected(t *testing.T) {
	// Not parallel — Raft instances + gRPC servers need clean sequential lifecycle.
	nodes := makeManagerCluster(t, []string{"node-1"})
	n := nodes[0]

	const groupID = "late-joiner"
	// hraft's stable-store key for the persisted term.
	if err := n.manager.wal.GroupStore(groupID).SetUint64([]byte("CurrentTerm"), 7); err != nil {
		t.Fatalf("stamp CurrentTerm: %v", err)
	}

	// A three-member symmetric seed, as every participating node passes.
	members := []hraft.Server{
		{ID: "node-1", Address: n.transport.LocalAddr()},
		{ID: "node-2", Address: hraft.ServerAddress("node-2")},
		{ID: "node-3", Address: hraft.ServerAddress("node-3")},
	}
	g, err := n.manager.CreateGroup(GroupConfig{
		GroupID:     groupID,
		FSM:         &counterFSM{},
		SeedMembers: members,
	})
	if err != nil {
		t.Fatalf("CreateGroup for a late joiner must succeed, got: %v", err)
	}
	if g == nil || n.manager.GetGroup(groupID) == nil {
		t.Fatal("late joiner's group is missing from the manager")
	}

	// The bootstrap really was refused — the node holds no configuration and
	// is waiting for the leader to replicate one. If it had bootstrapped, it
	// would be a three-server group here and this test would be exercising
	// the ordinary seeding path instead of the race.
	cfg := g.Raft.GetConfiguration()
	if err := cfg.Error(); err != nil {
		t.Fatalf("GetConfiguration: %v", err)
	}
	if servers := cfg.Configuration().Servers; len(servers) != 0 {
		t.Fatalf("late joiner bootstrapped anyway: %d servers configured, want 0", len(servers))
	}
}
