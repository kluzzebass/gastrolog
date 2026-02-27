package cluster_test

import (
	"context"
	"io"
	"testing"
	"time"

	"gastrolog/internal/cluster"
	"gastrolog/internal/config"
	"gastrolog/internal/config/raftfsm"
	"gastrolog/internal/config/raftstore"

	"github.com/Jille/raftadmin/proto"
	"github.com/google/uuid"
	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// testNode bundles a cluster server, raft instance, and config store for testing.
type testNode struct {
	srv   *cluster.Server
	raft  *hraft.Raft
	store *raftstore.Store
	fsm   *raftfsm.FSM
}

func (n *testNode) close() {
	n.srv.Stop()
	_ = n.raft.Shutdown().Error()
}

// newTestNode creates a cluster node listening on a random port.
func newTestNode(t *testing.T, nodeID string, bootstrap bool) *testNode {
	t.Helper()

	// Create cluster server on random port.
	srv, err := cluster.New(cluster.Config{
		ClusterAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("cluster.New: %v", err)
	}

	// Get transport before creating raft.
	transport := srv.Transport()

	fsm := raftfsm.New()

	conf := hraft.DefaultConfig()
	conf.LocalID = hraft.ServerID(nodeID)
	conf.LogOutput = io.Discard
	conf.HeartbeatTimeout = 500 * time.Millisecond
	conf.ElectionTimeout = 500 * time.Millisecond
	conf.LeaderLeaseTimeout = 250 * time.Millisecond

	logStore := hraft.NewInmemStore()
	stableStore := hraft.NewInmemStore()
	snapStore := hraft.NewInmemSnapshotStore()

	r, err := hraft.NewRaft(conf, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}

	if bootstrap {
		boot := hraft.Configuration{
			Servers: []hraft.Server{
				{ID: hraft.ServerID(nodeID), Address: transport.LocalAddr()},
			},
		}
		if err := r.BootstrapCluster(boot).Error(); err != nil {
			t.Fatalf("BootstrapCluster: %v", err)
		}
	}

	store := raftstore.New(r, fsm, 10*time.Second)

	// Wire the cluster server.
	srv.SetRaft(r)
	srv.SetApplyFn(func(ctx context.Context, data []byte) error {
		return store.ApplyRaw(data)
	})

	// Enable leader forwarding.
	fwd := cluster.NewForwarder(r)
	store.SetForwarder(fwd)
	t.Cleanup(func() { _ = fwd.Close() })

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	return &testNode{srv: srv, raft: r, store: store, fsm: fsm}
}

// waitLeader waits for a node to become leader.
func waitLeader(t *testing.T, r *hraft.Raft, timeout time.Duration) {
	t.Helper()
	select {
	case <-r.LeaderCh():
	case <-time.After(timeout):
		t.Fatal("timed out waiting for leadership")
	}
}

// addVoter adds a voter to the cluster via raftadmin gRPC.
func addVoter(t *testing.T, leaderAddr, voterID, voterAddr string) {
	t.Helper()
	conn, err := grpc.NewClient(leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial leader for AddVoter: %v", err)
	}
	defer conn.Close()

	client := proto.NewRaftAdminClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.AddVoter(ctx, &proto.AddVoterRequest{
		Id:      voterID,
		Address: voterAddr,
	})
	if err != nil {
		t.Fatalf("AddVoter: %v", err)
	}

	// Await the future.
	_, err = client.Await(ctx, resp)
	if err != nil {
		t.Fatalf("Await AddVoter: %v", err)
	}
}

func TestSingleNodeForwardApply(t *testing.T) {
	node := newTestNode(t, "node-1", true)
	defer node.close()

	waitLeader(t, node.raft, 5*time.Second)

	// Write a filter config via the store (goes through raft.Apply on leader).
	ctx := context.Background()
	filterID := uuid.Must(uuid.NewV7())
	err := node.store.PutFilter(ctx, config.FilterConfig{
		ID:         filterID,
		Name:       "test-filter",
		Expression: "*",
	})
	if err != nil {
		t.Fatalf("PutFilter: %v", err)
	}

	// Verify it's readable.
	got, err := node.store.GetFilter(ctx, filterID)
	if err != nil {
		t.Fatalf("GetFilter: %v", err)
	}
	if got == nil {
		t.Fatal("expected filter, got nil")
	}
	if got.Name != "test-filter" {
		t.Errorf("got name %q, want test-filter", got.Name)
	}
}

func TestThreeNodeCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-node cluster test in short mode")
	}

	// Bootstrap node 1.
	node1 := newTestNode(t, "node-1", true)
	defer node1.close()
	waitLeader(t, node1.raft, 5*time.Second)

	// Create nodes 2 and 3 (no bootstrap).
	node2 := newTestNode(t, "node-2", false)
	defer node2.close()

	node3 := newTestNode(t, "node-3", false)
	defer node3.close()

	// Add nodes 2 and 3 as voters via raftadmin.
	addVoter(t, node1.srv.Addr(), "node-2", node2.srv.Addr())
	addVoter(t, node1.srv.Addr(), "node-3", node3.srv.Addr())

	// Give Raft a moment to stabilize.
	time.Sleep(500 * time.Millisecond)

	// Write a filter on the leader.
	ctx := context.Background()
	filterID := uuid.Must(uuid.NewV7())
	if err := node1.store.PutFilter(ctx, config.FilterConfig{
		ID:         filterID,
		Name:       "leader-filter",
		Expression: "*",
	}); err != nil {
		t.Fatalf("PutFilter on leader: %v", err)
	}

	// Verify the filter is replicated to node 2 and 3.
	var got2, got3 *config.FilterConfig
	for range 20 {
		got2, _ = node2.store.GetFilter(ctx, filterID)
		got3, _ = node3.store.GetFilter(ctx, filterID)
		if got2 != nil && got3 != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got2 == nil {
		t.Error("filter not replicated to node-2")
	}
	if got3 == nil {
		t.Error("filter not replicated to node-3")
	}

	// Write on a follower — should be forwarded to the leader.
	followerFilterID := uuid.Must(uuid.NewV7())
	if err := node2.store.PutFilter(ctx, config.FilterConfig{
		ID:         followerFilterID,
		Name:       "follower-filter",
		Expression: "*",
	}); err != nil {
		t.Fatalf("PutFilter on follower: %v", err)
	}

	// Verify the filter written via follower is readable on the leader.
	var leaderGot *config.FilterConfig
	for range 20 {
		leaderGot, _ = node1.store.GetFilter(ctx, followerFilterID)
		if leaderGot != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if leaderGot == nil {
		t.Fatal("filter written on follower not found on leader")
	}
	if leaderGot.Name != "follower-filter" {
		t.Errorf("got name %q, want follower-filter", leaderGot.Name)
	}
}
