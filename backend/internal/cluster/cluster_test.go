package cluster_test

import (
	"context"
	"gastrolog/internal/glid"
	"io"
	"runtime"
	"testing"
	"time"

	"gastrolog/internal/cluster"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"
	"gastrolog/internal/system/raftstore"

	"github.com/Jille/raftadmin/proto"
	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// testNode bundles a cluster server, raft instance, and config store for testing.
type testNode struct {
	id     string
	srv    *cluster.Server
	raft   *hraft.Raft
	store  *raftstore.Store
	fsm    *raftfsm.FSM
	closed bool
}

func (n *testNode) close() {
	if n.closed {
		return
	}
	n.closed = true
	// Shut down Raft first — stops replication goroutines so they don't hold
	// open gRPC streams during server stop. Then stop the server, which closes
	// the transport (unblocking any remaining handlers) and the gRPC server.
	_ = n.raft.Shutdown().Error()
	n.srv.Stop()
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
	srv.SetApplyFn(func(ctx context.Context, data []byte) (uint64, error) {
		return store.ApplyRaw(data)
	})

	// Enable leader forwarding.
	fwd := cluster.NewForwarder(r, srv.PeerConns())
	store.SetForwarder(fwd)
	t.Cleanup(func() { _ = fwd.Close() })

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	return &testNode{id: nodeID, srv: srv, raft: r, store: store, fsm: fsm}
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
	t.Parallel()
	node := newTestNode(t, "node-1", true)
	defer node.close()

	waitLeader(t, node.raft, 5*time.Second)

	// gastrolog-4kkoo (Phase 5): exercise raft.Apply via rotation policy
	// instead of the deleted filter command.
	ctx := context.Background()
	probeID := glid.New()
	err := node.store.PutRotationPolicy(ctx, system.RotationPolicyConfig{
		ID:          probeID,
		Name:        "test-probe",
		MaxAge: &dummyMaxAge,
	})
	if err != nil {
		t.Fatalf("PutRotationPolicy: %v", err)
	}

	// Verify it's readable.
	got, err := node.store.GetRotationPolicy(ctx, probeID)
	if err != nil {
		t.Fatalf("GetRotationPolicy: %v", err)
	}
	if got == nil {
		t.Fatal("expected rotation policy, got nil")
	}
	if got.Name != "test-probe" {
		t.Errorf("got name %q, want test-probe", got.Name)
	}
}

func TestThreeNodeCluster(t *testing.T) {
	t.Parallel()
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

	// Wait for Raft configuration to include all 3 voters.
	deadline := time.After(5 * time.Second)
	for {
		cfg := node1.raft.GetConfiguration()
		if cfg.Error() == nil && len(cfg.Configuration().Servers) == 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for 3-node configuration")
		default:
			runtime.Gosched()
		}
	}

	// gastrolog-4kkoo (Phase 5): exercise replication via rotation policy
	// instead of the deleted filter command.
	ctx := context.Background()
	probeID := glid.New()
	if err := node1.store.PutRotationPolicy(ctx, system.RotationPolicyConfig{
		ID:          probeID,
		Name:        "leader-probe",
		MaxAge: &dummyMaxAge,
	}); err != nil {
		t.Fatalf("PutRotationPolicy on leader: %v", err)
	}

	// Verify the policy is replicated to node 2 and 3.
	var got2, got3 *system.RotationPolicyConfig
	replDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(replDeadline) {
		got2, _ = node2.store.GetRotationPolicy(ctx, probeID)
		got3, _ = node3.store.GetRotationPolicy(ctx, probeID)
		if got2 != nil && got3 != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got2 == nil {
		t.Error("policy not replicated to node-2")
	}
	if got3 == nil {
		t.Error("policy not replicated to node-3")
	}

	// Write on a follower — should be forwarded to the leader.
	followerProbeID := glid.New()
	if err := node2.store.PutRotationPolicy(ctx, system.RotationPolicyConfig{
		ID:          followerProbeID,
		Name:        "follower-probe",
		MaxAge: &dummyMaxAge,
	}); err != nil {
		t.Fatalf("PutRotationPolicy on follower: %v", err)
	}

	// Verify the policy written via follower is readable on the leader.
	var leaderGot *system.RotationPolicyConfig
	fwdDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(fwdDeadline) {
		leaderGot, _ = node1.store.GetRotationPolicy(ctx, followerProbeID)
		if leaderGot != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if leaderGot == nil {
		t.Fatal("policy written on follower not found on leader")
	}
	if leaderGot.Name != "follower-probe" {
		t.Errorf("got name %q, want follower-probe", leaderGot.Name)
	}

	// gastrolog-2nxij regression: after PutRotationPolicy returns on a
	// follower, the follower's OWN local FSM must already reflect the
	// write — no polling. Before the fix, the follower's Forward returned
	// as soon as the leader applied, and the follower's local FSM caught
	// up asynchronously; a read on the follower could miss its own write
	// for several milliseconds (longer under load), producing the stale
	// snapshot that caused the settings UI to display pre-mutation state.
	readBackProbeID := glid.New()
	if err := node2.store.PutRotationPolicy(ctx, system.RotationPolicyConfig{
		ID:          readBackProbeID,
		Name:        "read-back-probe",
		MaxAge: &dummyMaxAge,
	}); err != nil {
		t.Fatalf("PutRotationPolicy on follower for read-back: %v", err)
	}
	// No sleep, no polling — read immediately on the same follower that wrote.
	followerSelfRead, err := node2.store.GetRotationPolicy(ctx, readBackProbeID)
	if err != nil {
		t.Fatalf("immediate GetRotationPolicy on follower: %v", err)
	}
	if followerSelfRead == nil {
		t.Fatal("follower's local FSM did not reflect its own write — read-after-write race not fixed")
	}
	if followerSelfRead.Name != "read-back-probe" {
		t.Errorf("got name %q, want read-back-probe", followerSelfRead.Name)
	}
}
