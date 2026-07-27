package cluster

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"gastrolog/internal/multiraft"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

// Multi-node coverage for gastrolog-1lbifx. Everything above this file drives
// PeerState by hand; this one stands up three REAL hashicorp/raft instances
// speaking over the real multiraft gRPC transport and lets their own
// replication heartbeats be the only liveness input. No NodeStats broadcast is
// sent anywhere in this file — which is precisely the point, since the
// acceptance is that Live<->Unreachable follows genuine Raft contact loss and
// not the stats broadcast.

// raftContactNode is one node of an in-process multiraft cluster: a TCP
// listener, a gRPC server, a multiraft transport, a real Raft, and the
// PeerState that the transport reports reachability into.
type raftContactNode struct {
	id        string
	addr      string
	transport *multiraft.Transport[string]
	pool      *multiraft.DialerPeerPool
	server    *grpc.Server
	lis       net.Listener
	raft      *hraft.Raft
	peers     *PeerState
}

// stopServing kills only the node's INBOUND gRPC stack. Its Raft keeps
// running and keeps calling out, so this models a one-way failure — we can no
// longer reach it, but it can still reach us.
func (n *raftContactNode) stopServing() {
	n.server.Stop()
	_ = n.lis.Close()
}

// goOffline is the whole node going away: inbound stack down and Raft stopped,
// so it neither answers nor initiates. This is the process-death / node-loss
// shape the unreachable verdict exists for.
func (n *raftContactNode) goOffline() {
	_ = n.raft.Shutdown().Error()
	n.stopServing()
}

const raftContactGroup = "cluster-ctl"

// newRaftContactCluster builds an n-node cluster on loopback TCP (not
// bufconn: every node needs a distinct dialable address, and bufconn's is the
// constant "bufconn"). Each node's PeerState is wired as the transport's
// contact recorder, exactly as app.setupClusterStats does in production.
func newRaftContactCluster(t *testing.T, n int, raftTTL time.Duration) []*raftContactNode {
	t.Helper()

	nodes := make([]*raftContactNode, n)
	for i := range n {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		addr := lis.Addr().String()
		srv := grpc.NewServer()
		tp := multiraft.New[string](hraft.ServerAddress(addr),
			func(s string) []byte { return []byte(s) },
			func(b []byte) string { return string(b) },
		)
		tp.Register(srv)
		// statsTTL is deliberately huge: no broadcast ever arrives in this
		// file, so any liveness observed here can only have come from Raft.
		ps := NewPeerState(time.Hour, raftTTL)
		tp.SetContactRecorder(ps)
		go func() { _ = srv.Serve(lis) }()

		nodes[i] = &raftContactNode{
			id: "node-" + string(rune('a'+i)), addr: addr,
			transport: tp, server: srv, lis: lis, peers: ps,
		}
	}

	dialers := make(map[string]func(context.Context, string) (net.Conn, error), n)
	for _, node := range nodes {
		addr := node.addr
		dialers[addr] = func(ctx context.Context, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		}
	}

	members := make([]hraft.Server, n)
	for i, node := range nodes {
		members[i] = hraft.Server{ID: hraft.ServerID(node.id), Address: hraft.ServerAddress(node.addr)}
	}

	for _, node := range nodes {
		node.pool = multiraft.NewDialerPeerPool(dialers)
		node.transport.SetPeerConnPool(node.pool)

		cfg := hraft.DefaultConfig()
		cfg.LocalID = hraft.ServerID(node.id)
		cfg.HeartbeatTimeout = 300 * time.Millisecond
		cfg.ElectionTimeout = 300 * time.Millisecond
		cfg.LeaderLeaseTimeout = 150 * time.Millisecond
		cfg.CommitTimeout = 20 * time.Millisecond
		cfg.LogOutput = io.Discard

		store := hraft.NewInmemStore()
		r, err := hraft.NewRaft(cfg, &noopFSM{}, store, store, hraft.NewInmemSnapshotStore(),
			node.transport.GroupTransport(raftContactGroup))
		if err != nil {
			t.Fatalf("NewRaft %s: %v", node.id, err)
		}
		node.raft = r
		if err := r.BootstrapCluster(hraft.Configuration{Servers: members}).Error(); err != nil {
			t.Fatalf("BootstrapCluster %s: %v", node.id, err)
		}
	}

	t.Cleanup(func() {
		for _, node := range nodes {
			_ = node.raft.Shutdown().Error()
		}
		for _, node := range nodes {
			node.pool.Close()
			node.server.Stop()
			_ = node.lis.Close()
		}
	})
	return nodes
}

func waitRaftLeader(t *testing.T, nodes []*raftContactNode, timeout time.Duration) *raftContactNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.raft.State() == hraft.Leader {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader elected")
	return nil
}

// waitLive polls one node's PeerState until the named peer reaches `want`.
// Polling is unavoidable here — the thing under test is a real cluster
// converging — but the assertion is on the converged state, never on a fixed
// duration standing in for it.
func waitLive(t *testing.T, from *raftContactNode, peer string, want bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if from.peers.IsLive(peer) == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s: IsLive(%s) never became %v within %s", from.id, peer, want, timeout)
}

// A healthy three-node cluster with NO stats broadcast at all must still see
// every peer as live, in both directions: the leader from its outbound probes
// coming back, each follower from the inbound AppendEntries it handles. This
// is the parity claim — everything the deleted 1s Heartbeat broadcast was
// providing is already on the wire.
func TestRaftContact_MultiNodeLivenessWithoutAnyBroadcast(t *testing.T) {
	if testing.Short() {
		t.Skip("real multi-node raft election and replication timing")
	}
	nodes := newRaftContactCluster(t, 3, 2*time.Second)
	leader := waitRaftLeader(t, nodes, 10*time.Second)

	for _, n := range nodes {
		if n == leader {
			continue
		}
		// Leader -> follower: outbound probes are answered.
		waitLive(t, leader, n.id, true, 10*time.Second)
		// Follower -> leader: inbound heartbeats are contact.
		waitLive(t, n, leader.id, true, 10*time.Second)
		if n.peers.LastSeen(leader.id).IsZero() {
			t.Fatalf("%s never recorded contact from the leader", n.id)
		}
	}
}

// The transition that matters: a peer stops serving, so the leader's probes
// keep going out and stop coming back. It must flip to not-live within about
// the Raft TTL, while its healthy sibling is untouched — and it must do so
// with no stats broadcast anywhere in the picture, proving the verdict tracks
// real Raft contact rather than broadcast arrival.
func TestRaftContact_MultiNodeContactLossFlipsOnlyTheLostPeer(t *testing.T) {
	if testing.Short() {
		t.Skip("real multi-node raft election and replication timing")
	}
	const raftTTL = 2 * time.Second
	nodes := newRaftContactCluster(t, 3, raftTTL)
	leader := waitRaftLeader(t, nodes, 10*time.Second)

	var followers []*raftContactNode
	for _, n := range nodes {
		if n != leader {
			followers = append(followers, n)
		}
	}
	for _, f := range followers {
		waitLive(t, leader, f.id, true, 10*time.Second)
	}

	// One follower goes away entirely. Quorum survives (2 of 3), so the
	// leader keeps leading and keeps probing the node that left.
	gone, kept := followers[0], followers[1]
	gone.goOffline()

	waitLive(t, leader, gone.id, false, 4*raftTTL)

	if !leader.peers.IsLive(kept.id) {
		t.Fatalf("healthy follower %s flipped when %s went away", kept.id, gone.id)
	}
	// LastSeen is the long-horizon accessor and must still hold the last
	// moment we heard from the departed node — the five-minute unreachable
	// sweep needs that timestamp to anchor StateSince.
	if leader.peers.LastSeen(gone.id).IsZero() {
		t.Fatalf("LastSeen(%s) is zero; the departed node's last contact must be retained", gone.id)
	}
}

// The one-way case, pinned deliberately rather than left to chance: a peer we
// can no longer REACH but which can still reach us stays live, because it is
// demonstrably alive and we can hear it say so.
//
// This is the conservative half of "positive evidence wins", and it is the
// right trade for the consumer that matters. The unreachable sweep runs on the
// cluster-ctl leader and proposes a cluster-wide FSM state change; a leader
// whose own egress has wedged must not get to declare a healthy node
// unreachable for everyone on the strength of its local failure alone. The
// cost is that a genuinely one-way-partitioned node keeps reading live here —
// visible instead as election churn and failed-heartbeat counters, which are
// the signals that actually describe that condition.
func TestRaftContact_OneWayFailureKeepsPeerLive(t *testing.T) {
	if testing.Short() {
		t.Skip("real multi-node raft election and replication timing")
	}
	const raftTTL = 2 * time.Second
	nodes := newRaftContactCluster(t, 3, raftTTL)
	leader := waitRaftLeader(t, nodes, 10*time.Second)

	var follower *raftContactNode
	for _, n := range nodes {
		if n != leader {
			follower = n
			break
		}
	}
	waitLive(t, leader, follower.id, true, 10*time.Second)

	// Inbound stack only: the follower stops answering us, but its Raft
	// keeps running and keeps soliciting votes, which reaches us.
	follower.stopServing()

	deadline := time.Now().Add(3 * raftTTL)
	for time.Now().Before(deadline) {
		if !leader.peers.IsLive(follower.id) {
			t.Fatalf("%s flipped to not-live while still reaching us; positive evidence must win", follower.id)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Recovery: contact resuming must clear the verdict on its own, with no
// broadcast and no sweep involved. A liveness signal that could not recover
// would just be a slower way to lose a node permanently.
func TestRaftContact_MultiNodeContactResumeClearsVerdict(t *testing.T) {
	if testing.Short() {
		t.Skip("real multi-node raft election and replication timing")
	}
	const raftTTL = 2 * time.Second
	nodes := newRaftContactCluster(t, 3, raftTTL)
	leader := waitRaftLeader(t, nodes, 10*time.Second)

	var follower *raftContactNode
	for _, n := range nodes {
		if n != leader {
			follower = n
			break
		}
	}
	waitLive(t, leader, follower.id, true, 10*time.Second)

	// Simulate a transient outage by expiring the evidence the way the
	// forwarder's dead-stream detection does, then let real replication
	// restore it.
	leader.peers.MarkUnreachable(follower.id)
	if leader.peers.IsLive(follower.id) {
		t.Fatal("precondition: MarkUnreachable should have expired the follower")
	}

	waitLive(t, leader, follower.id, true, 10*time.Second)
}
