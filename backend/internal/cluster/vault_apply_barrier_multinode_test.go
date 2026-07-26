package cluster_test

import (
	"context"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/vaultraft"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// vaultCtlTestGroup is one node's member of a vault-ctl Raft group running
// on top of the shared cluster server's multiraft transport.
type vaultCtlTestGroup struct {
	node *testNode
	raft *hraft.Raft
	fsm  *vaultraft.FSM
}

// setupVaultCtlGroup creates a symmetric vault-ctl Raft group across the
// given cluster nodes — vaultraft FSM per node, multiraft group transport
// off each node's cluster server, ForwardVaultApply receiver wired the way
// wireClusterRaftApplies does in app.go (index-returning groupApplyFn).
func setupVaultCtlGroup(t *testing.T, nodes []*testNode, vaultID glid.GLID) (string, []*vaultCtlTestGroup) {
	t.Helper()
	gid := raftgroup.VaultControlPlaneGroupID(vaultID)

	members := make([]hraft.Server, 0, len(nodes))
	for _, n := range nodes {
		members = append(members, hraft.Server{
			ID:      hraft.ServerID(n.id),
			Address: hraft.ServerAddress(n.srv.Addr()),
		})
	}

	groups := make([]*vaultCtlTestGroup, 0, len(nodes))
	for _, n := range nodes {
		fsm := vaultraft.NewFSM()
		tp := n.srv.MultiRaftTransport().GroupTransport(gid)

		conf := hraft.DefaultConfig()
		conf.LocalID = hraft.ServerID(n.id)
		conf.LogOutput = io.Discard
		conf.HeartbeatTimeout = 300 * time.Millisecond
		conf.ElectionTimeout = 300 * time.Millisecond
		conf.LeaderLeaseTimeout = 150 * time.Millisecond

		store := hraft.NewInmemStore()
		r, err := hraft.NewRaft(conf, fsm, store, store, hraft.NewInmemSnapshotStore(), tp)
		if err != nil {
			t.Fatalf("NewRaft vault-ctl %s: %v", n.id, err)
		}
		t.Cleanup(func() { _ = r.Shutdown().Error() })

		if err := r.BootstrapCluster(hraft.Configuration{Servers: members}).Error(); err != nil {
			t.Fatalf("BootstrapCluster vault-ctl %s: %v", n.id, err)
		}

		// ForwardVaultApply receiver — mirrors wireClusterRaftApplies.
		groupRaft := r
		n.srv.SetGroupApplyFn(func(_ context.Context, groupID string, data []byte) (uint64, error) {
			if groupID != gid {
				return 0, errors.New("unexpected group " + groupID)
			}
			future := groupRaft.Apply(data, cluster.ReplicationTimeout)
			if err := future.Error(); err != nil {
				return 0, err
			}
			if resp := future.Response(); resp != nil {
				if err, ok := resp.(error); ok && err != nil {
					return future.Index(), err
				}
			}
			return future.Index(), nil
		})

		groups = append(groups, &vaultCtlTestGroup{node: n, raft: r, fsm: fsm})
	}
	return gid, groups
}

// waitVaultCtlLeader blocks until one group member reports leadership.
func waitVaultCtlLeader(t *testing.T, groups []*vaultCtlTestGroup, timeout time.Duration) *vaultCtlTestGroup {
	t.Helper()
	deadline := time.After(timeout)
	for {
		for _, g := range groups {
			if g.raft.State() == hraft.Leader {
				return g
			}
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for a vault-ctl group leader")
		default:
			runtime.Gosched()
		}
	}
}

// groupFollowersOf returns every group member that is not currently leader.
func groupFollowersOf(groups []*vaultCtlTestGroup) []*vaultCtlTestGroup {
	var out []*vaultCtlTestGroup
	for _, g := range groups {
		if g.raft.State() != hraft.Leader {
			out = append(out, g)
		}
	}
	return out
}

// applyWithLeaderRetry runs apply, retrying only leader-discovery
// transients ("no raft leader" immediately after election or transfer,
// same convention as the cluster-ctl tests). Any wait/barrier error is
// fatal immediately — the barrier is what's under test.
func applyWithLeaderRetry(t *testing.T, apply func() error, where string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := apply()
		if err == nil {
			return
		}
		if !errors.Is(err, cluster.ErrNoRaftLeader) && !errors.Is(err, cluster.ErrNoVaultRaftLeader) {
			t.Fatalf("apply %s: %v", where, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("apply %s: still no leader: %v", where, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// readChunkNow asserts the chunk entry is visible in the node's LOCAL
// vault-ctl sub-FSM with NO retry, NO sleep — the read-after-write barrier
// must have made the forwarded apply locally visible before Apply returned.
func readChunkNow(t *testing.T, g *vaultCtlTestGroup, vaultID glid.GLID, cid chunk.ChunkID, where string) {
	t.Helper()
	sub := g.fsm.VaultFSM(vaultID)
	if sub == nil {
		t.Fatalf("read-after-write barrier failed %s: vault sub-FSM absent on forwarding node", where)
	}
	if sub.Get(cid) == nil {
		t.Fatalf("read-after-write barrier failed %s: forwarded chunk apply not locally visible", where)
	}
}

// TestFourNodeVaultCtlFollowerReadAfterForwardedApply is the multi-node
// regression test for gastrolog-4l24u: on a 4-node vault-ctl Raft group, a
// chunk-FSM command forwarded from ANY non-leader node via ForwardVaultApply
// must be visible to an immediate read of that node's LOCAL group FSM —
// the leader returns its applied index and the forwarder blocks on the
// event-driven apply wait (gastrolog-3klg1 mechanism) until the local FSM
// catches up. Covers both forward paths (VaultCtlChunkApplyForwarder and
// VaultApplyForwarder) and a group leadership change.
func TestFourNodeVaultCtlFollowerReadAfterForwardedApply(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping multi-node cluster test in short mode")
	}

	nodes := fourNodeCluster(t)
	vaultID := glid.New()
	gid, groups := setupVaultCtlGroup(t, nodes, vaultID)
	leader := waitVaultCtlLeader(t, groups, 10*time.Second)

	now := time.Now()

	// Path 1: VaultCtlChunkApplyForwarder from every follower.
	for _, follower := range groupFollowersOf(groups) {
		cid := chunk.NewChunkID()
		fwd := cluster.NewVaultCtlChunkApplyForwarder(
			follower.raft, gid, vaultID, follower.fsm.ApplyWait(),
			follower.node.srv.PeerConns(), cluster.ReplicationTimeout)
		applyWithLeaderRetry(t, func() error {
			return fwd.Apply(vaultctlfsm.MarshalCreateChunk(cid, now, now, now))
		}, "chunk-apply forwarder")
		readChunkNow(t, follower, vaultID, cid, "chunk-apply forwarder follower "+follower.node.id)
	}

	// Path 2: VaultApplyForwarder (native vault-ctl command envelope) from
	// one follower.
	follower := groupFollowersOf(groups)[0]
	cid2 := chunk.NewChunkID()
	vfwd := cluster.NewVaultApplyForwarder(
		follower.raft, gid, follower.fsm.ApplyWait(),
		follower.node.srv.PeerConns(), cluster.ReplicationTimeout)
	applyWithLeaderRetry(t, func() error {
		return vfwd.Apply(vaultraft.MarshalVaultChunkCommand(vaultID,
			vaultctlfsm.MarshalCreateChunk(cid2, now, now, now)))
	}, "vault-apply forwarder")
	readChunkNow(t, follower, vaultID, cid2, "vault-apply forwarder follower")

	// Leadership change: transfer the group leadership away, then forward
	// from a (new) follower and read back immediately. The barrier must
	// hold regardless of which member committed the entry.
	if err := leader.raft.LeadershipTransfer().Error(); err != nil {
		t.Fatalf("vault-ctl LeadershipTransfer: %v", err)
	}
	newLeader := waitVaultCtlLeader(t, groups, 10*time.Second)
	if newLeader == leader {
		t.Fatal("vault-ctl leadership did not move")
	}
	postFollower := groupFollowersOf(groups)[0]
	cid3 := chunk.NewChunkID()
	postFwd := cluster.NewVaultCtlChunkApplyForwarder(
		postFollower.raft, gid, vaultID, postFollower.fsm.ApplyWait(),
		postFollower.node.srv.PeerConns(), cluster.ReplicationTimeout)
	applyWithLeaderRetry(t, func() error {
		return postFwd.Apply(vaultctlfsm.MarshalCreateChunk(cid3, now, now, now))
	}, "post-transfer chunk-apply forwarder")
	readChunkNow(t, postFollower, vaultID, cid3, "after group leadership transfer")
}
