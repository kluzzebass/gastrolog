package cluster_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"

	hraft "github.com/hashicorp/raft"
)

// fourNodeCluster creates and returns a 4-node Raft cluster over real
// cluster servers. The first node is bootstrapped as leader. All nodes are
// cleaned up on test end.
func fourNodeCluster(t *testing.T) []*testNode {
	t.Helper()

	node1 := newTestNode(t, "node-1", true)
	t.Cleanup(node1.close)
	waitLeader(t, node1.raft, 5*time.Second)

	nodes := []*testNode{node1}
	for _, id := range []string{"node-2", "node-3", "node-4"} {
		n := newTestNode(t, id, false)
		t.Cleanup(n.close)
		addVoter(t, node1.srv.Addr(), id, n.srv.Addr())
		nodes = append(nodes, n)
	}

	deadline := time.After(5 * time.Second)
	for {
		cfg := node1.raft.GetConfiguration()
		if cfg.Error() == nil && len(cfg.Configuration().Servers) == 4 {
			return nodes
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for 4-node configuration")
		default:
			runtime.Gosched()
		}
	}
}

// followersOf returns every node that is not currently the Raft leader.
func followersOf(nodes []*testNode) []*testNode {
	var out []*testNode
	for _, n := range nodes {
		if n.raft.State() != hraft.Leader {
			out = append(out, n)
		}
	}
	return out
}

// readBackNow reads a rotation policy on the given node with NO retry, NO
// sleep — the read-after-write barrier must have made the forwarded write
// locally visible by the time the mutation call returned.
func readBackNow(t *testing.T, node *testNode, id glid.GLID, where string) *system.RotationPolicyConfig {
	t.Helper()
	got, err := node.store.GetRotationPolicy(context.Background(), id)
	if err != nil {
		t.Fatalf("immediate GetRotationPolicy %s: %v", where, err)
	}
	if got == nil {
		t.Fatalf("read-after-write barrier failed %s: forwarded write not locally visible", where)
	}
	return got
}

// TestFourNodeFollowerReadAfterForwardedWrite is the multi-node regression
// test for the event-driven apply wait: on a 4-node cluster, a mutation
// forwarded from ANY follower must be visible to an immediate read on that
// same follower — woken by the FSM apply notification, never by polling.
// Also exercises leader change: after a leadership transfer the barrier
// must keep holding for writes forwarded by the deposed leader and the
// remaining followers.
func TestFourNodeFollowerReadAfterForwardedWrite(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping multi-node cluster test in short mode")
	}

	nodes := fourNodeCluster(t)
	leader := waitStableLeader(t, nodes, 5*time.Second)

	// Every follower forwards a write and immediately reads it back locally.
	for _, follower := range followersOf(nodes) {
		id := glid.New()
		putReplProbe(t, follower, id, "probe-"+id.String(), "on follower")
		got := readBackNow(t, follower, id, "on forwarding follower")
		if got.MaxAge == nil || *got.MaxAge != dummyMaxAge {
			t.Errorf("follower read-back returned wrong policy: %+v", got)
		}
	}

	// Leader change: transfer leadership away, then forward from the
	// deposed leader (now a follower) and read back immediately. The
	// barrier must hold across the change — the forwarder re-resolves the
	// leader and the wait releases on the local FSM apply, regardless of
	// which node committed the entry.
	if err := leader.raft.LeadershipTransfer().Error(); err != nil {
		t.Fatalf("LeadershipTransfer: %v", err)
	}
	newLeader := waitStableLeader(t, nodes, 10*time.Second)
	if newLeader == leader {
		t.Fatalf("leadership did not move")
	}

	for _, follower := range followersOf(nodes) {
		id := glid.New()
		// Retry the WRITE briefly — a follower may not have learned the
		// new leader yet ("no known leader" is a discovery transient, same
		// convention as TestNodeRemoval). The READ below is the assertion
		// under test and is never retried: once the write returns, the
		// barrier guarantees local visibility.
		fwdDeadline := time.Now().Add(5 * time.Second)
		for {
			err := follower.store.PutRotationPolicy(context.Background(), system.RotationPolicyConfig{
				ID: id, Name: "post-transfer-probe-" + id.String(), MaxAge: &dummyMaxAge,
			})
			if err == nil {
				break
			}
			if time.Now().After(fwdDeadline) {
				t.Fatalf("PutRotationPolicy via follower after transfer: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
		}
		readBackNow(t, follower, id, "after leadership transfer")
	}
}
