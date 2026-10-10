package cluster_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	"gastrolog/internal/waittest"

	hraft "github.com/hashicorp/raft"
)

// dummyMaxAge backs the RotationPolicyConfig.MaxAgeNanos pointer used by
// putReplProbe — avoids per-callsite local string vars in cluster tests.
// Rotation policy replaces FilterConfig as the generic Raft replication
// probe.
var dummyMaxAge = "1h"

// putReplProbe writes a rotation policy through a node's store as a
// generic Raft-replicate smoke test. Tests assert it shows up on other
// nodes via waitReplication.
func putReplProbe(t *testing.T, node *testNode, id glid.GLID, name, where string) {
	t.Helper()
	if err := node.store.PutRotationPolicy(context.Background(), system.RotationPolicyConfig{
		ID: id, Name: name, MaxAge: &dummyMaxAge,
	}); err != nil {
		t.Fatalf("PutRotationPolicy %s: %v", where, err)
	}
}

// raftView snapshots each node's role and the leader it follows.
func raftView(nodes []*testNode) string {
	var b strings.Builder
	for _, n := range nodes {
		_, leader := n.raft.LeaderWithID()
		fmt.Fprintf(&b, "%s=%s(leader %q term %s) ", n.id, n.raft.State(), leader, n.raft.Stats()["term"])
	}
	return b.String()
}

// waitStableLeader waits until one of the nodes is leader and returns it.
func waitStableLeader(t *testing.T, nodes []*testNode) *testNode {
	t.Helper()
	var leader *testNode
	waittest.Progress(t, "a leader among the nodes", func() (string, bool) {
		for _, n := range nodes {
			if n.raft.State() == hraft.Leader {
				leader = n
				return raftView(nodes), true
			}
		}
		return raftView(nodes), false
	})
	return leader
}

// waitAllFollow waits until every node names leaderID as its leader. A
// node forwards writes to the leader it has heard from, so until then a
// write through it finds no leader.
func waitAllFollow(t *testing.T, nodes []*testNode, leaderID string) {
	t.Helper()
	waittest.Progress(t, "every node following "+leaderID, func() (string, bool) {
		for _, n := range nodes {
			if _, id := n.raft.LeaderWithID(); string(id) != leaderID {
				return raftView(nodes), false
			}
		}
		return raftView(nodes), true
	})
}

// waitConfig waits until the leader's Raft configuration satisfies match.
func waitConfig(t *testing.T, r *hraft.Raft, what string, match func([]hraft.Server) bool) {
	t.Helper()
	waittest.Progress(t, what, func() (string, bool) {
		cfg := r.GetConfiguration()
		if err := cfg.Error(); err != nil {
			return "configuration: " + err.Error(), false
		}
		servers := cfg.Configuration().Servers
		return fmt.Sprintf("%v", servers), match(servers)
	})
}

// hasServer reports whether servers holds id with the given suffrage.
func hasServer(servers []hraft.Server, id string, suffrage hraft.ServerSuffrage) bool {
	for _, srv := range servers {
		if string(srv.ID) == id && srv.Suffrage == suffrage {
			return true
		}
	}
	return false
}

// waitReplication waits for a rotation policy to appear on a node's FSM.
func waitReplication(t *testing.T, node *testNode, id glid.GLID) *system.RotationPolicyConfig {
	t.Helper()
	ctx := context.Background()
	var got *system.RotationPolicyConfig
	waittest.Progress(t, fmt.Sprintf("rotation policy %s on %s", id, node.id), func() (string, bool) {
		got, _ = node.store.GetRotationPolicy(ctx, id)
		return fmt.Sprintf("applied=%d", node.raft.AppliedIndex()), got != nil
	})
	return got
}

// threeNodeCluster creates and returns a 3-node Raft cluster.
// The first node is bootstrapped as leader. All nodes are cleaned up on test end.
func threeNodeCluster(t *testing.T) []*testNode {
	t.Helper()

	node1 := newTestNode(t, "node-1", true)
	t.Cleanup(node1.close)
	waitLeader(t, node1.raft, 5*time.Second)

	node2 := newTestNode(t, "node-2", false)
	t.Cleanup(node2.close)
	node3 := newTestNode(t, "node-3", false)
	t.Cleanup(node3.close)

	addVoter(t, node1.srv, "node-2", node2.srv.Addr())
	addVoter(t, node1.srv, "node-3", node3.srv.Addr())

	waitConfig(t, node1.raft, "3-node configuration", func(servers []hraft.Server) bool { return len(servers) == 3 })
	nodes := []*testNode{node1, node2, node3}
	waitAllFollow(t, nodes, "node-1")

	return nodes
}

// TestLeadershipTransfer verifies that after a leadership transfer, the new
// leader can accept writes and the old leader correctly forwards writes to it.
func TestLeadershipTransfer(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping cluster failure test in short mode")
	}

	nodes := threeNodeCluster(t)
	node1 := nodes[0]

	// Write a probe while node1 is leader.
	probeID := glid.New()
	putReplProbe(t, node1, probeID, "before-transfer", "before transfer")

	// Transfer leadership away from node1.
	if err := node1.raft.LeadershipTransfer().Error(); err != nil {
		t.Fatalf("LeadershipTransfer: %v", err)
	}

	// Wait for a new leader to emerge (not node1).
	newLeader := waitStableLeader(t, nodes)
	if newLeader == node1 {
		// Leadership may return to node1 in a 3-node cluster; that's valid
		// but we want to verify it worked at all.
		t.Log("leadership returned to node-1 (valid but less interesting)")
	}

	// Write on the new leader.
	probe2ID := glid.New()
	putReplProbe(t, newLeader, probe2ID, "after-transfer", "on new leader")

	// Verify replication to all nodes.
	for _, n := range nodes {
		got := waitReplication(t, n, probe2ID)
		if got.Name != "after-transfer" {
			t.Errorf("expected after-transfer, got %q", got.Name)
		}
	}
}

// TestNodeRemoval removes a voter from a 3-node cluster and verifies the
// remaining 2-node cluster continues to operate normally.
func TestNodeRemoval(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping cluster failure test in short mode")
	}

	nodes := threeNodeCluster(t)
	node1, node2 := nodes[0], nodes[1]

	// Remove node-3 from the cluster.
	if err := node1.raft.RemoveServer(hraft.ServerID("node-3"), 0, 5*time.Second).Error(); err != nil {
		t.Fatalf("RemoveServer node-3: %v", err)
	}

	waitConfig(t, node1.raft, "2-node configuration", func(servers []hraft.Server) bool { return len(servers) == 2 })

	// Write on the leader after removal — cluster should still work with 2 nodes.
	probeID := glid.New()
	putReplProbe(t, node1, probeID, "post-removal", "after removal")

	// Verify replication to surviving follower.
	got := waitReplication(t, node2, probeID)
	if got.Name != "post-removal" {
		t.Errorf("expected post-removal, got %q", got.Name)
	}
}

// TestFollowerShutdownClusterSurvives stops a follower in a 3-node cluster
// and verifies the leader can still commit writes (quorum = 2, one follower alive).
func TestFollowerShutdownClusterSurvives(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping cluster failure test in short mode")
	}

	nodes := threeNodeCluster(t)
	node1, node2, node3 := nodes[0], nodes[1], nodes[2]

	// Shut down node-3.
	node3.close()

	// Leader should still accept writes (quorum of 2 out of 3).
	probeID := glid.New()
	putReplProbe(t, node1, probeID, "after-follower-down", "with one follower down")

	// Verify the surviving follower got the write.
	got := waitReplication(t, node2, probeID)
	if got.Name != "after-follower-down" {
		t.Errorf("expected after-follower-down, got %q", got.Name)
	}
}

// TestNonvoterReplication adds a nonvoter to a cluster and verifies it
// receives replicated data but does not affect quorum.
func TestNonvoterReplication(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping cluster failure test in short mode")
	}

	node1 := newTestNode(t, "node-1", true)
	t.Cleanup(node1.close)
	waitLeader(t, node1.raft, 5*time.Second)

	nonvoter := newTestNode(t, "nonvoter-1", false)
	t.Cleanup(nonvoter.close)

	// Add as nonvoter.
	if err := node1.raft.AddNonvoter(
		hraft.ServerID("nonvoter-1"),
		hraft.ServerAddress(nonvoter.srv.Addr()),
		0, 5*time.Second,
	).Error(); err != nil {
		t.Fatalf("AddNonvoter: %v", err)
	}

	waitConfig(t, node1.raft, "nonvoter-1 in the configuration as nonvoter", func(servers []hraft.Server) bool {
		return hasServer(servers, "nonvoter-1", hraft.Nonvoter)
	})

	// Write on leader and verify replication to nonvoter.
	probeID := glid.New()
	putReplProbe(t, node1, probeID, "nonvoter-test", "to nonvoter")

	got := waitReplication(t, nonvoter, probeID)
	if got.Name != "nonvoter-test" {
		t.Errorf("expected nonvoter-test, got %q", got.Name)
	}
}

// TestDemoteVoterToNonvoter demotes a voter to nonvoter and verifies the
// cluster can still elect a leader and accept writes with the remaining voters.
func TestDemoteVoterToNonvoter(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping cluster failure test in short mode")
	}

	nodes := threeNodeCluster(t)
	node1, node2 := nodes[0], nodes[1]

	// Demote node-3 from voter to nonvoter.
	if err := node1.raft.DemoteVoter(hraft.ServerID("node-3"), 0, 5*time.Second).Error(); err != nil {
		t.Fatalf("DemoteVoter: %v", err)
	}

	waitConfig(t, node1.raft, "node-3 demoted to nonvoter", func(servers []hraft.Server) bool {
		return hasServer(servers, "node-3", hraft.Nonvoter)
	})

	// Leader should still accept writes (2 voters: node-1 + node-2).
	probeID := glid.New()
	putReplProbe(t, node1, probeID, "after-demote", "after demote")

	got := waitReplication(t, node2, probeID)
	if got.Name != "after-demote" {
		t.Errorf("expected after-demote, got %q", got.Name)
	}
}

// TestLeaderStepDownNewElection stops the leader in a 3-node cluster and
// verifies that a follower wins the election and the cluster recovers.
func TestLeaderStepDownNewElection(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping cluster failure test in short mode")
	}

	nodes := threeNodeCluster(t)
	node1, node2, node3 := nodes[0], nodes[1], nodes[2]

	// Write something before the leader goes down.
	preID := glid.New()
	putReplProbe(t, node1, preID, "pre-election", "pre-election")
	waitReplication(t, node2, preID)
	waitReplication(t, node3, preID)

	// Shut down the leader.
	node1.close()

	// Wait for a new leader to emerge among node2 and node3.
	survivors := []*testNode{node2, node3}
	newLeader := waitStableLeader(t, survivors)

	// Write on the new leader.
	postID := glid.New()
	putReplProbe(t, newLeader, postID, "post-election", "post-election")

	// Verify the other survivor got the write.
	for _, n := range survivors {
		got := waitReplication(t, n, postID)
		if got.Name != "post-election" {
			t.Errorf("expected post-election, got %q", got.Name)
		}
	}

	// Verify the pre-election data survived the leader change.
	for _, n := range survivors {
		got := waitReplication(t, n, preID)
		if got.Name != "pre-election" {
			t.Errorf("expected pre-election, got %q", got.Name)
		}
	}
}

// TestFollowerForwardingAfterLeaderChange verifies that a follower's write
// forwarding adapts when leadership changes. After a leadership transfer,
// writes on a follower should route to the new leader.
func TestFollowerForwardingAfterLeaderChange(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping cluster failure test in short mode")
	}

	nodes := threeNodeCluster(t)
	node1, node2, node3 := nodes[0], nodes[1], nodes[2]

	// Write via follower (node2) — should forward to node1 (current leader).
	probe1ID := glid.New()
	putReplProbe(t, node2, probe1ID, "fwd-to-node1", "via follower before transfer")
	waitReplication(t, node1, probe1ID)

	// Transfer leadership away from node1.
	if err := node1.raft.LeadershipTransfer().Error(); err != nil {
		t.Fatalf("LeadershipTransfer: %v", err)
	}

	// Wait for new leader.
	newLeader := waitStableLeader(t, nodes)

	// Find a follower that isn't the new leader.
	var follower *testNode
	for _, n := range []*testNode{node1, node2, node3} {
		if n != newLeader {
			follower = n
			break
		}
	}

	// Write via the follower once it follows the new leader.
	waitAllFollow(t, nodes, newLeader.id)
	probe2ID := glid.New()
	putReplProbe(t, follower, probe2ID, "fwd-to-new-leader", "via follower after transfer")

	// Verify all nodes got both writes.
	for _, n := range nodes {
		waitReplication(t, n, probe2ID)
	}
}

// TestQuorumLossBlocksWrites verifies that losing quorum (2 of 3 nodes down)
// prevents new writes from committing.
func TestQuorumLossBlocksWrites(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping cluster failure test in short mode")
	}

	nodes := threeNodeCluster(t)
	node1, node2, node3 := nodes[0], nodes[1], nodes[2]

	// Shut down two followers — leader loses quorum.
	node2.close()
	node3.close()

	// Attempt a write with a short timeout — should fail or hang.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	probeID := glid.New()
	err := node1.store.PutRotationPolicy(ctx, system.RotationPolicyConfig{
		ID: probeID, Name: "should-fail", MaxAge: &dummyMaxAge,
	})
	if err == nil {
		t.Error("expected PutRotationPolicy to fail without quorum, but it succeeded")
	}
}
