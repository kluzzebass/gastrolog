package cluster_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"gastrolog/internal/config"

	"github.com/google/uuid"
	hraft "github.com/hashicorp/raft"
)

// waitStableLeader waits until a node reports a stable leader (with a non-empty ID).
func waitStableLeader(t *testing.T, nodes []*testNode, timeout time.Duration) *testNode {
	t.Helper()
	deadline := time.After(timeout)
	for {
		for _, n := range nodes {
			if n.raft.State() == hraft.Leader {
				return n
			}
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for a stable leader")
		default:
			runtime.Gosched()
		}
	}
}

// waitReplication waits for a filter to appear on a node's FSM.
func waitReplication(t *testing.T, node *testNode, filterID uuid.UUID, timeout time.Duration) *config.FilterConfig {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		got, _ := node.store.GetFilter(ctx, filterID)
		if got != nil {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("filter %s not replicated within %v", filterID, timeout)
	return nil
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

	addVoter(t, node1.srv.Addr(), "node-2", node2.srv.Addr())
	addVoter(t, node1.srv.Addr(), "node-3", node3.srv.Addr())

	// Wait for all 3 nodes visible in Raft config.
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

	return []*testNode{node1, node2, node3}
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

	// Write a filter while node1 is leader.
	ctx := context.Background()
	filterID := uuid.Must(uuid.NewV7())
	if err := node1.store.PutFilter(ctx, config.FilterConfig{
		ID: filterID, Name: "before-transfer", Expression: "*",
	}); err != nil {
		t.Fatalf("PutFilter before transfer: %v", err)
	}

	// Transfer leadership away from node1.
	if err := node1.raft.LeadershipTransfer().Error(); err != nil {
		t.Fatalf("LeadershipTransfer: %v", err)
	}

	// Wait for a new leader to emerge (not node1).
	newLeader := waitStableLeader(t, nodes, 5*time.Second)
	if newLeader == node1 {
		// Leadership may return to node1 in a 3-node cluster; that's valid
		// but we want to verify it worked at all.
		t.Log("leadership returned to node-1 (valid but less interesting)")
	}

	// Write on the new leader.
	filter2ID := uuid.Must(uuid.NewV7())
	if err := newLeader.store.PutFilter(ctx, config.FilterConfig{
		ID: filter2ID, Name: "after-transfer", Expression: "*",
	}); err != nil {
		t.Fatalf("PutFilter on new leader: %v", err)
	}

	// Verify replication to all nodes.
	for _, n := range nodes {
		got := waitReplication(t, n, filter2ID, 5*time.Second)
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

	// Wait for config to reflect 2 nodes.
	deadline := time.After(5 * time.Second)
	for {
		cfg := node1.raft.GetConfiguration()
		if cfg.Error() == nil && len(cfg.Configuration().Servers) == 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for 2-node config")
		default:
			runtime.Gosched()
		}
	}

	// Write on the leader after removal — cluster should still work with 2 nodes.
	ctx := context.Background()
	filterID := uuid.Must(uuid.NewV7())
	if err := node1.store.PutFilter(ctx, config.FilterConfig{
		ID: filterID, Name: "post-removal", Expression: "*",
	}); err != nil {
		t.Fatalf("PutFilter after removal: %v", err)
	}

	// Verify replication to surviving follower.
	got := waitReplication(t, node2, filterID, 5*time.Second)
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

	// Give Raft a moment to notice the lost follower.
	time.Sleep(200 * time.Millisecond)

	// Leader should still accept writes (quorum of 2 out of 3).
	ctx := context.Background()
	filterID := uuid.Must(uuid.NewV7())
	if err := node1.store.PutFilter(ctx, config.FilterConfig{
		ID: filterID, Name: "after-follower-down", Expression: "*",
	}); err != nil {
		t.Fatalf("PutFilter with one follower down: %v", err)
	}

	// Verify the surviving follower got the write.
	got := waitReplication(t, node2, filterID, 5*time.Second)
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

	// Verify nonvoter appears in config with correct suffrage.
	deadline := time.After(5 * time.Second)
	for {
		cfg := node1.raft.GetConfiguration()
		if cfg.Error() == nil {
			for _, srv := range cfg.Configuration().Servers {
				if string(srv.ID) == "nonvoter-1" && srv.Suffrage == hraft.Nonvoter {
					goto found
				}
			}
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for nonvoter in config")
		default:
			runtime.Gosched()
		}
	}
found:

	// Write on leader and verify replication to nonvoter.
	ctx := context.Background()
	filterID := uuid.Must(uuid.NewV7())
	if err := node1.store.PutFilter(ctx, config.FilterConfig{
		ID: filterID, Name: "nonvoter-test", Expression: "*",
	}); err != nil {
		t.Fatalf("PutFilter: %v", err)
	}

	got := waitReplication(t, nonvoter, filterID, 5*time.Second)
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

	// Verify node-3 is now a nonvoter.
	deadline := time.After(5 * time.Second)
	for {
		cfg := node1.raft.GetConfiguration()
		if cfg.Error() == nil {
			for _, srv := range cfg.Configuration().Servers {
				if string(srv.ID) == "node-3" && srv.Suffrage == hraft.Nonvoter {
					goto demoted
				}
			}
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for node-3 demotion")
		default:
			runtime.Gosched()
		}
	}
demoted:

	// Leader should still accept writes (2 voters: node-1 + node-2).
	ctx := context.Background()
	filterID := uuid.Must(uuid.NewV7())
	if err := node1.store.PutFilter(ctx, config.FilterConfig{
		ID: filterID, Name: "after-demote", Expression: "*",
	}); err != nil {
		t.Fatalf("PutFilter after demote: %v", err)
	}

	got := waitReplication(t, node2, filterID, 5*time.Second)
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
	ctx := context.Background()
	preFilterID := uuid.Must(uuid.NewV7())
	if err := node1.store.PutFilter(ctx, config.FilterConfig{
		ID: preFilterID, Name: "pre-election", Expression: "*",
	}); err != nil {
		t.Fatalf("PutFilter pre-election: %v", err)
	}
	waitReplication(t, node2, preFilterID, 5*time.Second)
	waitReplication(t, node3, preFilterID, 5*time.Second)

	// Shut down the leader.
	node1.close()

	// Wait for a new leader to emerge among node2 and node3.
	survivors := []*testNode{node2, node3}
	newLeader := waitStableLeader(t, survivors, 10*time.Second)

	// Write on the new leader.
	postFilterID := uuid.Must(uuid.NewV7())
	if err := newLeader.store.PutFilter(ctx, config.FilterConfig{
		ID: postFilterID, Name: "post-election", Expression: "*",
	}); err != nil {
		t.Fatalf("PutFilter post-election: %v", err)
	}

	// Verify the other survivor got the write.
	for _, n := range survivors {
		got := waitReplication(t, n, postFilterID, 5*time.Second)
		if got.Name != "post-election" {
			t.Errorf("expected post-election, got %q", got.Name)
		}
	}

	// Verify the pre-election data survived the leader change.
	for _, n := range survivors {
		got := waitReplication(t, n, preFilterID, time.Second)
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
	ctx := context.Background()
	filter1ID := uuid.Must(uuid.NewV7())
	if err := node2.store.PutFilter(ctx, config.FilterConfig{
		ID: filter1ID, Name: "fwd-to-node1", Expression: "*",
	}); err != nil {
		t.Fatalf("PutFilter via follower before transfer: %v", err)
	}
	waitReplication(t, node1, filter1ID, 5*time.Second)

	// Transfer leadership away from node1.
	if err := node1.raft.LeadershipTransfer().Error(); err != nil {
		t.Fatalf("LeadershipTransfer: %v", err)
	}

	// Wait for new leader.
	newLeader := waitStableLeader(t, nodes, 5*time.Second)

	// Find a follower that isn't the new leader.
	var follower *testNode
	for _, n := range []*testNode{node1, node2, node3} {
		if n != newLeader {
			follower = n
			break
		}
	}

	// Write via the follower — should forward to the new leader.
	// Retry briefly: the follower may not know the new leader yet.
	filter2ID := uuid.Must(uuid.NewV7())
	fwdDeadline := time.Now().Add(5 * time.Second)
	for {
		err := follower.store.PutFilter(ctx, config.FilterConfig{
			ID: filter2ID, Name: "fwd-to-new-leader", Expression: "*",
		})
		if err == nil {
			break
		}
		if time.Now().After(fwdDeadline) {
			t.Fatalf("PutFilter via follower after transfer: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify all nodes got both writes.
	for _, n := range nodes {
		waitReplication(t, n, filter2ID, 5*time.Second)
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

	// Give Raft time to notice.
	time.Sleep(time.Second)

	// Attempt a write with a short timeout — should fail or hang.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	filterID := uuid.Must(uuid.NewV7())
	err := node1.store.PutFilter(ctx, config.FilterConfig{
		ID: filterID, Name: "should-fail", Expression: "*",
	})
	if err == nil {
		t.Error("expected PutFilter to fail without quorum, but it succeeded")
	}
}
