package cluster_test

import (
	"context"
	"io"
	"testing"
	"time"

	"gastrolog/internal/cluster"
	tierfsm "gastrolog/internal/tier/raftfsm"

	hraft "github.com/hashicorp/raft"
)

// TestTierApplyForwarder_MultiNode_ForwardToLeader creates a 3-node config
// cluster and a separate 3-node tier Raft group. The forwarder on a non-tier-
// leader node forwards applies to the tier Raft leader via the cluster's
// ForwardTierApply RPC.
func TestTierApplyForwarder_MultiNode_ForwardToLeader(t *testing.T) {
	t.Parallel()
	nodes := threeNodeCluster(t)
	_ = waitStableLeader(t, nodes, 5*time.Second)

	nodeIDs := []string{"node-1", "node-2", "node-3"}
	tierGroupID := "tier-mn-fwd"

	// Build a 3-node tier Raft group using in-memory transport.
	tierFSMs, tierRafts, tierLeaderIdx := createTierRaftCluster(t, nodeIDs)

	// Wire ForwardTierApply handler on each cluster node.
	for i, n := range nodes {
		r := tierRafts[i]
		n.srv.SetTierApplyFn(func(_ context.Context, groupID string, data []byte) error {
			return r.Apply(data, cluster.ReplicationTimeout).Error()
		})
	}

	// Pick a non-tier-leader.
	nonLeaderIdx := (tierLeaderIdx + 1) % 3

	forwarder := cluster.NewTierApplyForwarder(
		tierRafts[nonLeaderIdx],
		tierGroupID,
		nodes[nonLeaderIdx].srv.PeerConns(),
		cluster.ReplicationTimeout,
	)

	// Apply via forwarder — should forward to the tier Raft leader.
	chunkID := [16]byte{0xDE, 0xAD, 0xBE, 0xEF}
	cmd := tierfsm.MarshalCreateChunk(chunkID, time.Now(), time.Now(), time.Now())
	if err := forwarder.Apply(cmd); err != nil {
		t.Fatalf("forwarded apply: %v", err)
	}

	if tierFSMs[tierLeaderIdx].Get(chunkID) == nil {
		t.Error("expected chunk in tier leader FSM")
	}

	// Wait for replication to the non-leader.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if tierFSMs[nonLeaderIdx].Get(chunkID) != nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Error("expected chunk replicated to non-leader FSM")
}

// TestTierApplyForwarder_MultiNode_LeaderDown creates a tier Raft group,
// shuts down the leader, and verifies the forwarder returns an error.
func TestTierApplyForwarder_MultiNode_LeaderDown(t *testing.T) {
	t.Parallel()
	nodes := threeNodeCluster(t)
	_ = waitStableLeader(t, nodes, 5*time.Second)

	nodeIDs := []string{"node-1", "node-2", "node-3"}
	_, tierRafts, tierLeaderIdx := createTierRaftCluster(t, nodeIDs)

	// Shut down the tier Raft leader.
	tierRafts[tierLeaderIdx].Shutdown()

	nonLeaderIdx := (tierLeaderIdx + 1) % 3
	forwarder := cluster.NewTierApplyForwarder(
		tierRafts[nonLeaderIdx],
		"tier-mn-down",
		nodes[nonLeaderIdx].srv.PeerConns(),
		2*time.Second,
	)

	cmd := tierfsm.MarshalCreateChunk([16]byte{0xFF}, time.Now(), time.Now(), time.Now())
	err := forwarder.Apply(cmd)
	if err == nil {
		t.Fatal("expected error when tier raft leader is down")
	}
}

// createTierRaftCluster creates a multi-node tier Raft group with in-memory
// transport and returns the FSMs, Raft instances, and the leader index.
func createTierRaftCluster(t *testing.T, nodeIDs []string) ([]*tierfsm.FSM, []*hraft.Raft, int) {
	t.Helper()
	n := len(nodeIDs)

	fsms := make([]*tierfsm.FSM, n)
	rafts := make([]*hraft.Raft, n)
	transports := make([]*hraft.InmemTransport, n)

	members := make([]hraft.Server, n)
	for i, nid := range nodeIDs {
		members[i] = hraft.Server{
			ID:      hraft.ServerID(nid),
			Address: hraft.ServerAddress(nid),
		}
	}

	// Create transports first, then connect all pairs.
	for i, nid := range nodeIDs {
		_, trans := hraft.NewInmemTransportWithTimeout(hraft.ServerAddress(nid), 1*time.Second)
		transports[i] = trans
		_ = nid
	}
	for i := range n {
		for j := range n {
			if i != j {
				transports[i].Connect(hraft.ServerAddress(nodeIDs[j]), transports[j])
			}
		}
	}

	// Create Raft instances — only the first bootstraps.
	for i, nid := range nodeIDs {
		fsms[i] = tierfsm.New()

		conf := hraft.DefaultConfig()
		conf.LocalID = hraft.ServerID(nid)
		conf.HeartbeatTimeout = 500 * time.Millisecond
		conf.ElectionTimeout = 500 * time.Millisecond
		conf.LeaderLeaseTimeout = 250 * time.Millisecond
		conf.LogOutput = io.Discard

		store := hraft.NewInmemStore()
		snap := hraft.NewInmemSnapshotStore()

		r, err := hraft.NewRaft(conf, fsms[i], store, store, snap, transports[i])
		if err != nil {
			t.Fatalf("create tier raft %s: %v", nid, err)
		}
		t.Cleanup(func() { r.Shutdown() })

		if i == 0 {
			r.BootstrapCluster(hraft.Configuration{Servers: members})
		}

		rafts[i] = r
	}

	// Wait for a tier Raft leader.
	var leaderIdx int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for i, r := range rafts {
			if r.State() == hraft.Leader {
				leaderIdx = i
				return fsms, rafts, leaderIdx
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no tier raft leader elected")
	return nil, nil, 0
}
