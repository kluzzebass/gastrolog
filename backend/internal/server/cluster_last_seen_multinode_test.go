package server_test

// ClusterNode.last_seen is what the inspector's "offline Xs" badge counts
// from. It has to come from the cluster: a duration measured in the browser,
// from the moment a tab first noticed a node's stats were missing, is a
// property of the tab, not of the node.
//
// The value is the cluster's own last positive evidence of life
// (cluster.PeerState.LastSeen: max of last Raft contact and last stats
// broadcast). These tests pin that it reaches the RPC every node's UI reads,
// and — the part that actually matters — that it is carried INDEPENDENTLY of
// NodeStats. A node renders as offline exactly when its stats are absent, so a
// last-seen nested inside stats would vanish in the only case it is for.

import (
	"context"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"connectrpc.com/connect"
)

func clusterNodes(t *testing.T, h *multiNodeHarness) []*gastrologv1.ClusterNode {
	t.Helper()
	resp, err := h.lifecycleClient.GetClusterStatus(context.Background(),
		connect.NewRequest(&gastrologv1.GetClusterStatusRequest{}))
	if err != nil {
		t.Fatalf("GetClusterStatus: %v", err)
	}
	if !resp.Msg.ClusterEnabled {
		t.Fatal("cluster not enabled; WithClusterStats wiring missing")
	}
	return resp.Msg.Nodes
}

func TestClusterStatusCarriesLastSeenForEveryNode(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithClusterStats())

	nodes := clusterNodes(t, h)
	if len(nodes) != 3 {
		t.Fatalf("got %d nodes, want 3", len(nodes))
	}
	for _, n := range nodes {
		if n.LastSeen == nil {
			t.Errorf("node %s has no last_seen: the UI has nothing to count from "+
				"and would have to invent the offline duration", n.Id)
			continue
		}
		if n.LastSeen.Seconds <= 0 {
			t.Errorf("node %s last_seen = %d, want a real instant", n.Id, n.LastSeen.Seconds)
		}
	}
}

// The load-bearing property: last_seen must NOT be reachable only through
// NodeStats, because a node reads as offline precisely when Stats is nil. This
// fails if someone later "tidies" the field into the stats message.
func TestLastSeenIsIndependentOfNodeStats(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithClusterStats())

	for _, n := range clusterNodes(t, h) {
		// Simulate the offline case the badge exists for: drop stats and
		// confirm the timestamp the UI needs is still on the node itself.
		n.Stats = nil
		if n.LastSeen == nil {
			t.Fatalf("node %s: last_seen unavailable once Stats is nil — "+
				"it must live on ClusterNode, not inside NodeStats", n.Id)
		}
	}
}
