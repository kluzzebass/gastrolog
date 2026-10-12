package server_test

// Every stats surface in the UI dates a broadcast value by
// ClusterNode.stats_received_at and warns past GetClusterStatusResponse.
// stats_ttl. Both have to come from the responding node: an age counted in
// the browser from the moment a tab happened to fetch is a property of the
// tab, and the TTL is PeerState's own cut-off, not a constant the browser
// could know.

import (
	"context"
	"net/http"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/cluster"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
	sysmem "gastrolog/internal/system/memory"

	"connectrpc.com/connect"
)

// fakePeerStats serves stats for exactly the peers it has a receipt for.
type fakePeerStats struct {
	received map[string]time.Time
	ttl      time.Duration
}

func (f *fakePeerStats) Get(id string) *gastrologv1.NodeStats {
	if _, ok := f.received[id]; !ok {
		return nil
	}
	return &gastrologv1.NodeStats{NodeName: id}
}

func (f *fakePeerStats) LastSeen(id string) time.Time        { return f.received[id] }
func (f *fakePeerStats) StatsReceivedAt(id string) time.Time { return f.received[id] }
func (f *fakePeerStats) StatsTTL() time.Duration             { return f.ttl }

const fakeStatsTTL = 15 * time.Second

func statusFromNode(t *testing.T, nodeID string, collected time.Time, peers *fakePeerStats) *gastrologv1.GetClusterStatusResponse {
	t.Helper()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(orch.Close)
	mc := &mockCluster{
		leaderAddr: "node-1:4566",
		leaderID:   "node-1",
		servers: []cluster.RaftServer{
			{ID: "node-1", Address: "node-1:4566", Suffrage: "Voter"},
			{ID: "node-2", Address: "node-2:4566", Suffrage: "Voter"},
			{ID: "node-3", Address: "node-3:4566", Suffrage: "Voter"},
		},
	}
	srv := server.New(orch, sysmem.NewStore(), orchestrator.Factories{}, nil, server.Config{
		NoAuth:         true,
		NodeID:         nodeID,
		ClusterAddress: ":4566",
		Cluster:        mc,
		PeerStats:      peers,
		LastLocalStats: func() (*gastrologv1.NodeStats, time.Time) {
			return &gastrologv1.NodeStats{NodeName: nodeID}, collected
		},
	})
	client := gastrologv1connect.NewLifecycleServiceClient(
		&http.Client{Transport: &embeddedTransport{handler: srv.Handler()}}, "http://embedded")
	resp, err := client.GetClusterStatus(context.Background(),
		connect.NewRequest(&gastrologv1.GetClusterStatusRequest{}))
	if err != nil {
		t.Fatalf("GetClusterStatus from %s: %v", nodeID, err)
	}
	return resp.Msg
}

// From whichever node answers, the TTL is the same and every node that has
// stats has the instant they were received: the responding node's own stats
// are dated by their collection, a peer's by its broadcast receipt, and a
// peer without stats gets no instant at all — an age without a value would
// date nothing.
func TestGetClusterStatus_StatsAgeFromEveryNode(t *testing.T) {
	t.Parallel()
	start := time.Now()
	received := map[string]time.Time{
		"node-1": start.Add(-2 * time.Second),
		"node-2": start.Add(-4 * time.Second),
	}
	collected := start.Add(-3 * time.Second)
	for _, responding := range []string{"node-1", "node-2", "node-3"} {
		resp := statusFromNode(t, responding, collected, &fakePeerStats{received: received, ttl: fakeStatsTTL})
		if resp.StatsTtl != fakeStatsTTL.String() {
			t.Errorf("from %s: stats_ttl = %q, want %q", responding, resp.StatsTtl, fakeStatsTTL)
		}
		for _, n := range resp.Nodes {
			id := string(n.Id)
			switch {
			case id == responding:
				if n.Stats == nil || n.StatsReceivedAt == nil {
					t.Errorf("from %s: own node carries stats=%v received_at=%v, want both", responding, n.Stats != nil, n.StatsReceivedAt != nil)
					continue
				}
				if at := n.StatsReceivedAt.AsTime(); !at.Equal(collected) {
					t.Errorf("from %s: own stats dated %v, want their collection at %v", responding, at, collected)
				}
			case n.Stats == nil:
				if n.StatsReceivedAt != nil {
					t.Errorf("from %s: node %s has no stats but is dated %v", responding, id, n.StatsReceivedAt.AsTime())
				}
			default:
				want, ok := received[id]
				if !ok {
					t.Errorf("from %s: node %s has stats the provider never received", responding, id)
					continue
				}
				if n.StatsReceivedAt == nil || !n.StatsReceivedAt.AsTime().Equal(want) {
					t.Errorf("from %s: node %s dated %v, want the receipt %v", responding, id, n.StatsReceivedAt, want)
				}
			}
		}
	}
}

// The in-process cluster harness, with real stats collectors behind the
// provider: every live node reaches the RPC dated, and the TTL is the
// provider's, not a number the handler made up.
func TestClusterStatusCarriesStatsReceivedAtForEveryNode(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithClusterStats())
	resp, err := h.lifecycleClient.GetClusterStatus(context.Background(),
		connect.NewRequest(&gastrologv1.GetClusterStatusRequest{}))
	if err != nil {
		t.Fatalf("GetClusterStatus: %v", err)
	}
	ttl, err := time.ParseDuration(resp.Msg.StatsTtl)
	if err != nil || ttl != mnStatsTTL {
		t.Fatalf("stats_ttl = %q (%v), want %v", resp.Msg.StatsTtl, err, mnStatsTTL)
	}
	if len(resp.Msg.Nodes) != 3 {
		t.Fatalf("got %d nodes, want 3", len(resp.Msg.Nodes))
	}
	for _, n := range resp.Msg.Nodes {
		if n.Stats == nil {
			t.Errorf("node %s has no stats; the harness provider serves every node", n.Id)
			continue
		}
		if n.StatsReceivedAt == nil {
			t.Errorf("node %s has stats but no stats_received_at: the UI would show them as a live reading", n.Id)
			continue
		}
		if age := time.Since(n.StatsReceivedAt.AsTime()); age < 0 || age > ttl {
			t.Errorf("node %s stats dated %v ago, outside [0, %v]", n.Id, age, ttl)
		}
	}
}
