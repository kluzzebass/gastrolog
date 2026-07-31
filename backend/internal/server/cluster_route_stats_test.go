package server

import (
	"testing"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
)

// A cluster total is assembled from two owners: the local orchestrator holds
// live truth for this node, PeerState caches what peers broadcast. Dropping the
// local half is the quiet failure — the total is wrong by exactly one node, and
// it is the node the operator is connected to, so it is the one they are least
// likely to cross-check against anything.
//
// These tests exist to make that omission loud. They gate the shared helper, so
// they cover every caller rather than one call site.

type fakeRouteSource struct {
	routed, unmatched, matched int64
	active                     bool
	vaults                     map[glid.GLID]*orchestrator.VaultRouteStats
	routes                     map[glid.GLID]*orchestrator.PerRouteStats
}

func (f *fakeRouteSource) GetRouteStats() *orchestrator.RouteStats {
	return &orchestrator.RouteStats{Routed: f.routed, Unmatched: f.unmatched, Matched: f.matched}
}
func (f *fakeRouteSource) IsRouteTableActive() bool { return f.active }
func (f *fakeRouteSource) VaultRouteStatsList() map[glid.GLID]*orchestrator.VaultRouteStats {
	return f.vaults
}
func (f *fakeRouteSource) PerRouteStatsList() map[glid.GLID]*orchestrator.PerRouteStats {
	return f.routes
}

type fakePeerRouteStats struct {
	routed, unmatched, matched int64
	active                     bool
	vaults                     []*apiv1.VaultRouteStats
	routes                     []*apiv1.PerRouteStats
}

func (f *fakePeerRouteStats) AggregateRouteStats() (int64, int64, int64, bool, []*apiv1.VaultRouteStats, []*apiv1.PerRouteStats) {
	return f.routed, f.unmatched, f.matched, f.active, f.vaults, f.routes
}
func (f *fakePeerRouteStats) AggregateRouteRates() (*apiv1.ThroughputRate, *apiv1.ThroughputRate) {
	return nil, nil
}

func TestClusterRouteStats_SumsLocalAndPeerCounters(t *testing.T) {
	t.Parallel()
	local := &fakeRouteSource{routed: 10, unmatched: 2, matched: 8}
	peers := &fakePeerRouteStats{routed: 90, unmatched: 3, matched: 87}

	got := clusterRouteStats(local, peers)

	if got.Routed != 100 {
		t.Errorf("routed = %d, want 100 (10 local + 90 peer) — a total missing the local node "+
			"is wrong by exactly the node the operator is looking at", got.Routed)
	}
	if got.Unmatched != 5 {
		t.Errorf("unmatched = %d, want 5", got.Unmatched)
	}
	if got.Matched != 95 {
		t.Errorf("matched = %d, want 95", got.Matched)
	}
}

func TestClusterRouteStats_LocalOnlyWhenThereAreNoPeers(t *testing.T) {
	t.Parallel()
	// Single-node mode passes a nil provider. The local half must still count;
	// a nil-guard that returns early before reading the orchestrator would
	// report zero traffic on a node that is actively routing.
	local := &fakeRouteSource{routed: 10, unmatched: 2, matched: 8, active: true}

	got := clusterRouteStats(local, nil)

	if got.Routed != 10 || got.Matched != 8 || got.Unmatched != 2 {
		t.Errorf("single-node totals = %d/%d/%d, want 10/2/8", got.Routed, got.Unmatched, got.Matched)
	}
	if !got.RouteTableActive {
		t.Error("route table reported inactive though the local node has it active")
	}
}

func TestClusterRouteStats_RouteTableActiveIsAnyNode(t *testing.T) {
	t.Parallel()
	// A route table live anywhere means routing is configured for the cluster,
	// so this is an OR and not a local read.
	got := clusterRouteStats(&fakeRouteSource{}, &fakePeerRouteStats{active: true})
	if !got.RouteTableActive {
		t.Error("a peer with an active route table did not make the cluster active")
	}
	got = clusterRouteStats(&fakeRouteSource{active: true}, &fakePeerRouteStats{})
	if !got.RouteTableActive {
		t.Error("an active local route table was lost when peers reported inactive")
	}
}

func TestClusterRouteStats_PerVaultCountsAddAcrossNodes(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	local := &fakeRouteSource{
		vaults: map[glid.GLID]*orchestrator.VaultRouteStats{vaultID: {Matched: 4}},
	}
	peers := &fakePeerRouteStats{
		vaults: []*apiv1.VaultRouteStats{{VaultId: vaultID.ToProto(), RecordsMatched: 6}},
	}

	got := clusterRouteStats(local, peers).VaultStats()
	if len(got) != 1 {
		t.Fatalf("got %d vault rows, want the two nodes' rows merged into one", len(got))
	}
	if got[0].RecordsMatched != 10 {
		t.Errorf("vault matched = %d, want 10 (4 local + 6 peer)", got[0].RecordsMatched)
	}
}

func TestClusterRouteStats_PerRouteCountsAddAcrossNodes(t *testing.T) {
	t.Parallel()
	routeID := glid.New()
	local := &fakeRouteSource{
		routes: map[glid.GLID]*orchestrator.PerRouteStats{routeID: {Matched: 7}},
	}
	peers := &fakePeerRouteStats{
		routes: []*apiv1.PerRouteStats{{RouteId: routeID.ToProto(), RecordsMatched: 3}},
	}

	got := clusterRouteStats(local, peers).RouteStats()
	if len(got) != 1 {
		t.Fatalf("got %d route rows, want one merged row", len(got))
	}
	if got[0].RecordsMatched != 10 {
		t.Errorf("route matched = %d, want 10 (7 local + 3 peer)", got[0].RecordsMatched)
	}
}

func TestClusterRouteStats_VaultOnLocalNodeOnlySurvives(t *testing.T) {
	t.Parallel()
	// A vault only this node routes to must still appear. Merging peer rows
	// into a map seeded from local is what preserves it; seeding from peers and
	// overlaying local would drop it whenever peers know nothing about it.
	vaultID := glid.New()
	local := &fakeRouteSource{
		vaults: map[glid.GLID]*orchestrator.VaultRouteStats{vaultID: {Matched: 5}},
	}
	got := clusterRouteStats(local, &fakePeerRouteStats{}).VaultStats()
	if len(got) != 1 || got[0].RecordsMatched != 5 {
		t.Errorf("vault rows = %+v, want the local-only vault preserved with 5", got)
	}
}

// Both callers feed the same UI — the RPC and the lifecycle stream — so an
// unstable order would make the stream look like it disagreed with the RPC.
func TestClusterRouteStats_OrderIsStable(t *testing.T) {
	t.Parallel()
	ids := []glid.GLID{glid.New(), glid.New(), glid.New(), glid.New()}
	vaults := map[glid.GLID]*orchestrator.VaultRouteStats{}
	for _, id := range ids {
		vaults[id] = &orchestrator.VaultRouteStats{Matched: 1}
	}
	local := &fakeRouteSource{vaults: vaults}

	var first []string
	for range 20 {
		var order []string
		for _, vs := range clusterRouteStats(local, nil).VaultStats() {
			order = append(order, glid.FromBytes(vs.VaultId).String())
		}
		if first == nil {
			first = order
			continue
		}
		for i := range order {
			if order[i] != first[i] {
				t.Fatalf("vault order varies between calls:\n  %v\n  %v", first, order)
			}
		}
	}
}
