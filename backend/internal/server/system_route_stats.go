package server

import (
	"context"
	"sort"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
)

// GetRouteStats returns live routing statistics aggregated across the cluster.
// Local node stats come from the pipeline routing counters; peer stats from
// broadcasts.
func (s *SystemServer) GetRouteStats(
	_ context.Context,
	_ *connect.Request[apiv1.GetRouteStatsRequest],
) (*connect.Response[apiv1.GetRouteStatsResponse], error) {
	// Start with local node stats.
	totals := clusterRouteStats(s.orch, s.peerRouteStats)

	// Cluster-total throughput. Preferred source: the stats collector's
	// window over SUMMED cluster counters — one server-side series carrying
	// instant/30s/1m AND spark history, so the UI never fabricates history
	// client-side. Fallback (single-node, tests): sum local + peer
	// per-horizon rates, sparkless.
	var routedRate, matchedRate *apiv1.ThroughputRate
	if s.clusterRouteRates != nil {
		routedRate, matchedRate = s.clusterRouteRates()
	} else {
		routedRate, matchedRate = clusterRouteRates(s.localStats, s.peerRouteStats)
	}

	return connect.NewResponse(&apiv1.GetRouteStatsResponse{
		TotalRouted:      totals.Routed,
		TotalUnmatched:   totals.Unmatched,
		TotalMatched:     totals.Matched,
		RouteTableActive: totals.RouteTableActive,
		RoutedRate:       routedRate,
		MatchedRate:      matchedRate,
		VaultStats:       totals.VaultStats(),
		RouteStats:       totals.RouteStats(),
	}), nil
}

// clusterRouteStats is the whole-cluster route picture: this node's counters
// summed with every live peer's, and the per-vault and per-route breakdowns
// merged across nodes.
//
// A free function over both halves, matching clusterRouteRates below, because
// the two halves come from different owners — the local orchestrator holds live
// truth, PeerState caches what peers broadcast — and assembling them at each
// call site is how the local half gets left out. That omission is quiet: the
// total is wrong by exactly one node, and it is the node the operator is
// connected to, so it is the one they are least likely to cross-check.
func clusterRouteStats(orch routeStatsSource, peers PeerRouteStatsProvider) clusterRouteTotals {
	rs := orch.GetRouteStats()
	out := clusterRouteTotals{
		Routed:           rs.Routed,
		Unmatched:        rs.Unmatched,
		Matched:          rs.Matched,
		RouteTableActive: orch.IsRouteTableActive(),
		vaults:           map[string]*apiv1.VaultRouteStats{},
		routes:           map[string]*apiv1.PerRouteStats{},
	}
	for vaultID, vs := range orch.VaultRouteStatsList() {
		out.vaults[vaultID.String()] = &apiv1.VaultRouteStats{
			VaultId:        vaultID.ToProto(),
			RecordsMatched: vs.Matched,
		}
	}
	for routeID, ps := range orch.PerRouteStatsList() {
		out.routes[routeID.String()] = &apiv1.PerRouteStats{
			RouteId:        routeID.ToProto(),
			RecordsMatched: ps.Matched,
		}
	}
	if peers == nil {
		return out
	}
	pRouted, pUnmatched, pMatched, pActive, pVaultStats, pRouteStats := peers.AggregateRouteStats()
	out.Routed += pRouted
	out.Unmatched += pUnmatched
	out.Matched += pMatched
	if pActive {
		out.RouteTableActive = true
	}
	mergeVaultRouteStats(out.vaults, pVaultStats)
	mergePerRouteStats(out.routes, pRouteStats)
	return out
}

// clusterRouteTotals carries the merged result. The per-vault and per-route
// maps stay unexported and are read through VaultStats/RouteStats so callers
// cannot accidentally consume a half-merged map.
type clusterRouteTotals struct {
	Routed           int64
	Unmatched        int64
	Matched          int64
	RouteTableActive bool

	vaults map[string]*apiv1.VaultRouteStats
	routes map[string]*apiv1.PerRouteStats
}

// VaultStats returns the merged per-vault breakdown, ordered by vault ID so
// two calls over the same data produce the same response.
func (c clusterRouteTotals) VaultStats() []*apiv1.VaultRouteStats {
	out := make([]*apiv1.VaultRouteStats, 0, len(c.vaults))
	for _, vs := range c.vaults {
		out = append(out, vs)
	}
	sort.Slice(out, func(i, j int) bool {
		return glid.FromBytes(out[i].VaultId).String() < glid.FromBytes(out[j].VaultId).String()
	})
	return out
}

// RouteStats returns the merged per-route breakdown, ordered by route ID.
func (c clusterRouteTotals) RouteStats() []*apiv1.PerRouteStats {
	out := make([]*apiv1.PerRouteStats, 0, len(c.routes))
	for _, rs := range c.routes {
		out = append(out, rs)
	}
	sort.Slice(out, func(i, j int) bool {
		return glid.FromBytes(out[i].RouteId).String() < glid.FromBytes(out[j].RouteId).String()
	})
	return out
}

// routeStatsSource is the local half: the orchestrator's own counters. Narrow
// so the merge can be exercised without standing one up.
type routeStatsSource interface {
	GetRouteStats() *orchestrator.RouteStats
	IsRouteTableActive() bool
	VaultRouteStatsList() map[glid.GLID]*orchestrator.VaultRouteStats
	PerRouteStatsList() map[glid.GLID]*orchestrator.PerRouteStats
}

func mergeVaultRouteStats(m map[string]*apiv1.VaultRouteStats, stats []*apiv1.VaultRouteStats) {
	for _, vs := range stats {
		key := glid.FromBytes(vs.VaultId).String()
		existing, ok := m[key]
		if !ok {
			m[key] = vs
			continue
		}
		existing.RecordsMatched += vs.RecordsMatched
	}
}

func mergePerRouteStats(m map[string]*apiv1.PerRouteStats, stats []*apiv1.PerRouteStats) {
	for _, rs := range stats {
		key := glid.FromBytes(rs.RouteId).String()
		existing, ok := m[key]
		if !ok {
			m[key] = rs
			continue
		}
		existing.RecordsMatched += rs.RecordsMatched
	}
}

// clusterRouteRates returns cluster-total routing throughput per horizon:
// the local node's rolling-window rates (stats collector snapshot) plus the
// sum of live peers' broadcast rates. Shared by the GetRouteStats RPC and
// the WatchSystemStatus stream builder — both must fill the rate fields,
// or the stream overwrites the UI cache with 0/s while the RPC reports
// correct rates. Sparks stay per-node (phase-skewed sums would fabricate
// a series no node observed).
func clusterRouteRates(localStats func() *apiv1.NodeStats, peers PeerRouteStatsProvider) (routed, matched *apiv1.ThroughputRate) {
	routed = &apiv1.ThroughputRate{}
	matched = &apiv1.ThroughputRate{}
	if peers != nil {
		routed, matched = peers.AggregateRouteRates()
	}
	if localStats != nil {
		if ls := localStats(); ls != nil {
			addRate(routed, ls.RouteRouted)
			addRate(matched, ls.RouteMatched)
		}
	}
	return routed, matched
}

func addRate(dst, src *apiv1.ThroughputRate) {
	if src == nil {
		return
	}
	dst.InstantPerSec += src.InstantPerSec
	dst.Avg_1MPerSec += src.Avg_1MPerSec
	dst.Avg_5MPerSec += src.Avg_5MPerSec
	dst.Avg_15MPerSec += src.Avg_15MPerSec
}
