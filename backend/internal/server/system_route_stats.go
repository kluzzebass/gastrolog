package server

import (
	"context"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
)

// GetRouteStats returns live routing statistics aggregated across the cluster.
// Local node stats come from the pipeline routing counters; peer stats from
// broadcasts.
func (s *SystemServer) GetRouteStats(
	_ context.Context,
	_ *connect.Request[apiv1.GetRouteStatsRequest],
) (*connect.Response[apiv1.GetRouteStatsResponse], error) {
	// Start with local node stats.
	rs := s.orch.GetRouteStats()
	totalRouted := rs.Routed
	totalUnmatched := rs.Unmatched
	totalMatched := rs.Matched
	filterActive := s.orch.IsFilterSetActive()

	// Merge per-vault stats into a map for dedup across nodes.
	vaultMap := make(map[string]*apiv1.VaultRouteStats)
	for vaultID, vs := range s.orch.VaultRouteStatsList() {
		vaultMap[vaultID.String()] = &apiv1.VaultRouteStats{
			VaultId:        vaultID.ToProto(),
			RecordsMatched: vs.Matched,
		}
	}

	// Merge per-route stats into a map for dedup across nodes.
	routeMap := make(map[string]*apiv1.PerRouteStats)
	for routeID, ps := range s.orch.PerRouteStatsList() {
		routeMap[routeID.String()] = &apiv1.PerRouteStats{
			RouteId:        routeID.ToProto(),
			RecordsMatched: ps.Matched,
		}
	}

	// Add peer stats if in cluster mode.
	if s.peerRouteStats != nil {
		pRouted, pUnmatched, pMatched, pFilterActive, pVaultStats, pRouteStats := s.peerRouteStats.AggregateRouteStats()
		totalRouted += pRouted
		totalUnmatched += pUnmatched
		totalMatched += pMatched
		if pFilterActive {
			filterActive = true
		}
		mergeVaultRouteStats(vaultMap, pVaultStats)
		mergePerRouteStats(routeMap, pRouteStats)
	}

	// Cluster-total throughput. Preferred source: the stats collector's
	// window over SUMMED cluster counters — one server-side series carrying
	// instant/30s/1m AND spark history, so the UI never fabricates history
	// client-side. Fallback (single-node, tests): sum local + peer
	// per-horizon rates, sparkless (gastrolog-4eh5ns).
	var routedRate, matchedRate *apiv1.ThroughputRate
	if s.clusterRouteRates != nil {
		routedRate, matchedRate = s.clusterRouteRates()
	} else {
		routedRate, matchedRate = clusterRouteRates(s.localStats, s.peerRouteStats)
	}

	resp := &apiv1.GetRouteStatsResponse{
		TotalRouted:     totalRouted,
		TotalUnmatched:    totalUnmatched,
		TotalMatched:    totalMatched,
		FilterSetActive: filterActive,
		RoutedRate:      routedRate,
		MatchedRate:     matchedRate,
	}
	for _, vs := range vaultMap {
		resp.VaultStats = append(resp.VaultStats, vs)
	}
	for _, rs := range routeMap {
		resp.RouteStats = append(resp.RouteStats, rs)
	}

	return connect.NewResponse(resp), nil
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
// the WatchSystemStatus stream builder — the stream previously shipped a
// response without the rate fields, so the UI cache was continuously
// overwritten with 0/s while the RPC reported correct rates
// (gastrolog-4eh5ns). Sparks stay per-node (phase-skewed sums would
// fabricate a series no node observed).
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
