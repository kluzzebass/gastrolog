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
	totalIngested := rs.Ingested
	totalDropped := rs.Dropped
	totalRouted := rs.Routed
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
		pIngested, pDropped, pRouted, pFilterActive, pVaultStats, pRouteStats := s.peerRouteStats.AggregateRouteStats()
		totalIngested += pIngested
		totalDropped += pDropped
		totalRouted += pRouted
		if pFilterActive {
			filterActive = true
		}
		mergeVaultRouteStats(vaultMap, pVaultStats)
		mergePerRouteStats(routeMap, pRouteStats)
	}

	// Cluster-total throughput: local rolling-window rates plus live peers'
	// broadcast rates, per horizon (gastrolog-4eh5ns).
	ingestedRate, routedRate := clusterRouteRates(s.localStats, s.peerRouteStats)

	resp := &apiv1.GetRouteStatsResponse{
		TotalIngested:   totalIngested,
		TotalDropped:    totalDropped,
		TotalRouted:     totalRouted,
		FilterSetActive: filterActive,
		IngestedRate:    ingestedRate,
		RoutedRate:      routedRate,
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
func clusterRouteRates(localStats func() *apiv1.NodeStats, peers PeerRouteStatsProvider) (ingested, routed *apiv1.ThroughputRate) {
	ingested = &apiv1.ThroughputRate{}
	routed = &apiv1.ThroughputRate{}
	if peers != nil {
		ingested, routed = peers.AggregateRouteRates()
	}
	if localStats != nil {
		if ls := localStats(); ls != nil {
			addRate(ingested, ls.RouteIngested)
			addRate(routed, ls.RouteRouted)
		}
	}
	return ingested, routed
}

func addRate(dst, src *apiv1.ThroughputRate) {
	if src == nil {
		return
	}
	dst.InstantPerSec += src.InstantPerSec
	dst.Avg_30SPerSec += src.Avg_30SPerSec
	dst.Avg_60SPerSec += src.Avg_60SPerSec
}
