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
	// broadcast rates (gastrolog-4eh5ns).
	var ingestedPerSec, routedPerSec float64
	if s.localStats != nil {
		if ls := s.localStats(); ls != nil {
			ingestedPerSec = ls.RouteIngestedPerSec
			routedPerSec = ls.RouteRoutedPerSec
		}
	}
	if s.peerRouteStats != nil {
		pIn, pRouted := s.peerRouteStats.AggregateRouteRates()
		ingestedPerSec += pIn
		routedPerSec += pRouted
	}

	resp := &apiv1.GetRouteStatsResponse{
		TotalIngested:   totalIngested,
		TotalDropped:    totalDropped,
		TotalRouted:     totalRouted,
		FilterSetActive: filterActive,
		IngestedPerSec:  ingestedPerSec,
		RoutedPerSec:    routedPerSec,
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
