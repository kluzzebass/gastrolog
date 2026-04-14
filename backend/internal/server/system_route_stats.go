package server

import (
	"context"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
)

// GetRouteStats returns live routing statistics aggregated across the cluster.
// Local node stats come from atomic counters; peer stats from broadcasts.
func (s *SystemServer) GetRouteStats(
	_ context.Context,
	_ *connect.Request[apiv1.GetRouteStatsRequest],
) (*connect.Response[apiv1.GetRouteStatsResponse], error) {
	// Start with local node stats.
	rs := s.orch.GetRouteStats()
	totalIngested := rs.Ingested.Load()
	totalDropped := rs.Dropped.Load()
	totalRouted := rs.Routed.Load()
	filterActive := s.orch.IsFilterSetActive()

	// Merge per-vault stats into a map for dedup across nodes.
	vaultMap := make(map[string]*apiv1.VaultRouteStats)
	for vaultID, vs := range s.orch.VaultRouteStatsList() {
		vaultMap[vaultID.String()] = &apiv1.VaultRouteStats{
			VaultId:          vaultID.ToProto(),
			RecordsMatched:   vs.Matched.Load(),
			RecordsForwarded: vs.Forwarded.Load(),
		}
	}

	// Merge per-route stats into a map for dedup across nodes.
	routeMap := make(map[string]*apiv1.PerRouteStats)
	for routeID, ps := range s.orch.PerRouteStatsList() {
		routeMap[routeID.String()] = &apiv1.PerRouteStats{
			RouteId:          routeID.ToProto(),
			RecordsMatched:   ps.Matched.Load(),
			RecordsForwarded: ps.Forwarded.Load(),
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

	resp := &apiv1.GetRouteStatsResponse{
		TotalIngested:   totalIngested,
		TotalDropped:    totalDropped,
		TotalRouted:     totalRouted,
		FilterSetActive: filterActive,
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
		existing.RecordsForwarded += vs.RecordsForwarded
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
		existing.RecordsForwarded += rs.RecordsForwarded
	}
}
