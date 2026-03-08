package server

import (
	"context"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
)

// GetRouteStats returns live routing statistics aggregated across the cluster.
// Local node stats come from atomic counters; peer stats from broadcasts.
func (s *ConfigServer) GetRouteStats(
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
			VaultId:          vaultID.String(),
			RecordsMatched:   vs.Matched.Load(),
			RecordsForwarded: vs.Forwarded.Load(),
		}
	}

	// Add peer stats if in cluster mode.
	if s.peerRouteStats != nil {
		pIngested, pDropped, pRouted, pFilterActive, pVaultStats := s.peerRouteStats.AggregateRouteStats()
		totalIngested += pIngested
		totalDropped += pDropped
		totalRouted += pRouted
		if pFilterActive {
			filterActive = true
		}
		for _, vs := range pVaultStats {
			existing, ok := vaultMap[vs.VaultId]
			if !ok {
				vaultMap[vs.VaultId] = vs
			} else {
				existing.RecordsMatched += vs.RecordsMatched
				existing.RecordsForwarded += vs.RecordsForwarded
			}
		}
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

	return connect.NewResponse(resp), nil
}
