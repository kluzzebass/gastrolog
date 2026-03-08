package server

import (
	"context"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
)

// GetRouteStats returns live routing statistics from the orchestrator.
func (s *ConfigServer) GetRouteStats(
	_ context.Context,
	_ *connect.Request[apiv1.GetRouteStatsRequest],
) (*connect.Response[apiv1.GetRouteStatsResponse], error) {
	rs := s.orch.GetRouteStats()

	resp := &apiv1.GetRouteStatsResponse{
		TotalIngested:  rs.Ingested.Load(),
		TotalDropped:   rs.Dropped.Load(),
		TotalRouted:    rs.Routed.Load(),
		FilterSetActive: s.orch.IsFilterSetActive(),
	}

	for vaultID, vs := range s.orch.VaultRouteStatsList() {
		resp.VaultStats = append(resp.VaultStats, &apiv1.VaultRouteStats{
			VaultId:          vaultID.String(),
			RecordsMatched:   vs.Matched.Load(),
			RecordsForwarded: vs.Forwarded.Load(),
		})
	}

	return connect.NewResponse(resp), nil
}
