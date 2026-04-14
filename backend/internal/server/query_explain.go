package server

import (
	"gastrolog/internal/glid"
	"context"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/query"
)

// Explain returns the query execution plan without executing.
// Explains the plan for all vaults; use vault_id=X in query expression to filter.
func (s *QueryServer) Explain(
	ctx context.Context,
	req *connect.Request[apiv1.ExplainRequest],
) (*connect.Response[apiv1.ExplainResponse], error) {
	eng := s.orch.PrimaryTierQueryEngine()

	q, pipeline, err := protoToQuery(req.Msg.Query)
	if err != nil {
		return nil, errInvalidArg(err)
	}

	plan, err := eng.Explain(ctx, q)
	if err != nil {
		return nil, errInternal(err)
	}

	resp := &apiv1.ExplainResponse{
		Chunks:      make([]*apiv1.ChunkPlan, 0, len(plan.ChunkPlans)),
		Direction:   plan.Direction,
		TotalChunks: int32(plan.TotalChunks), //nolint:gosec // G115: chunk count always fits in int32
	}
	resp.Expression = plan.Query.String()
	if !plan.Query.Start.IsZero() {
		resp.QueryStart = timestamppb.New(plan.Query.Start)
	}
	if !plan.Query.End.IsZero() {
		resp.QueryEnd = timestamppb.New(plan.Query.End)
	}

	// Append pipeline stages if the query has pipe operators.
	if pipeline != nil {
		resp.PipelineStages = buildPipelineStages(pipeline)
	}

	// Cache vault→nodeID lookups to avoid repeated config reads.
	vaultNodeCache := make(map[glid.GLID]string)
	vaultNodeID := func(vaultID glid.GLID) string {
		if nid, ok := vaultNodeCache[vaultID]; ok {
			return nid
		}
		// With tiered storage, vaults no longer have a NodeID.
		vaultNodeCache[vaultID] = ""
		return ""
	}

	for _, cp := range plan.ChunkPlans {
		chunkPlan := &apiv1.ChunkPlan{
			VaultId:          cp.VaultID.ToProto(),
			ChunkId:          glid.GLID(cp.ChunkID).ToProto(),
			Sealed:           cp.Sealed,
			RecordCount:      int64(cp.RecordCount),
			ScanMode:         cp.ScanMode,
			EstimatedRecords: int64(cp.EstimatedScan),
			RuntimeFilters:   []string{cp.RuntimeFilter},
			Steps:            PipelineStepsToProto(cp.Pipeline),
			SkipReason:       cp.SkipReason,
			NodeId:           []byte(vaultNodeID(cp.VaultID)),
		}
		if !cp.WriteStart.IsZero() {
			chunkPlan.WriteStart = timestamppb.New(cp.WriteStart)
		}
		if !cp.WriteEnd.IsZero() {
			chunkPlan.WriteEnd = timestamppb.New(cp.WriteEnd)
		}

		for _, bp := range cp.BranchPlans {
			chunkPlan.BranchPlans = append(chunkPlan.BranchPlans, &apiv1.BranchPlan{
				Expression:       bp.BranchExpr,
				Steps:            PipelineStepsToProto(bp.Pipeline),
				Skipped:          bp.Skipped,
				SkipReason:       bp.SkipReason,
				EstimatedRecords: int64(bp.EstimatedScan),
			})
		}

		resp.Chunks = append(resp.Chunks, chunkPlan)
	}

	// Fan out to remote nodes to collect their chunk plans.
	s.collectRemoteExplain(ctx, q, resp)

	return connect.NewResponse(resp), nil
}

// collectRemoteExplain fans out ForwardExplain RPCs to remote nodes and
// merges their chunk plans into the response.
func (s *QueryServer) collectRemoteExplain(ctx context.Context, q query.Query, resp *apiv1.ExplainResponse) {
	if s.remoteSearcher == nil || s.cfgStore == nil {
		return
	}
	selectedVaults, _ := query.ExtractVaultFilter(q.Normalize().BoolExpr, nil)
	byNode := s.remoteVaultsByNode(ctx, selectedVaults)
	queryExpr := q.String()
	for nodeID, vaultIDs := range byNode {
		vaultBytes := make([][]byte, len(vaultIDs))
		for i, v := range vaultIDs {
			vaultBytes[i] = v.ToProto()
		}
		remote, err := s.remoteSearcher.Explain(ctx, nodeID, &apiv1.ForwardExplainRequest{
			Query:    queryExpr,
			VaultIds: vaultBytes,
		})
		if err != nil {
			s.logger.Warn("explain: remote node failed", "node", nodeID, "err", err)
			continue
		}
		resp.Chunks = append(resp.Chunks, remote.GetChunks()...)
		resp.TotalChunks += remote.GetTotalChunks()
	}
}
