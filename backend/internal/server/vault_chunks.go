package server

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/config"
	"gastrolog/internal/index/analyzer"
	"gastrolog/internal/orchestrator"
)

// ListChunks returns all chunks in a vault from all tiers across all nodes.
// Routing: RouteFanOut — collects local chunks + remote chunks from all nodes.
func (s *VaultServer) ListChunks(
	ctx context.Context,
	req *connect.Request[apiv1.ListChunksRequest],
) (*connect.Response[apiv1.ListChunksResponse], error) {
	if req.Msg.Vault == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("vault required"))
	}
	vaultID, connErr := parseUUID(req.Msg.Vault)
	if connErr != nil {
		return nil, connErr
	}

	// Collect local chunks, marking any with retention-pending in the tier Raft.
	pending := s.orch.RetentionPendingChunks(vaultID)
	var allChunks []*apiv1.ChunkMeta
	metas, err := s.orch.ListAllChunkMetas(vaultID)
	if err != nil && !errors.Is(err, orchestrator.ErrVaultNotFound) {
		return nil, mapVaultError(err)
	}
	for _, meta := range metas {
		pb := TieredChunkMetaToProto(meta)
		if pending[meta.ID] {
			pb.RetentionPending = true
		}
		allChunks = append(allChunks, pb)
	}

	// Collect remote chunks from nodes that own other tiers.
	if s.remoteChunkLister != nil {
		remoteNodes := s.remoteTierNodes(ctx, vaultID)
		for _, nodeID := range remoteNodes {
			remote, err := s.remoteChunkLister.ListChunks(ctx, nodeID, &apiv1.ForwardListChunksRequest{
				VaultId: vaultID.String(),
			})
			if err != nil {
				s.logger.Warn("ListChunks: remote node failed", "node", nodeID, "vault", vaultID, "error", err)
				continue // best effort — show what we can
			}
			allChunks = append(allChunks, remote.Chunks...)
		}
	}

	return connect.NewResponse(&apiv1.ListChunksResponse{Chunks: allChunks}), nil
}

// remoteTierNodes returns node IDs of ALL remote nodes that host tiers for a
// vault — both primaries and secondaries. This ensures ListChunks collects
// replica chunks for accurate replica counting.
func (s *VaultServer) remoteTierNodes(ctx context.Context, vaultID uuid.UUID) []string {
	vaultCfg, err := s.cfgStore.GetVault(ctx, vaultID)
	if err != nil || vaultCfg == nil {
		return nil
	}
	tiers, err := s.cfgStore.ListTiers(ctx)
	if err != nil {
		return nil
	}
	nscs, err := s.cfgStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		return nil
	}
	vaultTierIDs := config.VaultTierIDs(tiers, vaultID)
	tierIDs := make(map[uuid.UUID]bool, len(vaultTierIDs))
	for _, tid := range vaultTierIDs {
		tierIDs[tid] = true
	}
	seen := make(map[string]bool)
	var nodes []string
	for _, t := range tiers {
		if !tierIDs[t.ID] {
			continue
		}
		// Leader node.
		leaderNodeID := t.LeaderNodeID(nscs)
		if leaderNodeID != "" && leaderNodeID != s.localNodeID && !seen[leaderNodeID] {
			seen[leaderNodeID] = true
			nodes = append(nodes, leaderNodeID)
		}
		// Follower nodes.
		for _, sid := range t.FollowerNodeIDs(nscs) {
			if sid != s.localNodeID && !seen[sid] {
				seen[sid] = true
				nodes = append(nodes, sid)
			}
		}
	}
	return nodes
}

// GetChunk returns details for a specific chunk.
// Routing: RouteTargeted — the interceptor forwards to the vault-owning node.
func (s *VaultServer) GetChunk(
	ctx context.Context,
	req *connect.Request[apiv1.GetChunkRequest],
) (*connect.Response[apiv1.GetChunkResponse], error) {
	if req.Msg.Vault == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("vault required"))
	}
	vaultID, connErr := parseUUID(req.Msg.Vault)
	if connErr != nil {
		return nil, connErr
	}

	chunkID, err := chunk.ParseChunkID(req.Msg.ChunkId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	meta, err := s.orch.GetTieredChunkMeta(vaultID, chunkID)
	if err != nil {
		return nil, mapVaultError(err)
	}

	return connect.NewResponse(&apiv1.GetChunkResponse{
		Chunk: TieredChunkMetaToProto(meta),
	}), nil
}

// GetIndexes returns index status for a chunk.
// Routing: RouteTargeted — the interceptor forwards to the vault-owning node.
func (s *VaultServer) GetIndexes(
	ctx context.Context,
	req *connect.Request[apiv1.GetIndexesRequest],
) (*connect.Response[apiv1.GetIndexesResponse], error) {
	if req.Msg.Vault == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("vault required"))
	}
	vaultID, connErr := parseUUID(req.Msg.Vault)
	if connErr != nil {
		return nil, connErr
	}

	chunkID, err := chunk.ParseChunkID(req.Msg.ChunkId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	report, err := s.orch.ChunkIndexInfos(vaultID, chunkID)
	if err != nil {
		return nil, mapVaultError(err)
	}

	resp := &apiv1.GetIndexesResponse{
		Sealed:  report.Sealed,
		Indexes: make([]*apiv1.IndexInfo, 0, len(report.Indexes)),
	}

	for _, idx := range report.Indexes {
		resp.Indexes = append(resp.Indexes, &apiv1.IndexInfo{
			Name:       idx.Name,
			Exists:     idx.Exists,
			EntryCount: idx.EntryCount,
			SizeBytes:  idx.SizeBytes,
		})
	}

	return connect.NewResponse(resp), nil
}

// AnalyzeChunk returns detailed index analysis for a chunk.
// Routing: RouteTargeted — the interceptor forwards to the vault-owning node.
func (s *VaultServer) AnalyzeChunk(
	ctx context.Context,
	req *connect.Request[apiv1.AnalyzeChunkRequest],
) (*connect.Response[apiv1.AnalyzeChunkResponse], error) {
	if req.Msg.Vault == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("vault required"))
	}
	vaultID, connErr := parseUUID(req.Msg.Vault)
	if connErr != nil {
		return nil, connErr
	}

	a, err := s.orch.NewAnalyzer(vaultID)
	if err != nil {
		return nil, mapVaultError(err)
	}

	var analyses []analyzer.ChunkAnalysis

	if req.Msg.ChunkId != "" {
		chunkID, parseErr := chunk.ParseChunkID(req.Msg.ChunkId)
		if parseErr != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, parseErr)
		}
		analysis, analyzeErr := a.AnalyzeChunk(chunkID)
		if analyzeErr != nil {
			return nil, connect.NewError(connect.CodeInternal, analyzeErr)
		}
		analyses = []analyzer.ChunkAnalysis{*analysis}
	} else {
		agg, err := a.AnalyzeAll()
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		analyses = agg.Chunks
	}

	resp := &apiv1.AnalyzeChunkResponse{
		Analyses: make([]*apiv1.ChunkAnalysis, 0, len(analyses)),
	}

	for _, ca := range analyses {
		resp.Analyses = append(resp.Analyses, ChunkAnalysisToProto(ca))
	}

	return connect.NewResponse(resp), nil
}

// ValidateVault checks chunk and index integrity for a vault.
// Routing: RouteTargeted — the interceptor forwards to the vault-owning node.
func (s *VaultServer) ValidateVault(
	ctx context.Context,
	req *connect.Request[apiv1.ValidateVaultRequest],
) (*connect.Response[apiv1.ValidateVaultResponse], error) {
	if req.Msg.Vault == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("vault required"))
	}
	vaultID, connErr := parseUUID(req.Msg.Vault)
	if connErr != nil {
		return nil, connErr
	}

	metas, err := s.orch.ListChunkMetas(vaultID)
	if err != nil {
		return nil, mapVaultError(err)
	}

	resp := ValidateVaultLocal(s.orch, vaultID, metas)
	return connect.NewResponse(resp), nil
}

// ValidateVaultLocal runs chunk and index integrity checks on a local vault.
// Exported so both the VaultServer RPC handler and the cluster executor can
// share the same validation logic.
func ValidateVaultLocal(orch *orchestrator.Orchestrator, vaultID uuid.UUID, metas []chunk.ChunkMeta) *apiv1.ValidateVaultResponse {
	resp := &apiv1.ValidateVaultResponse{Valid: true}
	for _, meta := range metas {
		cv := validateChunk(orch, vaultID, meta)
		if !cv.Valid {
			resp.Valid = false
		}
		resp.Chunks = append(resp.Chunks, cv)
	}
	return resp
}

// validateChunk checks a single chunk's cursor readability and index completeness.
func validateChunk(orch *orchestrator.Orchestrator, vaultID uuid.UUID, meta chunk.ChunkMeta) *apiv1.ChunkValidation {
	cv := &apiv1.ChunkValidation{
		ChunkId: meta.ID.String(),
		Valid:   true,
	}

	cursor, err := orch.OpenCursor(vaultID, meta.ID)
	if err != nil {
		cv.Valid = false
		cv.Issues = append(cv.Issues, fmt.Sprintf("cannot open cursor: %v", err))
		return cv
	}

	var recordCount int64
	for {
		_, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			cv.Valid = false
			cv.Issues = append(cv.Issues, fmt.Sprintf("read error at record %d: %v", recordCount, err))
			break
		}
		recordCount++
	}
	_ = cursor.Close()

	if meta.RecordCount > 0 && recordCount != meta.RecordCount {
		cv.Valid = false
		cv.Issues = append(cv.Issues,
			fmt.Sprintf("record count mismatch: metadata says %d, cursor read %d", meta.RecordCount, recordCount))
	}

	if meta.Sealed {
		complete, err := orch.IndexesComplete(vaultID, meta.ID)
		if err != nil {
			cv.Valid = false
			cv.Issues = append(cv.Issues, fmt.Sprintf("index check error: %v", err))
		} else if !complete {
			cv.Valid = false
			cv.Issues = append(cv.Issues, "indexes incomplete for sealed chunk")
		}
	}

	return cv
}
