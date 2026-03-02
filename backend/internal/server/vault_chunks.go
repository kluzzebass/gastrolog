package server

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/index/analyzer"
	"gastrolog/internal/orchestrator"
)

// ListChunks returns all chunks in a vault. Forwards to the owning node
// when the vault is remote.
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

	// Forward to remote node if the vault isn't local.
	if nodeID := s.remoteNodeForVault(ctx, vaultID); nodeID != "" {
		if s.remote == nil {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("remote vault forwarding not configured"))
		}
		fwdResp, err := s.remote.ListChunks(ctx, nodeID, &apiv1.ForwardListChunksRequest{VaultId: vaultID.String()})
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("forward to %s: %w", nodeID, err))
		}
		return connect.NewResponse(&apiv1.ListChunksResponse{Chunks: fwdResp.GetChunks()}), nil
	}

	metas, err := s.orch.ListChunkMetas(vaultID)
	if err != nil {
		return nil, mapVaultError(err)
	}

	resp := &apiv1.ListChunksResponse{
		Chunks: make([]*apiv1.ChunkMeta, 0, len(metas)),
	}

	for _, meta := range metas {
		resp.Chunks = append(resp.Chunks, ChunkMetaToProto(meta))
	}

	return connect.NewResponse(resp), nil
}

// GetChunk returns details for a specific chunk.
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

	meta, err := s.orch.GetChunkMeta(vaultID, chunkID)
	if err != nil {
		return nil, mapVaultError(err)
	}

	return connect.NewResponse(&apiv1.GetChunkResponse{
		Chunk: ChunkMetaToProto(meta),
	}), nil
}

// GetIndexes returns index status for a chunk. Forwards to the owning node
// when the vault is remote.
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

	// Forward to remote node if the vault isn't local.
	if nodeID := s.remoteNodeForVault(ctx, vaultID); nodeID != "" {
		if s.remote == nil {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("remote vault forwarding not configured"))
		}
		fwdResp, err := s.remote.GetIndexes(ctx, nodeID, &apiv1.ForwardGetIndexesRequest{
			VaultId: vaultID.String(),
			ChunkId: chunkID.String(),
		})
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("forward to %s: %w", nodeID, err))
		}
		return connect.NewResponse(&apiv1.GetIndexesResponse{
			Sealed:  fwdResp.GetSealed(),
			Indexes: fwdResp.GetIndexes(),
		}), nil
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
		protoAnalysis := &apiv1.ChunkAnalysis{
			ChunkId:     ca.ChunkID.String(),
			Sealed:      ca.Sealed,
			RecordCount: ca.ChunkRecords,
			Indexes:     make([]*apiv1.IndexAnalysis, 0),
		}

		// Token index
		if ca.TokenStats != nil {
			protoAnalysis.Indexes = append(protoAnalysis.Indexes, &apiv1.IndexAnalysis{
				Name:       "token",
				Complete:   true, // Token index doesn't have partial state
				Status:     tokenStatusString(ca.TokenStats),
				EntryCount: ca.TokenStats.UniqueTokens,
			})
		}

		// Attr index
		if ca.AttrKVStats != nil {
			protoAnalysis.Indexes = append(protoAnalysis.Indexes, &apiv1.IndexAnalysis{
				Name:       "attr",
				Complete:   true, // Attr index doesn't have budget limits
				Status:     "ok",
				EntryCount: ca.AttrKVStats.UniqueKeys + ca.AttrKVStats.UniqueValues + ca.AttrKVStats.UniqueKeyValuePairs,
			})
		}

		// KV index
		if ca.KVStats != nil {
			protoAnalysis.Indexes = append(protoAnalysis.Indexes, &apiv1.IndexAnalysis{
				Name:       "kv",
				Complete:   !ca.KVStats.BudgetExhausted,
				Status:     kvStatusString(ca.KVStats),
				EntryCount: ca.KVStats.KeysIndexed + ca.KVStats.ValuesIndexed + ca.KVStats.PairsIndexed,
			})
		}

		resp.Analyses = append(resp.Analyses, protoAnalysis)
	}

	return connect.NewResponse(resp), nil
}

// ValidateVault checks chunk and index integrity for a vault. Forwards to the
// owning node when the vault is remote.
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

	// Forward to remote node if the vault isn't local.
	if nodeID := s.remoteNodeForVault(ctx, vaultID); nodeID != "" {
		if s.remote == nil {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("remote vault forwarding not configured"))
		}
		fwdResp, err := s.remote.ValidateVault(ctx, nodeID, &apiv1.ForwardValidateVaultRequest{VaultId: vaultID.String()})
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("forward to %s: %w", nodeID, err))
		}
		return connect.NewResponse(&apiv1.ValidateVaultResponse{
			Valid:  fwdResp.GetValid(),
			Chunks: fwdResp.GetChunks(),
		}), nil
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
