package server

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/index/analyzer"
)

// ListChunks returns all chunks in a vault.
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

	metas, err := s.orch.ListChunkMetas(vaultID)
	if err != nil {
		return nil, mapVaultError(err)
	}

	resp := &apiv1.ListChunksResponse{
		Chunks: make([]*apiv1.ChunkMeta, 0, len(metas)),
	}

	for _, meta := range metas {
		resp.Chunks = append(resp.Chunks, chunkMetaToProto(meta))
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
		Chunk: chunkMetaToProto(meta),
	}), nil
}

// GetIndexes returns index status for a chunk.
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

// ValidateVault checks chunk and index integrity for a vault.
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

	resp := &apiv1.ValidateVaultResponse{Valid: true}

	for _, meta := range metas {
		cv := &apiv1.ChunkValidation{
			ChunkId: meta.ID.String(),
			Valid:   true,
		}

		// Check that we can read the chunk via cursor.
		cursor, err := s.orch.OpenCursor(vaultID, meta.ID)
		if err != nil {
			cv.Valid = false
			cv.Issues = append(cv.Issues, fmt.Sprintf("cannot open cursor: %v", err))
		} else {
			// Count records to verify consistency with metadata.
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
		}

		// For sealed chunks, check index completeness.
		if meta.Sealed {
			complete, err := s.orch.IndexesComplete(vaultID, meta.ID)
			if err != nil {
				cv.Valid = false
				cv.Issues = append(cv.Issues, fmt.Sprintf("index check error: %v", err))
			} else if !complete {
				cv.Valid = false
				cv.Issues = append(cv.Issues, "indexes incomplete for sealed chunk")
			}
		}

		if !cv.Valid {
			resp.Valid = false
		}
		resp.Chunks = append(resp.Chunks, cv)
	}

	return connect.NewResponse(resp), nil
}
