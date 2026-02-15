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

// ListChunks returns all chunks in a store.
func (s *StoreServer) ListChunks(
	ctx context.Context,
	req *connect.Request[apiv1.ListChunksRequest],
) (*connect.Response[apiv1.ListChunksResponse], error) {
	if req.Msg.Store == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("store required"))
	}
	storeID, connErr := parseUUID(req.Msg.Store)
	if connErr != nil {
		return nil, connErr
	}

	cm := s.orch.ChunkManager(storeID)
	if cm == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("store not found"))
	}

	metas, err := cm.List()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
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
func (s *StoreServer) GetChunk(
	ctx context.Context,
	req *connect.Request[apiv1.GetChunkRequest],
) (*connect.Response[apiv1.GetChunkResponse], error) {
	if req.Msg.Store == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("store required"))
	}
	storeID, connErr := parseUUID(req.Msg.Store)
	if connErr != nil {
		return nil, connErr
	}

	cm := s.orch.ChunkManager(storeID)
	if cm == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("store not found"))
	}

	chunkID, err := chunk.ParseChunkID(req.Msg.ChunkId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	meta, err := cm.Meta(chunkID)
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, err)
	}

	return connect.NewResponse(&apiv1.GetChunkResponse{
		Chunk: chunkMetaToProto(meta),
	}), nil
}

// GetIndexes returns index status for a chunk.
func (s *StoreServer) GetIndexes(
	ctx context.Context,
	req *connect.Request[apiv1.GetIndexesRequest],
) (*connect.Response[apiv1.GetIndexesResponse], error) {
	if req.Msg.Store == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("store required"))
	}
	storeID, connErr := parseUUID(req.Msg.Store)
	if connErr != nil {
		return nil, connErr
	}

	cm := s.orch.ChunkManager(storeID)
	im := s.orch.IndexManager(storeID)
	if cm == nil || im == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("store not found"))
	}

	chunkID, err := chunk.ParseChunkID(req.Msg.ChunkId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	meta, err := cm.Meta(chunkID)
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, err)
	}

	sizes := im.IndexSizes(chunkID)

	resp := &apiv1.GetIndexesResponse{
		Sealed:  meta.Sealed,
		Indexes: make([]*apiv1.IndexInfo, 0, 7),
	}

	// Token index
	if idx, err := im.OpenTokenIndex(chunkID); err == nil {
		resp.Indexes = append(resp.Indexes, &apiv1.IndexInfo{
			Name: "token", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["token"],
		})
	} else {
		resp.Indexes = append(resp.Indexes, &apiv1.IndexInfo{Name: "token"})
	}

	// Attr key index
	if idx, err := im.OpenAttrKeyIndex(chunkID); err == nil {
		resp.Indexes = append(resp.Indexes, &apiv1.IndexInfo{
			Name: "attr_key", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["attr_key"],
		})
	} else {
		resp.Indexes = append(resp.Indexes, &apiv1.IndexInfo{Name: "attr_key"})
	}

	// Attr value index
	if idx, err := im.OpenAttrValueIndex(chunkID); err == nil {
		resp.Indexes = append(resp.Indexes, &apiv1.IndexInfo{
			Name: "attr_val", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["attr_val"],
		})
	} else {
		resp.Indexes = append(resp.Indexes, &apiv1.IndexInfo{Name: "attr_val"})
	}

	// Attr kv index
	if idx, err := im.OpenAttrKVIndex(chunkID); err == nil {
		resp.Indexes = append(resp.Indexes, &apiv1.IndexInfo{
			Name: "attr_kv", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["attr_kv"],
		})
	} else {
		resp.Indexes = append(resp.Indexes, &apiv1.IndexInfo{Name: "attr_kv"})
	}

	// KV key index
	if idx, _, err := im.OpenKVKeyIndex(chunkID); err == nil {
		resp.Indexes = append(resp.Indexes, &apiv1.IndexInfo{
			Name: "kv_key", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["kv_key"],
		})
	} else {
		resp.Indexes = append(resp.Indexes, &apiv1.IndexInfo{Name: "kv_key"})
	}

	// KV value index
	if idx, _, err := im.OpenKVValueIndex(chunkID); err == nil {
		resp.Indexes = append(resp.Indexes, &apiv1.IndexInfo{
			Name: "kv_val", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["kv_val"],
		})
	} else {
		resp.Indexes = append(resp.Indexes, &apiv1.IndexInfo{Name: "kv_val"})
	}

	// KV combined index
	if idx, _, err := im.OpenKVIndex(chunkID); err == nil {
		resp.Indexes = append(resp.Indexes, &apiv1.IndexInfo{
			Name: "kv_kv", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["kv_kv"],
		})
	} else {
		resp.Indexes = append(resp.Indexes, &apiv1.IndexInfo{Name: "kv_kv"})
	}

	return connect.NewResponse(resp), nil
}

// AnalyzeChunk returns detailed index analysis for a chunk.
func (s *StoreServer) AnalyzeChunk(
	ctx context.Context,
	req *connect.Request[apiv1.AnalyzeChunkRequest],
) (*connect.Response[apiv1.AnalyzeChunkResponse], error) {
	if req.Msg.Store == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("store required"))
	}
	storeID, connErr := parseUUID(req.Msg.Store)
	if connErr != nil {
		return nil, connErr
	}

	cm := s.orch.ChunkManager(storeID)
	im := s.orch.IndexManager(storeID)
	if cm == nil || im == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("store not found"))
	}

	a := analyzer.New(cm, im)

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

// ValidateStore checks chunk and index integrity for a store.
func (s *StoreServer) ValidateStore(
	ctx context.Context,
	req *connect.Request[apiv1.ValidateStoreRequest],
) (*connect.Response[apiv1.ValidateStoreResponse], error) {
	if req.Msg.Store == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("store required"))
	}
	storeID, connErr := parseUUID(req.Msg.Store)
	if connErr != nil {
		return nil, connErr
	}

	cm := s.orch.ChunkManager(storeID)
	im := s.orch.IndexManager(storeID)
	if cm == nil || im == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("store not found"))
	}

	metas, err := cm.List()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &apiv1.ValidateStoreResponse{Valid: true}

	for _, meta := range metas {
		cv := &apiv1.ChunkValidation{
			ChunkId: meta.ID.String(),
			Valid:   true,
		}

		// Check that we can read the chunk via cursor.
		cursor, err := cm.OpenCursor(meta.ID)
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
			cursor.Close()

			if meta.RecordCount > 0 && recordCount != meta.RecordCount {
				cv.Valid = false
				cv.Issues = append(cv.Issues,
					fmt.Sprintf("record count mismatch: metadata says %d, cursor read %d", meta.RecordCount, recordCount))
			}
		}

		// For sealed chunks, check index completeness.
		if meta.Sealed {
			complete, err := im.IndexesComplete(meta.ID)
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
