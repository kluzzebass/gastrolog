package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	"gastrolog/internal/index/analyzer"
	"gastrolog/internal/orchestrator"
)

// StoreServer implements the StoreService.
type StoreServer struct {
	orch *orchestrator.Orchestrator
}

var _ gastrologv1connect.StoreServiceHandler = (*StoreServer)(nil)

// NewStoreServer creates a new StoreServer.
func NewStoreServer(orch *orchestrator.Orchestrator) *StoreServer {
	return &StoreServer{orch: orch}
}

// ListStores returns all registered stores.
func (s *StoreServer) ListStores(
	ctx context.Context,
	req *connect.Request[apiv1.ListStoresRequest],
) (*connect.Response[apiv1.ListStoresResponse], error) {
	stores := s.orch.ListStores()

	resp := &apiv1.ListStoresResponse{
		Stores: make([]*apiv1.StoreInfo, 0, len(stores)),
	}

	for _, id := range stores {
		info, err := s.getStoreInfo(id)
		if err != nil {
			continue // Skip stores with errors
		}
		resp.Stores = append(resp.Stores, info)
	}

	return connect.NewResponse(resp), nil
}

// GetStore returns details for a specific store.
func (s *StoreServer) GetStore(
	ctx context.Context,
	req *connect.Request[apiv1.GetStoreRequest],
) (*connect.Response[apiv1.GetStoreResponse], error) {
	info, err := s.getStoreInfo(req.Msg.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, err)
	}

	return connect.NewResponse(&apiv1.GetStoreResponse{Store: info}), nil
}

// ListChunks returns all chunks in a store.
func (s *StoreServer) ListChunks(
	ctx context.Context,
	req *connect.Request[apiv1.ListChunksRequest],
) (*connect.Response[apiv1.ListChunksResponse], error) {
	store := req.Msg.Store
	if store == "" {
		store = "default"
	}

	cm := s.orch.ChunkManager(store)
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
	store := req.Msg.Store
	if store == "" {
		store = "default"
	}

	cm := s.orch.ChunkManager(store)
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
	store := req.Msg.Store
	if store == "" {
		store = "default"
	}

	cm := s.orch.ChunkManager(store)
	im := s.orch.IndexManager(store)
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

	// Check which indexes exist
	resp := &apiv1.GetIndexesResponse{
		Sealed:  meta.Sealed,
		Indexes: make([]*apiv1.IndexInfo, 0),
	}

	// Token index
	if _, err := im.OpenTokenIndex(chunkID); err == nil {
		resp.Indexes = append(resp.Indexes, &apiv1.IndexInfo{
			Name:   "token",
			Exists: true,
		})
	} else {
		resp.Indexes = append(resp.Indexes, &apiv1.IndexInfo{
			Name:   "token",
			Exists: false,
		})
	}

	// TODO: check attr and kv indexes

	return connect.NewResponse(resp), nil
}

// AnalyzeChunk returns detailed index analysis for a chunk.
func (s *StoreServer) AnalyzeChunk(
	ctx context.Context,
	req *connect.Request[apiv1.AnalyzeChunkRequest],
) (*connect.Response[apiv1.AnalyzeChunkResponse], error) {
	store := req.Msg.Store
	if store == "" {
		store = "default"
	}

	cm := s.orch.ChunkManager(store)
	im := s.orch.IndexManager(store)
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

// GetStats returns overall statistics for a store.
func (s *StoreServer) GetStats(
	ctx context.Context,
	req *connect.Request[apiv1.GetStatsRequest],
) (*connect.Response[apiv1.GetStatsResponse], error) {
	resp := &apiv1.GetStatsResponse{}

	stores := s.orch.ListStores()
	if req.Msg.Store != "" {
		// Filter to specific store
		found := false
		for _, id := range stores {
			if id == req.Msg.Store {
				stores = []string{id}
				found = true
				break
			}
		}
		if !found {
			return nil, connect.NewError(connect.CodeNotFound, errors.New("store not found"))
		}
	}

	resp.TotalStores = int64(len(stores))

	for _, storeID := range stores {
		cm := s.orch.ChunkManager(storeID)
		if cm == nil {
			continue
		}

		metas, err := cm.List()
		if err != nil {
			continue
		}

		resp.TotalChunks += int64(len(metas))

		for _, meta := range metas {
			if meta.Sealed {
				resp.SealedChunks++
			}
			resp.TotalRecords += meta.RecordCount

			if resp.OldestRecord == nil || (!meta.StartTS.IsZero() && meta.StartTS.Before(resp.OldestRecord.AsTime())) {
				resp.OldestRecord = timestamppb.New(meta.StartTS)
			}
			if resp.NewestRecord == nil || meta.EndTS.After(resp.NewestRecord.AsTime()) {
				resp.NewestRecord = timestamppb.New(meta.EndTS)
			}
		}
	}

	return connect.NewResponse(resp), nil
}

func (s *StoreServer) getStoreInfo(id string) (*apiv1.StoreInfo, error) {
	cm := s.orch.ChunkManager(id)
	if cm == nil {
		return nil, errors.New("store not found")
	}

	metas, err := cm.List()
	if err != nil {
		return nil, err
	}

	var recordCount int64
	for _, meta := range metas {
		recordCount += meta.RecordCount
	}

	// Get route expression
	cfg, _ := s.orch.StoreConfig(id)

	info := &apiv1.StoreInfo{
		Id:          id,
		ChunkCount:  int64(len(metas)),
		RecordCount: recordCount,
	}
	if cfg.Route != nil {
		info.Route = *cfg.Route
	}
	return info, nil
}

func chunkMetaToProto(meta chunk.ChunkMeta) *apiv1.ChunkMeta {
	return &apiv1.ChunkMeta{
		Id:          meta.ID.String(),
		StartTs:     timestamppb.New(meta.StartTS),
		EndTs:       timestamppb.New(meta.EndTS),
		Sealed:      meta.Sealed,
		RecordCount: meta.RecordCount,
	}
}

func tokenStatusString(stats *analyzer.TokenIndexStats) string {
	if stats == nil {
		return "missing"
	}
	return "ok"
}

func kvStatusString(stats *analyzer.KVIndexStats) string {
	if stats == nil {
		return "missing"
	}
	if stats.BudgetExhausted {
		return "capped"
	}
	return "ok"
}
