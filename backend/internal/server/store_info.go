package server

import (
	"context"
	"errors"
	"slices"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/index/analyzer"
)

// storeName returns the human-readable name for a store, falling back to the ID.
func (s *StoreServer) storeName(ctx context.Context, id uuid.UUID) string {
	cfg, err := s.getFullStoreConfig(ctx, id)
	if err == nil && cfg.Name != "" {
		return cfg.Name
	}
	return id.String()
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
		info, err := s.getStoreInfo(ctx, id)
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
	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}
	info, err := s.getStoreInfo(ctx, id)
	if err != nil {
		return nil, mapStoreError(err)
	}

	return connect.NewResponse(&apiv1.GetStoreResponse{Store: info}), nil
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
		storeID, connErr := parseUUID(req.Msg.Store)
		if connErr != nil {
			return nil, connErr
		}
		found := false
		if slices.Contains(stores, storeID) {
			stores = []uuid.UUID{storeID}
			found = true
		}
		if !found {
			return nil, connect.NewError(connect.CodeNotFound, errors.New("store not found"))
		}
	}

	resp.TotalStores = int64(len(stores))

	for _, storeID := range stores {
		metas, err := s.orch.ListChunkMetas(storeID)
		if err != nil {
			continue
		}

		storeStat := &apiv1.StoreStats{
			Id:         storeID.String(),
			ChunkCount: int64(len(metas)),
			Enabled:    s.orch.IsStoreEnabled(storeID),
		}

		// Get store type and name from config store (orchestrator doesn't track these).
		if cfg, err := s.getFullStoreConfig(ctx, storeID); err == nil {
			storeStat.Type = cfg.Type
			storeStat.Name = cfg.Name
		}

		resp.TotalChunks += int64(len(metas))

		for _, meta := range metas {
			if meta.Sealed {
				resp.SealedChunks++
				storeStat.SealedChunks++
			} else {
				storeStat.ActiveChunks++
			}
			resp.TotalRecords += meta.RecordCount
			storeStat.RecordCount += meta.RecordCount
			storeStat.DataBytes += meta.Bytes

			// Sum index sizes for this chunk.
			if sizes, err := s.orch.IndexSizes(storeID, meta.ID); err == nil {
				for _, size := range sizes {
					storeStat.IndexBytes += size
				}
			}

			if !meta.StartTS.IsZero() {
				if resp.OldestRecord == nil || meta.StartTS.Before(resp.OldestRecord.AsTime()) {
					resp.OldestRecord = timestamppb.New(meta.StartTS)
				}
				if storeStat.OldestRecord == nil || meta.StartTS.Before(storeStat.OldestRecord.AsTime()) {
					storeStat.OldestRecord = timestamppb.New(meta.StartTS)
				}
			}
			if !meta.EndTS.IsZero() {
				if resp.NewestRecord == nil || meta.EndTS.After(resp.NewestRecord.AsTime()) {
					resp.NewestRecord = timestamppb.New(meta.EndTS)
				}
				if storeStat.NewestRecord == nil || meta.EndTS.After(storeStat.NewestRecord.AsTime()) {
					storeStat.NewestRecord = timestamppb.New(meta.EndTS)
				}
			}
		}

		resp.TotalBytes += storeStat.DataBytes + storeStat.IndexBytes
		resp.StoreStats = append(resp.StoreStats, storeStat)
	}

	return connect.NewResponse(resp), nil
}

func (s *StoreServer) getStoreInfo(ctx context.Context, id uuid.UUID) (*apiv1.StoreInfo, error) {
	metas, err := s.orch.ListChunkMetas(id)
	if err != nil {
		return nil, err
	}

	var recordCount int64
	for _, meta := range metas {
		recordCount += meta.RecordCount
	}

	// Get store config from config store (has name, type, params).
	cfg, _ := s.getFullStoreConfig(ctx, id)

	info := &apiv1.StoreInfo{
		Id:          id.String(),
		Name:        cfg.Name,
		Type:        cfg.Type,
		ChunkCount:  int64(len(metas)),
		RecordCount: recordCount,
		Enabled:     s.orch.IsStoreEnabled(id),
	}
	if cfg.Filter != nil {
		info.Filter = cfg.Filter.String()
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
		Bytes:       meta.Bytes,
		Compressed:  meta.Compressed,
		DiskBytes:   meta.DiskBytes,
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
