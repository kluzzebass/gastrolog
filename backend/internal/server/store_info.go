package server

import (
	"context"
	"errors"
	"slices"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/index/analyzer"
	"gastrolog/internal/sysmetrics"
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
	stores, err := s.resolveStoreIDs(req.Msg.Store)
	if err != nil {
		return nil, err
	}

	resp := &apiv1.GetStatsResponse{
		TotalStores: int64(len(stores)),
	}

	for _, storeID := range stores {
		metas, err := s.orch.ListChunkMetas(storeID)
		if err != nil {
			continue
		}
		storeStat := s.buildStoreStats(ctx, storeID, metas)
		s.accumulateGlobalStats(resp, storeStat, metas)
		resp.StoreStats = append(resp.StoreStats, storeStat)
	}

	s.fillProcessMetrics(resp)

	return connect.NewResponse(resp), nil
}

func (s *StoreServer) resolveStoreIDs(storeFilter string) ([]uuid.UUID, error) {
	stores := s.orch.ListStores()
	if storeFilter == "" {
		return stores, nil
	}
	storeID, connErr := parseUUID(storeFilter)
	if connErr != nil {
		return nil, connErr
	}
	if !slices.Contains(stores, storeID) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("store not found"))
	}
	return []uuid.UUID{storeID}, nil
}

func (s *StoreServer) buildStoreStats(ctx context.Context, storeID uuid.UUID, metas []chunk.ChunkMeta) *apiv1.StoreStats {
	stat := &apiv1.StoreStats{
		Id:         storeID.String(),
		ChunkCount: int64(len(metas)),
		Enabled:    s.orch.IsStoreEnabled(storeID),
	}
	if cfg, err := s.getFullStoreConfig(ctx, storeID); err == nil {
		stat.Type = cfg.Type
		stat.Name = cfg.Name
	}

	for _, meta := range metas {
		if meta.Sealed {
			stat.SealedChunks++
		} else {
			stat.ActiveChunks++
		}
		stat.RecordCount += meta.RecordCount
		s.accumulateChunkBytes(stat, storeID, meta)
		updateTimeBounds(&stat.OldestRecord, meta.StartTS, (*timestamppb.Timestamp).AsTime, func(a, b time.Time) bool { return a.Before(b) })
		updateTimeBounds(&stat.NewestRecord, meta.EndTS, (*timestamppb.Timestamp).AsTime, func(a, b time.Time) bool { return a.After(b) })
	}
	return stat
}

func (s *StoreServer) accumulateChunkBytes(stat *apiv1.StoreStats, storeID uuid.UUID, meta chunk.ChunkMeta) {
	if meta.DiskBytes > 0 {
		stat.DataBytes += meta.DiskBytes
		return
	}
	stat.DataBytes += meta.Bytes
	if sizes, err := s.orch.IndexSizes(storeID, meta.ID); err == nil {
		for _, size := range sizes {
			stat.IndexBytes += size
		}
	}
}

func (s *StoreServer) accumulateGlobalStats(resp *apiv1.GetStatsResponse, stat *apiv1.StoreStats, metas []chunk.ChunkMeta) {
	resp.TotalChunks += int64(len(metas))
	resp.TotalRecords += stat.RecordCount
	resp.TotalBytes += stat.DataBytes + stat.IndexBytes
	resp.SealedChunks += stat.SealedChunks

	for _, meta := range metas {
		updateTimeBounds(&resp.OldestRecord, meta.StartTS, (*timestamppb.Timestamp).AsTime, func(a, b time.Time) bool { return a.Before(b) })
		updateTimeBounds(&resp.NewestRecord, meta.EndTS, (*timestamppb.Timestamp).AsTime, func(a, b time.Time) bool { return a.After(b) })
	}
}

func updateTimeBounds(field **timestamppb.Timestamp, ts time.Time, asTime func(*timestamppb.Timestamp) time.Time, isBetter func(time.Time, time.Time) bool) {
	if ts.IsZero() {
		return
	}
	if *field == nil || isBetter(ts, asTime(*field)) {
		*field = timestamppb.New(ts)
	}
}

func (s *StoreServer) fillProcessMetrics(resp *apiv1.GetStatsResponse) {
	resp.ProcessCpuPercent = sysmetrics.CPUPercent()
	mem := sysmetrics.Memory()
	resp.ProcessMemoryBytes = mem.Inuse
	resp.ProcessMemoryStats = &apiv1.ProcessMemoryStats{
		RssBytes:          mem.RSS,
		HeapAllocBytes:    mem.HeapAlloc,
		HeapInuseBytes:    mem.HeapInuse,
		HeapIdleBytes:     mem.HeapIdle,
		HeapReleasedBytes: mem.HeapReleased,
		StackInuseBytes:   mem.StackInuse,
		SysBytes:          mem.Sys,
		HeapObjects:       mem.HeapObjects,
		NumGc:             mem.NumGC,
	}
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
