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

// vaultName returns the human-readable name for a vault, falling back to the ID.
func (s *VaultServer) vaultName(ctx context.Context, id uuid.UUID) string {
	cfg, err := s.getFullVaultConfig(ctx, id)
	if err == nil && cfg.Name != "" {
		return cfg.Name
	}
	return id.String()
}

// ListVaults returns all registered vaults, including remote vaults from
// other cluster nodes. Remote vaults are enriched with stats from peer
// broadcasts when available.
func (s *VaultServer) ListVaults(
	ctx context.Context,
	req *connect.Request[apiv1.ListVaultsRequest],
) (*connect.Response[apiv1.ListVaultsResponse], error) {
	localIDs := s.orch.ListVaults()

	resp := &apiv1.ListVaultsResponse{
		Vaults: make([]*apiv1.VaultInfo, 0, len(localIDs)),
	}

	localSet := make(map[uuid.UUID]struct{}, len(localIDs))
	for _, id := range localIDs {
		localSet[id] = struct{}{}
		info, err := s.getVaultInfo(ctx, id)
		if err != nil {
			continue
		}
		resp.Vaults = append(resp.Vaults, info)
	}

	// Append remote vaults from config store (vaults owned by other nodes).
	s.appendRemoteVaults(ctx, localSet, resp)

	return connect.NewResponse(resp), nil
}

// GetVault returns details for a specific vault.
func (s *VaultServer) GetVault(
	ctx context.Context,
	req *connect.Request[apiv1.GetVaultRequest],
) (*connect.Response[apiv1.GetVaultResponse], error) {
	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}
	info, err := s.getVaultInfo(ctx, id)
	if err != nil {
		return nil, mapVaultError(err)
	}

	return connect.NewResponse(&apiv1.GetVaultResponse{Vault: info}), nil
}

// GetStats returns overall statistics for a vault.
func (s *VaultServer) GetStats(
	ctx context.Context,
	req *connect.Request[apiv1.GetStatsRequest],
) (*connect.Response[apiv1.GetStatsResponse], error) {
	vaults, err := s.resolveVaultIDs(req.Msg.Vault)
	if err != nil {
		return nil, err
	}

	resp := &apiv1.GetStatsResponse{
		TotalVaults: int64(len(vaults)),
	}

	for _, vaultID := range vaults {
		metas, err := s.orch.ListChunkMetas(vaultID)
		if err != nil {
			continue
		}
		vaultStat := s.buildVaultStats(ctx, vaultID, metas)
		s.accumulateGlobalStats(resp, vaultStat, metas)
		resp.VaultStats = append(resp.VaultStats, vaultStat)
	}

	s.fillProcessMetrics(resp)

	return connect.NewResponse(resp), nil
}

func (s *VaultServer) resolveVaultIDs(vaultFilter string) ([]uuid.UUID, error) {
	vaults := s.orch.ListVaults()
	if vaultFilter == "" {
		return vaults, nil
	}
	vaultID, connErr := parseUUID(vaultFilter)
	if connErr != nil {
		return nil, connErr
	}
	if !slices.Contains(vaults, vaultID) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("vault not found"))
	}
	return []uuid.UUID{vaultID}, nil
}

func (s *VaultServer) buildVaultStats(ctx context.Context, vaultID uuid.UUID, metas []chunk.ChunkMeta) *apiv1.VaultStats {
	stat := &apiv1.VaultStats{
		Id:         vaultID.String(),
		ChunkCount: int64(len(metas)),
		Enabled:    s.orch.IsVaultEnabled(vaultID),
	}
	if cfg, err := s.getFullVaultConfig(ctx, vaultID); err == nil {
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
		s.accumulateChunkBytes(stat, vaultID, meta)
		updateTimeBounds(&stat.OldestRecord, meta.StartTS, (*timestamppb.Timestamp).AsTime, func(a, b time.Time) bool { return a.Before(b) })
		updateTimeBounds(&stat.NewestRecord, meta.EndTS, (*timestamppb.Timestamp).AsTime, func(a, b time.Time) bool { return a.After(b) })
	}
	return stat
}

func (s *VaultServer) accumulateChunkBytes(stat *apiv1.VaultStats, vaultID uuid.UUID, meta chunk.ChunkMeta) {
	if meta.DiskBytes > 0 {
		stat.DataBytes += meta.DiskBytes
		return
	}
	stat.DataBytes += meta.Bytes
	if sizes, err := s.orch.IndexSizes(vaultID, meta.ID); err == nil {
		for _, size := range sizes {
			stat.IndexBytes += size
		}
	}
}

func (s *VaultServer) accumulateGlobalStats(resp *apiv1.GetStatsResponse, stat *apiv1.VaultStats, metas []chunk.ChunkMeta) {
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

func (s *VaultServer) fillProcessMetrics(resp *apiv1.GetStatsResponse) {
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

// appendRemoteVaults adds vaults from the config store that aren't registered locally.
func (s *VaultServer) appendRemoteVaults(ctx context.Context, localSet map[uuid.UUID]struct{}, resp *apiv1.ListVaultsResponse) {
	if s.cfgStore == nil {
		return
	}
	allCfg, err := s.cfgStore.ListVaults(ctx)
	if err != nil {
		return
	}
	for _, vc := range allCfg {
		if _, local := localSet[vc.ID]; local {
			continue
		}
		info := &apiv1.VaultInfo{
			Id:      vc.ID.String(),
			Name:    vc.Name,
			Type:    vc.Type,
			NodeId:  vc.NodeID,
			Remote:  true,
			Enabled: vc.Enabled,
		}
		if s.peerStats != nil {
			if vs := s.peerStats.FindVaultStats(vc.ID.String()); vs != nil {
				info.RecordCount = vs.RecordCount
				info.ChunkCount = vs.ChunkCount
			}
		}
		resp.Vaults = append(resp.Vaults, info)
	}
}

func (s *VaultServer) getVaultInfo(ctx context.Context, id uuid.UUID) (*apiv1.VaultInfo, error) {
	metas, err := s.orch.ListChunkMetas(id)
	if err != nil {
		return nil, err
	}

	var recordCount int64
	for _, meta := range metas {
		recordCount += meta.RecordCount
	}

	// Get vault config from config vault (has name, type, params).
	cfg, _ := s.getFullVaultConfig(ctx, id)

	info := &apiv1.VaultInfo{
		Id:          id.String(),
		Name:        cfg.Name,
		Type:        cfg.Type,
		ChunkCount:  int64(len(metas)),
		RecordCount: recordCount,
		Enabled:     s.orch.IsVaultEnabled(id),
		NodeId:      cfg.NodeID,
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
