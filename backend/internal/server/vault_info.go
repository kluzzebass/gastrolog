package server

import (
	"context"
	"errors"
	"gastrolog/internal/glid"
	"slices"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/index/analyzer"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/sysmetrics"
	"gastrolog/internal/system"
)

// vaultName returns the human-readable name for a vault, falling back to the ID.
func (s *VaultServer) vaultName(ctx context.Context, id glid.GLID) string {
	cfg, err := s.getFullVaultConfig(ctx, id)
	if err == nil && cfg.Name != "" {
		return cfg.Name
	}
	return id.String()
}

// ListVaults returns all registered vaults, including remote vaults from
// other cluster nodes. The config store is the source of truth for vault
// identity; runtime stats come from the local orchestrator or peer broadcasts.
func (s *VaultServer) ListVaults(
	ctx context.Context,
	req *connect.Request[apiv1.ListVaultsRequest],
) (*connect.Response[apiv1.ListVaultsResponse], error) {
	vaults := s.allVaultInfos(ctx)
	return connect.NewResponse(&apiv1.ListVaultsResponse{Vaults: vaults}), nil
}

// GetVault returns details for a specific vault.
func (s *VaultServer) GetVault(
	ctx context.Context,
	req *connect.Request[apiv1.GetVaultRequest],
) (*connect.Response[apiv1.GetVaultResponse], error) {
	id, connErr := parseProtoID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	info := s.buildVaultInfo(ctx, id)
	if info == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("vault not found"))
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

	// Separate local from remote so each gets the right stats source.
	localIDs := s.orch.ListVaults()
	localSet := make(map[glid.GLID]struct{}, len(localIDs))
	for _, id := range localIDs {
		localSet[id] = struct{}{}
	}

	resp := &apiv1.GetStatsResponse{}

	for _, vaultID := range vaults {
		if _, local := localSet[vaultID]; !local {
			continue // handled below via peer broadcasts
		}
		tieredMetas, err := s.orch.ListAllChunkMetas(vaultID)
		if err != nil {
			continue
		}
		metas := make([]chunk.ChunkMeta, len(tieredMetas))
		for i, tm := range tieredMetas {
			metas[i] = tm.ChunkMeta
		}
		resp.TotalVaults++
		vaultStat := s.buildVaultStats(ctx, vaultID, metas)
		s.accumulateGlobalStats(resp, vaultStat, metas)
		resp.VaultStats = append(resp.VaultStats, vaultStat)
	}

	// Include remote vaults from peer broadcasts.
	// When no vault filter was provided, pass nil so all remote vaults are included.
	var remoteFilter []glid.GLID
	if req.Msg.Vault != "" {
		remoteFilter = vaults
	}
	s.accumulateRemoteVaultStats(ctx, localIDs, resp, remoteFilter)

	s.fillProcessMetrics(resp)

	return connect.NewResponse(resp), nil
}

func (s *VaultServer) resolveVaultIDs(vaultFilter string) ([]glid.GLID, error) {
	localVaults := s.orch.ListVaults()
	if vaultFilter == "" {
		return localVaults, nil
	}
	vaultID, connErr := parseUUID(vaultFilter)
	if connErr != nil {
		return nil, connErr
	}
	// Local vault — fast path.
	if slices.Contains(localVaults, vaultID) {
		return []glid.GLID{vaultID}, nil
	}
	// Check config store for remote vaults.
	if s.cfgStore != nil {
		if cfg, err := s.cfgStore.GetVault(context.Background(), vaultID); err == nil && cfg != nil {
			return []glid.GLID{vaultID}, nil
		}
	}
	return nil, connect.NewError(connect.CodeNotFound, errors.New("vault not found"))
}

func (s *VaultServer) buildVaultStats(ctx context.Context, vaultID glid.GLID, metas []chunk.ChunkMeta) *apiv1.VaultStats {
	stat := &apiv1.VaultStats{
		Id:         vaultID.ToProto(),
		ChunkCount: int64(len(metas)),
		Enabled:    s.orch.IsVaultEnabled(vaultID),
	}
	if cfg, err := s.getFullVaultConfig(ctx, vaultID); err == nil {
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
		updateTimeBounds(&stat.OldestRecord, meta.WriteStart, (*timestamppb.Timestamp).AsTime, func(a, b time.Time) bool { return a.Before(b) })
		updateTimeBounds(&stat.NewestRecord, meta.WriteEnd, (*timestamppb.Timestamp).AsTime, func(a, b time.Time) bool { return a.After(b) })
	}
	return stat
}

func (s *VaultServer) accumulateChunkBytes(stat *apiv1.VaultStats, vaultID glid.GLID, meta chunk.ChunkMeta) {
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
		updateTimeBounds(&resp.OldestRecord, meta.WriteStart, (*timestamppb.Timestamp).AsTime, func(a, b time.Time) bool { return a.Before(b) })
		updateTimeBounds(&resp.NewestRecord, meta.WriteEnd, (*timestamppb.Timestamp).AsTime, func(a, b time.Time) bool { return a.After(b) })
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

// accumulateRemoteVaultStats adds stats from remote vaults (via peer broadcasts)
// to the GetStats response. localVaults are skipped (already counted).
// If filter is non-empty, only the specified vaults are included.
func (s *VaultServer) accumulateRemoteVaultStats(ctx context.Context, localVaults []glid.GLID, resp *apiv1.GetStatsResponse, filter []glid.GLID) {
	if s.cfgStore == nil || s.peerStats == nil {
		return
	}
	allCfg, err := s.cfgStore.ListVaults(ctx)
	if err != nil {
		return
	}

	localSet := make(map[glid.GLID]struct{}, len(localVaults))
	for _, id := range localVaults {
		localSet[id] = struct{}{}
	}

	// If a filter was provided, only include those specific remote vaults.
	var filterSet map[glid.GLID]struct{}
	if len(filter) > 0 {
		filterSet = make(map[glid.GLID]struct{}, len(filter))
		for _, id := range filter {
			filterSet[id] = struct{}{}
		}
	}

	for _, vc := range allCfg {
		if _, local := localSet[vc.ID]; local {
			continue
		}
		if filterSet != nil {
			if _, wanted := filterSet[vc.ID]; !wanted {
				continue
			}
		}
		vs := s.peerStats.FindVaultStats(vc.ID.String())
		if vs == nil {
			continue
		}
		resp.TotalVaults++
		resp.TotalChunks += vs.ChunkCount
		resp.TotalRecords += vs.RecordCount
		resp.TotalBytes += vs.DataBytes + vs.IndexBytes
		resp.SealedChunks += vs.SealedChunks
		resp.VaultStats = append(resp.VaultStats, vs)
		if vs.OldestRecord != nil {
			updateTimeBounds(&resp.OldestRecord, vs.OldestRecord.AsTime(), (*timestamppb.Timestamp).AsTime, func(a, b time.Time) bool { return a.Before(b) })
		}
		if vs.NewestRecord != nil {
			updateTimeBounds(&resp.NewestRecord, vs.NewestRecord.AsTime(), (*timestamppb.Timestamp).AsTime, func(a, b time.Time) bool { return a.After(b) })
		}
	}
}

// allVaultInfos returns VaultInfo for every vault known to the config store,
// enriched with runtime stats from the local orchestrator or peer broadcasts.
// Vaults registered locally but missing from the config store (e.g. single-node
// mode with no config store) are included as a fallback.
func (s *VaultServer) allVaultInfos(ctx context.Context) []*apiv1.VaultInfo {
	localIDs := s.orch.ListVaults()
	localSet := make(map[glid.GLID]struct{}, len(localIDs))
	for _, id := range localIDs {
		localSet[id] = struct{}{}
	}

	// Config store is the source of truth for vault identity.
	if s.cfgStore != nil {
		allCfg, err := s.cfgStore.ListVaults(ctx)
		if err == nil {
			infos := make([]*apiv1.VaultInfo, 0, len(allCfg))
			seen := make(map[glid.GLID]struct{}, len(allCfg))
			for _, vc := range allCfg {
				seen[vc.ID] = struct{}{}
				infos = append(infos, s.vaultInfoFromConfig(vc, localSet))
			}
			// Include local vaults not yet in the config store (race during creation).
			for _, id := range localIDs {
				if _, ok := seen[id]; !ok {
					infos = append(infos, s.vaultInfoFromLocal(ctx, id))
				}
			}
			return infos
		}
	}

	// No config store — fall back to local orchestrator only.
	infos := make([]*apiv1.VaultInfo, 0, len(localIDs))
	for _, id := range localIDs {
		infos = append(infos, s.vaultInfoFromLocal(ctx, id))
	}
	return infos
}

// buildVaultInfo returns VaultInfo for a single vault, or nil if not found.
func (s *VaultServer) buildVaultInfo(ctx context.Context, id glid.GLID) *apiv1.VaultInfo {
	localIDs := s.orch.ListVaults()
	localSet := make(map[glid.GLID]struct{}, len(localIDs))
	for _, lid := range localIDs {
		localSet[lid] = struct{}{}
	}

	// Config store first.
	if s.cfgStore != nil {
		cfg, err := s.cfgStore.GetVault(ctx, id)
		if err == nil && cfg != nil {
			return s.vaultInfoFromConfig(*cfg, localSet)
		}
	}

	// Fall back to local orchestrator if no config store.
	if _, local := localSet[id]; local {
		return s.vaultInfoFromLocal(ctx, id)
	}

	return nil
}

// vaultInfoFromConfig builds a VaultInfo from a config store entry, enriching
// with runtime stats from the local orchestrator (if local) or peer broadcasts
// (if remote).
func (s *VaultServer) vaultInfoFromConfig(cfg system.VaultConfig, localSet map[glid.GLID]struct{}) *apiv1.VaultInfo {
	info := &apiv1.VaultInfo{
		Id:      cfg.ID.ToProto(),
		Name:    cfg.Name,
		Enabled: cfg.Enabled,
	}

	// Enrich with runtime stats. Local tiers are authoritative; peer
	// broadcast stats are only used for purely remote vaults where we
	// have no local data. Mixing the two double-counts shared tiers.
	_, registered := localSet[cfg.ID]
	if registered {
		info.Enabled = s.orch.IsVaultEnabled(cfg.ID)
		s.enrichLocalVaultInfo(info, cfg.ID)
	} else {
		info.Remote = true
		s.enrichRemoteVaultInfo(info, cfg.ID)
	}

	return info
}

func (s *VaultServer) enrichLocalVaultInfo(info *apiv1.VaultInfo, id glid.GLID) {
	metas, err := s.orch.ListAllChunkMetas(id)
	if err != nil {
		return
	}
	info.ChunkCount = int64(len(metas))
	for _, m := range metas {
		info.RecordCount += m.RecordCount
	}
}

func (s *VaultServer) enrichRemoteVaultInfo(info *apiv1.VaultInfo, id glid.GLID) {
	if s.peerStats == nil {
		return
	}
	vs := s.peerStats.FindVaultStats(id.String())
	if vs == nil {
		return
	}
	info.RecordCount += vs.RecordCount
	info.ChunkCount += vs.ChunkCount
}

// vaultInfoFromLocal builds a VaultInfo purely from the local orchestrator.
// Used as fallback when the config store is unavailable or missing the entry.
func (s *VaultServer) vaultInfoFromLocal(ctx context.Context, id glid.GLID) *apiv1.VaultInfo {
	info := &apiv1.VaultInfo{
		Id:      id.ToProto(),
		Enabled: s.orch.IsVaultEnabled(id),
	}

	// Try to get name from config store even in fallback path.
	if cfg, err := s.getFullVaultConfig(ctx, id); err == nil {
		info.Name = cfg.Name
	}

	if metas, err := s.orch.ListChunkMetas(id); err == nil {
		info.ChunkCount = int64(len(metas))
		for _, m := range metas {
			info.RecordCount += m.RecordCount
		}
	}

	return info
}

func ChunkMetaToProto(meta chunk.ChunkMeta) *apiv1.ChunkMeta {
	pb := &apiv1.ChunkMeta{
		Id:           glid.GLID(meta.ID).ToProto(),
		WriteStart:   timestamppb.New(meta.WriteStart),
		WriteEnd:     timestamppb.New(meta.WriteEnd),
		Sealed:       meta.Sealed,
		RecordCount:  meta.RecordCount,
		Bytes:        meta.Bytes,
		Compressed:   meta.Sealed, // sealed chunks are GLCB which is zstd-compressed (gastrolog-24m1t step 7f)
		DiskBytes:    meta.DiskBytes,
		CloudBacked:  meta.CloudBacked,
		Archived:     meta.Archived,
		StorageClass: meta.StorageClass,
		NumFrames:    meta.NumFrames,
	}
	if !meta.IngestStart.IsZero() {
		pb.IngestStart = timestamppb.New(meta.IngestStart)
	}
	if !meta.IngestEnd.IsZero() {
		pb.IngestEnd = timestamppb.New(meta.IngestEnd)
	}
	return pb
}

// TieredChunkMetaToProto converts a TieredChunkMeta to a proto ChunkMeta with tier info.
func TieredChunkMetaToProto(meta orchestrator.TieredChunkMeta) *apiv1.ChunkMeta {
	pb := ChunkMetaToProto(meta.ChunkMeta)
	pb.TierId = meta.TierID.ToProto()
	pb.TierType = meta.TierType
	return pb
}

// ChunkAnalysisToProto converts an analyzer.ChunkAnalysis to a proto ChunkAnalysis.
func ChunkAnalysisToProto(ca analyzer.ChunkAnalysis) *apiv1.ChunkAnalysis {
	protoAnalysis := &apiv1.ChunkAnalysis{
		ChunkId:     glid.GLID(ca.ChunkID).ToProto(),
		Sealed:      ca.Sealed,
		RecordCount: ca.ChunkRecords,
		Indexes:     make([]*apiv1.IndexAnalysis, 0),
	}
	if ca.TokenStats != nil {
		protoAnalysis.Indexes = append(protoAnalysis.Indexes, &apiv1.IndexAnalysis{
			Name:       "token",
			Complete:   true,
			Status:     tokenStatusString(ca.TokenStats),
			EntryCount: ca.TokenStats.UniqueTokens,
		})
	}
	if ca.AttrKVStats != nil {
		protoAnalysis.Indexes = append(protoAnalysis.Indexes, &apiv1.IndexAnalysis{
			Name:       "attr",
			Complete:   true,
			Status:     "ok",
			EntryCount: ca.AttrKVStats.UniqueKeys + ca.AttrKVStats.UniqueValues + ca.AttrKVStats.UniqueKeyValuePairs,
		})
	}
	if ca.KVStats != nil {
		protoAnalysis.Indexes = append(protoAnalysis.Indexes, &apiv1.IndexAnalysis{
			Name:       "kv",
			Complete:   !ca.KVStats.BudgetExhausted,
			Status:     kvStatusString(ca.KVStats),
			EntryCount: ca.KVStats.KeysIndexed + ca.KVStats.ValuesIndexed + ca.KVStats.PairsIndexed,
		})
	}
	return protoAnalysis
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
