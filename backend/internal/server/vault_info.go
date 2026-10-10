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

// GetStats returns cluster-wide figures for every vault, or for the one named
// in the request. Each vault is counted once from its manifest, so a vault
// homed on N nodes reports its chunk set once rather than N copies, and every
// vault-ctl voter answers the same.
func (s *VaultServer) GetStats(
	ctx context.Context,
	req *connect.Request[apiv1.GetStatsRequest],
) (*connect.Response[apiv1.GetStatsResponse], error) {
	vaults, err := s.statsVaultIDs(ctx, req.Msg.Vault)
	if err != nil {
		return nil, err
	}

	resp := &apiv1.GetStatsResponse{}
	for _, vaultID := range vaults {
		stat := s.vaultStats(ctx, vaultID)
		if stat == nil {
			continue
		}
		resp.TotalVaults++
		resp.TotalChunks += stat.ChunkCount
		resp.TotalRecords += stat.RecordCount
		resp.TotalBytes += stat.DataBytes
		resp.SealedChunks += stat.SealedChunks
		if stat.OldestRecord != nil {
			updateTimeBounds(&resp.OldestRecord, stat.OldestRecord.AsTime(), (*timestamppb.Timestamp).AsTime, func(a, b time.Time) bool { return a.Before(b) })
		}
		if stat.NewestRecord != nil {
			updateTimeBounds(&resp.NewestRecord, stat.NewestRecord.AsTime(), (*timestamppb.Timestamp).AsTime, func(a, b time.Time) bool { return a.After(b) })
		}
		resp.VaultStats = append(resp.VaultStats, stat)
	}

	s.fillProcessMetrics(resp)

	return connect.NewResponse(resp), nil
}

// statsVaultIDs resolves the vaults GetStats reports on: the named vault, or
// every configured vault plus any registered here ahead of its config entry.
func (s *VaultServer) statsVaultIDs(ctx context.Context, vaultFilter string) ([]glid.GLID, error) {
	if vaultFilter != "" {
		return s.namedStatsVault(ctx, vaultFilter)
	}
	localVaults := s.orch.ListVaults()
	if s.cfgStore == nil {
		return localVaults, nil
	}
	allCfg, err := s.cfgStore.ListVaults(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	ids := make([]glid.GLID, 0, len(allCfg))
	seen := make(map[glid.GLID]struct{}, len(allCfg))
	for _, vc := range allCfg {
		seen[vc.ID] = struct{}{}
		ids = append(ids, vc.ID)
	}
	for _, id := range localVaults {
		if _, ok := seen[id]; !ok {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func (s *VaultServer) namedStatsVault(ctx context.Context, vaultFilter string) ([]glid.GLID, error) {
	vaultID, connErr := parseUUID(vaultFilter)
	if connErr != nil {
		return nil, connErr
	}
	if slices.Contains(s.orch.ListVaults(), vaultID) {
		return []glid.GLID{vaultID}, nil
	}
	if s.cfgStore != nil {
		if cfg, err := s.cfgStore.GetVault(ctx, vaultID); err == nil && cfg != nil {
			return []glid.GLID{vaultID}, nil
		}
	}
	return nil, connect.NewError(connect.CodeNotFound, errors.New("vault not found"))
}

// vaultStats returns the vault's figures from its manifest. A node that can
// read no manifest for the vault falls back to a peer's broadcast, which
// describes that peer's own copy. Nil when neither source has the vault.
func (s *VaultServer) vaultStats(ctx context.Context, vaultID glid.GLID) *apiv1.VaultStats {
	metas, err := s.orch.ListClusterChunkMetasIncludingOpen(vaultID)
	if err == nil {
		return s.buildVaultStats(ctx, vaultID, metas)
	}
	if !errors.Is(err, orchestrator.ErrNoVaultManifest) && !errors.Is(err, orchestrator.ErrVaultNotFound) {
		return nil
	}
	if s.peerStats == nil {
		return nil
	}
	return s.peerStats.FindVaultStats(vaultID.String())
}

// buildVaultStats sums a vault's chunk set. DataBytes is the logical size of
// its records, one copy of each chunk: replica copies, indexes, and local disk
// use are per-node facts that a cluster-wide figure does not carry.
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
		stat.DataBytes += meta.Bytes
		updateTimeBounds(&stat.OldestRecord, meta.WriteStart, (*timestamppb.Timestamp).AsTime, func(a, b time.Time) bool { return a.Before(b) })
		updateTimeBounds(&stat.NewestRecord, meta.WriteEnd, (*timestamppb.Timestamp).AsTime, func(a, b time.Time) bool { return a.After(b) })
	}
	return stat
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
				infos = append(infos, s.vaultInfoFromConfig(ctx, vc, localSet))
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
			return s.vaultInfoFromConfig(ctx, *cfg, localSet)
		}
	}

	// Fall back to local orchestrator if no config store.
	if _, local := localSet[id]; local {
		return s.vaultInfoFromLocal(ctx, id)
	}

	return nil
}

// vaultInfoFromConfig builds a VaultInfo from a config store entry, with the
// vault's chunk and record counts from the same source as GetStats.
func (s *VaultServer) vaultInfoFromConfig(ctx context.Context, cfg system.VaultConfig, localSet map[glid.GLID]struct{}) *apiv1.VaultInfo {
	info := &apiv1.VaultInfo{
		Id:      cfg.ID.ToProto(),
		Name:    cfg.Name,
		Enabled: cfg.Enabled,
	}

	if _, registered := localSet[cfg.ID]; registered {
		info.Enabled = s.orch.IsVaultEnabled(cfg.ID)
	} else {
		info.Remote = true
	}
	if stat := s.vaultStats(ctx, cfg.ID); stat != nil {
		info.ChunkCount = stat.ChunkCount
		info.RecordCount = stat.RecordCount
	}
	s.fillAdmissionRefused(info, cfg.ID)

	return info
}

// fillAdmissionRefused populates VaultInfo.AdmissionRefused from the
// orchestrator's admission-causes collector — the responding node's own view
// (local disk guard + its live-peer broadcasts), the same cluster-aware
// inputs vaultAdmissionGate itself consults. This is deliberately NOT gated
// on whether the vault is locally placed: storage protect and max-size
// causes already fold in peer broadcasts, and the backlog budget is
// FSM-replicated, so the collector reports the correct cluster-wide verdict
// for local and remote vaults alike. Each entry carries the backend's own
// detail text for that cause — which storage and its free-vs-floor numbers,
// or the bound kind and value — never a client-side reconstruction.
func (s *VaultServer) fillAdmissionRefused(info *apiv1.VaultInfo, id glid.GLID) {
	causes := s.orch.VaultAdmissionCauseDetails(id)
	if len(causes) == 0 {
		return
	}
	info.AdmissionRefused = make([]*apiv1.VaultAdmissionRefusal, len(causes))
	for i, c := range causes {
		info.AdmissionRefused[i] = &apiv1.VaultAdmissionRefusal{
			Cause:  admissionCauseToProto(c.Cause),
			Detail: c.Detail,
		}
	}
}

// admissionCauseToProto maps the orchestrator's proto-free cause enum to the
// wire enum. UNSPECIFIED for anything unrecognized — defense in depth; every
// value orchestrator emits today is handled.
func admissionCauseToProto(c orchestrator.VaultAdmissionCause) apiv1.VaultAdmissionCause {
	switch c {
	case orchestrator.VaultAdmissionCauseStorageDiskProtect:
		return apiv1.VaultAdmissionCause_VAULT_ADMISSION_CAUSE_STORAGE_DISK_PROTECT
	case orchestrator.VaultAdmissionCauseMaxSizeBound:
		return apiv1.VaultAdmissionCause_VAULT_ADMISSION_CAUSE_MAX_SIZE_BOUND
	case orchestrator.VaultAdmissionCauseBacklogBudget:
		return apiv1.VaultAdmissionCause_VAULT_ADMISSION_CAUSE_BACKLOG_BUDGET
	case orchestrator.VaultAdmissionCauseAgeBound:
		return apiv1.VaultAdmissionCause_VAULT_ADMISSION_CAUSE_AGE_BOUND
	case orchestrator.VaultAdmissionCauseChunkCountBound:
		return apiv1.VaultAdmissionCause_VAULT_ADMISSION_CAUSE_CHUNK_COUNT_BOUND
	default:
		return apiv1.VaultAdmissionCause_VAULT_ADMISSION_CAUSE_UNSPECIFIED
	}
}

// vaultInfoFromLocal builds a VaultInfo purely from the local orchestrator.
// Used as fallback when the config store is unavailable or missing the entry.
func (s *VaultServer) vaultInfoFromLocal(ctx context.Context, id glid.GLID) *apiv1.VaultInfo {
	info := &apiv1.VaultInfo{
		Id:      id.ToProto(),
		Enabled: s.orch.IsVaultEnabled(id),
	}

	if stat := s.vaultStats(ctx, id); stat != nil {
		info.Name = stat.Name
		info.ChunkCount = stat.ChunkCount
		info.RecordCount = stat.RecordCount
	} else if cfg, err := s.getFullVaultConfig(ctx, id); err == nil {
		info.Name = cfg.Name
	}
	s.fillAdmissionRefused(info, id)

	return info
}

func ChunkMetaToProto(meta chunk.ChunkMeta) *apiv1.ChunkMeta {
	pb := &apiv1.ChunkMeta{
		Id:                glid.GLID(meta.ID).ToProto(),
		Sealed:            meta.Sealed,
		RecordCount:       meta.RecordCount,
		Bytes:             meta.Bytes,
		DiskBytes:         meta.DiskBytes,
		CloudBytes:        meta.CloudBytes,
		CloudBacked:       meta.CloudBacked,
		Archived:          meta.Archived,
		CloudStorageClass: meta.CloudStorageClass,
		State:             chunkStateToProto(meta.State, meta.Sealed),
	}
	if saneRecordTime(meta.WriteStart) {
		pb.WriteStart = timestamppb.New(meta.WriteStart)
	}
	if saneRecordTime(meta.WriteEnd) {
		pb.WriteEnd = timestamppb.New(meta.WriteEnd)
	}
	if saneRecordTime(meta.IngestStart) {
		pb.IngestStart = timestamppb.New(meta.IngestStart)
	}
	if saneRecordTime(meta.IngestEnd) {
		pb.IngestEnd = timestamppb.New(meta.IngestEnd)
	}
	return pb
}

func saneRecordTime(t time.Time) bool {
	return !t.IsZero() && t.Year() >= 2000
}

func chunkStateToProto(state chunk.ChunkState, sealed bool) apiv1.ChunkState {
	switch state {
	case chunk.ChunkStateActive:
		return apiv1.ChunkState_CHUNK_STATE_ACTIVE
	case chunk.ChunkStateSealing:
		return apiv1.ChunkState_CHUNK_STATE_SEALING
	case chunk.ChunkStateSealed:
		return apiv1.ChunkState_CHUNK_STATE_SEALED
	case chunk.ChunkStateUnknown:
		fallthrough
	default:
		// A local meta with no FSM-overlaid state (memory-mode vaults,
		// a head not yet in the FSM snapshot) has a two-state
		// lifecycle: there is no Sealing intermediate without an FSM
		// driving the announce protocol (same derivation as
		// chunkMetaToManifestEntry). Resolve it HERE, where the
		// manager semantics are known — consumers render UNSPECIFIED
		// as "unknown" and never guess, so leaving it on the wire
		// would badge every memory-mode active chunk as unknown.
		if sealed {
			return apiv1.ChunkState_CHUNK_STATE_SEALED
		}
		return apiv1.ChunkState_CHUNK_STATE_ACTIVE
	}
}

// VaultChunkMetaToProto converts a VaultChunkMeta to a proto ChunkMeta.
func VaultChunkMetaToProto(meta orchestrator.VaultChunkMeta) *apiv1.ChunkMeta {
	pb := ChunkMetaToProto(meta.ChunkMeta)
	pb.VaultId = meta.VaultID.ToProto()
	pb.VaultType = meta.VaultType
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
