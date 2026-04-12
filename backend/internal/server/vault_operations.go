package server

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/config"
	"gastrolog/internal/convert"
	"gastrolog/internal/orchestrator"
)

// makeCleanupFunc returns a callback that removes the source vault from the
// config vault and cleans up its vault directory. Safe to call from async jobs.
func (s *VaultServer) makeCleanupFunc(srcID uuid.UUID, srcFileDir string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		if s.cfgStore != nil {
			if err := s.cfgStore.DeleteVault(ctx, srcID, true); err != nil {
				s.logger.Warn("cleanup: delete vault config", "vault", srcID, "error", err)
			}
		}
		if srcFileDir != "" {
			if err := os.RemoveAll(srcFileDir); err != nil {
				s.logger.Warn("cleanup: remove source directory", "dir", srcFileDir, "error", err)
			}
		}
		return nil
	}
}

// SealVault seals the active chunk of a vault.
// Routing: RouteTargeted — the interceptor forwards to the vault-owning node.
func (s *VaultServer) SealVault(
	ctx context.Context,
	req *connect.Request[apiv1.SealVaultRequest],
) (*connect.Response[apiv1.SealVaultResponse], error) {
	if req.Msg.Vault == "" {
		return nil, errRequired("vault")
	}
	vaultID, connErr := parseUUID(req.Msg.Vault)
	if connErr != nil {
		return nil, connErr
	}

	if !s.orch.VaultExists(vaultID) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("vault not found"))
	}

	// Resolve optional tier filter.
	tierID := uuid.Nil
	if req.Msg.Tier != "" {
		tid, connErr := parseUUID(req.Msg.Tier)
		if connErr != nil {
			return nil, connErr
		}
		tierID = tid
	}

	sealed, err := s.orch.SealActive(vaultID, tierID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("seal active chunk: %w", err))
	}

	return connect.NewResponse(&apiv1.SealVaultResponse{SealedCount: int32(sealed)}), nil //nolint:gosec // G115: tier count is always small
}

// ReindexVault rebuilds all indexes for sealed chunks in a vault.
// Routing: RouteTargeted — the interceptor forwards to the vault-owning node.
func (s *VaultServer) ReindexVault(
	ctx context.Context,
	req *connect.Request[apiv1.ReindexVaultRequest],
) (*connect.Response[apiv1.ReindexVaultResponse], error) {
	if req.Msg.Vault == "" {
		return nil, errRequired("vault")
	}
	vaultID, connErr := parseUUID(req.Msg.Vault)
	if connErr != nil {
		return nil, connErr
	}

	if !s.orch.VaultExists(vaultID) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("vault not found"))
	}

	jobName := "reindex:" + vaultID.String()
	jobID := s.orch.Scheduler().Submit(jobName, func(ctx context.Context, job *orchestrator.JobProgress) {
		metas, err := s.orch.ListChunkMetas(vaultID)
		if err != nil {
			job.Fail(s.now(), err.Error())
			return
		}

		var sealedCount int64
		for _, m := range metas {
			if m.Sealed {
				sealedCount++
			}
		}
		job.SetRunning(sealedCount)

		for _, meta := range metas {
			if !meta.Sealed {
				continue
			}
			if err := s.orch.DeleteIndexes(vaultID, meta.ID); err != nil {
				job.AddErrorDetail(fmt.Sprintf("delete indexes for chunk %s: %v", meta.ID, err))
				continue
			}
			if err := s.orch.BuildIndexes(ctx, vaultID, meta.ID); err != nil {
				job.AddErrorDetail(fmt.Sprintf("build indexes for chunk %s: %v", meta.ID, err))
				continue
			}
			job.IncrChunks()
		}
	})
	s.orch.Scheduler().Describe(jobName, fmt.Sprintf("Rebuild all indexes for '%s'", s.vaultName(ctx, vaultID)))

	return connect.NewResponse(&apiv1.ReindexVaultResponse{JobId: jobID}), nil
}

// MigrateVault moves a vault to a new name, type, and/or location.
// Three-phase: create destination, freeze source, async merge+delete.
func (s *VaultServer) MigrateVault(
	ctx context.Context,
	req *connect.Request[apiv1.MigrateVaultRequest],
) (*connect.Response[apiv1.MigrateVaultResponse], error) {
	if req.Msg.Source == "" {
		return nil, errRequired("source")
	}
	if req.Msg.Destination == "" {
		return nil, errRequired("destination")
	}

	srcID, connErr := parseUUID(req.Msg.Source)
	if connErr != nil {
		return nil, connErr
	}

	// Source must exist.
	if !s.orch.VaultExists(srcID) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("source vault not found"))
	}

	// Get source config for filter/policy and to resolve destination type.
	srcCfg, err := s.getFullVaultConfig(ctx, srcID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("read source config: %w", err))
	}

	// Phase 1: Clone source tiers to destination vault, then create vault.
	// Tiers must exist before AddVault so buildTierInstances can find them.
	dstCfg := config.VaultConfig{
		ID:      uuid.Must(uuid.NewV7()),
		Name:    req.Msg.Destination,
		Enabled: true,
	}

	if s.cfgStore != nil {
		srcTiers, _ := s.cfgStore.ListTiers(ctx)
		for _, t := range config.VaultTiers(srcTiers, srcID) {
			t.ID = uuid.Must(uuid.NewV7())
			t.VaultID = dstCfg.ID
			if err := s.cfgStore.PutTier(ctx, t); err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("clone tier: %w", err))
			}
		}
	}

	if err := s.createVault(ctx, dstCfg); err != nil {
		return nil, err
	}

	// Phase 2: Freeze source — disable ingestion and persist.
	if err := s.orch.DisableVault(srcID); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("disable source: %w", err))
	}
	if s.cfgStore != nil {
		srcCfg.Enabled = false
		if err := s.cfgStore.PutVault(ctx, srcCfg); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("persist disabled source: %w", err))
		}
	}

	// Seal source's active chunk so all data is in sealed chunks.
	if _, err := s.orch.SealActive(srcID, uuid.Nil); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("seal source: %w", err))
	}

	// Phase 3: Async merge + delete.
	dstID := dstCfg.ID
	srcName := s.vaultName(ctx, srcID)

	jobID := s.orch.MigrateVault(ctx, orchestrator.TransferParams{
		SrcID:       srcID,
		DstID:       dstID,
		Description: fmt.Sprintf("Migrate '%s' to '%s'", srcName, s.vaultName(ctx, dstID)),
		CleanupSrc:  s.makeCleanupFunc(srcID, ""),
	})

	return connect.NewResponse(&apiv1.MigrateVaultResponse{JobId: jobID}), nil
}

// MergeVaults moves all chunks from a source vault into a destination vault,
// then deletes the source. Both vaults must support chunk-level moves (ChunkMover).
func (s *VaultServer) MergeVaults(
	ctx context.Context,
	req *connect.Request[apiv1.MergeVaultsRequest],
) (*connect.Response[apiv1.MergeVaultsResponse], error) {
	if req.Msg.Source == "" {
		return nil, errRequired("source")
	}
	if req.Msg.Destination == "" {
		return nil, errRequired("destination")
	}
	if req.Msg.Source == req.Msg.Destination {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("source and destination must differ"))
	}

	srcID, connErr := parseUUID(req.Msg.Source)
	if connErr != nil {
		return nil, connErr
	}
	dstID, connErr := parseUUID(req.Msg.Destination)
	if connErr != nil {
		return nil, connErr
	}

	// Both vaults must exist.
	if !s.orch.VaultExists(srcID) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("source vault not found"))
	}
	if !s.orch.VaultExists(dstID) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("destination vault not found"))
	}

	// Auto-disable source to prevent new data flowing in during merge.
	if s.orch.IsVaultEnabled(srcID) {
		if err := s.disableAndPersistVault(ctx, srcID); err != nil {
			return nil, err
		}
	}

	jobID := s.orch.MergeVaults(ctx, orchestrator.TransferParams{
		SrcID:       srcID,
		DstID:       dstID,
		Description: fmt.Sprintf("Merge '%s' into '%s'", s.vaultName(ctx, srcID), s.vaultName(ctx, dstID)),
		CleanupSrc:  s.makeCleanupFunc(srcID, ""),
	})

	return connect.NewResponse(&apiv1.MergeVaultsResponse{JobId: jobID}), nil
}

// ExportVault streams all records from a vault.
func (s *VaultServer) ExportVault(
	ctx context.Context,
	req *connect.Request[apiv1.ExportVaultRequest],
	stream *connect.ServerStream[apiv1.ExportVaultResponse],
) error {
	if req.Msg.Vault == "" {
		return errRequired("vault")
	}
	vaultID, connErr := parseUUID(req.Msg.Vault)
	if connErr != nil {
		return connErr
	}

	metas, err := s.orch.ListChunkMetas(vaultID)
	if err != nil {
		return mapVaultError(err)
	}

	for _, meta := range metas {
		if err := s.exportChunk(vaultID, meta.ID, stream); err != nil {
			return err
		}
	}

	return stream.Send(&apiv1.ExportVaultResponse{HasMore: false})
}

func (s *VaultServer) exportChunk(vaultID uuid.UUID, chunkID chunk.ChunkID, stream *connect.ServerStream[apiv1.ExportVaultResponse]) error {
	cursor, err := s.orch.OpenCursor(vaultID, chunkID)
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("open chunk %s: %w", chunkID, err))
	}
	defer func() { _ = cursor.Close() }()

	const batchSize = 100
	batch := make([]*apiv1.ExportRecord, 0, batchSize)

	for {
		rec, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			return connect.NewError(connect.CodeInternal, fmt.Errorf("read chunk %s: %w", chunkID, err))
		}

		batch = append(batch, convert.RecordToExport(rec))

		if len(batch) >= batchSize {
			if err := stream.Send(&apiv1.ExportVaultResponse{Records: batch, HasMore: true}); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		return stream.Send(&apiv1.ExportVaultResponse{Records: batch, HasMore: true})
	}
	return nil
}

func (s *VaultServer) disableAndPersistVault(ctx context.Context, id uuid.UUID) error {
	if err := s.orch.DisableVault(id); err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("disable source: %w", err))
	}
	if s.cfgStore == nil {
		return nil
	}
	srcCfg, err := s.getFullVaultConfig(ctx, id)
	if err != nil {
		// Vault is already disabled in memory; best-effort config persistence.
		s.logger.Warn("get vault config for persist", "vault", id, "error", err)
		return nil
	}
	srcCfg.Enabled = false
	if err := s.cfgStore.PutVault(ctx, srcCfg); err != nil {
		s.logger.Warn("persist disabled source config", "vault", id, "error", err)
	}
	return nil
}

// ImportRecords appends a batch of records to a vault.
func (s *VaultServer) ImportRecords(
	ctx context.Context,
	req *connect.Request[apiv1.ImportRecordsRequest],
) (*connect.Response[apiv1.ImportRecordsResponse], error) {
	if req.Msg.Vault == "" {
		return nil, errRequired("vault")
	}
	vaultID, connErr := parseUUID(req.Msg.Vault)
	if connErr != nil {
		return nil, connErr
	}

	var imported int64
	for _, exportRec := range req.Msg.Records {
		rec := chunk.Record{
			Raw: exportRec.Raw,
		}
		if exportRec.SourceTs != nil {
			rec.SourceTS = exportRec.SourceTs.AsTime()
		}
		if exportRec.IngestTs != nil {
			rec.IngestTS = exportRec.IngestTs.AsTime()
		}
		if len(exportRec.Attrs) > 0 {
			rec.Attrs = make(chunk.Attributes, len(exportRec.Attrs))
			maps.Copy(rec.Attrs, exportRec.Attrs)
		}

		if _, _, err := s.orch.Append(vaultID, rec); err != nil {
			return nil, mapVaultError(err)
		}
		imported++
	}

	return connect.NewResponse(&apiv1.ImportRecordsResponse{
		RecordsImported: imported,
	}), nil
}

// getFullVaultConfig retrieves vault config from the config vault (with type/params),
// falling back to the orchestrator's limited config.
func (s *VaultServer) getFullVaultConfig(ctx context.Context, id uuid.UUID) (config.VaultConfig, error) {
	if s.cfgStore != nil {
		cfg, err := s.cfgStore.GetVault(ctx, id)
		if err == nil && cfg != nil {
			return *cfg, nil
		}
	}
	return s.orch.VaultConfig(id)
}

// createVault persists a vault config and adds it to the orchestrator.
func (s *VaultServer) createVault(ctx context.Context, cfg config.VaultConfig) *connect.Error {
	// Persist to config vault.
	if s.cfgStore != nil {
		if err := s.cfgStore.PutVault(ctx, cfg); err != nil {
			return connect.NewError(connect.CodeInternal, fmt.Errorf("persist config: %w", err))
		}
	}

	// Add to orchestrator.
	if err := s.orch.AddVault(ctx, cfg, s.factories); err != nil {
		// Rollback config entry on orchestrator failure.
		if s.cfgStore != nil {
			if delErr := s.cfgStore.DeleteVault(ctx, cfg.ID, false); delErr != nil {
				s.logger.Warn("rollback: delete vault config", "vault", cfg.ID, "error", delErr)
			}
		}
		return connect.NewError(connect.CodeInternal, fmt.Errorf("add vault: %w", err))
	}

	return nil
}

// ArchiveChunk transitions a cloud-backed sealed chunk to an offline storage class.
// Routing: RouteTargeted — forwarded to the vault-owning node.
func (s *VaultServer) ArchiveChunk(
	ctx context.Context,
	req *connect.Request[apiv1.ArchiveChunkRequest],
) (*connect.Response[apiv1.ArchiveChunkResponse], error) {
	vaultID, connErr := parseUUID(req.Msg.Vault)
	if connErr != nil {
		return nil, connErr
	}
	chunkID, err := chunk.ParseChunkID(req.Msg.ChunkId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid chunk_id: %w", err))
	}
	storageClass := req.Msg.StorageClass
	if storageClass == "" {
		// Resolve from the cloud service's first transition.
		if cs := s.lookupCloudServiceForChunk(ctx, vaultID, chunkID); cs != nil && len(cs.Transitions) > 0 {
			storageClass = cs.Transitions[0].StorageClass
		}
	}
	if storageClass == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("storage_class is required (no default transition configured)"))
	}

	if err := s.orch.ArchiveChunk(ctx, vaultID, chunkID, storageClass); err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.ArchiveChunkResponse{}), nil
}

// RestoreChunk initiates retrieval of an archived chunk from offline storage.
// Routing: RouteTargeted — forwarded to the vault-owning node.
func (s *VaultServer) RestoreChunk(
	ctx context.Context,
	req *connect.Request[apiv1.RestoreChunkRequest],
) (*connect.Response[apiv1.RestoreChunkResponse], error) {
	vaultID, connErr := parseUUID(req.Msg.Vault)
	if connErr != nil {
		return nil, connErr
	}
	chunkID, err := chunk.ParseChunkID(req.Msg.ChunkId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid chunk_id: %w", err))
	}

	// Use request values, falling back to cloud service defaults, then hardcoded defaults.
	tier, days := s.resolveRestoreDefaults(ctx, vaultID, chunkID, req.Msg.RestoreTier, int(req.Msg.RestoreDays))

	if err := s.orch.RestoreChunk(ctx, vaultID, chunkID, tier, days); err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.RestoreChunkResponse{}), nil
}

// resolveRestoreDefaults fills in restore tier and days from cloud service config.
func (s *VaultServer) resolveRestoreDefaults(ctx context.Context, vaultID uuid.UUID, chunkID chunk.ChunkID, reqTier string, reqDays int) (string, int) {
	tier, days := reqTier, reqDays
	if (tier == "" || days <= 0) && s.cfgStore != nil {
		cs := s.lookupCloudServiceForChunk(ctx, vaultID, chunkID)
		if cs != nil && tier == "" {
			tier = cs.RestoreTier
		}
		if cs != nil && days <= 0 {
			days = int(cs.RestoreDays)
		}
	}
	if tier == "" {
		tier = "Standard"
	}
	if days <= 0 {
		days = 7
	}
	return tier, days
}

// lookupCloudServiceForChunk finds the CloudService config for a chunk's tier.
func (s *VaultServer) lookupCloudServiceForChunk(ctx context.Context, vaultID uuid.UUID, _ chunk.ChunkID) *config.CloudService {
	cfg, err := s.cfgStore.Load(ctx)
	if err != nil || cfg == nil {
		return nil
	}
	// Find vault → tiers → cloud service.
	for i := range cfg.Tiers {
		t := &cfg.Tiers[i]
		if t.VaultID != vaultID || t.CloudServiceID == nil {
			continue
		}
		for j := range cfg.CloudServices {
			if cfg.CloudServices[j].ID == *t.CloudServiceID {
				return &cfg.CloudServices[j]
			}
		}
	}
	return nil
}
