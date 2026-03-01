package server

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/config"
	"gastrolog/internal/orchestrator"
)

// makeCleanupFunc returns a callback that removes the source vault from the
// config vault and cleans up its vault directory. Safe to call from async jobs.
func (s *VaultServer) makeCleanupFunc(srcID uuid.UUID, srcFileDir string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		if s.cfgStore != nil {
			if err := s.cfgStore.DeleteVault(ctx, srcID); err != nil {
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
func (s *VaultServer) SealVault(
	ctx context.Context,
	req *connect.Request[apiv1.SealVaultRequest],
) (*connect.Response[apiv1.SealVaultResponse], error) {
	if req.Msg.Vault == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("vault required"))
	}
	vaultID, connErr := parseUUID(req.Msg.Vault)
	if connErr != nil {
		return nil, connErr
	}

	if !s.orch.VaultExists(vaultID) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("vault not found"))
	}

	if err := s.orch.SealActive(vaultID); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("seal active chunk: %w", err))
	}

	return connect.NewResponse(&apiv1.SealVaultResponse{}), nil
}

// ReindexVault rebuilds all indexes for sealed chunks in a vault.
// The work is submitted as an async job; the response contains the job ID.
func (s *VaultServer) ReindexVault(
	ctx context.Context,
	req *connect.Request[apiv1.ReindexVaultRequest],
) (*connect.Response[apiv1.ReindexVaultResponse], error) {
	if req.Msg.Vault == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("vault required"))
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
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("source required"))
	}
	if req.Msg.Destination == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("destination required"))
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

	// Resolve destination type: explicit or same as source.
	dstType := req.Msg.DestinationType
	if dstType == "" {
		dstType = srcCfg.Type
	}

	dstParams := req.Msg.DestinationParams
	if dstParams == nil {
		dstParams = make(map[string]string)
	}
	// File vaults require an explicit dir — no auto-derive.
	if dstType == "file" && dstParams[chunkfile.ParamDir] == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("destination_params.dir required for file vaults"))
	}

	// Phase 1: Create destination vault with inherited policy.
	dstCfg := config.VaultConfig{
		ID:        uuid.Must(uuid.NewV7()),
		Name:      req.Msg.Destination,
		Type:      dstType,
		Policy:    srcCfg.Policy,
		RetentionRules: srcCfg.RetentionRules,
		Enabled:   true,
		Params:    dstParams,
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
	if err := s.orch.SealActive(srcID); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("seal source: %w", err))
	}

	// Phase 3: Async merge + delete.
	dstID := dstCfg.ID
	srcName := s.vaultName(ctx, srcID)

	// Capture file dir before the job runs (source config will be deleted).
	var srcFileDir string
	if srcCfg.Type == "file" {
		srcFileDir = srcCfg.Params[chunkfile.ParamDir]
	}

	jobID := s.orch.MigrateVault(ctx, orchestrator.TransferParams{
		SrcID:       srcID,
		DstID:       dstID,
		Description: fmt.Sprintf("Migrate '%s' to '%s'", srcName, s.vaultName(ctx, dstID)),
		CleanupSrc:  s.makeCleanupFunc(srcID, srcFileDir),
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
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("source required"))
	}
	if req.Msg.Destination == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("destination required"))
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

	// Capture source file dir before job runs (source config will be deleted).
	var srcFileDir string
	if srcCfg, err := s.getFullVaultConfig(ctx, srcID); err == nil && srcCfg.Type == "file" {
		srcFileDir = srcCfg.Params[chunkfile.ParamDir]
	}

	jobID := s.orch.MergeVaults(ctx, orchestrator.TransferParams{
		SrcID:       srcID,
		DstID:       dstID,
		Description: fmt.Sprintf("Merge '%s' into '%s'", s.vaultName(ctx, srcID), s.vaultName(ctx, dstID)),
		CleanupSrc:  s.makeCleanupFunc(srcID, srcFileDir),
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
		return connect.NewError(connect.CodeInvalidArgument, errors.New("vault required"))
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

		batch = append(batch, recordToExportProto(rec))

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

func recordToExportProto(rec chunk.Record) *apiv1.ExportRecord {
	exportRec := &apiv1.ExportRecord{Raw: rec.Raw}
	if !rec.SourceTS.IsZero() {
		exportRec.SourceTs = timestamppb.New(rec.SourceTS)
	}
	if !rec.IngestTS.IsZero() {
		exportRec.IngestTs = timestamppb.New(rec.IngestTS)
	}
	if len(rec.Attrs) > 0 {
		exportRec.Attrs = make(map[string]string, len(rec.Attrs))
		maps.Copy(exportRec.Attrs, rec.Attrs)
	}
	return exportRec
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
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("vault required"))
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
			if delErr := s.cfgStore.DeleteVault(ctx, cfg.ID); delErr != nil {
				s.logger.Warn("rollback: delete vault config", "vault", cfg.ID, "error", delErr)
			}
		}
		return connect.NewError(connect.CodeInternal, fmt.Errorf("add vault: %w", err))
	}

	return nil
}
