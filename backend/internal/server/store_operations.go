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

// ReindexStore rebuilds all indexes for sealed chunks in a store.
// The work is submitted as an async job; the response contains the job ID.
func (s *StoreServer) ReindexStore(
	ctx context.Context,
	req *connect.Request[apiv1.ReindexStoreRequest],
) (*connect.Response[apiv1.ReindexStoreResponse], error) {
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

	jobName := "reindex:" + storeID.String()
	jobID := s.orch.Scheduler().Submit(jobName, func(ctx context.Context, job *orchestrator.JobProgress) {
		metas, err := cm.List()
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
			if err := im.DeleteIndexes(meta.ID); err != nil {
				job.AddErrorDetail(fmt.Sprintf("delete indexes for chunk %s: %v", meta.ID, err))
				continue
			}
			if err := im.BuildIndexes(ctx, meta.ID); err != nil {
				job.AddErrorDetail(fmt.Sprintf("build indexes for chunk %s: %v", meta.ID, err))
				continue
			}
			job.IncrChunks()
		}
	})
	s.orch.Scheduler().Describe(jobName, fmt.Sprintf("Rebuild all indexes for '%s'", s.storeName(ctx, storeID)))

	return connect.NewResponse(&apiv1.ReindexStoreResponse{JobId: jobID}), nil
}

// MigrateStore moves a store to a new name, type, and/or location.
// Three-phase: create destination, freeze source, async merge+delete.
func (s *StoreServer) MigrateStore(
	ctx context.Context,
	req *connect.Request[apiv1.MigrateStoreRequest],
) (*connect.Response[apiv1.MigrateStoreResponse], error) {
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
	srcCM := s.orch.ChunkManager(srcID)
	if srcCM == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("source store not found"))
	}

	// Get source config for filter/policy and to resolve destination type.
	srcCfg, err := s.getFullStoreConfig(ctx, srcID)
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
	// File stores require an explicit dir — no auto-derive.
	if dstType == "file" && dstParams[chunkfile.ParamDir] == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("destination_params.dir required for file stores"))
	}

	// Phase 1: Create destination store with inherited filter/policy.
	dstCfg := config.StoreConfig{
		ID:        uuid.Must(uuid.NewV7()),
		Name:      req.Msg.Destination,
		Type:      dstType,
		Filter:    srcCfg.Filter,
		Policy:    srcCfg.Policy,
		Retention: srcCfg.Retention,
		Enabled:   true,
		Params:    dstParams,
	}

	if err := s.createStore(ctx, dstCfg); err != nil {
		return nil, err
	}

	// Phase 2: Freeze source — disable ingestion and persist.
	if err := s.orch.DisableStore(srcID); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("disable source: %w", err))
	}
	if s.cfgStore != nil {
		srcCfg.Enabled = false
		if err := s.cfgStore.PutStore(ctx, srcCfg); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("persist disabled source: %w", err))
		}
	}

	// Seal source's active chunk so all data is in sealed chunks.
	if active := srcCM.Active(); active != nil && active.RecordCount > 0 {
		if err := srcCM.Seal(); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("seal source: %w", err))
		}
	}

	// Phase 3: Async merge + delete.
	dstID := dstCfg.ID
	srcName := s.storeName(ctx, srcID)

	// Capture file dir before the job runs (source config will be deleted).
	var srcFileDir string
	if srcCfg.Type == "file" {
		srcFileDir = srcCfg.Params[chunkfile.ParamDir]
	}

	// Detect chunk mover support.
	_, srcMovable := srcCM.(chunk.ChunkMover)
	dstCM := s.orch.ChunkManager(dstID)
	_, dstMovable := dstCM.(chunk.ChunkMover)
	canMoveChunks := srcMovable && dstMovable

	jobName := "migrate:" + srcID.String() + "->" + dstID.String()
	jobID := s.orch.Scheduler().Submit(jobName, func(ctx context.Context, job *orchestrator.JobProgress) {
		var mergeErr error
		if canMoveChunks {
			mergeErr = s.moveChunksTracked(ctx, srcID, dstID, job)
		} else {
			mergeErr = s.copyRecordsTracked(ctx, srcID, dstID, job)
		}
		if mergeErr != nil {
			job.Fail(s.now(), fmt.Sprintf("merge records: %v", mergeErr))
			return
		}

		// Delete the source store.
		if err := s.orch.ForceRemoveStore(srcID); err != nil {
			job.Fail(s.now(), fmt.Sprintf("delete source: %v", err))
			return
		}

		// Remove source from config store and clean up file directory.
		if s.cfgStore != nil {
			if err := s.cfgStore.DeleteStore(ctx, srcID); err != nil {
				s.logger.Warn("cleanup: delete store config", "store", srcID, "error", err)
			}
		}
		if srcFileDir != "" {
			if err := os.RemoveAll(srcFileDir); err != nil {
				s.logger.Warn("cleanup: remove source directory", "dir", srcFileDir, "error", err)
			}
		}
	})
	s.orch.Scheduler().Describe(jobName, fmt.Sprintf("Migrate '%s' to '%s'", srcName, s.storeName(ctx, dstID)))

	return connect.NewResponse(&apiv1.MigrateStoreResponse{JobId: jobID}), nil
}

// MergeStores moves all chunks from a source store into a destination store,
// then deletes the source. Both stores must support chunk-level moves (ChunkMover).
func (s *StoreServer) MergeStores(
	ctx context.Context,
	req *connect.Request[apiv1.MergeStoresRequest],
) (*connect.Response[apiv1.MergeStoresResponse], error) {
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

	// Both stores must exist.
	srcCM := s.orch.ChunkManager(srcID)
	dstCM := s.orch.ChunkManager(dstID)
	if srcCM == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("source store not found"))
	}
	if dstCM == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("destination store not found"))
	}

	// Auto-disable source to prevent new data flowing in during merge.
	if s.orch.IsStoreEnabled(srcID) {
		if err := s.orch.DisableStore(srcID); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("disable source: %w", err))
		}
		if s.cfgStore != nil {
			srcCfg, err := s.getFullStoreConfig(ctx, srcID)
			if err == nil {
				srcCfg.Enabled = false
				if err := s.cfgStore.PutStore(ctx, srcCfg); err != nil {
					s.logger.Warn("persist disabled source config", "store", srcID, "error", err)
				}
			}
		}
	}

	// Use chunk-level moves when both sides support it (preserves WriteTS).
	// Fall back to record-by-record copy otherwise (rewrites WriteTS).
	_, srcMovable := srcCM.(chunk.ChunkMover)
	_, dstMovable := dstCM.(chunk.ChunkMover)
	canMoveChunks := srcMovable && dstMovable

	// Capture source file dir before job runs (source config will be deleted).
	var srcFileDir string
	if srcCfg, err := s.getFullStoreConfig(ctx, srcID); err == nil && srcCfg.Type == "file" {
		srcFileDir = srcCfg.Params[chunkfile.ParamDir]
	}

	jobName := "merge:" + srcID.String() + "->" + dstID.String()
	jobID := s.orch.Scheduler().Submit(jobName, func(ctx context.Context, job *orchestrator.JobProgress) {
		// Seal source's active chunk before merging.
		srcCM := s.orch.ChunkManager(srcID)
		if srcCM != nil {
			if active := srcCM.Active(); active != nil && active.RecordCount > 0 {
				if err := srcCM.Seal(); err != nil {
					job.Fail(s.now(), fmt.Sprintf("seal source: %v", err))
					return
				}
			}
		}

		var err error
		if canMoveChunks {
			err = s.moveChunksTracked(ctx, srcID, dstID, job)
		} else {
			err = s.copyRecordsTracked(ctx, srcID, dstID, job)
		}
		if err != nil {
			job.Fail(s.now(), fmt.Sprintf("merge records: %v", err))
			return
		}

		// Force-delete the source store (now empty).
		if err := s.orch.ForceRemoveStore(srcID); err != nil {
			job.Fail(s.now(), fmt.Sprintf("delete source: %v", err))
			return
		}

		// Remove source from config store and clean up data directory.
		if s.cfgStore != nil {
			if err := s.cfgStore.DeleteStore(ctx, srcID); err != nil {
				s.logger.Warn("cleanup: delete store config", "store", srcID, "error", err)
			}
		}
		if srcFileDir != "" {
			if err := os.RemoveAll(srcFileDir); err != nil {
				s.logger.Warn("cleanup: remove source directory", "dir", srcFileDir, "error", err)
			}
		}
	})
	s.orch.Scheduler().Describe(jobName, fmt.Sprintf("Merge '%s' into '%s'", s.storeName(ctx, srcID), s.storeName(ctx, dstID)))

	return connect.NewResponse(&apiv1.MergeStoresResponse{JobId: jobID}), nil
}

// ExportStore streams all records from a store.
func (s *StoreServer) ExportStore(
	ctx context.Context,
	req *connect.Request[apiv1.ExportStoreRequest],
	stream *connect.ServerStream[apiv1.ExportStoreResponse],
) error {
	if req.Msg.Store == "" {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("store required"))
	}
	storeID, connErr := parseUUID(req.Msg.Store)
	if connErr != nil {
		return connErr
	}

	cm := s.orch.ChunkManager(storeID)
	if cm == nil {
		return connect.NewError(connect.CodeNotFound, errors.New("store not found"))
	}

	metas, err := cm.List()
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}

	const batchSize = 100

	for _, meta := range metas {
		cursor, err := cm.OpenCursor(meta.ID)
		if err != nil {
			return connect.NewError(connect.CodeInternal, fmt.Errorf("open chunk %s: %w", meta.ID, err))
		}

		batch := make([]*apiv1.ExportRecord, 0, batchSize)
		for {
			rec, _, err := cursor.Next()
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				break
			}
			if err != nil {
				cursor.Close()
				return connect.NewError(connect.CodeInternal, fmt.Errorf("read chunk %s: %w", meta.ID, err))
			}

			exportRec := &apiv1.ExportRecord{
				Raw: rec.Raw,
			}
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
			batch = append(batch, exportRec)

			if len(batch) >= batchSize {
				if err := stream.Send(&apiv1.ExportStoreResponse{Records: batch, HasMore: true}); err != nil {
					cursor.Close()
					return err
				}
				batch = make([]*apiv1.ExportRecord, 0, batchSize)
			}
		}
		cursor.Close()

		// Flush remaining records for this chunk.
		if len(batch) > 0 {
			if err := stream.Send(&apiv1.ExportStoreResponse{Records: batch, HasMore: true}); err != nil {
				return err
			}
		}
	}

	// Final empty message to signal end.
	if err := stream.Send(&apiv1.ExportStoreResponse{HasMore: false}); err != nil {
		return err
	}

	return nil
}

// ImportRecords appends a batch of records to a store.
func (s *StoreServer) ImportRecords(
	ctx context.Context,
	req *connect.Request[apiv1.ImportRecordsRequest],
) (*connect.Response[apiv1.ImportRecordsResponse], error) {
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

		if _, _, err := cm.Append(rec); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("append record %d: %w", imported, err))
		}
		imported++
	}

	return connect.NewResponse(&apiv1.ImportRecordsResponse{
		RecordsImported: imported,
	}), nil
}

// getFullStoreConfig retrieves store config from the config store (with type/params),
// falling back to the orchestrator's limited config.
func (s *StoreServer) getFullStoreConfig(ctx context.Context, id uuid.UUID) (config.StoreConfig, error) {
	if s.cfgStore != nil {
		cfg, err := s.cfgStore.GetStore(ctx, id)
		if err == nil && cfg != nil {
			return *cfg, nil
		}
	}
	return s.orch.StoreConfig(id)
}

// createStore persists a store config and adds it to the orchestrator.
func (s *StoreServer) createStore(ctx context.Context, cfg config.StoreConfig) *connect.Error {
	// Persist to config store.
	if s.cfgStore != nil {
		if err := s.cfgStore.PutStore(ctx, cfg); err != nil {
			return connect.NewError(connect.CodeInternal, fmt.Errorf("persist config: %w", err))
		}
	}

	// Load full config for filter resolution.
	var fullCfg *config.Config
	if s.cfgStore != nil {
		var err error
		fullCfg, err = s.cfgStore.Load(ctx)
		if err != nil {
			return connect.NewError(connect.CodeInternal, fmt.Errorf("reload config: %w", err))
		}
	}

	// Add to orchestrator.
	if err := s.orch.AddStore(cfg, fullCfg, s.factories); err != nil {
		// Rollback config entry on orchestrator failure.
		if s.cfgStore != nil {
			if delErr := s.cfgStore.DeleteStore(ctx, cfg.ID); delErr != nil {
				s.logger.Warn("rollback: delete store config", "store", cfg.ID, "error", delErr)
			}
		}
		return connect.NewError(connect.CodeInternal, fmt.Errorf("add store: %w", err))
	}

	return nil
}

// copyRecordsTracked copies all records from source to destination, reporting
// progress via the tracked job.
func (s *StoreServer) copyRecordsTracked(ctx context.Context, srcID, dstID uuid.UUID, job *orchestrator.JobProgress) error {
	srcCM := s.orch.ChunkManager(srcID)
	dstCM := s.orch.ChunkManager(dstID)
	dstIM := s.orch.IndexManager(dstID)

	metas, err := srcCM.List()
	if err != nil {
		return err
	}

	job.SetRunning(int64(len(metas)))

	for _, meta := range metas {
		cursor, err := srcCM.OpenCursor(meta.ID)
		if err != nil {
			return fmt.Errorf("open chunk %s: %w", meta.ID, err)
		}

		for {
			rec, _, readErr := cursor.Next()
			if errors.Is(readErr, chunk.ErrNoMoreRecords) {
				break
			}
			if readErr != nil {
				cursor.Close()
				return fmt.Errorf("read chunk %s: %w", meta.ID, readErr)
			}

			rec = rec.Copy()
			if _, _, appendErr := dstCM.AppendPreserved(rec); appendErr != nil {
				cursor.Close()
				return fmt.Errorf("append record: %w", appendErr)
			}
			job.AddRecords(1)
		}
		cursor.Close()
		job.IncrChunks()
	}

	// Seal the active chunk if it has data, so we can build indexes.
	if active := dstCM.Active(); active != nil && active.RecordCount > 0 {
		if err := dstCM.Seal(); err != nil {
			return fmt.Errorf("seal final chunk: %w", err)
		}
	}

	// Build indexes for all sealed chunks.
	dstMetas, err := dstCM.List()
	if err != nil {
		return err
	}
	for _, meta := range dstMetas {
		if meta.Sealed && dstIM != nil {
			if err := dstIM.BuildIndexes(ctx, meta.ID); err != nil {
				return fmt.Errorf("build indexes for chunk %s: %w", meta.ID, err)
			}
		}
	}

	return nil
}

// moveChunksTracked moves sealed chunks from source to destination by
// moving chunk directories on the filesystem. This preserves all record
// timestamps (including WriteTS) since no records are rewritten.
func (s *StoreServer) moveChunksTracked(ctx context.Context, srcID, dstID uuid.UUID, job *orchestrator.JobProgress) error {
	srcCM := s.orch.ChunkManager(srcID)
	dstCM := s.orch.ChunkManager(dstID)
	dstIM := s.orch.IndexManager(dstID)

	srcMover := srcCM.(chunk.ChunkMover)
	dstMover := dstCM.(chunk.ChunkMover)

	metas, err := srcCM.List()
	if err != nil {
		return err
	}

	job.SetRunning(int64(len(metas)))

	for _, meta := range metas {
		if !meta.Sealed {
			continue
		}

		srcDir := srcMover.ChunkDir(meta.ID)
		dstDir := dstMover.ChunkDir(meta.ID)

		// Untrack from source.
		if err := srcMover.Disown(meta.ID); err != nil {
			return fmt.Errorf("disown chunk %s: %w", meta.ID, err)
		}

		// Move directory.
		if err := chunkfile.MoveDir(srcDir, dstDir); err != nil {
			// Attempt to restore source tracking on failure.
			if _, adoptErr := srcMover.Adopt(meta.ID); adoptErr != nil {
				job.AddErrorDetail(fmt.Sprintf("failed to restore chunk %s after move error: %v", meta.ID, adoptErr))
			}
			return fmt.Errorf("move chunk %s: %w", meta.ID, err)
		}

		// Register in destination.
		if _, err := dstMover.Adopt(meta.ID); err != nil {
			return fmt.Errorf("adopt chunk %s in destination: %w", meta.ID, err)
		}

		// Build indexes for the moved chunk in the destination.
		if dstIM != nil {
			if err := dstIM.BuildIndexes(ctx, meta.ID); err != nil {
				job.AddErrorDetail(fmt.Sprintf("build indexes for chunk %s: %v", meta.ID, err))
			}
		}

		job.AddRecords(meta.RecordCount)
		job.IncrChunks()
	}

	return nil
}
