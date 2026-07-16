package app

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"iter"
	"slices"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
	"gastrolog/internal/index/analyzer"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
	"gastrolog/internal/server"
)

// orchStatsAdapter bridges orchestrator methods to the cluster.StatsProvider interface.
type orchStatsAdapter struct {
	orch *orchestrator.Orchestrator
}

func (a *orchStatsAdapter) IngestQueueDepth() int    { return a.orch.IngestQueueDepth() }
func (a *orchStatsAdapter) IngestQueueCapacity() int { return a.orch.IngestQueueCapacity() }

// IngestPressureLevel reports the pressure gate's level for the health
// surfaces. The gate is non-nil for the orchestrator's whole lifetime.
func (a *orchStatsAdapter) IngestPressureLevel() string {
	return a.orch.PressureGate().Level().String()
}

func (a *orchStatsAdapter) VaultSnapshots() []cluster.StatsVaultSnapshot {
	snaps := a.orch.VaultSnapshots()
	out := make([]cluster.StatsVaultSnapshot, len(snaps))
	for i, s := range snaps {
		out[i] = cluster.StatsVaultSnapshot{
			ID:               s.ID,
			Name:             s.Name,
			RecordCount:      s.RecordCount,
			ChunkCount:       s.ChunkCount,
			SealedChunks:     s.SealedChunks,
			DataBytes:        s.DataBytes,
			Enabled:          s.Enabled,
			RaftAppliedIndex: s.RaftAppliedIndex,
		}
	}
	return out
}

func (a *orchStatsAdapter) IngesterIDs() []string {
	ids := a.orch.ListIngesters()
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	return out
}

func (a *orchStatsAdapter) IngesterStats(id string) (name string, messages, bytes, errors int64, running bool) {
	uid, err := glid.ParseUUID(id)
	if err != nil {
		return "", 0, 0, 0, false
	}
	s := a.orch.GetIngesterStats(uid)
	if s == nil {
		return "", 0, 0, 0, false
	}
	return a.orch.IngesterName(uid), s.MessagesIngested.Load(), s.BytesIngested.Load(), s.Errors.Load(), a.orch.IsIngesterRunning(uid)
}

func (a *orchStatsAdapter) VaultAppendStats() []cluster.StatsVaultAppendSnapshot {
	byVault := make(map[glid.GLID]*cluster.StatsVaultAppendSnapshot)
	get := func(id glid.GLID) *cluster.StatsVaultAppendSnapshot {
		if s, ok := byVault[id]; ok {
			return s
		}
		s := &cluster.StatsVaultAppendSnapshot{VaultID: id}
		byVault[id] = s
		return s
	}
	for _, s := range a.orch.VaultAppendStats() {
		snap := get(s.VaultID)
		snap.RecordsAppended = s.RecordsAppended
		snap.BytesAppended = s.BytesAppended
		snap.RecordsDurable = s.RecordsDurable
		snap.QueueDepth = s.QueueDepth
		snap.QueueCap = s.QueueCap
		snap.SegmentsCompleted = s.SegmentsCompleted
	}
	for _, s := range a.orch.VaultCollectStats() {
		snap := get(s.VaultID)
		snap.CollectedRecords = s.CollectedRecords
		snap.CollectedBytes = s.CollectedBytes
	}
	for _, s := range a.orch.VaultSealStats() {
		snap := get(s.VaultID)
		snap.SealedRecords = s.SealedRecords
		snap.SealedBytes = s.SealedBytes
	}
	for _, s := range a.orch.VaultPublishStats() {
		snap := get(s.VaultID)
		snap.SegmentsPublished = s.Published
	}
	for _, s := range a.orch.VaultChunkStageStats() {
		snap := get(s.VaultID)
		snap.ChunksPlanned = s.ChunksPlanned
		snap.ChunksBuilt = s.ChunksBuilt
		snap.ChunksSealed = s.ChunksSealed
		snap.SegmentsReleased = s.SegmentsReleased
		snap.HeadPurges = s.HeadPurges
	}
	for _, s := range a.orch.VaultStageEventStats() {
		snap := get(s.VaultID)
		snap.GLCBPullsAttempted = s.GLCBPullsAttempted
		snap.GLCBPullsFailed = s.GLCBPullsFailed
		snap.RetentionDeletes = s.RetentionDeletes
	}
	out := make([]cluster.StatsVaultAppendSnapshot, 0, len(byVault))
	for _, s := range byVault {
		out = append(out, *s)
	}
	slices.SortFunc(out, func(a, b cluster.StatsVaultAppendSnapshot) int {
		return a.VaultID.Compare(b.VaultID)
	})
	return out
}

func (a *orchStatsAdapter) RouteStats() cluster.StatsRouteSnapshot {
	rs := a.orch.GetRouteStats()
	snap := cluster.StatsRouteSnapshot{
		Routed:       rs.Routed,
		Unmatched:    rs.Unmatched,
		Matched:      rs.Matched,
		RouteTableActive: a.orch.IsRouteTableActive(),
	}
	for vaultID, vs := range a.orch.VaultRouteStatsList() {
		snap.VaultStats = append(snap.VaultStats, cluster.StatsVaultRouteSnapshot{
			VaultID: vaultID,
			Matched: vs.Matched,
		})
	}
	for routeID, rs := range a.orch.PerRouteStatsList() {
		snap.RouteStats = append(snap.RouteStats, cluster.StatsPerRouteSnapshot{
			RouteID: routeID,
			Matched: rs.Matched,
		})
	}
	return snap
}

func (a *orchStatsAdapter) PipelineDiskSnapshots() []cluster.StatsVaultPipelineDiskSnapshot {
	vaultIDs := a.orch.ListVaults()
	out := make([]cluster.StatsVaultPipelineDiskSnapshot, 0, len(vaultIDs))
	for _, vaultID := range vaultIDs {
		disk, err := a.orch.LocalPipelineDiskSegmentCounts(vaultID)
		if err != nil {
			continue
		}
		out = append(out, cluster.StatsVaultPipelineDiskSnapshot{
			VaultID:          vaultID,
			Working:          disk.Working,
			CompletedStaging: disk.CompletedStaging,
			Head:             disk.Head,
			PreHead:          disk.PreHead,
		})
	}
	return out
}

func (a *orchStatsAdapter) LocalStorageBytes() int64 {
	return a.orch.LocalStorageBytes()
}

func (a *orchStatsAdapter) DiskProtectedVaults() []glid.GLID {
	return a.orch.DiskProtectedVaults()
}

func (a *orchStatsAdapter) SizeCappedVaults() []glid.GLID {
	return a.orch.SizeCappedVaults()
}

// jobBroadcastAdapter bridges the scheduler to the cluster.JobsProvider interface.
type jobBroadcastAdapter struct {
	scheduler *orchestrator.Scheduler
	nodeID    string
}

func (a *jobBroadcastAdapter) ListJobsProto() []*gastrologv1.Job {
	jobs := a.scheduler.ListJobs()
	out := make([]*gastrologv1.Job, 0, len(jobs))
	for _, info := range jobs {
		out = append(out, server.JobInfoToProto(info.Snapshot(), a.nodeID))
	}
	return out
}

// forwardSearchAfterParse runs the ForwardSearch body after query parse and
// engine resolution (keeps newSearchExecutor's cognitive complexity in budget).
func forwardSearchAfterParse(
	ctx context.Context,
	eng *query.Engine,
	q query.Query,
	pipeline *querylang.Pipeline,
	resumeTokenData []byte,
	includeHistogram bool,
) (iter.Seq2[chunk.Record, error], func() []byte, *gastrologv1.TableResult, []*gastrologv1.HistogramBucket, error) {
	// Filtered forward searches already pay a full lazy-prime record scan in
	// Search below. Skip the histogram pre-pass (timechartScanPath → Search)
	// which would duplicate that work and spike RSS on scatterbox nodes.
	// Unfiltered legacy full-vault forwards keep the fast binary-search path.
	var histogram []*gastrologv1.HistogramBucket
	if includeHistogram {
		histogram = server.HistogramToProto(eng.ComputeSearchPageHistogram(ctx, q, 50))
	}

	if pipeline != nil && len(pipeline.Pipes) > 0 && !query.CanStreamPipeline(pipeline) {
		result, err := eng.RunPipeline(ctx, q, pipeline)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		if result.Table != nil {
			return nil, nil, server.TableResultToBasicProto(result.Table), histogram, nil
		}
		records := result.Records
		return func(yield func(chunk.Record, error) bool) {
			for _, rec := range records {
				if !yield(rec, nil) {
					return
				}
			}
		}, nil, nil, histogram, nil
	}

	var resume *query.ResumeToken
	if len(resumeTokenData) > 0 {
		var err error
		resume, err = server.ProtoToLocalResumeToken(resumeTokenData)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("invalid resume token: %w", err)
		}
	}

	searchIter, getToken := eng.Search(ctx, q, resume)
	getTokenBytes := func() []byte {
		token := getToken()
		if token == nil {
			return nil
		}
		return server.ResumeTokenToProto(token)
	}
	return searchIter, getTokenBytes, nil, histogram, nil
}

// newSearchExecutor creates a cluster.SearchExecutor that runs local vault
// searches for ForwardSearch RPCs received from peer nodes. When the query
// contains a pipeline (stats, timechart), runs RunPipeline and returns the
// TableResult instead of individual records. For regular searches, returns
// the iterator directly — the streaming handler sends records as it iterates.
func newSearchExecutor(o *orchestrator.Orchestrator) cluster.SearchExecutor {
	return func(ctx context.Context, req *gastrologv1.ForwardSearchRequest) (iter.Seq2[chunk.Record, error], func() []byte, *gastrologv1.TableResult, []*gastrologv1.HistogramBucket, error) {
		if glid.FromBytes(req.GetVaultId()).IsZero() {
			return nil, nil, nil, nil, errors.New("invalid vault_id")
		}
		q, pipeline, err := server.ParseExpression(req.GetQuery())
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("parse query: %w", err)
		}

		eng, err := server.ForwardSearchEngine(o, req)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		if eng == nil {
			return nil, nil, nil, nil, nil
		}

		includeHist := server.ForwardSearchIncludesHistogram(req, q)
		return forwardSearchAfterParse(ctx, eng, q, pipeline, req.GetResumeToken(), includeHist)
	}
}

// newExplainExecutor creates a cluster.ExplainExecutor that runs explain on
// local vaults for ForwardExplain RPCs received from peer nodes. Scopes the
// query to the requested vault IDs and sets the node_id on each ChunkPlan.
func newExplainExecutor(o *orchestrator.Orchestrator, localNodeID string) cluster.ExplainExecutor {
	return func(ctx context.Context, vaultIDs []glid.GLID, queryExpr string) ([]*gastrologv1.ChunkPlan, int32, error) {
		var allChunks []*gastrologv1.ChunkPlan
		var totalChunks int32

		// Parse the query once — don't add vault_id= scope because the
		// engine is already scoped to the vault's leader instances.
		q, _, err := server.ParseExpression(queryExpr)
		if err != nil {
			return nil, 0, fmt.Errorf("parse query: %w", err)
		}

		for _, vid := range vaultIDs {
			eng, err := o.LeaderQueryEngineForVault(vid)
			if err != nil {
				return nil, 0, fmt.Errorf("vault %s: %w", vid, err)
			}
			if eng == nil {
				continue // no leader instance for this vault
			}
			plan, err := eng.Explain(ctx, q)
			if err != nil {
				return nil, 0, fmt.Errorf("explain vault %s: %w", vid, err)
			}

			totalChunks += int32(plan.TotalChunks) //nolint:gosec // G115: chunk count fits in int32
			for _, cp := range plan.ChunkPlans {
				chunkPlan := &gastrologv1.ChunkPlan{
					VaultId:          cp.VaultID.ToProto(),
					ChunkId:          glid.GLID(cp.ChunkID).ToProto(),
					Sealed:           cp.Sealed,
					RecordCount:      int64(cp.RecordCount),
					ScanMode:         cp.ScanMode,
					EstimatedRecords: int64(cp.EstimatedScan),
					RuntimeFilters:   []string{cp.RuntimeFilter},
					Steps:            server.PipelineStepsToProto(cp.Pipeline),
					SkipReason:       cp.SkipReason,
					NodeId:           []byte(localNodeID),
				}
				if !cp.WriteStart.IsZero() {
					chunkPlan.WriteStart = timestamppb.New(cp.WriteStart)
				}
				if !cp.WriteEnd.IsZero() {
					chunkPlan.WriteEnd = timestamppb.New(cp.WriteEnd)
				}
				for _, bp := range cp.BranchPlans {
					chunkPlan.BranchPlans = append(chunkPlan.BranchPlans, &gastrologv1.BranchPlan{
						Expression:       bp.BranchExpr,
						Steps:            server.PipelineStepsToProto(bp.Pipeline),
						Skipped:          bp.Skipped,
						SkipReason:       bp.SkipReason,
						EstimatedRecords: int64(bp.EstimatedScan),
					})
				}
				allChunks = append(allChunks, chunkPlan)
			}
		}
		return allChunks, totalChunks, nil
	}
}

// newFollowExecutor creates a cluster.FollowExecutor that runs a follow on
// local vaults for ForwardFollow RPCs received from peer nodes.
func newFollowExecutor(o *orchestrator.Orchestrator) cluster.FollowExecutor {
	return func(ctx context.Context, vaultIDs []glid.GLID, queryExpr string) (iter.Seq2[chunk.Record, error], error) {
		// Scope the query to the requested vaults by prepending vault_id= predicates.
		var scopedExpr string
		for _, vid := range vaultIDs {
			if scopedExpr != "" {
				scopedExpr += " OR "
			}
			scopedExpr += "vault_id=" + vid.String()
		}
		if queryExpr != "" {
			if len(vaultIDs) > 1 {
				scopedExpr = "(" + scopedExpr + ") " + queryExpr
			} else {
				scopedExpr += " " + queryExpr
			}
		}

		q, _, err := server.ParseExpression(scopedExpr)
		if err != nil {
			return nil, fmt.Errorf("parse query: %w", err)
		}

		eng := o.LeaderVaultQueryEngine()
		return eng.Follow(ctx, q), nil
	}
}

func newContextExecutor(o *orchestrator.Orchestrator) cluster.ContextExecutor {
	return func(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID, pos uint64, before, after int) ([]chunk.Record, chunk.Record, []chunk.Record, error) {
		eng, err := o.LeaderQueryEngineForVault(vaultID)
		if err != nil {
			return nil, chunk.Record{}, nil, err
		}
		if eng == nil {
			return nil, chunk.Record{}, nil, fmt.Errorf("no leader instance for vault %s", vaultID)
		}
		result, err := eng.GetContext(ctx, query.ContextRef{
			VaultID: vaultID,
			ChunkID: chunkID,
			Pos:     pos,
		}, before, after)
		if err != nil {
			return nil, chunk.Record{}, nil, err
		}
		return result.Before, result.Anchor, result.After, nil
	}
}

func newListChunksExecutor(o *orchestrator.Orchestrator) cluster.ListChunksExecutor {
	return func(ctx context.Context, vaultID glid.GLID) ([]*gastrologv1.ChunkMeta, error) {
		metas, err := o.ListAllChunkMetas(vaultID)
		if err != nil {
			return nil, err
		}
		pending := o.RetentionPendingChunks(vaultID)
		out := make([]*gastrologv1.ChunkMeta, 0, len(metas))
		for _, m := range metas {
			pb := server.VaultChunkMetaToProto(m)
			if pending[m.ID] {
				pb.RetentionPending = true
			}
			out = append(out, pb)
		}
		return out, nil
	}
}

func newPipelineBacklogDiskExecutor(o *orchestrator.Orchestrator) cluster.PipelineBacklogDiskExecutor {
	return func(ctx context.Context, vaultID glid.GLID) (*gastrologv1.ForwardGetPipelineBacklogResponse, error) {
		disk, err := o.LocalPipelineDiskSegmentCounts(vaultID)
		if err != nil {
			return nil, err
		}
		return &gastrologv1.ForwardGetPipelineBacklogResponse{
			WorkingSegments:          uint32(disk.Working),               //nolint:gosec
			CompletedStagingSegments: uint32(disk.CompletedStaging),      //nolint:gosec
			HeadSegments:             uint32(disk.Head),                  //nolint:gosec
			PreHeadSegments:          uint32(disk.PreHead),               //nolint:gosec
			WorkingBytes:             uint64(disk.WorkingBytes),          //nolint:gosec
			CompletedStagingBytes:    uint64(disk.CompletedStagingBytes), //nolint:gosec
			HeadBytes:                uint64(disk.HeadBytes),             //nolint:gosec
			PreHeadBytes:             uint64(disk.PreHeadBytes),          //nolint:gosec
		}, nil
	}
}

func newGetIndexesExecutor(o *orchestrator.Orchestrator) cluster.GetIndexesExecutor {
	return func(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID) (*gastrologv1.GetIndexesResponse, error) {
		report, err := o.ChunkIndexInfos(vaultID, chunkID)
		if err != nil {
			return nil, err
		}
		resp := &gastrologv1.GetIndexesResponse{
			Sealed:  report.Sealed,
			Indexes: make([]*gastrologv1.IndexInfo, 0, len(report.Indexes)),
		}
		for _, idx := range report.Indexes {
			resp.Indexes = append(resp.Indexes, &gastrologv1.IndexInfo{
				Name:       idx.Name,
				Exists:     idx.Exists,
				EntryCount: idx.EntryCount,
				SizeBytes:  idx.SizeBytes,
			})
		}
		return resp, nil
	}
}

func newValidateVaultExecutor(o *orchestrator.Orchestrator) cluster.ValidateVaultExecutor {
	return func(_ context.Context, vaultID glid.GLID) (*gastrologv1.ValidateVaultResponse, error) {
		metas, err := o.ListLocalChunkMetas(vaultID)
		if err != nil {
			return nil, err
		}
		return server.ValidateVaultLocal(o, vaultID, metas), nil
	}
}

func newGetChunkExecutor(o *orchestrator.Orchestrator) cluster.GetChunkExecutor {
	return func(_ context.Context, vaultID glid.GLID, chunkID chunk.ChunkID) (*gastrologv1.ChunkMeta, error) {
		meta, err := o.GetChunkMeta(vaultID, chunkID)
		if err != nil {
			return nil, err
		}
		return server.ChunkMetaToProto(meta), nil
	}
}

func newAnalyzeChunkExecutor(o *orchestrator.Orchestrator) cluster.AnalyzeChunkExecutor {
	return func(_ context.Context, vaultID glid.GLID, chunkIDStr string) ([]*gastrologv1.ChunkAnalysis, error) {
		var analyses []analyzer.ChunkAnalysis
		var err error
		if chunkIDStr == "" {
			analyses, err = analyzeChunkAllForExecutor(o, vaultID)
		} else {
			analyses, err = analyzeChunkOneForExecutor(o, vaultID, chunkIDStr)
		}
		if err != nil {
			return nil, err
		}
		out := make([]*gastrologv1.ChunkAnalysis, 0, len(analyses))
		for _, ca := range analyses {
			out = append(out, server.ChunkAnalysisToProto(ca))
		}
		return out, nil
	}
}

func analyzeChunkAllForExecutor(o *orchestrator.Orchestrator, vaultID glid.GLID) ([]analyzer.ChunkAnalysis, error) {
	a, err := o.NewAnalyzer(vaultID)
	if err != nil {
		return nil, err
	}
	agg, err := a.AnalyzeAll()
	if err != nil {
		return nil, err
	}
	return agg.Chunks, nil
}

func analyzeChunkOneForExecutor(o *orchestrator.Orchestrator, vaultID glid.GLID, chunkIDStr string) ([]analyzer.ChunkAnalysis, error) {
	chunkID, err := chunk.ParseChunkID(chunkIDStr)
	if err != nil {
		return nil, err
	}
	a, err := o.NewAnalyzerForChunk(vaultID, chunkID)
	if err != nil {
		return nil, err
	}
	analysis, err := a.AnalyzeChunk(chunkID)
	if err != nil {
		return nil, err
	}
	return []analyzer.ChunkAnalysis{*analysis}, nil
}

func newSealVaultExecutor(o *orchestrator.Orchestrator) cluster.SealVaultExecutor {
	return func(_ context.Context, vaultID glid.GLID) error {
		_, err := o.SealActive(vaultID)
		return err
	}
}

func newReindexVaultExecutor(o *orchestrator.Orchestrator) cluster.ReindexVaultExecutor {
	return func(_ context.Context, vaultID glid.GLID) (string, error) {
		if !o.VaultExists(vaultID) {
			return "", errors.New("vault not found")
		}
		jobName := "reindex:" + vaultID.String()
		jobID := o.Scheduler().Submit(jobName, func(ctx context.Context, job *orchestrator.JobProgress) {
			metas, err := o.ListLocalChunkMetas(vaultID)
			if err != nil {
				job.Fail(time.Now(), err.Error())
				return
			}
			var sealedCount int64
			for _, m := range metas {
				if m.Sealed {
					sealedCount++
				}
			}
			job.SetRunning(sealedCount)
			for _, m := range metas {
				if !m.Sealed {
					continue
				}
				if err := o.DeleteIndexes(vaultID, m.ID); err != nil {
					job.AddErrorDetail(fmt.Sprintf("delete indexes for chunk %s: %v", m.ID, err))
					continue
				}
				if err := o.BuildIndexes(ctx, vaultID, m.ID); err != nil {
					job.AddErrorDetail(fmt.Sprintf("build indexes for chunk %s: %v", m.ID, err))
					continue
				}
				job.IncrChunks()
			}
		})
		return jobID, nil
	}
}

// newChunkEventSubscriber returns a cluster.ChunkEventSubscriber that
// subscribes to this node's orchestrator ChunkBus and translates each
// ChunkChangeEvent into a ForwardWatchChunksResponse for the peer's
// streaming receiver. Loop exits cleanly on ctx cancellation; falls
// through silently if the peer's send fails (peer reconnects via its
// own backoff loop).
func newChunkEventSubscriber(o *orchestrator.Orchestrator) cluster.ChunkEventSubscriber {
	return func(ctx context.Context, send func(*gastrologv1.ForwardWatchChunksResponse) error) error {
		bus := o.ChunkBus()
		subID, events, baseline := bus.Subscribe()
		defer bus.Unsubscribe(subID)
		// Initial heartbeat so the peer knows the version baseline.
		if err := send(&gastrologv1.ForwardWatchChunksResponse{
			Op:      gastrologv1.ChunkChangeOp_CHUNK_CHANGE_OP_UNSPECIFIED,
			Version: baseline,
		}); err != nil {
			return err
		}
		for {
			select {
			case <-ctx.Done():
				return nil
			case ev, ok := <-events:
				if !ok {
					return nil
				}
				msg := chunkChangeEventToForwardProto(o, ev.Event)
				msg.Version = ev.Version
				if err := send(msg); err != nil {
					return err
				}
			}
		}
	}
}

// chunkChangeEventToForwardProto mirrors server.chunkChangeEventToProto
// but produces the cluster-internal ForwardWatchChunksResponse rather
// than the public WatchChunksResponse. Vault type lookup happens here
// (same orchestrator registry) so peer messages already carry the
// inspector-required field; the API node's per-message wrap then just
// copies fields.
func chunkChangeEventToForwardProto(o *orchestrator.Orchestrator, ev orchestrator.ChunkChangeEvent) *gastrologv1.ForwardWatchChunksResponse {
	msg := &gastrologv1.ForwardWatchChunksResponse{
		VaultId: ev.VaultID.ToProto(),
		ChunkId: ev.ChunkID[:],
		Op:      chunkOpToForwardProto(ev.Op),
	}
	if ev.Meta != nil {
		msg.Meta = server.ChunkMetaToProto(*ev.Meta)
		msg.Meta.VaultId = ev.VaultID.ToProto()
		msg.Meta.VaultType = o.VaultType(ev.VaultID)
	}
	if ev.Op == orchestrator.ChunkChangeOpProgress {
		msg.RecordCount = ev.RecordCount
	}
	return msg
}

func chunkOpToForwardProto(op orchestrator.ChunkChangeOp) gastrologv1.ChunkChangeOp {
	switch op {
	case orchestrator.ChunkChangeOpUnspecified:
		return gastrologv1.ChunkChangeOp_CHUNK_CHANGE_OP_UNSPECIFIED
	case orchestrator.ChunkChangeOpCreated:
		return gastrologv1.ChunkChangeOp_CHUNK_CHANGE_OP_CREATED
	case orchestrator.ChunkChangeOpProgress:
		return gastrologv1.ChunkChangeOp_CHUNK_CHANGE_OP_PROGRESS
	case orchestrator.ChunkChangeOpSealed:
		return gastrologv1.ChunkChangeOp_CHUNK_CHANGE_OP_SEALED
	case orchestrator.ChunkChangeOpDeleted:
		return gastrologv1.ChunkChangeOp_CHUNK_CHANGE_OP_DELETED
	case orchestrator.ChunkChangeOpUploaded:
		return gastrologv1.ChunkChangeOp_CHUNK_CHANGE_OP_UPLOADED
	default:
		return gastrologv1.ChunkChangeOp_CHUNK_CHANGE_OP_UNSPECIFIED
	}
}
