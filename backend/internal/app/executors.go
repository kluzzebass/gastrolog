package app

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
	"gastrolog/internal/index/analyzer"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/server"
)

// orchStatsAdapter bridges orchestrator methods to the cluster.StatsProvider interface.
type orchStatsAdapter struct {
	orch *orchestrator.Orchestrator
}

func (a *orchStatsAdapter) IngestQueueDepth() int    { return a.orch.IngestQueueDepth() }
func (a *orchStatsAdapter) IngestQueueCapacity() int { return a.orch.IngestQueueCapacity() }

func (a *orchStatsAdapter) VaultSnapshots() []cluster.StatsVaultSnapshot {
	snaps := a.orch.VaultSnapshots()
	out := make([]cluster.StatsVaultSnapshot, len(snaps))
	for i, s := range snaps {
		out[i] = cluster.StatsVaultSnapshot{
			ID:           s.ID.String(),
			RecordCount:  s.RecordCount,
			ChunkCount:   s.ChunkCount,
			SealedChunks: s.SealedChunks,
			DataBytes:    s.DataBytes,
			Enabled:      s.Enabled,
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
	uid, err := uuid.Parse(id)
	if err != nil {
		return "", 0, 0, 0, false
	}
	s := a.orch.GetIngesterStats(uid)
	if s == nil {
		return "", 0, 0, 0, false
	}
	return a.orch.IngesterName(uid), s.MessagesIngested.Load(), s.BytesIngested.Load(), s.Errors.Load(), a.orch.IsIngesterRunning(uid)
}

func (a *orchStatsAdapter) RouteStats() cluster.StatsRouteSnapshot {
	rs := a.orch.GetRouteStats()
	snap := cluster.StatsRouteSnapshot{
		Ingested:     rs.Ingested.Load(),
		Dropped:      rs.Dropped.Load(),
		Routed:       rs.Routed.Load(),
		FilterActive: a.orch.IsFilterSetActive(),
	}
	for vaultID, vs := range a.orch.VaultRouteStatsList() {
		snap.VaultStats = append(snap.VaultStats, cluster.StatsVaultRouteSnapshot{
			VaultID:   vaultID.String(),
			Matched:   vs.Matched.Load(),
			Forwarded: vs.Forwarded.Load(),
		})
	}
	return snap
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

// newSearchExecutor creates a cluster.SearchExecutor that runs local vault
// searches for ForwardSearch RPCs received from peer nodes. When the query
// contains a pipeline (stats, timechart), runs RunPipeline and returns the
// TableResult instead of individual records.
func newSearchExecutor(o *orchestrator.Orchestrator) cluster.SearchExecutor {
	return func(ctx context.Context, vaultID uuid.UUID, queryExpr string, resumeToken []byte) ([]*gastrologv1.ExportRecord, *gastrologv1.TableResult, []*gastrologv1.HistogramBucket, []byte, bool, error) {
		scopedExpr := fmt.Sprintf("vault_id=%s %s", vaultID, queryExpr)

		q, pipeline, err := server.ParseExpression(scopedExpr)
		if err != nil {
			return nil, nil, nil, nil, false, fmt.Errorf("parse query: %w", err)
		}

		eng := o.MultiVaultQueryEngine()

		// Compute volume histogram for this vault.
		histogram := histogramBucketsToProto(eng.ComputeHistogramForVaults(ctx, q, 50, []uuid.UUID{vaultID}))

		// Pipeline query: run aggregation locally and return table result.
		if pipeline != nil && len(pipeline.Pipes) > 0 && !query.CanStreamPipeline(pipeline) {
			result, err := eng.RunPipeline(ctx, q, pipeline)
			if err != nil {
				return nil, nil, nil, nil, false, err
			}
			if result.Table != nil {
				return nil, server.TableResultToBasicProto(result.Table), histogram, nil, false, nil
			}
			// Non-table pipeline (sort/tail/slice): return as records.
			records := make([]*gastrologv1.ExportRecord, 0, len(result.Records))
			for _, rec := range result.Records {
				records = append(records, cluster.RecordToExportRecord(rec))
			}
			return records, nil, histogram, nil, false, nil
		}

		// Regular search path.
		const maxBatch = 500
		if q.Limit == 0 || q.Limit > maxBatch {
			q.Limit = maxBatch
		}

		var resume *query.ResumeToken
		if len(resumeToken) > 0 {
			resume, err = server.ProtoToResumeToken(resumeToken)
			if err != nil {
				return nil, nil, nil, nil, false, fmt.Errorf("parse resume token: %w", err)
			}
		}

		searchIter, getToken := eng.Search(ctx, q, resume)
		var records []*gastrologv1.ExportRecord
		for rec, err := range searchIter {
			if err != nil {
				return records, nil, histogram, nil, false, err
			}
			records = append(records, cluster.RecordToExportRecord(rec))
		}
		token := getToken()
		var tokenBytes []byte
		if token != nil {
			tokenBytes = server.ResumeTokenToProto(token)
		}
		return records, nil, histogram, tokenBytes, token != nil, nil
	}
}

// histogramBucketsToProto converts internal histogram buckets to proto type.
func histogramBucketsToProto(buckets []query.HistogramBucket) []*gastrologv1.HistogramBucket {
	if len(buckets) == 0 {
		return nil
	}
	out := make([]*gastrologv1.HistogramBucket, len(buckets))
	for i, b := range buckets {
		out[i] = &gastrologv1.HistogramBucket{
			TimestampMs: b.TimestampMs,
			Count:       b.Count,
			GroupCounts: b.GroupCounts,
		}
	}
	return out
}

// newExplainExecutor creates a cluster.ExplainExecutor that runs explain on
// local vaults for ForwardExplain RPCs received from peer nodes. Scopes the
// query to the requested vault IDs and sets the node_id on each ChunkPlan.
func newExplainExecutor(o *orchestrator.Orchestrator, localNodeID string) cluster.ExplainExecutor {
	return func(ctx context.Context, vaultIDs []uuid.UUID, queryExpr string) ([]*gastrologv1.ChunkPlan, int32, error) {
		var allChunks []*gastrologv1.ChunkPlan
		var totalChunks int32

		for _, vid := range vaultIDs {
			scopedExpr := fmt.Sprintf("vault_id=%s %s", vid, queryExpr)
			q, _, err := server.ParseExpression(scopedExpr)
			if err != nil {
				return nil, 0, fmt.Errorf("parse query for vault %s: %w", vid, err)
			}

			eng := o.MultiVaultQueryEngine()
			plan, err := eng.Explain(ctx, q)
			if err != nil {
				return nil, 0, fmt.Errorf("explain vault %s: %w", vid, err)
			}

			totalChunks += int32(plan.TotalChunks) //nolint:gosec // G115: chunk count fits in int32
			for _, cp := range plan.ChunkPlans {
				chunkPlan := &gastrologv1.ChunkPlan{
					VaultId:          cp.VaultID.String(),
					ChunkId:          cp.ChunkID.String(),
					Sealed:           cp.Sealed,
					RecordCount:      int64(cp.RecordCount),
					ScanMode:         cp.ScanMode,
					EstimatedRecords: int64(cp.EstimatedScan),
					RuntimeFilters:   []string{cp.RuntimeFilter},
					Steps:            server.PipelineStepsToProto(cp.Pipeline),
					SkipReason:       cp.SkipReason,
					NodeId:           localNodeID,
				}
				if !cp.StartTS.IsZero() {
					chunkPlan.StartTs = timestamppb.New(cp.StartTS)
				}
				if !cp.EndTS.IsZero() {
					chunkPlan.EndTs = timestamppb.New(cp.EndTS)
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
	return func(ctx context.Context, vaultIDs []uuid.UUID, queryExpr string) (iter.Seq2[chunk.Record, error], error) {
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

		eng := o.MultiVaultQueryEngine()
		return eng.Follow(ctx, q), nil
	}
}

func newContextExecutor(o *orchestrator.Orchestrator) cluster.ContextExecutor {
	return func(ctx context.Context, vaultID uuid.UUID, chunkID chunk.ChunkID, pos uint64, before, after int) ([]chunk.Record, chunk.Record, []chunk.Record, error) {
		eng := o.MultiVaultQueryEngine()
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
	return func(ctx context.Context, vaultID uuid.UUID) ([]*gastrologv1.ChunkMeta, error) {
		metas, err := o.ListChunkMetas(vaultID)
		if err != nil {
			return nil, err
		}
		out := make([]*gastrologv1.ChunkMeta, 0, len(metas))
		for _, m := range metas {
			out = append(out, server.ChunkMetaToProto(m))
		}
		return out, nil
	}
}

func newGetIndexesExecutor(o *orchestrator.Orchestrator) cluster.GetIndexesExecutor {
	return func(ctx context.Context, vaultID uuid.UUID, chunkID chunk.ChunkID) (*gastrologv1.GetIndexesResponse, error) {
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
	return func(_ context.Context, vaultID uuid.UUID) (*gastrologv1.ValidateVaultResponse, error) {
		metas, err := o.ListChunkMetas(vaultID)
		if err != nil {
			return nil, err
		}
		return server.ValidateVaultLocal(o, vaultID, metas), nil
	}
}

func newGetChunkExecutor(o *orchestrator.Orchestrator) cluster.GetChunkExecutor {
	return func(_ context.Context, vaultID uuid.UUID, chunkID chunk.ChunkID) (*gastrologv1.ChunkMeta, error) {
		meta, err := o.GetChunkMeta(vaultID, chunkID)
		if err != nil {
			return nil, err
		}
		return server.ChunkMetaToProto(meta), nil
	}
}

func newAnalyzeChunkExecutor(o *orchestrator.Orchestrator) cluster.AnalyzeChunkExecutor {
	return func(_ context.Context, vaultID uuid.UUID, chunkIDStr string) ([]*gastrologv1.ChunkAnalysis, error) {
		a, err := o.NewAnalyzer(vaultID)
		if err != nil {
			return nil, err
		}
		var analyses []analyzer.ChunkAnalysis
		if chunkIDStr != "" {
			chunkID, parseErr := chunk.ParseChunkID(chunkIDStr)
			if parseErr != nil {
				return nil, parseErr
			}
			analysis, analyzeErr := a.AnalyzeChunk(chunkID)
			if analyzeErr != nil {
				return nil, analyzeErr
			}
			analyses = []analyzer.ChunkAnalysis{*analysis}
		} else {
			agg, aggErr := a.AnalyzeAll()
			if aggErr != nil {
				return nil, aggErr
			}
			analyses = agg.Chunks
		}
		out := make([]*gastrologv1.ChunkAnalysis, 0, len(analyses))
		for _, ca := range analyses {
			out = append(out, server.ChunkAnalysisToProto(ca))
		}
		return out, nil
	}
}

func newSealVaultExecutor(o *orchestrator.Orchestrator) cluster.SealVaultExecutor {
	return func(_ context.Context, vaultID uuid.UUID) error {
		return o.SealActive(vaultID)
	}
}

func newReindexVaultExecutor(o *orchestrator.Orchestrator) cluster.ReindexVaultExecutor {
	return func(_ context.Context, vaultID uuid.UUID) (string, error) {
		if !o.VaultExists(vaultID) {
			return "", errors.New("vault not found")
		}
		jobName := "reindex:" + vaultID.String()
		jobID := o.Scheduler().Submit(jobName, func(ctx context.Context, job *orchestrator.JobProgress) {
			metas, err := o.ListChunkMetas(vaultID)
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
