package app

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
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
// searches for ForwardSearch RPCs received from peer nodes.
func newSearchExecutor(o *orchestrator.Orchestrator) cluster.SearchExecutor {
	return func(ctx context.Context, vaultID uuid.UUID, queryExpr string, _ []byte) ([]*gastrologv1.ExportRecord, []byte, bool, error) {
		scopedExpr := fmt.Sprintf("vault=%s %s", vaultID, queryExpr)

		q, _, err := server.ParseExpression(scopedExpr)
		if err != nil {
			return nil, nil, false, fmt.Errorf("parse query: %w", err)
		}

		const maxBatch = 500
		if q.Limit == 0 || q.Limit > maxBatch {
			q.Limit = maxBatch
		}

		eng := o.MultiVaultQueryEngine()
		iter, _ := eng.Search(ctx, q, nil)
		var records []*gastrologv1.ExportRecord
		for rec, err := range iter {
			if err != nil {
				return records, nil, false, err
			}
			records = append(records, cluster.RecordToExportRecord(rec))
		}
		return records, nil, false, nil
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
