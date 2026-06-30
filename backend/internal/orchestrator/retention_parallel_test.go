package orchestrator

import (
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// seedMultipleSealedChunks registers a memory vault, rotates every
// recordsPerChunk records, and seals chunkCount chunks.
func seedMultipleSealedChunks(t *testing.T, orch *Orchestrator, sourceID glid.GLID, chunkCount, recordsPerChunk int) chunk.ChunkManager {
	t.Helper()
	cm, err := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(uint64(recordsPerChunk)),
	})
	if err != nil {
		t.Fatalf("source CM: %v", err)
	}
	orch.RegisterVault(NewVaultFromComponents(sourceID, cm, nil, nil))

	for c := range chunkCount {
		for i := range recordsPerChunk {
			rec := chunk.Record{
				Attrs: chunk.Attributes{"chunk": string(rune('a' + c)), "i": string(rune('0' + i))},
				Raw:   []byte{byte('a' + c), byte('0' + i)},
			}
			if _, _, err := cm.Append(rec); err != nil {
				t.Fatalf("Append chunk %d record %d: %v", c, i, err)
			}
		}
		if err := cm.Seal(); err != nil {
			t.Fatalf("Seal chunk %d: %v", c, err)
		}
	}
	return cm
}

// TestFireRetentionEventFansOutManyRecords verifies parallel submit workers
// still route every record from a large chunk through the pipeline.
func TestFireRetentionEventFansOutManyRecords(t *testing.T) {
	t.Parallel()

	const recordCount = 40
	sourceID := glid.New()
	archiveID := glid.New()

	orch := newRoutingOrch(t, retentionRouteTable(t, sourceID, archiveID))
	sourceCM := seedSealedSourceVault(t, orch, sourceID, recordCount)

	metas, err := sourceCM.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var sealedID chunk.ChunkID
	for _, m := range metas {
		if m.Sealed {
			sealedID = m.ID
			break
		}
	}

	r := &retentionRunner{
		vaultID: sourceID,
		orch:    orch,
		logger:  slog.Default(),
	}
	r.fireRetentionEvent(sealedID)

	waitForRouteStats(t, orch, "all records routed", func(s *RouteStats) bool {
		return s.Routed == recordCount
	})
}

// TestSweepParallelChunkRouteFanOut verifies that a sweep processing multiple
// ripe chunks concurrently still routes every record when disposition=route.
func TestSweepParallelChunkRouteFanOut(t *testing.T) {
	t.Parallel()

	const (
		chunkCount       = 4
		recordsPerChunk  = 5
		expectedRouted   = (chunkCount - 1) * recordsPerChunk // keep newest chunk
	)

	sourceID := glid.New()
	archiveID := glid.New()

	orch := newRoutingOrch(t, retentionRouteTable(t, sourceID, archiveID))
	sourceCM := seedMultipleSealedChunks(t, orch, sourceID, chunkCount, recordsPerChunk)

	r := &retentionRunner{
		vaultID:     sourceID,
		cm:          sourceCM,
		im:          &retentionFakeIndexManager{},
		isLeader:    true,
		orch:        orch,
		logger:      slog.Default(),
		now:         time.Now,
		disposition: system.RetentionDispositionRoute,
		inflight:    make(map[chunk.ChunkID]bool),
	}

	rules := []retentionRule{{policy: chunk.NewCountRetentionPolicy(1)}}
	r.sweep(rules)

	waitForRouteStats(t, orch, "parallel chunk fan-out", func(s *RouteStats) bool {
		return s.Routed == expectedRouted
	})
}

// TestFireRetentionEventPipelineStopped logs submit errors without panicking
// when the pipeline is stopped mid-fan-out.
func TestFireRetentionEventPipelineStopped(t *testing.T) {
	t.Parallel()

	fx := newDispositionFixture(t)
	_ = fx.orch.pipeline.Stop()

	r := &retentionRunner{
		vaultID: fx.sourceID,
		orch:    fx.orch,
		logger:  slog.Default(),
	}
	r.fireRetentionEvent(fx.sealedID)

	time.Sleep(20 * time.Millisecond)
	if s := fx.orch.GetRouteStats(); s.Ingested != 0 {
		t.Errorf("stopped pipeline should not ingest records, got Ingested=%d", s.Ingested)
	}
}
