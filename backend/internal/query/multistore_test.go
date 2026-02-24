package query_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	"gastrolog/internal/memtest"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"

	"github.com/google/uuid"
)

type testRegistry struct {
	stores map[uuid.UUID]struct {
		cm chunk.ChunkManager
		im index.IndexManager
	}
}

func (r *testRegistry) ListStores() []uuid.UUID {
	var keys []uuid.UUID
	for k := range r.stores {
		keys = append(keys, k)
	}
	return keys
}

func (r *testRegistry) ChunkManager(storeID uuid.UUID) chunk.ChunkManager {
	if s, ok := r.stores[storeID]; ok {
		return s.cm
	}
	return nil
}

func (r *testRegistry) IndexManager(storeID uuid.UUID) index.IndexManager {
	if s, ok := r.stores[storeID]; ok {
		return s.im
	}
	return nil
}

func TestMultiStoreSearch(t *testing.T) {
	reg := &testRegistry{
		stores: make(map[uuid.UUID]struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}),
	}

	// Create two stores
	for range 2 {
		storeID := uuid.Must(uuid.NewV7())
		s := memtest.MustNewStore(t, chunkmem.Config{
			RotationPolicy: chunk.NewRecordCountPolicy(1000),
		})

		// Add some records
		t0 := time.Now()
		for i := range 5 {
			s.CM.Append(chunk.Record{
				IngestTS: t0.Add(time.Duration(i) * time.Second),
				Raw:      fmt.Appendf(nil, "store-%s-record-%d", storeID, i),
			})
		}
		s.CM.Seal()

		reg.stores[storeID] = struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}{s.CM, s.IM}
	}

	// Create multi-store engine
	eng := query.NewWithRegistry(reg, nil)

	// Run query
	t.Log("Running query...")
	iter, _ := eng.Search(context.Background(), query.Query{}, nil)

	count := 0
	for rec, err := range iter {
		if err != nil {
			t.Fatalf("Error: %v", err)
		}
		t.Logf("Record: %s", rec.Raw)
		count++
	}
	t.Logf("Total: %d records", count)

	if count != 10 {
		t.Errorf("expected 10 records, got %d", count)
	}
}

// TestRunPipelineIgnoresIncomingLimit verifies that RunPipeline clears the
// incoming query limit (e.g. from proto-level pagination) so stats pipelines
// process all matching records, not just the first page.
func TestRunPipelineIgnoresIncomingLimit(t *testing.T) {
	storeID := uuid.Must(uuid.NewV7())
	s := memtest.MustNewStore(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})

	// Insert 50 records spread over 50 minutes using AppendPreserved
	// so WriteTS is respected by the chunk manager.
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 50 {
		ts := t0.Add(time.Duration(i) * time.Minute)
		s.CM.AppendPreserved(chunk.Record{
			WriteTS:  ts,
			IngestTS: ts,
			Attrs:    chunk.Attributes{"level": "info"},
			Raw:      fmt.Appendf(nil, "record-%d", i),
		})
	}
	s.CM.Seal()

	reg := &testRegistry{
		stores: map[uuid.UUID]struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}{
			storeID: {s.CM, s.IM},
		},
	}
	eng := query.NewWithRegistry(reg, nil)

	// Build a stats pipeline: "| stats count"
	pipeline := &querylang.Pipeline{
		Pipes: []querylang.PipeOp{
			&querylang.StatsOp{
				Aggs: []querylang.AggExpr{{Func: "count"}},
			},
		},
	}

	// Set an incoming limit (simulating proto-level pagination limit=10).
	// RunPipeline must clear this so all 50 records are aggregated.
	q := query.Query{
		Start: t0,
		End:   t0.Add(50 * time.Minute),
		Limit: 10,
	}

	result, err := eng.RunPipeline(context.Background(), q, pipeline)
	if err != nil {
		t.Fatalf("RunPipeline: %v", err)
	}
	if result.Table == nil {
		t.Fatal("expected table result")
	}
	if len(result.Table.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Table.Rows))
	}

	// The count should be 50 (all records), not 10 (the incoming limit).
	countVal := result.Table.Rows[0][0]
	count, err := strconv.Atoi(countVal)
	if err != nil {
		t.Fatalf("parsing count %q: %v", countVal, err)
	}
	if count != 50 {
		t.Errorf("stats count = %d, want 50 (incoming limit should be ignored)", count)
	}
}
