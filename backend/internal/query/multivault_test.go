package query_test

import (
	"context"
	"fmt"
	"gastrolog/internal/glid"
	"strconv"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	"gastrolog/internal/manifest"
	"gastrolog/internal/memtest"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
)

type testRegistry struct {
	vaults map[glid.GLID]struct {
		cm chunk.ChunkManager
		im index.IndexManager
	}
	// streamed lets a test mark specific chunks as TransitionStreamed for a
	// given vault. Empty map / nil means "no streamed chunks", which is the
	// default for the bulk of the test suite.
	streamed map[glid.GLID]map[chunk.ChunkID]bool
}

func (r *testRegistry) ListVaults() []glid.GLID {
	var keys []glid.GLID
	for k := range r.vaults {
		keys = append(keys, k)
	}
	return keys
}

func (r *testRegistry) ChunkManager(vaultID glid.GLID) chunk.ChunkManager {
	if s, ok := r.vaults[vaultID]; ok {
		return s.cm
	}
	return nil
}

func (r *testRegistry) IndexManager(vaultID glid.GLID) index.IndexManager {
	if s, ok := r.vaults[vaultID]; ok {
		return s.im
	}
	return nil
}

// TransitionStreamedChunks: tests that don't model transitions can leave
// the streamed map nil. Tests exercising the transition-window filter
// populate r.streamed[vaultID].
func (r *testRegistry) TransitionStreamedChunks(vaultID glid.GLID) map[chunk.ChunkID]bool {
	if r.streamed == nil {
		return nil
	}
	return r.streamed[vaultID]
}

// Reader returns a projecting Reader so test fakes that don't model an FSM
// still satisfy the manifest.VaultRegistry contract. The projection reads
// each vault's chunk manager List() / Meta() to synthesize ManifestEntries.
func (r *testRegistry) Reader() manifest.Reader { return manifest.NewProjectingReader(r) }

// IndexReader returns a tier-walking IndexReader that dispatches to each
// vault's chunk/index manager. testRegistry's "vault" map is effectively
// flat (no tier hierarchy), so the lookup is just a linear walk.
func (r *testRegistry) IndexReader() manifest.IndexReader { return &testIndexReader{r: r} }

type testIndexReader struct{ r *testRegistry }

func (ir *testIndexReader) FindIngestRank(id chunk.ChunkID, ts time.Time) (uint64, bool) {
	cm, im := ir.r.lookupManagers(id)
	if cm != nil {
		if rank, found, err := cm.FindIngestEntryIndex(id, ts); err == nil && found {
			return rank, true
		}
	}
	if im != nil {
		if rank, found, err := im.FindIngestEntryIndex(id, ts); err == nil && found {
			return rank, true
		}
	}
	return 0, false
}

func (ir *testIndexReader) FindIngestPos(id chunk.ChunkID, ts time.Time) (uint64, bool) {
	cm, im := ir.r.lookupManagers(id)
	if cm != nil {
		if pos, found, err := cm.FindIngestStartPosition(id, ts); err == nil && found {
			return pos, true
		}
	}
	if im != nil {
		if pos, found, err := im.FindIngestStartPosition(id, ts); err == nil && found {
			return pos, true
		}
	}
	return 0, false
}

func (r *testRegistry) lookupManagers(id chunk.ChunkID) (chunk.ChunkManager, index.IndexManager) {
	for _, s := range r.vaults {
		if s.cm == nil {
			continue
		}
		if _, err := s.cm.Meta(id); err == nil {
			return s.cm, s.im
		}
	}
	return nil, nil
}

func TestMultiVaultSearch(t *testing.T) {
	reg := &testRegistry{
		vaults: make(map[glid.GLID]struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}),
	}

	// Create two vaults
	for range 2 {
		vaultID := glid.New()
		s := memtest.MustNewVault(t, chunkmem.Config{
			RotationPolicy: chunk.NewRecordCountPolicy(1000),
		})

		// Add some records
		t0 := time.Now()
		for i := range 5 {
			s.CM.Append(chunk.Record{
				IngestTS: t0.Add(time.Duration(i) * time.Second),
				Raw:      fmt.Appendf(nil, "vault-%s-record-%d", vaultID, i),
			})
		}
		s.CM.Seal()

		reg.vaults[vaultID] = struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}{s.CM, s.IM}
	}

	// Create multi-vault engine
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

// TestMultiVaultDedup verifies that the | dedup pipeline operator removes
// duplicate records from multi-vault routing, while plain Search returns all
// copies (dedup is now opt-in, not automatic).
func TestMultiVaultDedup(t *testing.T) {
	reg := &testRegistry{
		vaults: make(map[glid.GLID]struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}),
	}

	// Create 3 vaults with identical records (simulating route fan-out).
	// Each record has a distinct EventID so dedup can identify duplicates
	// across vaults while keeping unique records.
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	ingesterID := glid.New()
	records := []chunk.Record{
		{IngestTS: t0, EventID: chunk.EventID{IngesterID: ingesterID, IngestTS: t0, IngestSeq: 0}, Raw: []byte("line A")},
		{IngestTS: t0.Add(1 * time.Second), EventID: chunk.EventID{IngesterID: ingesterID, IngestTS: t0.Add(1 * time.Second), IngestSeq: 1}, Raw: []byte("line B")},
		{IngestTS: t0.Add(2 * time.Second), EventID: chunk.EventID{IngesterID: ingesterID, IngestTS: t0.Add(2 * time.Second), IngestSeq: 2}, Raw: []byte("line C")},
	}

	for range 3 {
		vaultID := glid.New()
		s := memtest.MustNewVault(t, chunkmem.Config{
			RotationPolicy: chunk.NewRecordCountPolicy(1000),
		})
		for _, rec := range records {
			s.CM.Append(rec)
		}
		s.CM.Seal()

		reg.vaults[vaultID] = struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}{s.CM, s.IM}
	}

	eng := query.NewWithRegistry(reg, nil)

	// Without dedup: all 9 records come back (3 records × 3 vaults).
	iter, _ := eng.Search(context.Background(), query.Query{}, nil)
	count := 0
	for _, err := range iter {
		if err != nil {
			t.Fatalf("Error: %v", err)
		}
		count++
	}
	if count != 9 {
		t.Errorf("forward without dedup: expected 9 records, got %d", count)
	}

	// With | dedup pipeline: only 3 unique records.
	pipeline := &querylang.Pipeline{
		Pipes: []querylang.PipeOp{&querylang.DedupOp{}},
	}
	result, err := eng.RunPipeline(context.Background(), query.Query{}, pipeline)
	if err != nil {
		t.Fatalf("RunPipeline error: %v", err)
	}
	if len(result.Records) != 3 {
		t.Errorf("forward with | dedup: expected 3 unique records, got %d", len(result.Records))
	}
}

// TestRunPipelineIgnoresIncomingLimit verifies that RunPipeline clears the
// incoming query limit (e.g. from proto-level pagination) so stats pipelines
// process all matching records, not just the first page.
func TestRunPipelineIgnoresIncomingLimit(t *testing.T) {
	vaultID := glid.New()
	s := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})

	// Insert 50 records spread over 50 minutes.
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 50 {
		ts := t0.Add(time.Duration(i) * time.Minute)
		s.CM.Append(chunk.Record{
			WriteTS:  ts,
			IngestTS: ts,
			Attrs:    chunk.Attributes{"level": "info"},
			Raw:      fmt.Appendf(nil, "record-%d", i),
		})
	}
	s.CM.Seal()

	reg := &testRegistry{
		vaults: map[glid.GLID]struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}{
			vaultID: {s.CM, s.IM},
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

func TestPipelineNeedsGlobalRecords(t *testing.T) {
	tests := []struct {
		name string
		ops  []querylang.PipeOp
		want bool
	}{
		{
			name: "head before stats",
			ops: []querylang.PipeOp{
				&querylang.HeadOp{N: 10},
				&querylang.StatsOp{Aggs: []querylang.AggExpr{{Func: "count"}}},
			},
			want: true,
		},
		{
			name: "tail before stats",
			ops: []querylang.PipeOp{
				&querylang.TailOp{N: 10},
				&querylang.StatsOp{Aggs: []querylang.AggExpr{{Func: "count"}}},
			},
			want: true,
		},
		{
			name: "slice before stats",
			ops: []querylang.PipeOp{
				&querylang.SliceOp{Start: 0, End: 10},
				&querylang.StatsOp{Aggs: []querylang.AggExpr{{Func: "count"}}},
			},
			want: true,
		},
		{
			name: "stats alone",
			ops: []querylang.PipeOp{
				&querylang.StatsOp{Aggs: []querylang.AggExpr{{Func: "count"}}},
			},
			want: false,
		},
		{
			name: "head alone",
			ops: []querylang.PipeOp{
				&querylang.HeadOp{N: 10},
			},
			want: false,
		},
		{
			name: "where before stats",
			ops: []querylang.PipeOp{
				&querylang.WhereOp{},
				&querylang.StatsOp{Aggs: []querylang.AggExpr{{Func: "count"}}},
			},
			want: false,
		},
		{
			name: "head before timechart",
			ops: []querylang.PipeOp{
				&querylang.HeadOp{N: 10},
				&querylang.TimechartOp{N: 10},
			},
			want: true,
		},
		{
			name: "stats with avg is non-distributive",
			ops: []querylang.PipeOp{
				&querylang.StatsOp{Aggs: []querylang.AggExpr{
					{Func: "avg", Arg: &querylang.FieldRef{Name: "duration"}},
				}},
			},
			want: true,
		},
		{
			name: "stats with dcount is non-distributive",
			ops: []querylang.PipeOp{
				&querylang.StatsOp{Aggs: []querylang.AggExpr{
					{Func: "dcount", Arg: &querylang.FieldRef{Name: "host"}},
				}},
			},
			want: true,
		},
		{
			name: "stats with median is non-distributive",
			ops: []querylang.PipeOp{
				&querylang.StatsOp{Aggs: []querylang.AggExpr{
					{Func: "median", Arg: &querylang.FieldRef{Name: "latency"}},
				}},
			},
			want: true,
		},
		{
			name: "stats with count and sum are distributive",
			ops: []querylang.PipeOp{
				&querylang.StatsOp{Aggs: []querylang.AggExpr{
					{Func: "count"},
					{Func: "sum", Arg: &querylang.FieldRef{Name: "bytes"}},
				}},
			},
			want: false,
		},
		{
			name: "stats with mix of distributive and non-distributive",
			ops: []querylang.PipeOp{
				&querylang.StatsOp{Aggs: []querylang.AggExpr{
					{Func: "count"},
					{Func: "avg", Arg: &querylang.FieldRef{Name: "duration"}},
				}},
			},
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pipeline := &querylang.Pipeline{Pipes: tc.ops}
			got := query.PipelineNeedsGlobalRecords(pipeline)
			if got != tc.want {
				t.Errorf("PipelineNeedsGlobalRecords = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRunPipelineOnRecords(t *testing.T) {
	// Set up a single-vault engine with 20 local records.
	vaultID := glid.New()
	s := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})

	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 20 {
		ts := t0.Add(time.Duration(i) * time.Minute)
		s.CM.Append(chunk.Record{
			WriteTS:  ts,
			IngestTS: ts,
			Raw:      fmt.Appendf(nil, "local-%d", i),
		})
	}
	s.CM.Seal()

	reg := &testRegistry{
		vaults: map[glid.GLID]struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}{
			vaultID: {s.CM, s.IM},
		},
	}
	eng := query.NewWithRegistry(reg, nil)

	// Create 30 "remote" records interleaved with local ones.
	var extra []chunk.Record
	for i := range 30 {
		ts := t0.Add(time.Duration(i)*time.Minute + 30*time.Second)
		extra = append(extra, chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "remote-%d", i),
		})
	}

	// Pipeline: | head 10 | stats count — should count exactly 10.
	pipeline := &querylang.Pipeline{
		Pipes: []querylang.PipeOp{
			&querylang.HeadOp{N: 10},
			&querylang.StatsOp{Aggs: []querylang.AggExpr{{Func: "count"}}},
		},
	}

	q := query.Query{
		Start: t0,
		End:   t0.Add(60 * time.Minute),
	}

	result, err := eng.RunPipelineOnRecords(context.Background(), q, pipeline, extra)
	if err != nil {
		t.Fatalf("RunPipelineOnRecords: %v", err)
	}
	if result.Table == nil {
		t.Fatal("expected table result")
	}
	if len(result.Table.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Table.Rows))
	}

	countVal := result.Table.Rows[0][0]
	count, err := strconv.Atoi(countVal)
	if err != nil {
		t.Fatalf("parsing count %q: %v", countVal, err)
	}
	if count != 10 {
		t.Errorf("stats count = %d, want 10 (head should cap merged records)", count)
	}
}
