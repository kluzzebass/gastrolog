package query_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	"gastrolog/internal/memtest"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
)

// newStatsTestEngine builds an engine over a single vault holding n records:
// raw "rec-%04d", attrs level (info/error alternating) and value (i).
func newStatsTestEngine(t *testing.T, n int) *query.Engine {
	t.Helper()
	s := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(100_000),
	})
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range n {
		ts := t0.Add(time.Duration(i) * time.Second)
		level := "info"
		if i%2 == 1 {
			level = "error"
		}
		s.CM.Append(chunk.Record{
			WriteTS:  ts,
			IngestTS: ts,
			Attrs:    chunk.Attributes{"level": level, "value": fmt.Sprintf("%d", i)},
			Raw:      fmt.Appendf(nil, "rec-%04d", i),
		})
	}
	s.CM.Seal()

	reg := &testRegistry{
		vaults: map[glid.GLID]struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}{
			glid.New(): {s.CM, s.IM},
		},
	}
	return query.NewWithRegistry(reg, nil)
}

func runStatsPipeline(t *testing.T, eng *query.Engine, pipes []querylang.PipeOp) *query.TableResult {
	t.Helper()
	result, err := eng.RunPipeline(context.Background(), query.Query{}, &querylang.Pipeline{Pipes: pipes})
	if err != nil {
		t.Fatalf("RunPipeline: %v", err)
	}
	if result.Table == nil {
		t.Fatal("expected table result")
	}
	return result.Table
}

func assertSameTable(t *testing.T, got, want *query.TableResult) {
	t.Helper()
	if len(got.Columns) != len(want.Columns) {
		t.Fatalf("columns: got %v, want %v", got.Columns, want.Columns)
	}
	for i := range got.Columns {
		if got.Columns[i] != want.Columns[i] {
			t.Fatalf("columns: got %v, want %v", got.Columns, want.Columns)
		}
	}
	if len(got.Rows) != len(want.Rows) {
		t.Fatalf("rows: got %d, want %d", len(got.Rows), len(want.Rows))
	}
	for i := range got.Rows {
		for j := range got.Rows[i] {
			if got.Rows[i][j] != want.Rows[i][j] {
				t.Errorf("row %d col %d: got %q, want %q", i, j, got.Rows[i][j], want.Rows[i][j])
			}
		}
	}
}

// TestStreamingStatsMatchesBatchStats verifies that a sortless stats pipeline
// (streamed straight into the aggregator) produces exactly the same table as
// the materializing path (forced by an order-preserving sort on value, which
// reproduces the stream order).
func TestStreamingStatsMatchesBatchStats(t *testing.T) {
	eng := newStatsTestEngine(t, 60)

	statsOp := func() *querylang.StatsOp {
		return &querylang.StatsOp{
			Aggs: []querylang.AggExpr{
				{Func: "count"},
				{Func: "sum", Arg: &querylang.FieldRef{Name: "value"}},
				{Func: "first", Arg: &querylang.FieldRef{Name: "raw"}},
				{Func: "last", Arg: &querylang.FieldRef{Name: "raw"}},
			},
			Groups: []querylang.GroupExpr{{Field: &querylang.FieldRef{Name: "level"}}},
		}
	}

	// Streaming path: no pre-ops beyond a where.
	whereInfoOrError := func() querylang.PipeOp {
		return &querylang.WhereOp{Expr: &querylang.PredicateExpr{Kind: querylang.PredKeyExists, Key: "level"}}
	}
	streamed := runStatsPipeline(t, eng, []querylang.PipeOp{whereInfoOrError(), statsOp()})

	// Batch path: an ascending numeric sort on value reproduces stream
	// order but forces full materialization before aggregation.
	batched := runStatsPipeline(t, eng, []querylang.PipeOp{
		whereInfoOrError(),
		&querylang.SortOp{Fields: []querylang.SortField{{Name: "value"}}},
		statsOp(),
	})

	assertSameTable(t, streamed, batched)

	// Sanity: 30 info + 30 error.
	if len(streamed.Rows) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(streamed.Rows))
	}
}

// TestStatsWithoutSortStreams verifies plain stats correctness on the
// streaming path (count, group counts).
func TestStatsWithoutSortStreams(t *testing.T) {
	eng := newStatsTestEngine(t, 50)
	table := runStatsPipeline(t, eng, []querylang.PipeOp{
		&querylang.StatsOp{
			Aggs:   []querylang.AggExpr{{Func: "count"}},
			Groups: []querylang.GroupExpr{{Field: &querylang.FieldRef{Name: "level"}}},
		},
	})
	if len(table.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(table.Rows))
	}
	for _, row := range table.Rows {
		if row[1] != "25" {
			t.Errorf("group %q count = %s, want 25", row[0], row[1])
		}
	}
}

// TestStatsAfterSortAppliesOrder verifies that a descending sort before an
// order-sensitive aggregation (first) actually reorders the records feeding
// the aggregator — the materializing fallback, not the streaming path.
func TestStatsAfterSortAppliesOrder(t *testing.T) {
	eng := newStatsTestEngine(t, 40)
	table := runStatsPipeline(t, eng, []querylang.PipeOp{
		&querylang.SortOp{Fields: []querylang.SortField{{Name: "value", Desc: true}}},
		&querylang.StatsOp{
			Aggs: []querylang.AggExpr{{Func: "first", Arg: &querylang.FieldRef{Name: "raw"}}},
		},
	})
	if len(table.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(table.Rows))
	}
	if table.Rows[0][0] != "rec-0039" {
		t.Errorf("first(raw) after sort -value = %q, want %q", table.Rows[0][0], "rec-0039")
	}
}

// TestStatsWithHeadBeforeStats verifies head caps the records feeding a
// streamed aggregation and the scan stops early.
func TestStatsWithHeadBeforeStats(t *testing.T) {
	eng := newStatsTestEngine(t, 50)
	table := runStatsPipeline(t, eng, []querylang.PipeOp{
		&querylang.HeadOp{N: 7},
		&querylang.StatsOp{Aggs: []querylang.AggExpr{{Func: "count"}}},
	})
	if len(table.Rows) != 1 || table.Rows[0][0] != "7" {
		t.Fatalf("head 7 | stats count: got %v, want [[7]]", table.Rows)
	}
}

// TestStatsWithDedupBeforeStats verifies dedup works on the streaming
// aggregation path (records share EventIDs across fan-out copies).
func TestStatsWithDedupBeforeStats(t *testing.T) {
	// Two vaults with identical records (route fan-out): dedup | stats count
	// must count each event once.
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	ingesterID := glid.New()
	reg := &testRegistry{
		vaults: make(map[glid.GLID]struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}),
	}
	for range 2 {
		s := memtest.MustNewVault(t, chunkmem.Config{
			RotationPolicy: chunk.NewRecordCountPolicy(1000),
		})
		for i := range 10 {
			ts := t0.Add(time.Duration(i) * time.Second)
			s.CM.Append(chunk.Record{
				WriteTS:  ts,
				IngestTS: ts,
				EventID:  chunk.EventID{IngesterID: ingesterID, IngestTS: ts, IngestSeq: uint32(i)},
				Raw:      fmt.Appendf(nil, "line-%d", i),
			})
		}
		s.CM.Seal()
		reg.vaults[glid.New()] = struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}{s.CM, s.IM}
	}
	eng := query.NewWithRegistry(reg, nil)

	table := runStatsPipeline(t, eng, []querylang.PipeOp{
		&querylang.DedupOp{},
		&querylang.StatsOp{Aggs: []querylang.AggExpr{{Func: "count"}}},
	})
	if len(table.Rows) != 1 || table.Rows[0][0] != "10" {
		t.Fatalf("dedup | stats count: got %v, want [[10]]", table.Rows)
	}
}
