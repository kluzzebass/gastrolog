package query

import (
	"context"
	"fmt"
	"maps"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/querylang"
)

// makeSortRecords creates n records with unique raw messages, a numeric
// attr "n" cycling mod `mod` (to exercise sort ties and stable ordering),
// and distinct EventIDs (for dedup tests).
func makeSortRecords(n, mod int) []chunk.Record {
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	ingesterID := glid.New()
	records := make([]chunk.Record, n)
	for i := range n {
		ts := t0.Add(time.Duration(i) * time.Second)
		records[i] = chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			EventID:  chunk.EventID{IngesterID: ingesterID, IngestTS: ts, IngestSeq: uint32(i)},
			Raw:      fmt.Appendf(nil, "msg-%04d", i),
			Attrs:    chunk.Attributes{"n": fmt.Sprintf("%d", i%mod)},
		}
	}
	return records
}

// batchReference runs ops through the full-materialization batch path, which
// is the semantic reference the bounded paths must match exactly.
func batchReference(t *testing.T, records []chunk.Record, ops []querylang.PipeOp) []chunk.Record {
	t.Helper()
	copies := make([]chunk.Record, len(records))
	for i, r := range records {
		copies[i] = r.Copy()
	}
	out, err := applyBatchOps(context.Background(), copies, ops, nil)
	if err != nil {
		t.Fatalf("batch reference: %v", err)
	}
	return out
}

// assertSameRecords fails unless got and want are identical record sequences
// (raw, attrs, timestamps), in the same order.
func assertSameRecords(t *testing.T, got, want []chunk.Record) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("record count: got %d, want %d", len(got), len(want))
	}
	for i := range got {
		if string(got[i].Raw) != string(want[i].Raw) {
			t.Errorf("record %d raw: got %q, want %q", i, got[i].Raw, want[i].Raw)
		}
		if !got[i].WriteTS.Equal(want[i].WriteTS) {
			t.Errorf("record %d write_ts: got %v, want %v", i, got[i].WriteTS, want[i].WriteTS)
		}
		if !maps.Equal(got[i].Attrs, want[i].Attrs) {
			t.Errorf("record %d attrs: got %v, want %v", i, got[i].Attrs, want[i].Attrs)
		}
	}
}

// runOpsEquivalence asserts applyRecordOpsLimit output equals the batch
// reference. Both sides get the caller's post-pipeline truncation (as
// RunPipeline applies it): applyRecordOpsLimit may exploit the limit to
// bound collection but is not required to truncate itself.
func runOpsEquivalence(t *testing.T, records []chunk.Record, ops []querylang.PipeOp, implicitLimit int) {
	t.Helper()
	got, err := applyRecordOpsLimit(context.Background(), recordIter(records), ops, nil, implicitLimit)
	if err != nil {
		t.Fatalf("applyRecordOpsLimit: %v", err)
	}
	want := batchReference(t, records, ops)
	if implicitLimit > 0 {
		if len(got) > implicitLimit {
			got = got[:implicitLimit]
		}
		if len(want) > implicitLimit {
			want = want[:implicitLimit]
		}
	}
	assertSameRecords(t, got, want)
}

func TestBoundedSortHeadEquivalence(t *testing.T) {
	records := makeSortRecords(500, 7) // ties on n
	ops := []querylang.PipeOp{
		&querylang.SortOp{Fields: []querylang.SortField{{Name: "n"}}},
		&querylang.HeadOp{N: 10},
	}
	runOpsEquivalence(t, records, ops, 0)
}

func TestBoundedSortTailEquivalence(t *testing.T) {
	records := makeSortRecords(500, 7)
	ops := []querylang.PipeOp{
		&querylang.SortOp{Fields: []querylang.SortField{{Name: "n"}}},
		&querylang.TailOp{N: 13},
	}
	runOpsEquivalence(t, records, ops, 0)
}

func TestBoundedSortSliceEquivalence(t *testing.T) {
	records := makeSortRecords(500, 7)
	ops := []querylang.PipeOp{
		&querylang.SortOp{Fields: []querylang.SortField{{Name: "n"}}},
		&querylang.SliceOp{Start: 5, End: 25},
	}
	runOpsEquivalence(t, records, ops, 0)
}

func TestBoundedSortDescendingEquivalence(t *testing.T) {
	records := makeSortRecords(300, 5)
	ops := []querylang.PipeOp{
		&querylang.SortOp{Fields: []querylang.SortField{{Name: "n", Desc: true}}},
		&querylang.HeadOp{N: 12},
	}
	runOpsEquivalence(t, records, ops, 0)
}

func TestBoundedSortMultiFieldEquivalence(t *testing.T) {
	records := makeSortRecords(300, 5)
	ops := []querylang.PipeOp{
		&querylang.SortOp{Fields: []querylang.SortField{{Name: "n"}, {Name: "raw", Desc: true}}},
		&querylang.TailOp{N: 9},
	}
	runOpsEquivalence(t, records, ops, 0)
}

func TestBoundedSortWithPreWhereEquivalence(t *testing.T) {
	records := makeSortRecords(400, 4)
	ops := []querylang.PipeOp{
		&querylang.WhereOp{Expr: &querylang.PredicateExpr{Kind: querylang.PredKV, Key: "n", Value: "2"}},
		&querylang.SortOp{Fields: []querylang.SortField{{Name: "raw", Desc: true}}},
		&querylang.HeadOp{N: 6},
	}
	runOpsEquivalence(t, records, ops, 0)
}

func TestBoundedSortWithTrailingOpsEquivalence(t *testing.T) {
	records := makeSortRecords(200, 6)
	ops := []querylang.PipeOp{
		&querylang.SortOp{Fields: []querylang.SortField{{Name: "n"}}},
		&querylang.HeadOp{N: 8},
		&querylang.RenameOp{Renames: []querylang.RenameMapping{{Old: "n", New: "bucket"}}},
	}
	runOpsEquivalence(t, records, ops, 0)
}

func TestBoundedSortCapAfterEvalEquivalence(t *testing.T) {
	// eval between sort and head preserves count and order, so the cap
	// still bounds the sort's working set.
	records := makeSortRecords(200, 6)
	ops := []querylang.PipeOp{
		&querylang.SortOp{Fields: []querylang.SortField{{Name: "n"}}},
		&querylang.EvalOp{Assignments: []querylang.EvalAssignment{
			{Field: "doubled", Expr: &querylang.ArithExpr{
				Left:  &querylang.FieldRef{Name: "n"},
				Op:    querylang.ArithMul,
				Right: &querylang.NumberLit{Value: "2"},
			}},
		}},
		&querylang.HeadOp{N: 5},
	}
	runOpsEquivalence(t, records, ops, 0)
}

func TestSortWithImplicitLimitEquivalence(t *testing.T) {
	// Sort with no explicit cap: the caller's implicit limit (origLimit)
	// acts as a trailing head and bounds the working set.
	records := makeSortRecords(400, 7)
	ops := []querylang.PipeOp{
		&querylang.SortOp{Fields: []querylang.SortField{{Name: "n"}}},
	}
	runOpsEquivalence(t, records, ops, 15)
}

func TestSortImplicitLimitWithTrailingEvalEquivalence(t *testing.T) {
	records := makeSortRecords(300, 7)
	ops := []querylang.PipeOp{
		&querylang.SortOp{Fields: []querylang.SortField{{Name: "n", Desc: true}}},
		&querylang.EvalOp{Assignments: []querylang.EvalAssignment{
			{Field: "copy", Expr: &querylang.FieldRef{Name: "n"}},
		}},
	}
	runOpsEquivalence(t, records, ops, 10)
}

func TestSortThenWhereFallsBackAndMatches(t *testing.T) {
	// A filter after the sort means the implicit limit cannot bound the
	// sort's input; the fallback must still produce reference output.
	records := makeSortRecords(200, 4)
	ops := []querylang.PipeOp{
		&querylang.SortOp{Fields: []querylang.SortField{{Name: "raw", Desc: true}}},
		&querylang.WhereOp{Expr: &querylang.PredicateExpr{Kind: querylang.PredKV, Key: "n", Value: "1"}},
	}
	fired := false
	testBoundedSortObserver = func(int) { fired = true }
	defer func() { testBoundedSortObserver = nil }()

	runOpsEquivalence(t, records, ops, 10)
	if fired {
		t.Error("bounded-sort path must not be used when a filter follows the sort")
	}
}

func TestSortlessImplicitLimitEquivalence(t *testing.T) {
	records := makeSortRecords(400, 2)
	ops := []querylang.PipeOp{
		&querylang.WhereOp{Expr: &querylang.PredicateExpr{Kind: querylang.PredKV, Key: "n", Value: "1"}},
	}
	runOpsEquivalence(t, records, ops, 20)
}

func TestSortlessImplicitLimitStopsEarly(t *testing.T) {
	// Record-count instrumentation: with an implicit limit and only
	// per-record operators, collection must stop once the limit is reached
	// instead of scanning (and materializing) the whole window.
	records := makeSortRecords(10000, 1)
	iterCount := 0
	countingIter := func(yield func(chunk.Record, error) bool) {
		for _, r := range records {
			iterCount++
			if !yield(r, nil) {
				return
			}
		}
	}
	ops := []querylang.PipeOp{
		&querylang.WhereOp{Expr: &querylang.PredicateExpr{Kind: querylang.PredKV, Key: "n", Value: "0"}},
	}
	result, err := applyRecordOpsLimit(context.Background(), countingIter, ops, nil, 25)
	if err != nil {
		t.Fatalf("applyRecordOpsLimit: %v", err)
	}
	if len(result) != 25 {
		t.Fatalf("expected 25 records, got %d", len(result))
	}
	if iterCount > 30 {
		t.Errorf("expected early exit after ~25 records, iterated %d", iterCount)
	}
}

func TestDedupWithoutCapEquivalence(t *testing.T) {
	// Duplicate every record 3× (same EventID, as in multi-vault fan-out).
	base := makeSortRecords(50, 5)
	var records []chunk.Record
	for range 3 {
		for _, r := range base {
			records = append(records, r.Copy())
		}
	}
	ops := []querylang.PipeOp{&querylang.DedupOp{}}
	runOpsEquivalence(t, records, ops, 0)

	// And with an implicit limit (streaming-limit path).
	runOpsEquivalence(t, records, ops, 7)
}

func TestHeadMidPipelineMatchesBatch(t *testing.T) {
	// head before a filter must truncate at its position in the pipeline,
	// exactly like the batch path: head 10 | where n=0 | tail 3 keeps the
	// even records among the first 10, then the last 3 of those.
	records := makeSortRecords(100, 2) // n alternates 0/1
	ops := []querylang.PipeOp{
		&querylang.HeadOp{N: 10},
		&querylang.WhereOp{Expr: &querylang.PredicateExpr{Kind: querylang.PredKV, Key: "n", Value: "0"}},
		&querylang.TailOp{N: 3},
	}
	runOpsEquivalence(t, records, ops, 0)
}

func TestCapBeforeSortStreamsAndMatches(t *testing.T) {
	// tail N | sort: the cap precedes the sort, so the ring buffer bounds
	// collection and the sort runs batch-wise on N records.
	records := makeSortRecords(500, 7)
	ops := []querylang.PipeOp{
		&querylang.TailOp{N: 20},
		&querylang.SortOp{Fields: []querylang.SortField{{Name: "n"}}},
	}
	runOpsEquivalence(t, records, ops, 0)
}

func TestBoundedSortDoesNotMaterialize(t *testing.T) {
	// A large window with `sort n | head 10` must keep a bounded working
	// set (≤ 2×N items), never the full record set. Asserted via the
	// collector's high-water mark — no timing involved.
	if testing.Short() {
		t.Skip("30k-record bounded-sort memory test") // slow: large synthetic window
	}
	records := makeSortRecords(30000, 97)
	ops := []querylang.PipeOp{
		&querylang.SortOp{Fields: []querylang.SortField{{Name: "n"}}},
		&querylang.HeadOp{N: 10},
	}
	maxItems := -1
	testBoundedSortObserver = func(n int) { maxItems = n }
	defer func() { testBoundedSortObserver = nil }()

	got, err := applyRecordOps(context.Background(), recordIter(records), ops, nil)
	if err != nil {
		t.Fatalf("applyRecordOps: %v", err)
	}
	if len(got) != 10 {
		t.Fatalf("expected 10 records, got %d", len(got))
	}
	if maxItems < 0 {
		t.Fatal("bounded-sort path was not taken for sort|head")
	}
	if maxItems > 20 {
		t.Errorf("working set reached %d items, want ≤ 20 (2×N)", maxItems)
	}
	want := batchReference(t, records, ops)
	assertSameRecords(t, got, want)
}

func TestBoundedSortEmptyInput(t *testing.T) {
	ops := []querylang.PipeOp{
		&querylang.SortOp{Fields: []querylang.SortField{{Name: "n"}}},
		&querylang.HeadOp{N: 10},
	}
	got, err := applyRecordOps(context.Background(), recordIter(nil), ops, nil)
	if err != nil {
		t.Fatalf("applyRecordOps: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 records, got %d", len(got))
	}
}

func TestBoundedSortFewerRecordsThanCap(t *testing.T) {
	records := makeSortRecords(5, 3)
	ops := []querylang.PipeOp{
		&querylang.SortOp{Fields: []querylang.SortField{{Name: "n"}}},
		&querylang.TailOp{N: 100},
	}
	runOpsEquivalence(t, records, ops, 0)
}

func TestBoundedSortIterError(t *testing.T) {
	errIter := func(yield func(chunk.Record, error) bool) {
		yield(chunk.Record{Raw: []byte("ok")}, nil)
		yield(chunk.Record{}, fmt.Errorf("boom"))
	}
	ops := []querylang.PipeOp{
		&querylang.SortOp{Fields: []querylang.SortField{{Name: "raw"}}},
		&querylang.HeadOp{N: 3},
	}
	if _, err := applyRecordOps(context.Background(), errIter, ops, nil); err == nil {
		t.Fatal("expected iterator error to propagate")
	}
}
