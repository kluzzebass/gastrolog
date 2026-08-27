package query

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	indexmem "gastrolog/internal/index/memory"
	memattr "gastrolog/internal/index/memory/attr"
	memjson "gastrolog/internal/index/memory/json"
	memkv "gastrolog/internal/index/memory/kv"
	memtoken "gastrolog/internal/index/memory/token"
	"gastrolog/internal/querylang"
)

// newBudgetEngine builds a single-vault engine over n records, each carrying a
// unique "id" and "latency" attribute plus a repeating "host". memLimit is the
// working-set ceiling the engine hands to the query it runs.
//
// The vault is built here rather than through memtest because memtest imports
// this package.
func newBudgetEngine(t *testing.T, n int, memLimit int64) *Engine {
	t.Helper()
	cm, err := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(100_000),
	})
	if err != nil {
		t.Fatalf("chunk manager: %v", err)
	}
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	ingesterID := glid.New()
	for i := range n {
		ts := t0.Add(time.Duration(i) * time.Second)
		cm.Append(chunk.Record{
			WriteTS:  ts,
			IngestTS: ts,
			// Distinct events, so dedup actually accumulates state.
			EventID: chunk.EventID{IngesterID: ingesterID, IngestTS: ts, IngestSeq: uint32(i)}, //nolint:gosec // G115: loop index is small and non-negative
			Attrs: chunk.Attributes{
				"id":      fmt.Sprintf("request-%09d", i),
				"latency": fmt.Sprintf("%d", i%997),
				"host":    fmt.Sprintf("host-%02d", i%8),
			},
			Raw: fmt.Appendf(nil, "rec-%09d handled", i),
		})
	}
	cm.Seal()

	tokIdx := memtoken.NewIndexer(cm)
	attrIdx := memattr.NewIndexer(cm)
	kvIdx := memkv.NewIndexer(cm)
	jsonIdx := memjson.NewIndexer(cm)
	im := indexmem.NewManagerWithJSON(
		[]index.Indexer{tokIdx, attrIdx, kvIdx, jsonIdx}, tokIdx, attrIdx, kvIdx, jsonIdx, nil)
	metas, err := cm.List()
	if err != nil {
		t.Fatalf("list chunks: %v", err)
	}
	for _, m := range metas {
		if !m.Sealed {
			continue
		}
		if err := im.BuildIndexes(context.Background(), m.ID); err != nil {
			t.Fatalf("build indexes: %v", err)
		}
	}

	e := New(cm, im, nil)
	e.memLimit = memLimit
	return e
}

// runQuery parses expr and runs it exactly the way a Search request does, so
// the test exercises whichever execution path the pipeline actually selects
// rather than one chosen by the test.
func runQuery(t *testing.T, e *Engine, expr string) (*PipelineResult, error) {
	t.Helper()
	pipeline, err := querylang.ParsePipeline(expr)
	if err != nil {
		t.Fatalf("parse %q: %v", expr, err)
	}
	if len(pipeline.Pipes) == 0 {
		t.Fatalf("expression %q has no pipeline", expr)
	}
	return e.RunPipeline(context.Background(), Query{BoolExpr: pipeline.Filter}, pipeline)
}

// assertMemoryLimit fails unless err is a budget failure naming consumer and
// reporting the ceiling in its message.
func assertMemoryLimit(t *testing.T, err error, consumer string, wantLimit int64) {
	t.Helper()
	if err == nil {
		t.Fatal("expected the query to fail against its memory budget, got success")
	}
	var memErr *MemoryLimitError
	if !errors.As(err, &memErr) {
		t.Fatalf("expected *MemoryLimitError, got %T: %v", err, err)
	}
	if memErr.Consumer != consumer {
		t.Errorf("consumer: got %q, want %q", memErr.Consumer, consumer)
	}
	if memErr.Limit != wantLimit {
		t.Errorf("limit: got %d, want %d", memErr.Limit, wantLimit)
	}
	msg := memErr.Error()
	if !strings.Contains(msg, consumer) {
		t.Errorf("error message %q does not name what overflowed (%q)", msg, consumer)
	}
	if !strings.Contains(msg, fmt.Sprintf("%d MiB", wantLimit>>20)) {
		t.Errorf("error message %q does not state the limit (%d MiB)", msg, wantLimit>>20)
	}
}

// The per-accumulator collections have no structural bound: they grow one
// entry per matching record (or per distinct value), so before the budget any
// of these queries allocated until the node died.
func TestAggregatorStateFailsAtMemoryBudget(t *testing.T) {
	const limit = 64 << 10 // small enough that 20k records overrun it
	tests := []struct {
		name     string
		expr     string
		consumer string
	}{
		{"dcount distinct set", "| stats dcount(id)", consumerDistinctSet},
		{"median sample buffer", "| stats median(latency)", consumerMedianSamples},
		{"values list", "| stats values(id)", consumerValuesList},
		{"group cardinality", "| stats count by id", consumerGroupState},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			eng := newBudgetEngine(t, 20_000, limit)
			_, err := runQuery(t, eng, tc.expr)
			assertMemoryLimit(t, err, tc.consumer, limit)
		})
	}
}

// A stats pipeline whose pre-ops are all per-record runs through the streaming
// aggregation path, which is the path most stats queries take. A budget wired
// only into the materializing path would leave this one unguarded.
func TestStreamingAggregationFailsAtMemoryBudget(t *testing.T) {
	const limit = 64 << 10
	eng := newBudgetEngine(t, 20_000, limit)

	// where + eval are streamable, so RunPipeline feeds the search iterator
	// straight into the aggregator instead of materializing records first.
	_, err := runQuery(t, eng, "| where latency>=0 | eval tag=id | stats dcount(tag)")
	assertMemoryLimit(t, err, consumerDistinctSet, limit)
}

// An uncapped sort buffers every matching record on the coordinating node.
func TestUncappedSortFailsAtMemoryBudget(t *testing.T) {
	const limit = 64 << 10
	eng := newBudgetEngine(t, 20_000, limit)

	_, err := runQuery(t, eng, "| sort latency")
	assertMemoryLimit(t, err, consumerRecordBuffer, limit)
}

// A capped sort keeps a bounded top-N working set, but the cap comes from the
// query, so a cap larger than the node can hold must fail rather than fill it.
func TestCappedSortFailsAtMemoryBudget(t *testing.T) {
	const limit = 64 << 10
	eng := newBudgetEngine(t, 20_000, limit)

	_, err := runQuery(t, eng, "| sort latency | head 20000")
	assertMemoryLimit(t, err, consumerSortBuffer, limit)
}

// tail and slice size their collector straight from the operator's N, so the
// allocation happens before any record is read.
func TestOversizedCollectorFailsBeforeAllocating(t *testing.T) {
	tests := []struct {
		name string
		expr string
	}{
		{"tail", "| tail 200000000"},
		{"slice", "| slice 1 200000000"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// One record in the vault: nothing about the data is large, only
			// the collector the query asks for.
			eng := newBudgetEngine(t, 1, MaxQueryMemoryBytes)
			_, err := runQuery(t, eng, tc.expr)
			assertMemoryLimit(t, err, consumerRecordBuffer, MaxQueryMemoryBytes)
		})
	}
}

// A too-tight bound would be worse than no bound: the product's main feature is
// searching and aggregating a lot of records. These are the shapes an
// investigation actually runs, at the real ceiling, and they must all succeed.
func TestLargeLegitimateQueriesStayWithinBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a 200k-record vault")
	}
	const records = 200_000
	eng := newBudgetEngine(t, records, MaxQueryMemoryBytes)

	tests := []struct {
		name     string
		expr     string
		wantRows int
	}{
		// Every record aggregated, grouped by a real dimension.
		{"count by host", "| stats count by host", 8},
		// A distinct count over a field that is unique per record: 200k
		// distinct values held at once.
		{"dcount over unique ids", "| stats dcount(id)", 1},
		// A median over every record, plus a per-group breakdown.
		{"median by host", "| stats median(latency) by host", 8},
		// A values list per group.
		{"values by host", "| stats values(latency) by host", 8},
		// A sort over the whole match set, capped the way the UI caps it.
		{"sorted page", "| sort latency | head 1000", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := runQuery(t, eng, tc.expr)
			if err != nil {
				t.Fatalf("legitimate query %q was refused: %v", tc.expr, err)
			}
			if tc.wantRows > 0 {
				if result.Table == nil {
					t.Fatalf("expected a table result for %q", tc.expr)
				}
				if len(result.Table.Rows) != tc.wantRows {
					t.Fatalf("rows: got %d, want %d", len(result.Table.Rows), tc.wantRows)
				}
				if result.Table.Truncated {
					t.Errorf("result was truncated for %q", tc.expr)
				}
			}
		})
	}

	// The sorted page must return the records it promised, not an empty set.
	result, err := runQuery(t, eng, "| sort latency | head 1000")
	if err != nil {
		t.Fatalf("sorted page: %v", err)
	}
	if len(result.Records) != 1000 {
		t.Fatalf("sorted page records: got %d, want 1000", len(result.Records))
	}
}

// newSizeSkewedEngine builds a vault whose record size is inversely
// correlated with the sort key: the earliest arrivals carry the highest k and
// the smallest payloads, everything after them is bulky.
//
// The correlation is what makes a compaction accounting bug visible. With
// uniform record sizes a collector that releases the wrong K items still
// releases the right *count*, so the ledger self-cancels and a test proves
// nothing. Here "| sort k | tail N" retains the tiny records and drops the
// bulky ones on every compaction, so crediting back the retained set instead
// of the dropped one makes the ledger climb without bound.
func newSizeSkewedEngine(t *testing.T, n, smallCount int, memLimit int64) *Engine {
	t.Helper()
	cm, err := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(100_000),
	})
	if err != nil {
		t.Fatalf("chunk manager: %v", err)
	}
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	bulky := strings.Repeat("x", 4096)
	for i := range n {
		ts := t0.Add(time.Duration(i) * time.Second)
		body := bulky
		if i < smallCount {
			body = "x"
		}
		cm.Append(chunk.Record{
			WriteTS:  ts,
			IngestTS: ts,
			// Descending sort key: the first records sort highest, so a
			// trailing tail keeps them and drops everything that follows.
			Attrs: chunk.Attributes{"k": fmt.Sprintf("%09d", n-i)},
			Raw:   []byte(body),
		})
	}
	cm.Seal()

	tokIdx := memtoken.NewIndexer(cm)
	attrIdx := memattr.NewIndexer(cm)
	kvIdx := memkv.NewIndexer(cm)
	jsonIdx := memjson.NewIndexer(cm)
	im := indexmem.NewManagerWithJSON(
		[]index.Indexer{tokIdx, attrIdx, kvIdx, jsonIdx}, tokIdx, attrIdx, kvIdx, jsonIdx, nil)

	e := New(cm, im, nil)
	e.memLimit = memLimit
	return e
}

// A trailing tail on a sort keeps the LARGEST keys, which sends compaction
// down a different branch than head does. Releasing the wrong side there
// inflates the ledger on a perfectly ordinary query until it is refused.
func TestSortWithTailStaysWithinBudget(t *testing.T) {
	const (
		records = 20_000
		keep    = 100
		// The true working set never exceeds 2*keep records, all of them the
		// small ones, so a few hundred KiB. 8 MiB leaves generous headroom
		// while staying far below what a drifting ledger reaches.
		limit = 8 << 20
	)
	eng := newSizeSkewedEngine(t, records, keep, limit)

	result, err := runQuery(t, eng, "| sort k | tail 100")
	if err != nil {
		t.Fatalf("sort with a trailing tail was refused: %v", err)
	}
	if len(result.Records) != keep {
		t.Fatalf("records: got %d, want %d", len(result.Records), keep)
	}
	// The retained records must be the highest keys — the small early arrivals.
	for _, rec := range result.Records {
		if len(rec.Raw) != 1 {
			t.Fatalf("tail kept a bulky record (%d bytes): the wrong side was retained", len(rec.Raw))
		}
	}
}

// The ledger must reflect what the collector still holds, not what it dropped.
// Both branches are checked: head keeps the smallest items, tail the largest,
// and only one of them moves the slice before trimming.
func TestTopNCollectorReleasesDroppedItems(t *testing.T) {
	for _, largest := range []bool{false, true} {
		name := "head"
		if largest {
			name = "tail"
		}
		t.Run(name, func(t *testing.T) {
			const keep = 4
			budget := &Budget{limit: MaxQueryMemoryBytes}
			col := newTopNCollector([]querylang.SortField{{Name: "k"}}, keep, largest, budget)

			// Size is inversely correlated with the key, so retaining the
			// wrong side is visible in the byte total.
			for i := range 2 * keep {
				rec := chunk.Record{
					Attrs: chunk.Attributes{"k": fmt.Sprintf("%03d", i)},
					Raw:   []byte(strings.Repeat("x", 1000*(2*keep-i))),
				}
				if err := col.add(rec); err != nil {
					t.Fatalf("add: %v", err)
				}
			}

			out := col.result()
			if len(out) != keep {
				t.Fatalf("kept %d records, want %d", len(out), keep)
			}

			var want int64
			for _, item := range col.items {
				want += item.footprint()
			}
			if budget.used != want {
				t.Errorf("ledger holds %d bytes for a working set of %d bytes (drift %+d)",
					budget.used, want, budget.used-want)
			}
		})
	}
}

// dedup remembers every EventID it sees for the whole scan; the window is a
// comparison against the remembered timestamp, not an eviction deadline.
func TestDedupStateFailsAtMemoryBudget(t *testing.T) {
	const limit = 64 << 10
	eng := newBudgetEngine(t, 20_000, limit)

	_, err := runQuery(t, eng, "| dedup | stats count")
	assertMemoryLimit(t, err, consumerDedupState, limit)
}

// Extracted JSON and logfmt fields land in Attrs and are most of what a
// buffered record retains, so the charge has to happen after materialization
// or the ledger admits several times the records it thinks it does.
func TestBufferedRecordChargedAfterFieldExtraction(t *testing.T) {
	rec := chunk.Record{
		Raw: []byte(`{"service":"checkout","level":"error","user_id":"u-99213","latency_ms":"412","region":"eu-north-1"}`),
	}
	raw := RecordFootprint(rec)

	materializeRecord(&rec)
	materialized := RecordFootprint(rec)

	if materialized <= raw {
		t.Fatalf("materialization did not grow the record: %d then %d", raw, materialized)
	}
	// The gap is the undercount a pre-materialization charge would carry.
	if materialized < 2*raw {
		t.Logf("materialized footprint %d vs raw %d", materialized, raw)
	}
}

func TestNilBudgetFailsClosed(t *testing.T) {
	var b *Budget
	err := b.Charge(consumerRecordBuffer, 1)
	if err == nil {
		t.Fatal("a missing budget must fail the query, not silently disable the bound")
	}
	var memErr *MemoryLimitError
	if !errors.As(err, &memErr) {
		t.Fatalf("expected *MemoryLimitError, got %T", err)
	}
	if !strings.Contains(memErr.Error(), "no memory budget installed") {
		t.Errorf("error %q does not say the budget was missing", memErr.Error())
	}
}

// A zero-value Budget must be bounded, not broken: it resolves to the standard
// ceiling rather than rejecting the first byte charged to it.
func TestZeroValueBudgetUsesStandardCeiling(t *testing.T) {
	var b Budget
	if err := b.Charge(consumerRecordBuffer, MaxQueryMemoryBytes); err != nil {
		t.Fatalf("zero-value budget rejected a charge inside the standard ceiling: %v", err)
	}
	if err := b.Charge(consumerRecordBuffer, 1); err == nil {
		t.Fatal("zero-value budget accepted a charge past the standard ceiling")
	}
}

func TestBudgetReleaseReturnsBytes(t *testing.T) {
	b := &Budget{limit: 100}
	if err := b.Charge(consumerRecordBuffer, 100); err != nil {
		t.Fatalf("charge to exactly the limit must succeed: %v", err)
	}
	if err := b.Charge(consumerRecordBuffer, 1); err == nil {
		t.Fatal("charge past the limit must fail")
	}
	b.Release(101)
	if b.used != 0 {
		t.Fatalf("used after release: got %d, want 0", b.used)
	}
	if err := b.Charge(consumerRecordBuffer, 100); err != nil {
		t.Fatalf("released bytes must be reusable: %v", err)
	}
}
