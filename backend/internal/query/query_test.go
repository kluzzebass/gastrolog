package query_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	indexmem "gastrolog/internal/index/memory"
	memsource "gastrolog/internal/index/memory/source"
	memtime "gastrolog/internal/index/memory/time"
	memtoken "gastrolog/internal/index/memory/token"
	"gastrolog/internal/query"
)

// collect gathers all records from the iterator, returning the first error encountered.
func collect(seq func(yield func(chunk.Record, error) bool)) ([]chunk.Record, error) {
	var results []chunk.Record
	var firstErr error
	for rec, err := range seq {
		if err != nil {
			firstErr = err
			break
		}
		results = append(results, rec)
	}
	return results, firstErr
}

// search is a helper that calls Search with nil resume and returns just the iterator.
func search(eng *query.Engine, ctx context.Context, q query.Query) func(yield func(chunk.Record, error) bool) {
	seq, _ := eng.Search(ctx, q, nil)
	return seq
}

// buildIndexes builds indexes for all sealed chunks.
func buildIndexes(t *testing.T, cm chunk.ChunkManager, im index.IndexManager) {
	t.Helper()
	metas, err := cm.List()
	if err != nil {
		t.Fatalf("list chunks: %v", err)
	}
	for _, m := range metas {
		if !m.Sealed {
			continue
		}
		if err := im.BuildIndexes(context.Background(), m.ID); err != nil {
			t.Fatalf("build indexes for %s: %v", m.ID, err)
		}
	}
}

var (
	t0 = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 = t0.Add(1 * time.Second)
	t2 = t0.Add(2 * time.Second)
	t3 = t0.Add(3 * time.Second)
	t4 = t0.Add(4 * time.Second)
	t5 = t0.Add(5 * time.Second)

	srcA = chunk.NewSourceID()
	srcB = chunk.NewSourceID()
)

// allRecords collects all records from batches in order, for building a fake clock.
func allRecords(batches [][]chunk.Record) []chunk.Record {
	var all []chunk.Record
	for _, b := range batches {
		all = append(all, b...)
	}
	return all
}

// fakeClock returns a Now function that returns IngestTS of each record in order,
// so that WriteTS == IngestTS in tests.
func fakeClock(records []chunk.Record) func() time.Time {
	idx := 0
	return func() time.Time {
		if idx < len(records) {
			ts := records[idx].IngestTS
			idx++
			return ts
		}
		return time.Now()
	}
}

// setup creates a memory chunk manager, appends records, seals the chunk,
// and wires up a memory index manager. Returns the query engine ready to use.
func setup(t *testing.T, batches ...[]chunk.Record) *query.Engine {
	t.Helper()

	all := allRecords(batches)
	cm, err := chunkmem.NewManager(chunkmem.Config{
		MaxRecords: 10000, // Large enough to not auto-rotate
		Now:        fakeClock(all),
	})
	if err != nil {
		t.Fatalf("new chunk manager: %v", err)
	}

	for _, records := range batches {
		for _, rec := range records {
			if _, _, err := cm.Append(rec); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		if err := cm.Seal(); err != nil {
			t.Fatalf("seal: %v", err)
		}
	}

	timeIdx := memtime.NewIndexer(cm, 1) // sparsity 1 = index every record
	srcIdx := memsource.NewIndexer(cm)
	tokIdx := memtoken.NewIndexer(cm)

	im := indexmem.NewManager(
		[]index.Indexer{timeIdx, srcIdx, tokIdx},
		timeIdx,
		srcIdx,
		tokIdx,
		nil,
	)

	buildIndexes(t, cm, im)

	return query.New(cm, im, nil)
}

// setupWithActive is like setup but leaves the last batch unsealed (active chunk).
func setupWithActive(t *testing.T, sealed [][]chunk.Record, active []chunk.Record) *query.Engine {
	t.Helper()

	all := append(allRecords(sealed), active...)
	cm, err := chunkmem.NewManager(chunkmem.Config{
		MaxRecords: 10000,
		Now:        fakeClock(all),
	})
	if err != nil {
		t.Fatalf("new chunk manager: %v", err)
	}

	for _, records := range sealed {
		for _, rec := range records {
			if _, _, err := cm.Append(rec); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		if err := cm.Seal(); err != nil {
			t.Fatalf("seal: %v", err)
		}
	}

	// Append active chunk records without sealing.
	for _, rec := range active {
		if _, _, err := cm.Append(rec); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	timeIdx := memtime.NewIndexer(cm, 1)
	srcIdx := memsource.NewIndexer(cm)
	tokIdx := memtoken.NewIndexer(cm)

	im := indexmem.NewManager(
		[]index.Indexer{timeIdx, srcIdx, tokIdx},
		timeIdx,
		srcIdx,
		tokIdx,
		nil,
	)

	buildIndexes(t, cm, im)

	return query.New(cm, im, nil)
}

func TestSearchNoChunks(t *testing.T) {
	eng := setup(t)

	results, err := collect(search(eng, context.Background(), query.Query{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestSearchActiveChunkNoFilters(t *testing.T) {
	active := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("one")},
		{IngestTS: t2, SourceID: srcB, Raw: []byte("two")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("three")},
	}

	eng := setupWithActive(t, nil, active)

	results, err := collect(search(eng, context.Background(), query.Query{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	for i, rec := range results {
		if string(rec.Raw) != string(active[i].Raw) {
			t.Errorf("result[%d]: got %q, want %q", i, rec.Raw, active[i].Raw)
		}
	}
}

func TestSearchActiveChunkWithTimeFilter(t *testing.T) {
	active := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("one")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("two")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("three")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("four")},
	}

	eng := setupWithActive(t, nil, active)

	results, err := collect(search(eng, context.Background(), query.Query{Start: t2, End: t4}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Raw) != "two" {
		t.Errorf("result[0]: got %q, want %q", results[0].Raw, "two")
	}
	if string(results[1].Raw) != "three" {
		t.Errorf("result[1]: got %q, want %q", results[1].Raw, "three")
	}
}

func TestSearchActiveChunkWithSourceFilter(t *testing.T) {
	active := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("a1")},
		{IngestTS: t2, SourceID: srcB, Raw: []byte("b1")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("a2")},
	}

	eng := setupWithActive(t, nil, active)

	results, err := collect(search(eng, context.Background(), query.Query{Sources: []chunk.SourceID{srcA}}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Raw) != "a1" {
		t.Errorf("result[0]: got %q, want %q", results[0].Raw, "a1")
	}
	if string(results[1].Raw) != "a2" {
		t.Errorf("result[1]: got %q, want %q", results[1].Raw, "a2")
	}
}

func TestSearchSealedAndActiveChunks(t *testing.T) {
	sealed := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("s1")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("s2")},
	}
	active := []chunk.Record{
		{IngestTS: t3, SourceID: srcA, Raw: []byte("a1")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("a2")},
	}

	eng := setupWithActive(t, [][]chunk.Record{sealed}, active)

	results, err := collect(search(eng, context.Background(), query.Query{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 4 {
		t.Fatalf("expected 4 results, got %d", len(results))
	}
	want := []string{"s1", "s2", "a1", "a2"}
	for i, w := range want {
		if string(results[i].Raw) != w {
			t.Errorf("result[%d]: got %q, want %q", i, results[i].Raw, w)
		}
	}
}

func TestSearchSingleChunkNoFilters(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("one")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("two")},
		{IngestTS: t3, SourceID: srcB, Raw: []byte("three")},
	}

	eng := setup(t, records)

	results, err := collect(search(eng, context.Background(), query.Query{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	for i, rec := range results {
		if string(rec.Raw) != string(records[i].Raw) {
			t.Errorf("result[%d]: got %q, want %q", i, rec.Raw, records[i].Raw)
		}
	}
}

func TestSearchTimeRangeFilter(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("one")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("two")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("three")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("four")},
	}

	eng := setup(t, records)

	// Start=t2, End=t4 → records at t2 and t3
	results, err := collect(search(eng, context.Background(), query.Query{Start: t2, End: t4}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Raw) != "two" {
		t.Errorf("result[0]: got %q, want %q", results[0].Raw, "two")
	}
	if string(results[1].Raw) != "three" {
		t.Errorf("result[1]: got %q, want %q", results[1].Raw, "three")
	}
}

func TestSearchSourceFilter(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("a1")},
		{IngestTS: t2, SourceID: srcB, Raw: []byte("b1")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("a2")},
		{IngestTS: t4, SourceID: srcB, Raw: []byte("b2")},
	}

	eng := setup(t, records)

	results, err := collect(search(eng, context.Background(), query.Query{Sources: []chunk.SourceID{srcA}}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Raw) != "a1" {
		t.Errorf("result[0]: got %q, want %q", results[0].Raw, "a1")
	}
	if string(results[1].Raw) != "a2" {
		t.Errorf("result[1]: got %q, want %q", results[1].Raw, "a2")
	}
}

func TestSearchCombinedTimeAndSource(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("a1")},
		{IngestTS: t2, SourceID: srcB, Raw: []byte("b1")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("a2")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("a3")},
		{IngestTS: t5, SourceID: srcB, Raw: []byte("b2")},
	}

	eng := setup(t, records)

	// Source A, time [t2, t5) → a2 (t3) and a3 (t4)
	results, err := collect(search(eng, context.Background(), query.Query{
		Start:   t2,
		End:     t5,
		Sources: []chunk.SourceID{srcA},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Raw) != "a2" {
		t.Errorf("result[0]: got %q, want %q", results[0].Raw, "a2")
	}
	if string(results[1].Raw) != "a3" {
		t.Errorf("result[1]: got %q, want %q", results[1].Raw, "a3")
	}
}

func TestSearchLimit(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("one")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("two")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("three")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("four")},
	}

	eng := setup(t, records)

	results, err := collect(search(eng, context.Background(), query.Query{Limit: 2}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Raw) != "one" {
		t.Errorf("result[0]: got %q, want %q", results[0].Raw, "one")
	}
	if string(results[1].Raw) != "two" {
		t.Errorf("result[1]: got %q, want %q", results[1].Raw, "two")
	}
}

func TestSearchMultiChunkMerge(t *testing.T) {
	batch1 := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("c1r1")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("c1r2")},
	}
	batch2 := []chunk.Record{
		{IngestTS: t3, SourceID: srcA, Raw: []byte("c2r1")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("c2r2")},
	}

	eng := setup(t, batch1, batch2)

	results, err := collect(search(eng, context.Background(), query.Query{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 4 {
		t.Fatalf("expected 4 results, got %d", len(results))
	}
	want := []string{"c1r1", "c1r2", "c2r1", "c2r2"}
	for i, w := range want {
		if string(results[i].Raw) != w {
			t.Errorf("result[%d]: got %q, want %q", i, results[i].Raw, w)
		}
	}
}

func TestSearchMultiChunkLimit(t *testing.T) {
	batch1 := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("c1r1")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("c1r2")},
	}
	batch2 := []chunk.Record{
		{IngestTS: t3, SourceID: srcA, Raw: []byte("c2r1")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("c2r2")},
	}

	eng := setup(t, batch1, batch2)

	// Limit 3 across 2 chunks (2 + 1)
	results, err := collect(search(eng, context.Background(), query.Query{Limit: 3}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	want := []string{"c1r1", "c1r2", "c2r1"}
	for i, w := range want {
		if string(results[i].Raw) != w {
			t.Errorf("result[%d]: got %q, want %q", i, results[i].Raw, w)
		}
	}
}

func TestSearchContextCancellation(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("one")},
	}

	eng := setup(t, records)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := collect(search(eng, ctx, query.Query{}))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestSearchSkipsNonOverlappingChunks(t *testing.T) {
	early := []chunk.Record{
		{IngestTS: t0, SourceID: srcA, Raw: []byte("early")},
	}
	mid := []chunk.Record{
		{IngestTS: t2, SourceID: srcA, Raw: []byte("mid")},
	}
	late := []chunk.Record{
		{IngestTS: t5, SourceID: srcA, Raw: []byte("late")},
	}

	eng := setup(t, early, mid, late)

	// Query [t1, t4) — only "mid" chunk overlaps
	results, err := collect(search(eng, context.Background(), query.Query{Start: t1, End: t4}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if string(results[0].Raw) != "mid" {
		t.Errorf("got %q, want %q", results[0].Raw, "mid")
	}
}

func TestSearchSourceNotInChunk(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("a1")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("a2")},
	}

	eng := setup(t, records)

	// srcB is not in this chunk
	results, err := collect(search(eng, context.Background(), query.Query{Sources: []chunk.SourceID{srcB}}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestSearchEarlyBreak(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("one")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("two")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("three")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("four")},
	}

	eng := setup(t, records)

	// Consume only the first 2 records by breaking early.
	var results []chunk.Record
	for rec, err := range search(eng, context.Background(), query.Query{}) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		results = append(results, rec)
		if len(results) == 2 {
			break
		}
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Raw) != "one" {
		t.Errorf("result[0]: got %q, want %q", results[0].Raw, "one")
	}
	if string(results[1].Raw) != "two" {
		t.Errorf("result[1]: got %q, want %q", results[1].Raw, "two")
	}
}

func TestSearchContextCancelledMidIteration(t *testing.T) {
	batch1 := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("c1r1")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("c1r2")},
	}
	batch2 := []chunk.Record{
		{IngestTS: t3, SourceID: srcA, Raw: []byte("c2r1")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("c2r2")},
	}

	eng := setup(t, batch1, batch2)

	ctx, cancel := context.WithCancel(context.Background())

	var results []chunk.Record
	var gotErr error
	for rec, err := range search(eng, ctx, query.Query{}) {
		if err != nil {
			gotErr = err
			break
		}
		results = append(results, rec)
		// Cancel after first chunk
		if len(results) == 2 {
			cancel()
		}
	}

	if !errors.Is(gotErr, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", gotErr)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results before cancellation, got %d", len(results))
	}
}

func TestSearchStartOnlyFilter(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("one")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("two")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("three")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("four")},
	}

	eng := setup(t, records)

	// Start=t3, no End → records at t3 and t4
	results, err := collect(search(eng, context.Background(), query.Query{Start: t3}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Raw) != "three" {
		t.Errorf("result[0]: got %q, want %q", results[0].Raw, "three")
	}
	if string(results[1].Raw) != "four" {
		t.Errorf("result[1]: got %q, want %q", results[1].Raw, "four")
	}
}

func TestSearchEndOnlyFilter(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("one")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("two")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("three")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("four")},
	}

	eng := setup(t, records)

	// No Start, End=t3 → records at t1 and t2
	results, err := collect(search(eng, context.Background(), query.Query{End: t3}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Raw) != "one" {
		t.Errorf("result[0]: got %q, want %q", results[0].Raw, "one")
	}
	if string(results[1].Raw) != "two" {
		t.Errorf("result[1]: got %q, want %q", results[1].Raw, "two")
	}
}

func TestSearchLimitWithSourceFilter(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("a1")},
		{IngestTS: t2, SourceID: srcB, Raw: []byte("b1")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("a2")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("a3")},
		{IngestTS: t5, SourceID: srcB, Raw: []byte("b2")},
	}

	eng := setup(t, records)

	// Source A has 3 records, limit to 2
	results, err := collect(search(eng, context.Background(), query.Query{Sources: []chunk.SourceID{srcA}, Limit: 2}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Raw) != "a1" {
		t.Errorf("result[0]: got %q, want %q", results[0].Raw, "a1")
	}
	if string(results[1].Raw) != "a2" {
		t.Errorf("result[1]: got %q, want %q", results[1].Raw, "a2")
	}
}

func TestSearchLimitWithTimeFilter(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("one")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("two")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("three")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("four")},
		{IngestTS: t5, SourceID: srcA, Raw: []byte("five")},
	}

	eng := setup(t, records)

	// Time range [t2, t5) has 3 records, limit to 2
	results, err := collect(search(eng, context.Background(), query.Query{Start: t2, End: t5, Limit: 2}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Raw) != "two" {
		t.Errorf("result[0]: got %q, want %q", results[0].Raw, "two")
	}
	if string(results[1].Raw) != "three" {
		t.Errorf("result[1]: got %q, want %q", results[1].Raw, "three")
	}
}

func TestSearchActiveChunkWithLimit(t *testing.T) {
	active := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("one")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("two")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("three")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("four")},
	}

	eng := setupWithActive(t, nil, active)

	results, err := collect(search(eng, context.Background(), query.Query{Limit: 2}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Raw) != "one" {
		t.Errorf("result[0]: got %q, want %q", results[0].Raw, "one")
	}
	if string(results[1].Raw) != "two" {
		t.Errorf("result[1]: got %q, want %q", results[1].Raw, "two")
	}
}

func TestSearchMultiSourceFilter(t *testing.T) {
	srcC := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("from A")},
		{IngestTS: t2, SourceID: srcB, Raw: []byte("from B")},
		{IngestTS: t3, SourceID: srcC, Raw: []byte("from C")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("from A again")},
	}

	eng := setup(t, records)

	// Search for records from srcA OR srcC
	results, err := collect(search(eng, context.Background(), query.Query{
		Sources: []chunk.SourceID{srcA, srcC},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	if string(results[0].Raw) != "from A" {
		t.Errorf("result[0]: got %q", results[0].Raw)
	}
	if string(results[1].Raw) != "from C" {
		t.Errorf("result[1]: got %q", results[1].Raw)
	}
	if string(results[2].Raw) != "from A again" {
		t.Errorf("result[2]: got %q", results[2].Raw)
	}
}

func TestSearchMultiSourceActiveChunk(t *testing.T) {
	srcC := chunk.NewSourceID()
	active := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("from A")},
		{IngestTS: t2, SourceID: srcB, Raw: []byte("from B")},
		{IngestTS: t3, SourceID: srcC, Raw: []byte("from C")},
	}

	eng := setupWithActive(t, nil, active)

	// Multi-source filter on active chunk
	results, err := collect(search(eng, context.Background(), query.Query{
		Sources: []chunk.SourceID{srcA, srcC},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Raw) != "from A" {
		t.Errorf("result[0]: got %q", results[0].Raw)
	}
	if string(results[1].Raw) != "from C" {
		t.Errorf("result[1]: got %q", results[1].Raw)
	}
}

func TestSearchMultiSourceWithTokens(t *testing.T) {
	srcC := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("error from A")},
		{IngestTS: t2, SourceID: srcB, Raw: []byte("error from B")},
		{IngestTS: t3, SourceID: srcC, Raw: []byte("error from C")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("warning from A")},
	}

	eng := setup(t, records)

	// Search for "error" from srcA OR srcC
	results, err := collect(search(eng, context.Background(), query.Query{
		Tokens:  []string{"error"},
		Sources: []chunk.SourceID{srcA, srcC},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Raw) != "error from A" {
		t.Errorf("result[0]: got %q", results[0].Raw)
	}
	if string(results[1].Raw) != "error from C" {
		t.Errorf("result[1]: got %q", results[1].Raw)
	}
}

func TestSearchTokenFilter(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("error connecting to database")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("connection established")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("error timeout waiting for response")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("request completed successfully")},
	}

	eng := setup(t, records)

	// Search for "error" - should match records 1 and 3
	results, err := collect(search(eng, context.Background(), query.Query{Tokens: []string{"error"}}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Raw) != "error connecting to database" {
		t.Errorf("result[0]: got %q", results[0].Raw)
	}
	if string(results[1].Raw) != "error timeout waiting for response" {
		t.Errorf("result[1]: got %q", results[1].Raw)
	}
}

func TestSearchMultiTokenFilter(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("error connecting to database")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("error timeout waiting")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("timeout connecting to server")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("request completed")},
	}

	eng := setup(t, records)

	// Search for "error" AND "connecting" - only record 1 matches
	results, err := collect(search(eng, context.Background(), query.Query{Tokens: []string{"error", "connecting"}}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if string(results[0].Raw) != "error connecting to database" {
		t.Errorf("result[0]: got %q", results[0].Raw)
	}
}

func TestSearchTokenNotFound(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("hello world")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("foo bar")},
	}

	eng := setup(t, records)

	results, err := collect(search(eng, context.Background(), query.Query{Tokens: []string{"notfound"}}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestSearchTokenWithSourceFilter(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("error from source A")},
		{IngestTS: t2, SourceID: srcB, Raw: []byte("error from source B")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("warning from source A")},
	}

	eng := setup(t, records)

	// Search for "error" from srcA only
	results, err := collect(search(eng, context.Background(), query.Query{
		Tokens:  []string{"error"},
		Sources: []chunk.SourceID{srcA},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if string(results[0].Raw) != "error from source A" {
		t.Errorf("result[0]: got %q", results[0].Raw)
	}
}

func TestSearchTokenWithTimeFilter(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("error early")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("error middle")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("error late")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("success late")},
	}

	eng := setup(t, records)

	// Search for "error" in time range [t2, t4)
	results, err := collect(search(eng, context.Background(), query.Query{
		Tokens: []string{"error"},
		Start:  t2,
		End:    t4,
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Raw) != "error middle" {
		t.Errorf("result[0]: got %q", results[0].Raw)
	}
	if string(results[1].Raw) != "error late" {
		t.Errorf("result[1]: got %q", results[1].Raw)
	}
}

func TestSearchTokenActiveChunk(t *testing.T) {
	active := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("error connecting")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("connection ok")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("error timeout")},
	}

	eng := setupWithActive(t, nil, active)

	// Token search on active chunk uses on-the-fly tokenization
	results, err := collect(search(eng, context.Background(), query.Query{Tokens: []string{"error"}}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Raw) != "error connecting" {
		t.Errorf("result[0]: got %q", results[0].Raw)
	}
	if string(results[1].Raw) != "error timeout" {
		t.Errorf("result[1]: got %q", results[1].Raw)
	}
}

func TestSearchTokenSealedNoMatchActiveMatch(t *testing.T) {
	// Sealed chunk has no matching tokens, active chunk does.
	// Guards against accidental early pruning or index-only logic.
	sealed := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("hello world")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("foo bar")},
	}
	active := []chunk.Record{
		{IngestTS: t3, SourceID: srcA, Raw: []byte("error happened")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("all good")},
	}

	eng := setupWithActive(t, [][]chunk.Record{sealed}, active)

	results, err := collect(search(eng, context.Background(), query.Query{Tokens: []string{"error"}}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if string(results[0].Raw) != "error happened" {
		t.Errorf("result[0]: got %q", results[0].Raw)
	}
}

func TestSearchTokenAndSourceSealedNoMatchActiveMatch(t *testing.T) {
	// Combined token + source filter: sealed has neither, active has both.
	sealed := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("hello world")},
		{IngestTS: t2, SourceID: srcB, Raw: []byte("error from B")},
	}
	active := []chunk.Record{
		{IngestTS: t3, SourceID: srcA, Raw: []byte("error from A")},
		{IngestTS: t4, SourceID: srcB, Raw: []byte("warning from B")},
	}

	eng := setupWithActive(t, [][]chunk.Record{sealed}, active)

	// Search for "error" from srcA - only active chunk matches
	results, err := collect(search(eng, context.Background(), query.Query{
		Tokens:  []string{"error"},
		Sources: []chunk.SourceID{srcA},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if string(results[0].Raw) != "error from A" {
		t.Errorf("result[0]: got %q", results[0].Raw)
	}
}

func TestSearchTokenWithLimit(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("error one")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("error two")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("error three")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("error four")},
	}

	eng := setup(t, records)

	results, err := collect(search(eng, context.Background(), query.Query{
		Tokens: []string{"error"},
		Limit:  2,
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Raw) != "error one" {
		t.Errorf("result[0]: got %q", results[0].Raw)
	}
	if string(results[1].Raw) != "error two" {
		t.Errorf("result[1]: got %q", results[1].Raw)
	}
}

func TestSearchTokenCaseInsensitive(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("ERROR uppercase")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("Error mixed")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("error lowercase")},
	}

	eng := setup(t, records)

	// All should match since tokenizer lowercases
	results, err := collect(search(eng, context.Background(), query.Query{Tokens: []string{"error"}}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
}

func TestSearchPaginationWithLimit(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("one")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("two")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("three")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("four")},
	}

	eng := setup(t, records)

	// First page: limit 2
	seq1, nextToken1 := eng.Search(context.Background(), query.Query{Limit: 2}, nil)
	results1, err := collect(seq1)
	if err != nil {
		t.Fatalf("page 1: unexpected error: %v", err)
	}
	if len(results1) != 2 {
		t.Fatalf("page 1: expected 2 results, got %d", len(results1))
	}
	if string(results1[0].Raw) != "one" || string(results1[1].Raw) != "two" {
		t.Errorf("page 1: got %q, %q", results1[0].Raw, results1[1].Raw)
	}

	token := nextToken1()
	if token == nil {
		t.Fatal("expected resume token after page 1")
	}

	// Second page: resume with same limit
	seq2, nextToken2 := eng.Search(context.Background(), query.Query{Limit: 2}, token)
	results2, err := collect(seq2)
	if err != nil {
		t.Fatalf("page 2: unexpected error: %v", err)
	}
	if len(results2) != 2 {
		t.Fatalf("page 2: expected 2 results, got %d", len(results2))
	}
	if string(results2[0].Raw) != "three" || string(results2[1].Raw) != "four" {
		t.Errorf("page 2: got %q, %q", results2[0].Raw, results2[1].Raw)
	}

	// Token is returned because limit was hit (we don't know if more records exist).
	// Try to resume - should get 0 results and nil token.
	token = nextToken2()
	if token == nil {
		t.Fatal("expected token after hitting limit")
	}

	// Third page: should be empty
	seq3, nextToken3 := eng.Search(context.Background(), query.Query{Limit: 2}, token)
	results3, err := collect(seq3)
	if err != nil {
		t.Fatalf("page 3: unexpected error: %v", err)
	}
	if len(results3) != 0 {
		t.Fatalf("page 3: expected 0 results, got %d", len(results3))
	}

	token = nextToken3()
	if token != nil {
		t.Errorf("expected nil token after empty page, got %+v", token)
	}
}

func TestSearchPaginationAcrossChunks(t *testing.T) {
	batch1 := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("c1r1")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("c1r2")},
	}
	batch2 := []chunk.Record{
		{IngestTS: t3, SourceID: srcA, Raw: []byte("c2r1")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("c2r2")},
	}

	eng := setup(t, batch1, batch2)

	// First page: limit 3 (spans both chunks)
	seq1, nextToken1 := eng.Search(context.Background(), query.Query{Limit: 3}, nil)
	results1, err := collect(seq1)
	if err != nil {
		t.Fatalf("page 1: unexpected error: %v", err)
	}
	if len(results1) != 3 {
		t.Fatalf("page 1: expected 3 results, got %d", len(results1))
	}
	want := []string{"c1r1", "c1r2", "c2r1"}
	for i, w := range want {
		if string(results1[i].Raw) != w {
			t.Errorf("page 1 result[%d]: got %q, want %q", i, results1[i].Raw, w)
		}
	}

	token := nextToken1()
	if token == nil {
		t.Fatal("expected resume token after page 1")
	}

	// Second page: resume, should get remaining record
	seq2, nextToken2 := eng.Search(context.Background(), query.Query{Limit: 3}, token)
	results2, err := collect(seq2)
	if err != nil {
		t.Fatalf("page 2: unexpected error: %v", err)
	}
	if len(results2) != 1 {
		t.Fatalf("page 2: expected 1 result, got %d", len(results2))
	}
	if string(results2[0].Raw) != "c2r2" {
		t.Errorf("page 2: got %q, want %q", results2[0].Raw, "c2r2")
	}

	// Iteration completed fully (got fewer than limit), so nil token.
	token = nextToken2()
	if token != nil {
		t.Errorf("expected nil token after last page, got %+v", token)
	}
}

func TestSearchPaginationWithEarlyBreak(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("one")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("two")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("three")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("four")},
	}

	eng := setup(t, records)

	// Break after 2 records (no limit set)
	seq1, nextToken1 := eng.Search(context.Background(), query.Query{}, nil)
	var results1 []chunk.Record
	for rec, err := range seq1 {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		results1 = append(results1, rec)
		if len(results1) == 2 {
			break
		}
	}

	if len(results1) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results1))
	}

	token := nextToken1()
	if token == nil {
		t.Fatal("expected resume token after early break")
	}

	// Resume from where we left off
	seq2, nextToken2 := eng.Search(context.Background(), query.Query{}, token)
	results2, err := collect(seq2)
	if err != nil {
		t.Fatalf("page 2: unexpected error: %v", err)
	}
	if len(results2) != 2 {
		t.Fatalf("page 2: expected 2 results, got %d", len(results2))
	}
	if string(results2[0].Raw) != "three" || string(results2[1].Raw) != "four" {
		t.Errorf("page 2: got %q, %q", results2[0].Raw, results2[1].Raw)
	}

	token = nextToken2()
	if token != nil {
		t.Errorf("expected nil token after completion, got %+v", token)
	}
}

func TestSearchPaginationActiveChunk(t *testing.T) {
	active := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("one")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("two")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("three")},
	}

	eng := setupWithActive(t, nil, active)

	// First page
	seq1, nextToken1 := eng.Search(context.Background(), query.Query{Limit: 2}, nil)
	results1, err := collect(seq1)
	if err != nil {
		t.Fatalf("page 1: unexpected error: %v", err)
	}
	if len(results1) != 2 {
		t.Fatalf("page 1: expected 2 results, got %d", len(results1))
	}

	token := nextToken1()
	if token == nil {
		t.Fatal("expected resume token after page 1")
	}

	// Second page
	seq2, nextToken2 := eng.Search(context.Background(), query.Query{Limit: 2}, token)
	results2, err := collect(seq2)
	if err != nil {
		t.Fatalf("page 2: unexpected error: %v", err)
	}
	if len(results2) != 1 {
		t.Fatalf("page 2: expected 1 result, got %d", len(results2))
	}
	if string(results2[0].Raw) != "three" {
		t.Errorf("page 2: got %q, want %q", results2[0].Raw, "three")
	}

	token = nextToken2()
	if token != nil {
		t.Errorf("expected nil token after last page, got %+v", token)
	}
}

func TestSearchPaginationWithFilters(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("error one")},
		{IngestTS: t2, SourceID: srcB, Raw: []byte("error two")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("error three")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("warning four")},
		{IngestTS: t5, SourceID: srcA, Raw: []byte("error five")},
	}

	eng := setup(t, records)

	// Search for "error" from srcA with pagination
	q := query.Query{
		Tokens:  []string{"error"},
		Sources: []chunk.SourceID{srcA},
		Limit:   2,
	}

	// First page: should get "error one" and "error three"
	seq1, nextToken1 := eng.Search(context.Background(), q, nil)
	results1, err := collect(seq1)
	if err != nil {
		t.Fatalf("page 1: unexpected error: %v", err)
	}
	if len(results1) != 2 {
		t.Fatalf("page 1: expected 2 results, got %d", len(results1))
	}
	if string(results1[0].Raw) != "error one" || string(results1[1].Raw) != "error three" {
		t.Errorf("page 1: got %q, %q", results1[0].Raw, results1[1].Raw)
	}

	token := nextToken1()
	if token == nil {
		t.Fatal("expected resume token after page 1")
	}

	// Second page: should get "error five"
	seq2, nextToken2 := eng.Search(context.Background(), q, token)
	results2, err := collect(seq2)
	if err != nil {
		t.Fatalf("page 2: unexpected error: %v", err)
	}
	if len(results2) != 1 {
		t.Fatalf("page 2: expected 1 result, got %d", len(results2))
	}
	if string(results2[0].Raw) != "error five" {
		t.Errorf("page 2: got %q, want %q", results2[0].Raw, "error five")
	}

	token = nextToken2()
	if token != nil {
		t.Errorf("expected nil token after last page, got %+v", token)
	}
}

func TestSearchNoResultsNoToken(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("hello")},
	}

	eng := setup(t, records)

	// Search for something that doesn't exist
	seq, nextToken := eng.Search(context.Background(), query.Query{Tokens: []string{"notfound"}}, nil)
	results, err := collect(seq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}

	token := nextToken()
	if token != nil {
		t.Errorf("expected nil token for empty results, got %+v", token)
	}
}

func TestSearchInvalidResumeToken(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("hello")},
	}

	eng := setup(t, records)

	// Create a token with a non-existent chunk ID
	badToken := &query.ResumeToken{
		Next: chunk.RecordRef{
			ChunkID: chunk.NewChunkID(), // random, doesn't exist
			Pos:     0,
		},
	}

	seq, _ := eng.Search(context.Background(), query.Query{}, badToken)
	_, err := collect(seq)
	if !errors.Is(err, query.ErrInvalidResumeToken) {
		t.Fatalf("expected ErrInvalidResumeToken, got %v", err)
	}
}

func TestSearchReverseOrder(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("one")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("two")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("three")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("four")},
	}

	eng := setup(t, records)

	// Reverse order: End < Start (t4 down to t1)
	results, err := collect(search(eng, context.Background(), query.Query{Start: t5, End: t0}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 4 {
		t.Fatalf("expected 4 results, got %d", len(results))
	}
	// Results should be newest first
	want := []string{"four", "three", "two", "one"}
	for i, w := range want {
		if string(results[i].Raw) != w {
			t.Errorf("result[%d]: got %q, want %q", i, results[i].Raw, w)
		}
	}
}

func TestSearchReverseOrderWithTimeFilter(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("one")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("two")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("three")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("four")},
	}

	eng := setup(t, records)

	// Reverse in range [t2, t4): should get three, two in that order
	// In reverse: Start is upper bound (exclusive), End is lower bound (inclusive)
	results, err := collect(search(eng, context.Background(), query.Query{Start: t4, End: t2}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Raw) != "three" {
		t.Errorf("result[0]: got %q, want %q", results[0].Raw, "three")
	}
	if string(results[1].Raw) != "two" {
		t.Errorf("result[1]: got %q, want %q", results[1].Raw, "two")
	}
}

func TestSearchReverseOrderWithSourceFilter(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("a1")},
		{IngestTS: t2, SourceID: srcB, Raw: []byte("b1")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("a2")},
		{IngestTS: t4, SourceID: srcB, Raw: []byte("b2")},
	}

	eng := setup(t, records)

	// Reverse order, source A only
	results, err := collect(search(eng, context.Background(), query.Query{
		Start:   t5,
		End:     t0,
		Sources: []chunk.SourceID{srcA},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Raw) != "a2" {
		t.Errorf("result[0]: got %q, want %q", results[0].Raw, "a2")
	}
	if string(results[1].Raw) != "a1" {
		t.Errorf("result[1]: got %q, want %q", results[1].Raw, "a1")
	}
}

func TestSearchReverseOrderWithTokenFilter(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("error early")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("info message")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("error middle")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("error late")},
	}

	eng := setup(t, records)

	// Reverse order with token filter
	results, err := collect(search(eng, context.Background(), query.Query{
		Start:  t5,
		End:    t0,
		Tokens: []string{"error"},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	want := []string{"error late", "error middle", "error early"}
	for i, w := range want {
		if string(results[i].Raw) != w {
			t.Errorf("result[%d]: got %q, want %q", i, results[i].Raw, w)
		}
	}
}

func TestSearchReverseOrderWithLimit(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("one")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("two")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("three")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("four")},
	}

	eng := setup(t, records)

	// Reverse with limit
	results, err := collect(search(eng, context.Background(), query.Query{
		Start: t5,
		End:   t0,
		Limit: 2,
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Raw) != "four" {
		t.Errorf("result[0]: got %q, want %q", results[0].Raw, "four")
	}
	if string(results[1].Raw) != "three" {
		t.Errorf("result[1]: got %q, want %q", results[1].Raw, "three")
	}
}

func TestSearchReverseOrderActiveChunk(t *testing.T) {
	active := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("one")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("two")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("three")},
	}

	eng := setupWithActive(t, nil, active)

	// Reverse on active chunk
	results, err := collect(search(eng, context.Background(), query.Query{Start: t4, End: t0}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	want := []string{"three", "two", "one"}
	for i, w := range want {
		if string(results[i].Raw) != w {
			t.Errorf("result[%d]: got %q, want %q", i, results[i].Raw, w)
		}
	}
}

func TestSearchReverseOrderMultiChunk(t *testing.T) {
	batch1 := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("c1r1")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("c1r2")},
	}
	batch2 := []chunk.Record{
		{IngestTS: t3, SourceID: srcA, Raw: []byte("c2r1")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("c2r2")},
	}

	eng := setup(t, batch1, batch2)

	// Reverse across multiple chunks
	results, err := collect(search(eng, context.Background(), query.Query{Start: t5, End: t0}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 4 {
		t.Fatalf("expected 4 results, got %d", len(results))
	}
	// Should process chunk2 first (newer), then chunk1
	want := []string{"c2r2", "c2r1", "c1r2", "c1r1"}
	for i, w := range want {
		if string(results[i].Raw) != w {
			t.Errorf("result[%d]: got %q, want %q", i, results[i].Raw, w)
		}
	}
}

func TestSearchReverseOrderPagination(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("one")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("two")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("three")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("four")},
	}

	eng := setup(t, records)

	// First page: reverse with limit 2
	q := query.Query{Start: t5, End: t0, Limit: 2}
	seq1, nextToken1 := eng.Search(context.Background(), q, nil)
	results1, err := collect(seq1)
	if err != nil {
		t.Fatalf("page 1: unexpected error: %v", err)
	}
	if len(results1) != 2 {
		t.Fatalf("page 1: expected 2 results, got %d", len(results1))
	}
	if string(results1[0].Raw) != "four" || string(results1[1].Raw) != "three" {
		t.Errorf("page 1: got %q, %q", results1[0].Raw, results1[1].Raw)
	}

	token := nextToken1()
	if token == nil {
		t.Fatal("expected resume token after page 1")
	}

	// Second page
	seq2, nextToken2 := eng.Search(context.Background(), q, token)
	results2, err := collect(seq2)
	if err != nil {
		t.Fatalf("page 2: unexpected error: %v", err)
	}
	if len(results2) != 2 {
		t.Fatalf("page 2: expected 2 results, got %d", len(results2))
	}
	if string(results2[0].Raw) != "two" || string(results2[1].Raw) != "one" {
		t.Errorf("page 2: got %q, %q", results2[0].Raw, results2[1].Raw)
	}

	// Try to get more - should be empty
	token = nextToken2()
	if token == nil {
		t.Fatal("expected token after hitting limit")
	}

	seq3, nextToken3 := eng.Search(context.Background(), q, token)
	results3, err := collect(seq3)
	if err != nil {
		t.Fatalf("page 3: unexpected error: %v", err)
	}
	if len(results3) != 0 {
		t.Fatalf("page 3: expected 0 results, got %d", len(results3))
	}

	token = nextToken3()
	if token != nil {
		t.Errorf("expected nil token after empty page, got %+v", token)
	}
}

func TestSearchReverseOrderPaginationAcrossChunks(t *testing.T) {
	batch1 := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("c1r1")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("c1r2")},
	}
	batch2 := []chunk.Record{
		{IngestTS: t3, SourceID: srcA, Raw: []byte("c2r1")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("c2r2")},
	}

	eng := setup(t, batch1, batch2)

	// First page: limit 3, reverse
	q := query.Query{Start: t5, End: t0, Limit: 3}
	seq1, nextToken1 := eng.Search(context.Background(), q, nil)
	results1, err := collect(seq1)
	if err != nil {
		t.Fatalf("page 1: unexpected error: %v", err)
	}
	if len(results1) != 3 {
		t.Fatalf("page 1: expected 3 results, got %d", len(results1))
	}
	// c2r2, c2r1, c1r2
	want := []string{"c2r2", "c2r1", "c1r2"}
	for i, w := range want {
		if string(results1[i].Raw) != w {
			t.Errorf("page 1 result[%d]: got %q, want %q", i, results1[i].Raw, w)
		}
	}

	token := nextToken1()
	if token == nil {
		t.Fatal("expected resume token after page 1")
	}

	// Second page: should get remaining record
	seq2, nextToken2 := eng.Search(context.Background(), q, token)
	results2, err := collect(seq2)
	if err != nil {
		t.Fatalf("page 2: unexpected error: %v", err)
	}
	if len(results2) != 1 {
		t.Fatalf("page 2: expected 1 result, got %d", len(results2))
	}
	if string(results2[0].Raw) != "c1r1" {
		t.Errorf("page 2: got %q, want %q", results2[0].Raw, "c1r1")
	}

	token = nextToken2()
	if token != nil {
		t.Errorf("expected nil token after last page, got %+v", token)
	}
}

func TestQueryReverse(t *testing.T) {
	// Test Query.Reverse() method
	tests := []struct {
		name    string
		q       query.Query
		reverse bool
	}{
		{"forward: both set, Start < End", query.Query{Start: t1, End: t3}, false},
		{"reverse: both set, End < Start", query.Query{Start: t3, End: t1}, true},
		{"forward: only Start set", query.Query{Start: t1}, false},
		{"forward: only End set", query.Query{End: t3}, false},
		{"forward: neither set", query.Query{}, false},
		{"forward: equal times", query.Query{Start: t1, End: t1}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.q.Reverse(); got != tt.reverse {
				t.Errorf("Query.Reverse() = %v, want %v", got, tt.reverse)
			}
		})
	}
}

func TestQueryTimeBounds(t *testing.T) {
	// Test Query.TimeBounds() method
	tests := []struct {
		name      string
		q         query.Query
		wantLower time.Time
		wantUpper time.Time
	}{
		{"forward", query.Query{Start: t1, End: t3}, t1, t3},
		{"reverse", query.Query{Start: t3, End: t1}, t1, t3},
		{"only Start", query.Query{Start: t2}, t2, time.Time{}},
		{"only End", query.Query{End: t2}, time.Time{}, t2},
		{"neither", query.Query{}, time.Time{}, time.Time{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lower, upper := tt.q.TimeBounds()
			if !lower.Equal(tt.wantLower) {
				t.Errorf("TimeBounds() lower = %v, want %v", lower, tt.wantLower)
			}
			if !upper.Equal(tt.wantUpper) {
				t.Errorf("TimeBounds() upper = %v, want %v", upper, tt.wantUpper)
			}
		})
	}
}

// =============================================================================
// SearchThenFollow tests
// =============================================================================

func TestSearchThenFollowSealedChunk(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("info start")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("error happened")},
		{IngestTS: t3, SourceID: srcB, Raw: []byte("info from B")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("info recovery")},
	}

	eng := setup(t, records)

	// Search for "error", then follow all subsequent records.
	seq, _ := eng.SearchThenFollow(context.Background(), query.Query{
		Tokens: []string{"error"},
	}, nil)

	results, err := collect(seq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should get: error record + all following records (regardless of source/token)
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	want := []string{"error happened", "info from B", "info recovery"}
	for i, w := range want {
		if string(results[i].Raw) != w {
			t.Errorf("result[%d]: got %q, want %q", i, results[i].Raw, w)
		}
	}
}

func TestSearchThenFollowSealedAndActive(t *testing.T) {
	sealed := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("info one")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("error found")},
	}
	active := []chunk.Record{
		{IngestTS: t3, SourceID: srcB, Raw: []byte("active one")},
		{IngestTS: t4, SourceID: srcB, Raw: []byte("active two")},
	}

	eng := setupWithActive(t, [][]chunk.Record{sealed}, active)

	// Search for "error", then follow into active chunk.
	seq, _ := eng.SearchThenFollow(context.Background(), query.Query{
		Tokens: []string{"error"},
	}, nil)

	results, err := collect(seq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should get: error record + all following (crossing into active chunk)
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	want := []string{"error found", "active one", "active two"}
	for i, w := range want {
		if string(results[i].Raw) != w {
			t.Errorf("result[%d]: got %q, want %q", i, results[i].Raw, w)
		}
	}
}

func TestSearchThenFollowWithSourceFilter(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("A info")},
		{IngestTS: t2, SourceID: srcB, Raw: []byte("B info")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("A error")},
		{IngestTS: t4, SourceID: srcB, Raw: []byte("B after")},
		{IngestTS: t5, SourceID: srcA, Raw: []byte("A after")},
	}

	eng := setup(t, records)

	// Search for "error" from srcA, then follow all.
	seq, _ := eng.SearchThenFollow(context.Background(), query.Query{
		Tokens:  []string{"error"},
		Sources: []chunk.SourceID{srcA},
	}, nil)

	results, err := collect(seq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should get: A error + B after + A after (source filter dropped after match)
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	want := []string{"A error", "B after", "A after"}
	for i, w := range want {
		if string(results[i].Raw) != w {
			t.Errorf("result[%d]: got %q, want %q", i, results[i].Raw, w)
		}
	}
}

func TestSearchThenFollowWithLimit(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("info")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("error")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("after1")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("after2")},
		{IngestTS: t5, SourceID: srcA, Raw: []byte("after3")},
	}

	eng := setup(t, records)

	// Search for "error", follow with limit 3.
	seq, _ := eng.SearchThenFollow(context.Background(), query.Query{
		Tokens: []string{"error"},
		Limit:  3,
	}, nil)

	results, err := collect(seq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	want := []string{"error", "after1", "after2"}
	for i, w := range want {
		if string(results[i].Raw) != w {
			t.Errorf("result[%d]: got %q, want %q", i, results[i].Raw, w)
		}
	}
}

func TestSearchThenFollowNoMatch(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("info one")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("info two")},
	}

	eng := setup(t, records)

	// Search for "error" - no match.
	seq, _ := eng.SearchThenFollow(context.Background(), query.Query{
		Tokens: []string{"error"},
	}, nil)

	results, err := collect(seq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

// =============================================================================
// SearchWithContext tests
// =============================================================================

func TestSearchWithContextBeforeAndAfter(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("before2")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("before1")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("error match")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("after1")},
		{IngestTS: t5, SourceID: srcA, Raw: []byte("after2")},
	}

	eng := setup(t, records)

	seq, _ := eng.SearchWithContext(context.Background(), query.Query{
		Tokens:        []string{"error"},
		ContextBefore: 2,
		ContextAfter:  2,
	})

	results, err := collect(seq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should get: 2 before + match + 2 after = 5
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	want := []string{"before2", "before1", "error match", "after1", "after2"}
	for i, w := range want {
		if string(results[i].Raw) != w {
			t.Errorf("result[%d]: got %q, want %q", i, results[i].Raw, w)
		}
	}
}

func TestSearchWithContextBeforeOnly(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("ctx1")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("ctx2")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("ctx3")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("error")},
		{IngestTS: t5, SourceID: srcA, Raw: []byte("after")},
	}

	eng := setup(t, records)

	seq, _ := eng.SearchWithContext(context.Background(), query.Query{
		Tokens:        []string{"error"},
		ContextBefore: 2,
	})

	results, err := collect(seq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should get: 2 before + match = 3
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	want := []string{"ctx2", "ctx3", "error"}
	for i, w := range want {
		if string(results[i].Raw) != w {
			t.Errorf("result[%d]: got %q, want %q", i, results[i].Raw, w)
		}
	}
}

func TestSearchWithContextAfterOnly(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("before")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("error")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("after1")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("after2")},
		{IngestTS: t5, SourceID: srcA, Raw: []byte("after3")},
	}

	eng := setup(t, records)

	seq, _ := eng.SearchWithContext(context.Background(), query.Query{
		Tokens:       []string{"error"},
		ContextAfter: 2,
	})

	results, err := collect(seq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should get: match + 2 after = 3
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	want := []string{"error", "after1", "after2"}
	for i, w := range want {
		if string(results[i].Raw) != w {
			t.Errorf("result[%d]: got %q, want %q", i, results[i].Raw, w)
		}
	}
}

func TestSearchWithContextCrossChunkBefore(t *testing.T) {
	batch1 := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("chunk1 rec1")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("chunk1 rec2")},
	}
	batch2 := []chunk.Record{
		{IngestTS: t3, SourceID: srcA, Raw: []byte("error in chunk2")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("chunk2 after")},
	}

	eng := setup(t, batch1, batch2)

	seq, _ := eng.SearchWithContext(context.Background(), query.Query{
		Tokens:        []string{"error"},
		ContextBefore: 3, // Request 3, but only 2 exist before
	})

	results, err := collect(seq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should get: 2 from chunk1 + match = 3
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	want := []string{"chunk1 rec1", "chunk1 rec2", "error in chunk2"}
	for i, w := range want {
		if string(results[i].Raw) != w {
			t.Errorf("result[%d]: got %q, want %q", i, results[i].Raw, w)
		}
	}
}

func TestSearchWithContextCrossChunkAfter(t *testing.T) {
	batch1 := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("before")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("error in chunk1")},
	}
	batch2 := []chunk.Record{
		{IngestTS: t3, SourceID: srcA, Raw: []byte("chunk2 rec1")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("chunk2 rec2")},
	}

	eng := setup(t, batch1, batch2)

	seq, _ := eng.SearchWithContext(context.Background(), query.Query{
		Tokens:       []string{"error"},
		ContextAfter: 3, // Request 3, but only 2 exist after
	})

	results, err := collect(seq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should get: match + 2 from chunk2 = 3
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	want := []string{"error in chunk1", "chunk2 rec1", "chunk2 rec2"}
	for i, w := range want {
		if string(results[i].Raw) != w {
			t.Errorf("result[%d]: got %q, want %q", i, results[i].Raw, w)
		}
	}
}

func TestSearchWithContextActiveChunk(t *testing.T) {
	sealed := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("sealed before")},
	}
	active := []chunk.Record{
		{IngestTS: t2, SourceID: srcA, Raw: []byte("active before")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("error active")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("active after")},
	}

	eng := setupWithActive(t, [][]chunk.Record{sealed}, active)

	seq, _ := eng.SearchWithContext(context.Background(), query.Query{
		Tokens:        []string{"error"},
		ContextBefore: 2,
		ContextAfter:  1,
	})

	results, err := collect(seq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should get: 2 before (crossing from sealed) + match + 1 after = 4
	if len(results) != 4 {
		t.Fatalf("expected 4 results, got %d", len(results))
	}
	want := []string{"sealed before", "active before", "error active", "active after"}
	for i, w := range want {
		if string(results[i].Raw) != w {
			t.Errorf("result[%d]: got %q, want %q", i, results[i].Raw, w)
		}
	}
}

func TestSearchWithContextReverse(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("oldest")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("before match")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("error match")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("after match")},
		{IngestTS: t5, SourceID: srcA, Raw: []byte("newest")},
	}

	eng := setup(t, records)

	// Reverse search with context.
	// In reverse, "before" means chronologically after, "after" means chronologically before.
	seq, _ := eng.SearchWithContext(context.Background(), query.Query{
		Start:         t5.Add(time.Second), // Upper bound (exclusive in forward terms)
		End:           t0,                  // Lower bound
		Tokens:        []string{"error"},
		ContextBefore: 1, // 1 record that comes before in iteration order (newer)
		ContextAfter:  1, // 1 record that comes after in iteration order (older)
	})

	results, err := collect(seq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// In reverse: iteration yields newest first.
	// Context before (in iteration order) = after match (newer)
	// Context after (in iteration order) = before match (older)
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	// Reverse iteration order: after match, error match, before match
	want := []string{"after match", "error match", "before match"}
	for i, w := range want {
		if string(results[i].Raw) != w {
			t.Errorf("result[%d]: got %q, want %q", i, results[i].Raw, w)
		}
	}
}

func TestSearchWithContextMultipleMatches(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("ctx1")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("error one")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("middle")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("error two")},
		{IngestTS: t5, SourceID: srcA, Raw: []byte("ctx2")},
	}

	eng := setup(t, records)

	seq, _ := eng.SearchWithContext(context.Background(), query.Query{
		Tokens:        []string{"error"},
		ContextBefore: 1,
		ContextAfter:  1,
	})

	results, err := collect(seq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// First match: ctx1, error one, middle
	// Second match: middle, error two, ctx2
	// Total: 6 records (middle appears twice)
	if len(results) != 6 {
		t.Fatalf("expected 6 results, got %d", len(results))
	}
	want := []string{"ctx1", "error one", "middle", "middle", "error two", "ctx2"}
	for i, w := range want {
		if string(results[i].Raw) != w {
			t.Errorf("result[%d]: got %q, want %q", i, results[i].Raw, w)
		}
	}
}

func TestSearchWithContextNoMatch(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("info one")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("info two")},
	}

	eng := setup(t, records)

	seq, _ := eng.SearchWithContext(context.Background(), query.Query{
		Tokens:        []string{"error"},
		ContextBefore: 2,
		ContextAfter:  2,
	})

	results, err := collect(seq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestSearchWithContextLimit(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("before")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("error")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("after1")},
		{IngestTS: t4, SourceID: srcA, Raw: []byte("after2")},
	}

	eng := setup(t, records)

	seq, _ := eng.SearchWithContext(context.Background(), query.Query{
		Tokens:        []string{"error"},
		ContextBefore: 1,
		ContextAfter:  2,
		Limit:         3, // Limit to 3 total
	})

	results, err := collect(seq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should get: before + error + after1 = 3 (limit reached)
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	want := []string{"before", "error", "after1"}
	for i, w := range want {
		if string(results[i].Raw) != w {
			t.Errorf("result[%d]: got %q, want %q", i, results[i].Raw, w)
		}
	}
}

// TestSearchSealedWithoutIndexes verifies that sealed chunks without indexes
// fall back to sequential scanning instead of returning an error.
func TestSearchSealedWithoutIndexes(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("error one")},
		{IngestTS: t2, SourceID: srcB, Raw: []byte("warning two")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("error three")},
	}

	// Create chunk manager and append records.
	all := records
	cm, err := chunkmem.NewManager(chunkmem.Config{
		MaxRecords: 10000,
		Now:        fakeClock(all),
	})
	if err != nil {
		t.Fatalf("new chunk manager: %v", err)
	}

	for _, rec := range records {
		if _, _, err := cm.Append(rec); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// Seal the chunk but DO NOT build indexes.
	if err := cm.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Create index manager that will return ErrIndexNotFound.
	timeIdx := memtime.NewIndexer(cm, 1)
	srcIdx := memsource.NewIndexer(cm)
	tokIdx := memtoken.NewIndexer(cm)
	im := indexmem.NewManager(
		[]index.Indexer{timeIdx, srcIdx, tokIdx},
		timeIdx,
		srcIdx,
		tokIdx,
		nil,
	)
	// Note: NOT calling buildIndexes - indexes don't exist.

	eng := query.New(cm, im, nil)

	// Query with time filter - should fall back to sequential scan.
	t.Run("time filter", func(t *testing.T) {
		seq := search(eng, context.Background(), query.Query{
			Start: t1,
			End:   t4,
		})
		results, err := collect(seq)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 3 {
			t.Fatalf("expected 3 results, got %d", len(results))
		}
	})

	// Query with source filter - should fall back to sequential scan.
	t.Run("source filter", func(t *testing.T) {
		seq := search(eng, context.Background(), query.Query{
			Sources: []chunk.SourceID{srcA},
		})
		results, err := collect(seq)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(results))
		}
		if string(results[0].Raw) != "error one" || string(results[1].Raw) != "error three" {
			t.Errorf("unexpected results: %v", results)
		}
	})

	// Query with token filter - should fall back to sequential scan.
	t.Run("token filter", func(t *testing.T) {
		seq := search(eng, context.Background(), query.Query{
			Tokens: []string{"error"},
		})
		results, err := collect(seq)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(results))
		}
		if string(results[0].Raw) != "error one" || string(results[1].Raw) != "error three" {
			t.Errorf("unexpected results: %v", results)
		}
	})

	// Combined filters - should fall back to sequential scan.
	t.Run("combined filters", func(t *testing.T) {
		seq := search(eng, context.Background(), query.Query{
			Sources: []chunk.SourceID{srcA},
			Tokens:  []string{"error"},
		})
		results, err := collect(seq)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(results))
		}
	})
}

// TestCrossChunkOrdering verifies that records are returned in correct order
// when spanning multiple chunks.
func TestCrossChunkOrdering(t *testing.T) {
	// Create multiple chunks with sequential timestamps
	chunk1 := []chunk.Record{
		{IngestTS: t0.Add(0 * time.Second), SourceID: srcA, Raw: []byte("c1-r1")},
		{IngestTS: t0.Add(1 * time.Second), SourceID: srcA, Raw: []byte("c1-r2")},
		{IngestTS: t0.Add(2 * time.Second), SourceID: srcA, Raw: []byte("c1-r3")},
	}
	chunk2 := []chunk.Record{
		{IngestTS: t0.Add(3 * time.Second), SourceID: srcA, Raw: []byte("c2-r1")},
		{IngestTS: t0.Add(4 * time.Second), SourceID: srcA, Raw: []byte("c2-r2")},
		{IngestTS: t0.Add(5 * time.Second), SourceID: srcA, Raw: []byte("c2-r3")},
	}
	chunk3 := []chunk.Record{
		{IngestTS: t0.Add(6 * time.Second), SourceID: srcA, Raw: []byte("c3-r1")},
		{IngestTS: t0.Add(7 * time.Second), SourceID: srcA, Raw: []byte("c3-r2")},
		{IngestTS: t0.Add(8 * time.Second), SourceID: srcA, Raw: []byte("c3-r3")},
	}

	eng := setup(t, chunk1, chunk2, chunk3)

	t.Run("full scan ordering", func(t *testing.T) {
		results, err := collect(search(eng, context.Background(), query.Query{}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 9 {
			t.Fatalf("expected 9 results, got %d", len(results))
		}

		// Verify strict ordering
		want := []string{"c1-r1", "c1-r2", "c1-r3", "c2-r1", "c2-r2", "c2-r3", "c3-r1", "c3-r2", "c3-r3"}
		for i, w := range want {
			if string(results[i].Raw) != w {
				t.Errorf("result[%d]: got %q, want %q", i, results[i].Raw, w)
			}
		}

		// Verify timestamps are monotonically increasing
		for i := 1; i < len(results); i++ {
			if !results[i].IngestTS.After(results[i-1].IngestTS) {
				t.Errorf("timestamp not monotonic at %d: %v <= %v",
					i, results[i].IngestTS, results[i-1].IngestTS)
			}
		}
	})

	t.Run("time range spanning chunks", func(t *testing.T) {
		// Query from middle of chunk1 to middle of chunk3
		start := t0.Add(1 * time.Second) // c1-r2
		end := t0.Add(7 * time.Second)   // exclusive, so up to c3-r1

		results, err := collect(search(eng, context.Background(), query.Query{Start: start, End: end}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		want := []string{"c1-r2", "c1-r3", "c2-r1", "c2-r2", "c2-r3", "c3-r1"}
		if len(results) != len(want) {
			t.Fatalf("expected %d results, got %d", len(want), len(results))
		}
		for i, w := range want {
			if string(results[i].Raw) != w {
				t.Errorf("result[%d]: got %q, want %q", i, results[i].Raw, w)
			}
		}
	})
}

// TestCrossChunkResume verifies that resume tokens work correctly across chunk boundaries.
func TestCrossChunkResume(t *testing.T) {
	chunk1 := []chunk.Record{
		{IngestTS: t0.Add(0 * time.Second), SourceID: srcA, Raw: []byte("c1-r1")},
		{IngestTS: t0.Add(1 * time.Second), SourceID: srcA, Raw: []byte("c1-r2")},
	}
	chunk2 := []chunk.Record{
		{IngestTS: t0.Add(2 * time.Second), SourceID: srcA, Raw: []byte("c2-r1")},
		{IngestTS: t0.Add(3 * time.Second), SourceID: srcA, Raw: []byte("c2-r2")},
	}

	eng := setup(t, chunk1, chunk2)

	t.Run("resume at chunk boundary", func(t *testing.T) {
		// Get first 2 records (all of chunk1)
		seq, getToken := eng.Search(context.Background(), query.Query{Limit: 2}, nil)
		results, err := collect(seq)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(results))
		}

		// Resume should get chunk2
		token := getToken()
		if token == nil {
			t.Fatal("expected non-nil resume token")
		}

		seq2, _ := eng.Search(context.Background(), query.Query{}, token)
		results2, err := collect(seq2)
		if err != nil {
			t.Fatalf("unexpected error on resume: %v", err)
		}
		if len(results2) != 2 {
			t.Fatalf("expected 2 results on resume, got %d", len(results2))
		}

		want := []string{"c2-r1", "c2-r2"}
		for i, w := range want {
			if string(results2[i].Raw) != w {
				t.Errorf("result[%d]: got %q, want %q", i, results2[i].Raw, w)
			}
		}
	})

	t.Run("resume mid-chunk", func(t *testing.T) {
		// Get first 3 records (all of chunk1 + first of chunk2)
		seq, getToken := eng.Search(context.Background(), query.Query{Limit: 3}, nil)
		results, err := collect(seq)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 3 {
			t.Fatalf("expected 3 results, got %d", len(results))
		}

		// Resume should get last record of chunk2
		token := getToken()
		if token == nil {
			t.Fatal("expected non-nil resume token")
		}

		seq2, _ := eng.Search(context.Background(), query.Query{}, token)
		results2, err := collect(seq2)
		if err != nil {
			t.Fatalf("unexpected error on resume: %v", err)
		}
		if len(results2) != 1 {
			t.Fatalf("expected 1 result on resume, got %d", len(results2))
		}
		if string(results2[0].Raw) != "c2-r2" {
			t.Errorf("got %q, want %q", results2[0].Raw, "c2-r2")
		}
	})

	t.Run("repeated resume with no new records", func(t *testing.T) {
		// Get all 4 records
		seq, getToken := eng.Search(context.Background(), query.Query{}, nil)
		results, err := collect(seq)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 4 {
			t.Fatalf("expected 4 results, got %d", len(results))
		}

		// Token after completion should be nil (fully consumed)
		token := getToken()
		if token != nil {
			// If token is non-nil, resuming should return empty
			seq2, _ := eng.Search(context.Background(), query.Query{}, token)
			results2, err := collect(seq2)
			if err != nil {
				t.Fatalf("unexpected error on resume: %v", err)
			}
			if len(results2) != 0 {
				t.Errorf("expected 0 results on resume after full consumption, got %d", len(results2))
			}
		}
	})
}

// TestCrossChunkWithActiveChunk verifies ordering when there's an unsealed chunk.
func TestCrossChunkWithActiveChunk(t *testing.T) {
	sealed1 := []chunk.Record{
		{IngestTS: t0.Add(0 * time.Second), SourceID: srcA, Raw: []byte("s1-r1")},
		{IngestTS: t0.Add(1 * time.Second), SourceID: srcA, Raw: []byte("s1-r2")},
	}
	sealed2 := []chunk.Record{
		{IngestTS: t0.Add(2 * time.Second), SourceID: srcA, Raw: []byte("s2-r1")},
		{IngestTS: t0.Add(3 * time.Second), SourceID: srcA, Raw: []byte("s2-r2")},
	}
	active := []chunk.Record{
		{IngestTS: t0.Add(4 * time.Second), SourceID: srcA, Raw: []byte("a-r1")},
		{IngestTS: t0.Add(5 * time.Second), SourceID: srcA, Raw: []byte("a-r2")},
	}

	eng := setupWithActive(t, [][]chunk.Record{sealed1, sealed2}, active)

	t.Run("full scan includes active", func(t *testing.T) {
		results, err := collect(search(eng, context.Background(), query.Query{}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		want := []string{"s1-r1", "s1-r2", "s2-r1", "s2-r2", "a-r1", "a-r2"}
		if len(results) != len(want) {
			t.Fatalf("expected %d results, got %d", len(want), len(results))
		}
		for i, w := range want {
			if string(results[i].Raw) != w {
				t.Errorf("result[%d]: got %q, want %q", i, results[i].Raw, w)
			}
		}
	})

	t.Run("resume into active chunk", func(t *testing.T) {
		// Get first 5 records (all sealed + first active)
		seq, getToken := eng.Search(context.Background(), query.Query{Limit: 5}, nil)
		results, err := collect(seq)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 5 {
			t.Fatalf("expected 5 results, got %d", len(results))
		}

		// Resume should get last active record
		token := getToken()
		if token == nil {
			t.Fatal("expected non-nil resume token")
		}

		seq2, _ := eng.Search(context.Background(), query.Query{}, token)
		results2, err := collect(seq2)
		if err != nil {
			t.Fatalf("unexpected error on resume: %v", err)
		}
		if len(results2) != 1 {
			t.Fatalf("expected 1 result on resume, got %d", len(results2))
		}
		if string(results2[0].Raw) != "a-r2" {
			t.Errorf("got %q, want %q", results2[0].Raw, "a-r2")
		}
	})
}
