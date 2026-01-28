package query_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kluzzebass/gastrolog/internal/chunk"
	chunkmem "github.com/kluzzebass/gastrolog/internal/chunk/memory"
	"github.com/kluzzebass/gastrolog/internal/index"
	indexmem "github.com/kluzzebass/gastrolog/internal/index/memory"
	memsource "github.com/kluzzebass/gastrolog/internal/index/memory/source"
	memtime "github.com/kluzzebass/gastrolog/internal/index/memory/time"
	"github.com/kluzzebass/gastrolog/internal/query"
)

// collect gathers all records from the iterator, returning the first error encountered.
func collect(it func(yield func(chunk.Record, error) bool)) ([]chunk.Record, error) {
	var results []chunk.Record
	var firstErr error
	for rec, err := range it {
		if err != nil {
			firstErr = err
			break
		}
		results = append(results, rec)
	}
	return results, firstErr
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
		MaxChunkBytes: 1 << 20, // 1 MiB per chunk — large enough to not auto-rotate
		Now:           fakeClock(all),
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

	im := indexmem.NewManager(
		[]index.Indexer{timeIdx, srcIdx},
		timeIdx,
		srcIdx,
		nil, // no token index
	)

	buildIndexes(t, cm, im)

	return query.New(cm, im)
}

// setupWithActive is like setup but leaves the last batch unsealed (active chunk).
func setupWithActive(t *testing.T, sealed [][]chunk.Record, active []chunk.Record) *query.Engine {
	t.Helper()

	all := append(allRecords(sealed), active...)
	cm, err := chunkmem.NewManager(chunkmem.Config{
		MaxChunkBytes: 1 << 20,
		Now:           fakeClock(all),
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

	im := indexmem.NewManager(
		[]index.Indexer{timeIdx, srcIdx},
		timeIdx,
		srcIdx,
		nil, // no token index
	)

	buildIndexes(t, cm, im)

	return query.New(cm, im)
}

func TestSearchNoChunks(t *testing.T) {
	eng := setup(t)

	results, err := collect(eng.Search(context.Background(), query.Query{}))
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

	results, err := collect(eng.Search(context.Background(), query.Query{}))
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

	results, err := collect(eng.Search(context.Background(), query.Query{Start: t2, End: t4}))
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

	results, err := collect(eng.Search(context.Background(), query.Query{Source: &srcA}))
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

	results, err := collect(eng.Search(context.Background(), query.Query{}))
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

	results, err := collect(eng.Search(context.Background(), query.Query{}))
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
	results, err := collect(eng.Search(context.Background(), query.Query{Start: t2, End: t4}))
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

	results, err := collect(eng.Search(context.Background(), query.Query{Source: &srcA}))
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
	results, err := collect(eng.Search(context.Background(), query.Query{
		Start:  t2,
		End:    t5,
		Source: &srcA,
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

	results, err := collect(eng.Search(context.Background(), query.Query{Limit: 2}))
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

	results, err := collect(eng.Search(context.Background(), query.Query{}))
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
	results, err := collect(eng.Search(context.Background(), query.Query{Limit: 3}))
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

	_, err := collect(eng.Search(ctx, query.Query{}))
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
	results, err := collect(eng.Search(context.Background(), query.Query{Start: t1, End: t4}))
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
	results, err := collect(eng.Search(context.Background(), query.Query{Source: &srcB}))
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
	for rec, err := range eng.Search(context.Background(), query.Query{}) {
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
	for rec, err := range eng.Search(ctx, query.Query{}) {
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
	results, err := collect(eng.Search(context.Background(), query.Query{Start: t3}))
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
	results, err := collect(eng.Search(context.Background(), query.Query{End: t3}))
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
	results, err := collect(eng.Search(context.Background(), query.Query{Source: &srcA, Limit: 2}))
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
	results, err := collect(eng.Search(context.Background(), query.Query{Start: t2, End: t5, Limit: 2}))
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

	results, err := collect(eng.Search(context.Background(), query.Query{Limit: 2}))
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
