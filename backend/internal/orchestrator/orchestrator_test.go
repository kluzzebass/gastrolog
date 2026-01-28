package orchestrator_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	indexmem "gastrolog/internal/index/memory"
	memsource "gastrolog/internal/index/memory/source"
	memtime "gastrolog/internal/index/memory/time"
	memtoken "gastrolog/internal/index/memory/token"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
)

var (
	t0 = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 = t0.Add(1 * time.Second)
	t2 = t0.Add(2 * time.Second)
	t3 = t0.Add(3 * time.Second)

	srcA = chunk.NewSourceID()
)

// trackingIndexManager wraps an IndexManager to track BuildIndexes calls.
type trackingIndexManager struct {
	index.IndexManager
	buildCount atomic.Int32
	lastBuilt  atomic.Value // chunk.ChunkID
}

func (t *trackingIndexManager) BuildIndexes(ctx context.Context, chunkID chunk.ChunkID) error {
	t.buildCount.Add(1)
	t.lastBuilt.Store(chunkID)
	return t.IndexManager.BuildIndexes(ctx, chunkID)
}

func newTestSetup(maxChunkBytes int64) (*orchestrator.Orchestrator, chunk.ChunkManager, *trackingIndexManager) {
	cm, _ := chunkmem.NewManager(chunkmem.Config{
		MaxChunkBytes: maxChunkBytes,
	})

	timeIdx := memtime.NewIndexer(cm, 1)
	srcIdx := memsource.NewIndexer(cm)
	tokIdx := memtoken.NewIndexer(cm)

	im := indexmem.NewManager(
		[]index.Indexer{timeIdx, srcIdx, tokIdx},
		timeIdx,
		srcIdx,
		tokIdx,
	)

	tracker := &trackingIndexManager{IndexManager: im}
	qe := query.New(cm, im)

	orch := orchestrator.New()
	orch.RegisterChunkManager("default", cm)
	orch.RegisterIndexManager("default", tracker)
	orch.RegisterQueryEngine("default", qe)

	return orch, cm, tracker
}

func TestIngestReachesChunkManager(t *testing.T) {
	orch, cm, _ := newTestSetup(1 << 20) // Large chunk, no auto-seal

	rec := chunk.Record{
		IngestTS: t1,
		SourceID: srcA,
		Raw:      []byte("test message"),
	}

	if err := orch.Ingest(rec); err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Verify record reached chunk manager by querying.
	cursor, err := cm.OpenCursor(cm.Active().ID)
	if err != nil {
		t.Fatalf("OpenCursor failed: %v", err)
	}
	defer cursor.Close()

	got, _, err := cursor.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	if string(got.Raw) != "test message" {
		t.Errorf("got %q, want %q", got.Raw, "test message")
	}
}

func TestIngestMultipleRecords(t *testing.T) {
	orch, cm, _ := newTestSetup(1 << 20)

	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("one")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("two")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("three")},
	}

	for _, rec := range records {
		if err := orch.Ingest(rec); err != nil {
			t.Fatalf("Ingest failed: %v", err)
		}
	}

	// Verify all records present.
	cursor, err := cm.OpenCursor(cm.Active().ID)
	if err != nil {
		t.Fatalf("OpenCursor failed: %v", err)
	}
	defer cursor.Close()

	var got []string
	for {
		rec, _, err := cursor.Next()
		if err == chunk.ErrNoMoreRecords {
			break
		}
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		got = append(got, string(rec.Raw))
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 records, got %d", len(got))
	}
	want := []string{"one", "two", "three"}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("record[%d]: got %q, want %q", i, got[i], w)
		}
	}
}

func TestSealedChunkTriggersIndexBuild(t *testing.T) {
	// Memory manager's MaxChunkBytes is actually record count.
	// Set to 2 so third record triggers seal.
	orch, _, tracker := newTestSetup(2)

	// Ingest 3 records to trigger seal (chunk fills at 2, third causes seal).
	for i := 0; i < 3; i++ {
		rec := chunk.Record{
			IngestTS: t1.Add(time.Duration(i) * time.Second),
			SourceID: srcA,
			Raw:      []byte("record"),
		}
		if err := orch.Ingest(rec); err != nil {
			t.Fatalf("Ingest failed: %v", err)
		}
	}

	// Wait for async build to complete.
	time.Sleep(100 * time.Millisecond)

	// Should have triggered at least one build.
	count := tracker.buildCount.Load()
	if count == 0 {
		t.Error("expected at least one index build, got none")
	}

	// Verify the built chunk ID is valid.
	lastBuilt := tracker.lastBuilt.Load()
	if lastBuilt == nil {
		t.Error("lastBuilt is nil")
	}
}

func TestIndexBuildTriggeredOncePerSeal(t *testing.T) {
	// Set chunk size to 2 records.
	orch, _, tracker := newTestSetup(2)

	// Ingest 3 records to trigger exactly one seal.
	for i := 0; i < 3; i++ {
		rec := chunk.Record{
			IngestTS: t1.Add(time.Duration(i) * time.Second),
			SourceID: srcA,
			Raw:      []byte("record"),
		}
		if err := orch.Ingest(rec); err != nil {
			t.Fatalf("Ingest failed: %v", err)
		}
	}

	// Wait for async build.
	time.Sleep(100 * time.Millisecond)

	count := tracker.buildCount.Load()
	if count != 1 {
		t.Errorf("expected exactly 1 index build, got %d", count)
	}
}

func TestSearchViaOrchestrator(t *testing.T) {
	orch, cm, _ := newTestSetup(1 << 20)

	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("one")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("two")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("three")},
	}

	for _, rec := range records {
		if err := orch.Ingest(rec); err != nil {
			t.Fatalf("Ingest failed: %v", err)
		}
	}

	// Search via orchestrator.
	seq, _, err := orch.Search(context.Background(), "default", query.Query{}, nil)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	var results []string
	for rec, err := range seq {
		if err != nil {
			t.Fatalf("iteration error: %v", err)
		}
		results = append(results, string(rec.Raw))
	}

	// Compare with direct query engine call.
	qe := query.New(cm, indexmem.NewManager(nil, nil, nil, nil))
	directSeq, _ := qe.Search(context.Background(), query.Query{}, nil)

	var directResults []string
	for rec, err := range directSeq {
		if err != nil {
			t.Fatalf("direct iteration error: %v", err)
		}
		directResults = append(directResults, string(rec.Raw))
	}

	if len(results) != len(directResults) {
		t.Fatalf("result count mismatch: orchestrator=%d, direct=%d", len(results), len(directResults))
	}

	for i := range results {
		if results[i] != directResults[i] {
			t.Errorf("result[%d]: orchestrator=%q, direct=%q", i, results[i], directResults[i])
		}
	}
}

func TestSearchWithFilter(t *testing.T) {
	orch, _, _ := newTestSetup(1 << 20)

	srcB := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("from A")},
		{IngestTS: t2, SourceID: srcB, Raw: []byte("from B")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("from A again")},
	}

	for _, rec := range records {
		if err := orch.Ingest(rec); err != nil {
			t.Fatalf("Ingest failed: %v", err)
		}
	}

	// Search with source filter.
	seq, _, err := orch.Search(context.Background(), "default", query.Query{
		Sources: []chunk.SourceID{srcA},
	}, nil)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	var results []string
	for rec, err := range seq {
		if err != nil {
			t.Fatalf("iteration error: %v", err)
		}
		results = append(results, string(rec.Raw))
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0] != "from A" || results[1] != "from A again" {
		t.Errorf("unexpected results: %v", results)
	}
}

func TestSearchDefaultKey(t *testing.T) {
	orch, _, _ := newTestSetup(1 << 20)

	rec := chunk.Record{IngestTS: t1, SourceID: srcA, Raw: []byte("test")}
	if err := orch.Ingest(rec); err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Empty key should use "default".
	seq, _, err := orch.Search(context.Background(), "", query.Query{}, nil)
	if err != nil {
		t.Fatalf("Search with empty key failed: %v", err)
	}

	count := 0
	for _, err := range seq {
		if err != nil {
			t.Fatalf("iteration error: %v", err)
		}
		count++
	}

	if count != 1 {
		t.Errorf("expected 1 result, got %d", count)
	}
}

func TestSearchUnknownRegistry(t *testing.T) {
	orch, _, _ := newTestSetup(1 << 20)

	_, _, err := orch.Search(context.Background(), "nonexistent", query.Query{}, nil)
	if err != orchestrator.ErrUnknownRegistry {
		t.Errorf("expected ErrUnknownRegistry, got %v", err)
	}
}

func TestIngestNoChunkManagers(t *testing.T) {
	orch := orchestrator.New()

	rec := chunk.Record{IngestTS: t1, SourceID: srcA, Raw: []byte("test")}
	err := orch.Ingest(rec)
	if err != orchestrator.ErrNoChunkManagers {
		t.Errorf("expected ErrNoChunkManagers, got %v", err)
	}
}

func TestSearchNoQueryEngines(t *testing.T) {
	orch := orchestrator.New()

	_, _, err := orch.Search(context.Background(), "default", query.Query{}, nil)
	if err != orchestrator.ErrNoQueryEngines {
		t.Errorf("expected ErrNoQueryEngines, got %v", err)
	}
}

func TestSearchThenFollowViaOrchestrator(t *testing.T) {
	orch, _, _ := newTestSetup(1 << 20)

	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("info")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("error found")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("after")},
	}

	for _, rec := range records {
		if err := orch.Ingest(rec); err != nil {
			t.Fatalf("Ingest failed: %v", err)
		}
	}

	seq, _, err := orch.SearchThenFollow(context.Background(), "default", query.Query{
		Tokens: []string{"error"},
	}, nil)
	if err != nil {
		t.Fatalf("SearchThenFollow failed: %v", err)
	}

	var results []string
	for rec, err := range seq {
		if err != nil {
			t.Fatalf("iteration error: %v", err)
		}
		results = append(results, string(rec.Raw))
	}

	// Should get error + after.
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0] != "error found" || results[1] != "after" {
		t.Errorf("unexpected results: %v", results)
	}
}

func TestSearchWithContextViaOrchestrator(t *testing.T) {
	orch, _, _ := newTestSetup(1 << 20)

	records := []chunk.Record{
		{IngestTS: t1, SourceID: srcA, Raw: []byte("before")},
		{IngestTS: t2, SourceID: srcA, Raw: []byte("error match")},
		{IngestTS: t3, SourceID: srcA, Raw: []byte("after")},
	}

	for _, rec := range records {
		if err := orch.Ingest(rec); err != nil {
			t.Fatalf("Ingest failed: %v", err)
		}
	}

	seq, _, err := orch.SearchWithContext(context.Background(), "default", query.Query{
		Tokens:        []string{"error"},
		ContextBefore: 1,
		ContextAfter:  1,
	})
	if err != nil {
		t.Fatalf("SearchWithContext failed: %v", err)
	}

	var results []string
	for rec, err := range seq {
		if err != nil {
			t.Fatalf("iteration error: %v", err)
		}
		results = append(results, string(rec.Raw))
	}

	// Should get before + match + after.
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	want := []string{"before", "error match", "after"}
	for i, w := range want {
		if results[i] != w {
			t.Errorf("result[%d]: got %q, want %q", i, results[i], w)
		}
	}
}
