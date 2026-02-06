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
	memattr "gastrolog/internal/index/memory/attr"
	"gastrolog/internal/index/memory/kv"
	memtoken "gastrolog/internal/index/memory/token"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
)

// recordCountPolicy creates a rotation policy for testing that rotates at maxRecords.
func recordCountPolicy(maxRecords int64) chunk.RotationPolicy {
	return chunk.NewRecordCountPolicy(uint64(maxRecords))
}

var (
	t0 = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 = t0.Add(1 * time.Second)
	t2 = t0.Add(2 * time.Second)
	t3 = t0.Add(3 * time.Second)

	attrsA = chunk.Attributes{"source": "srcA"}
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

func newTestSetup(maxRecords int64) (*orchestrator.Orchestrator, chunk.ChunkManager, *trackingIndexManager) {
	cm, _ := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: recordCountPolicy(maxRecords),
	})

	tokIdx := memtoken.NewIndexer(cm)
	attrIdx := memattr.NewIndexer(cm)
	kvIdx := kv.NewIndexer(cm)

	im := indexmem.NewManager(
		[]index.Indexer{tokIdx, attrIdx, kvIdx},
		tokIdx,
		attrIdx,
		kvIdx,
		nil,
	)

	tracker := &trackingIndexManager{IndexManager: im}
	qe := query.New(cm, im, nil)

	orch := orchestrator.New(orchestrator.Config{})
	orch.RegisterChunkManager("default", cm)
	orch.RegisterIndexManager("default", tracker)
	orch.RegisterQueryEngine("default", qe)

	return orch, cm, tracker
}

func TestIngestReachesChunkManager(t *testing.T) {
	orch, cm, _ := newTestSetup(1 << 20) // Large chunk, no auto-seal

	rec := chunk.Record{
		IngestTS: t1,
		Attrs:    attrsA,
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
		{IngestTS: t1, Attrs: attrsA, Raw: []byte("one")},
		{IngestTS: t2, Attrs: attrsA, Raw: []byte("two")},
		{IngestTS: t3, Attrs: attrsA, Raw: []byte("three")},
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
	// Set MaxRecords to 2 so third record triggers seal.
	orch, _, tracker := newTestSetup(2)

	// Ingest 3 records to trigger seal (chunk fills at 2, third causes seal).
	for i := 0; i < 3; i++ {
		rec := chunk.Record{
			IngestTS: t1.Add(time.Duration(i) * time.Second),
			Attrs:    attrsA,
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
			Attrs:    attrsA,
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
		{IngestTS: t1, Attrs: attrsA, Raw: []byte("one")},
		{IngestTS: t2, Attrs: attrsA, Raw: []byte("two")},
		{IngestTS: t3, Attrs: attrsA, Raw: []byte("three")},
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
	qe := query.New(cm, indexmem.NewManager(nil, nil, nil, nil, nil), nil)
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

func TestSearchDefaultKey(t *testing.T) {
	orch, _, _ := newTestSetup(1 << 20)

	rec := chunk.Record{IngestTS: t1, Attrs: attrsA, Raw: []byte("test")}
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
	orch := orchestrator.New(orchestrator.Config{})

	rec := chunk.Record{IngestTS: t1, Attrs: attrsA, Raw: []byte("test")}
	err := orch.Ingest(rec)
	if err != orchestrator.ErrNoChunkManagers {
		t.Errorf("expected ErrNoChunkManagers, got %v", err)
	}
}

func TestSearchNoQueryEngines(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})

	_, _, err := orch.Search(context.Background(), "default", query.Query{}, nil)
	if err != orchestrator.ErrNoQueryEngines {
		t.Errorf("expected ErrNoQueryEngines, got %v", err)
	}
}

func TestSearchThenFollowViaOrchestrator(t *testing.T) {
	orch, _, _ := newTestSetup(1 << 20)

	records := []chunk.Record{
		{IngestTS: t1, Attrs: attrsA, Raw: []byte("info")},
		{IngestTS: t2, Attrs: attrsA, Raw: []byte("error found")},
		{IngestTS: t3, Attrs: attrsA, Raw: []byte("after")},
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
		{IngestTS: t1, Attrs: attrsA, Raw: []byte("before")},
		{IngestTS: t2, Attrs: attrsA, Raw: []byte("error match")},
		{IngestTS: t3, Attrs: attrsA, Raw: []byte("after")},
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

// mockIngester is a test ingester that emits fixed messages.
type mockIngester struct {
	messages []orchestrator.IngestMessage
	started  chan struct{}
	stopped  chan struct{}
}

func newMockIngester(messages []orchestrator.IngestMessage) *mockIngester {
	return &mockIngester{
		messages: messages,
		started:  make(chan struct{}),
		stopped:  make(chan struct{}),
	}
}

func (r *mockIngester) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	close(r.started)
	defer close(r.stopped)

	for _, msg := range r.messages {
		// Set IngestTS to now, simulating when the ingester received the message.
		msg.IngestTS = time.Now()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- msg:
		}
	}

	// Wait for context cancellation.
	<-ctx.Done()
	return ctx.Err()
}

// blockingIngester blocks until context is cancelled.
type blockingIngester struct {
	started chan struct{}
	stopped chan struct{}
}

func newBlockingIngester() *blockingIngester {
	return &blockingIngester{
		started: make(chan struct{}),
		stopped: make(chan struct{}),
	}
}

func (r *blockingIngester) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	close(r.started)
	defer close(r.stopped)
	<-ctx.Done()
	return ctx.Err()
}

func newIngesterTestSetup() (*orchestrator.Orchestrator, chunk.ChunkManager) {
	cm, _ := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: recordCountPolicy(10000),
	})

	tokIdx := memtoken.NewIndexer(cm)
	attrIdx := memattr.NewIndexer(cm)
	kvIdx := kv.NewIndexer(cm)

	im := indexmem.NewManager(
		[]index.Indexer{tokIdx, attrIdx, kvIdx},
		tokIdx,
		attrIdx,
		kvIdx,
		nil,
	)

	qe := query.New(cm, im, nil)

	orch := orchestrator.New(orchestrator.Config{})
	orch.RegisterChunkManager("default", cm)
	orch.RegisterIndexManager("default", im)
	orch.RegisterQueryEngine("default", qe)

	return orch, cm
}

func TestIngesterMessageReachesChunkManager(t *testing.T) {
	orch, cm := newIngesterTestSetup()

	recv := newMockIngester([]orchestrator.IngestMessage{
		{Attrs: map[string]string{"host": "server1"}, Raw: []byte("test message")},
	})
	orch.RegisterIngester("test", recv)

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for ingester to start and message to be processed.
	<-recv.started
	time.Sleep(50 * time.Millisecond)

	// Stop orchestrator.
	if err := orch.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Verify record reached chunk manager.
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

func TestIngesterContextCancellation(t *testing.T) {
	orch, _ := newIngesterTestSetup()

	recv := newBlockingIngester()
	orch.RegisterIngester("test", recv)

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for ingester to start.
	<-recv.started

	// Stop should cancel context and ingester should exit.
	if err := orch.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Verify ingester stopped.
	select {
	case <-recv.stopped:
		// Good.
	case <-time.After(time.Second):
		t.Error("ingester did not stop after Stop()")
	}
}

func TestMultipleIngesters(t *testing.T) {
	orch, cm := newIngesterTestSetup()

	recv1 := newMockIngester([]orchestrator.IngestMessage{
		{Attrs: map[string]string{"source": "recv1"}, Raw: []byte("from recv1")},
	})
	recv2 := newMockIngester([]orchestrator.IngestMessage{
		{Attrs: map[string]string{"source": "recv2"}, Raw: []byte("from recv2")},
	})

	orch.RegisterIngester("recv1", recv1)
	orch.RegisterIngester("recv2", recv2)

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	<-recv1.started
	<-recv2.started
	time.Sleep(50 * time.Millisecond)

	if err := orch.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Verify both messages reached chunk manager.
	cursor, _ := cm.OpenCursor(cm.Active().ID)
	defer cursor.Close()

	var messages []string
	for {
		rec, _, err := cursor.Next()
		if err == chunk.ErrNoMoreRecords {
			break
		}
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		messages = append(messages, string(rec.Raw))
	}

	if len(messages) != 2 {
		t.Errorf("expected 2 messages, got %d", len(messages))
	}
}

func TestStartAlreadyRunning(t *testing.T) {
	orch, _ := newIngesterTestSetup()

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer orch.Stop()

	err := orch.Start(context.Background())
	if err != orchestrator.ErrAlreadyRunning {
		t.Errorf("expected ErrAlreadyRunning, got %v", err)
	}
}

func TestStopNotRunning(t *testing.T) {
	orch, _ := newIngesterTestSetup()

	err := orch.Stop()
	if err != orchestrator.ErrNotRunning {
		t.Errorf("expected ErrNotRunning, got %v", err)
	}
}

func TestIngesterIndexBuildOnSeal(t *testing.T) {
	// Set up with small chunk size to trigger seal.
	cm, _ := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: recordCountPolicy(2), // 2 records per chunk
	})

	tokIdx := memtoken.NewIndexer(cm)
	attrIdx := memattr.NewIndexer(cm)
	kvIdx := kv.NewIndexer(cm)

	im := indexmem.NewManager(
		[]index.Indexer{tokIdx, attrIdx, kvIdx},
		tokIdx,
		attrIdx,
		kvIdx,
		nil,
	)

	tracker := &trackingIndexManager{IndexManager: im}
	qe := query.New(cm, im, nil)

	orch := orchestrator.New(orchestrator.Config{})
	orch.RegisterChunkManager("default", cm)
	orch.RegisterIndexManager("default", tracker)
	orch.RegisterQueryEngine("default", qe)

	// Create ingester with 3 messages to trigger seal.
	recv := newMockIngester([]orchestrator.IngestMessage{
		{Attrs: map[string]string{"host": "s1"}, Raw: []byte("one")},
		{Attrs: map[string]string{"host": "s1"}, Raw: []byte("two")},
		{Attrs: map[string]string{"host": "s1"}, Raw: []byte("three")},
	})
	orch.RegisterIngester("test", recv)

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	<-recv.started
	time.Sleep(100 * time.Millisecond) // Wait for processing and async build.

	if err := orch.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Should have triggered at least one index build.
	count := tracker.buildCount.Load()
	if count == 0 {
		t.Error("expected at least one index build")
	}
}

func TestUnregisterIngester(t *testing.T) {
	orch, _ := newIngesterTestSetup()

	recv := newBlockingIngester()
	orch.RegisterIngester("test", recv)
	orch.UnregisterIngester("test")

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// No ingesters, so nothing should be started.
	// Give a moment then stop.
	time.Sleep(10 * time.Millisecond)

	if err := orch.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Ingester should not have been started.
	select {
	case <-recv.started:
		t.Error("ingester should not have been started after unregister")
	default:
		// Good.
	}
}

// countingIngester counts how many messages it sends.
type countingIngester struct {
	count   int
	started chan struct{}
}

func newCountingIngester(count int) *countingIngester {
	return &countingIngester{
		count:   count,
		started: make(chan struct{}),
	}
}

func (r *countingIngester) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	close(r.started)

	for i := 0; i < r.count; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- orchestrator.IngestMessage{
			Attrs:    map[string]string{"index": string(rune('a' + i))},
			Raw:      []byte("message"),
			IngestTS: time.Now(),
		}:
		}
	}

	<-ctx.Done()
	return ctx.Err()
}

func TestHighVolumeIngestion(t *testing.T) {
	orch, cm := newIngesterTestSetup()

	recv := newCountingIngester(100)
	orch.RegisterIngester("test", recv)

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	<-recv.started
	time.Sleep(100 * time.Millisecond) // Wait for all messages.

	if err := orch.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Count records in chunk manager.
	cursor, _ := cm.OpenCursor(cm.Active().ID)
	defer cursor.Close()

	count := 0
	for {
		_, _, err := cursor.Next()
		if err == chunk.ErrNoMoreRecords {
			break
		}
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		count++
	}

	if count != 100 {
		t.Errorf("expected 100 records, got %d", count)
	}
}

// Registry accessor tests

func TestChunkManagerAccessor(t *testing.T) {
	orch, cm, _ := newTestSetup(1 << 20)

	// Get by key.
	got := orch.ChunkManager("default")
	if got != cm {
		t.Error("expected ChunkManager to return registered manager")
	}

	// Empty key defaults to "default".
	got = orch.ChunkManager("")
	if got != cm {
		t.Error("expected empty key to default to 'default'")
	}

	// Unknown key returns nil.
	got = orch.ChunkManager("nonexistent")
	if got != nil {
		t.Error("expected nil for unknown key")
	}
}

func TestChunkManagersAccessor(t *testing.T) {
	orch, _, _ := newTestSetup(1 << 20)

	keys := orch.ChunkManagers()
	if len(keys) != 1 {
		t.Fatalf("expected 1 key, got %d", len(keys))
	}
	if keys[0] != "default" {
		t.Errorf("expected 'default', got %q", keys[0])
	}
}

func TestIndexManagerAccessor(t *testing.T) {
	orch, _, tracker := newTestSetup(1 << 20)

	// Get by key.
	got := orch.IndexManager("default")
	if got != tracker {
		t.Error("expected IndexManager to return registered manager")
	}

	// Empty key defaults to "default".
	got = orch.IndexManager("")
	if got != tracker {
		t.Error("expected empty key to default to 'default'")
	}

	// Unknown key returns nil.
	got = orch.IndexManager("nonexistent")
	if got != nil {
		t.Error("expected nil for unknown key")
	}
}

func TestIndexManagersAccessor(t *testing.T) {
	orch, _, _ := newTestSetup(1 << 20)

	keys := orch.IndexManagers()
	if len(keys) != 1 {
		t.Fatalf("expected 1 key, got %d", len(keys))
	}
	if keys[0] != "default" {
		t.Errorf("expected 'default', got %q", keys[0])
	}
}

func TestIngestersAccessor(t *testing.T) {
	orch, _ := newIngesterTestSetup()

	recv1 := newBlockingIngester()
	recv2 := newBlockingIngester()
	orch.RegisterIngester("recv1", recv1)
	orch.RegisterIngester("recv2", recv2)

	keys := orch.Ingesters()
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}

	// Keys may be in any order.
	found := make(map[string]bool)
	for _, k := range keys {
		found[k] = true
	}
	if !found["recv1"] || !found["recv2"] {
		t.Errorf("expected recv1 and recv2, got %v", keys)
	}
}

func TestRunningAccessor(t *testing.T) {
	orch, _ := newIngesterTestSetup()

	if orch.Running() {
		t.Error("expected Running() = false before Start()")
	}

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if !orch.Running() {
		t.Error("expected Running() = true after Start()")
	}

	if err := orch.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if orch.Running() {
		t.Error("expected Running() = false after Stop()")
	}
}

func TestRebuildMissingIndexes(t *testing.T) {
	// Set up with small chunk to seal it.
	cm, _ := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: recordCountPolicy(2),
	})

	// Append 3 records to seal the first chunk.
	for i := 0; i < 3; i++ {
		cm.Append(chunk.Record{
			IngestTS: t1.Add(time.Duration(i) * time.Second),
			Attrs:    attrsA,
			Raw:      []byte("record"),
		})
	}

	// Create fresh indexers that haven't indexed anything.
	tokIdx := memtoken.NewIndexer(cm)
	attrIdx := memattr.NewIndexer(cm)
	kvIdx := kv.NewIndexer(cm)

	im := indexmem.NewManager(
		[]index.Indexer{tokIdx, attrIdx, kvIdx},
		tokIdx,
		attrIdx,
		kvIdx,
		nil,
	)

	tracker := &trackingIndexManager{IndexManager: im}

	orch := orchestrator.New(orchestrator.Config{})
	orch.RegisterChunkManager("default", cm)
	orch.RegisterIndexManager("default", tracker)

	// RebuildMissingIndexes should find the sealed chunk and build indexes.
	if err := orch.RebuildMissingIndexes(context.Background()); err != nil {
		t.Fatalf("RebuildMissingIndexes failed: %v", err)
	}

	// Wait for async build.
	time.Sleep(100 * time.Millisecond)

	// Should have triggered at least one build.
	count := tracker.buildCount.Load()
	if count == 0 {
		t.Error("expected at least one index build from RebuildMissingIndexes")
	}
}

func TestSearchThenFollowUnknownRegistry(t *testing.T) {
	orch, _, _ := newTestSetup(1 << 20)

	_, _, err := orch.SearchThenFollow(context.Background(), "nonexistent", query.Query{}, nil)
	if err != orchestrator.ErrUnknownRegistry {
		t.Errorf("expected ErrUnknownRegistry, got %v", err)
	}
}

func TestSearchWithContextUnknownRegistry(t *testing.T) {
	orch, _, _ := newTestSetup(1 << 20)

	_, _, err := orch.SearchWithContext(context.Background(), "nonexistent", query.Query{})
	if err != orchestrator.ErrUnknownRegistry {
		t.Errorf("expected ErrUnknownRegistry, got %v", err)
	}
}

// newRoutedTestSetup creates an orchestrator with multiple stores and a router.
func newRoutedTestSetup(t *testing.T) (*orchestrator.Orchestrator, map[string]chunk.ChunkManager) {
	t.Helper()

	stores := make(map[string]chunk.ChunkManager)
	storeNames := []string{"prod", "staging", "archive", "unrouted"}

	orch := orchestrator.New(orchestrator.Config{})

	for _, name := range storeNames {
		cm, err := chunkmem.NewManager(chunkmem.Config{
			RotationPolicy: recordCountPolicy(10000),
		})
		if err != nil {
			t.Fatalf("NewManager failed: %v", err)
		}
		stores[name] = cm

		tokIdx := memtoken.NewIndexer(cm)
		attrIdx := memattr.NewIndexer(cm)
		kvIdx := kv.NewIndexer(cm)

		im := indexmem.NewManager(
			[]index.Indexer{tokIdx, attrIdx, kvIdx},
			tokIdx,
			attrIdx,
			kvIdx,
			nil,
		)

		qe := query.New(cm, im, nil)

		orch.RegisterChunkManager(name, cm)
		orch.RegisterIndexManager(name, im)
		orch.RegisterQueryEngine(name, qe)
	}

	return orch, stores
}

// countRecords counts records in a chunk manager's active chunk.
func countRecords(t *testing.T, cm chunk.ChunkManager) int {
	t.Helper()
	active := cm.Active()
	if active == nil {
		return 0 // No active chunk means no records.
	}
	cursor, err := cm.OpenCursor(active.ID)
	if err != nil {
		t.Fatalf("OpenCursor failed: %v", err)
	}
	defer cursor.Close()

	count := 0
	for {
		_, _, err := cursor.Next()
		if err == chunk.ErrNoMoreRecords {
			break
		}
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		count++
	}
	return count
}

// getRecordMessages returns all Raw messages from a chunk manager's active chunk.
func getRecordMessages(t *testing.T, cm chunk.ChunkManager) []string {
	t.Helper()
	active := cm.Active()
	if active == nil {
		return nil // No active chunk means no records.
	}
	cursor, err := cm.OpenCursor(active.ID)
	if err != nil {
		t.Fatalf("OpenCursor failed: %v", err)
	}
	defer cursor.Close()

	var msgs []string
	for {
		rec, _, err := cursor.Next()
		if err == chunk.ErrNoMoreRecords {
			break
		}
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		msgs = append(msgs, string(rec.Raw))
	}
	return msgs
}

func TestRoutingIntegration(t *testing.T) {
	orch, stores := newRoutedTestSetup(t)

	// Compile routes:
	// - prod: receives env=prod messages
	// - staging: receives env=staging messages
	// - archive: catch-all (*)
	// - unrouted: catch-the-rest (+)
	prodRoute, err := orchestrator.CompileRoute("prod", "env=prod")
	if err != nil {
		t.Fatalf("CompileRoute prod failed: %v", err)
	}
	stagingRoute, err := orchestrator.CompileRoute("staging", "env=staging")
	if err != nil {
		t.Fatalf("CompileRoute staging failed: %v", err)
	}
	archiveRoute, err := orchestrator.CompileRoute("archive", "*")
	if err != nil {
		t.Fatalf("CompileRoute archive failed: %v", err)
	}
	unroutedRoute, err := orchestrator.CompileRoute("unrouted", "+")
	if err != nil {
		t.Fatalf("CompileRoute unrouted failed: %v", err)
	}

	router := orchestrator.NewRouter([]*orchestrator.CompiledRoute{
		prodRoute,
		stagingRoute,
		archiveRoute,
		unroutedRoute,
	})
	orch.SetRouter(router)

	// Test cases: message attrs -> expected stores
	testCases := []struct {
		name     string
		attrs    chunk.Attributes
		raw      string
		expected []string // stores that should receive the message
	}{
		{
			name:     "prod message goes to prod and archive",
			attrs:    chunk.Attributes{"env": "prod", "level": "error"},
			raw:      "production error",
			expected: []string{"prod", "archive"},
		},
		{
			name:     "staging message goes to staging and archive",
			attrs:    chunk.Attributes{"env": "staging", "level": "info"},
			raw:      "staging info",
			expected: []string{"staging", "archive"},
		},
		{
			name:     "dev message goes to archive and unrouted",
			attrs:    chunk.Attributes{"env": "dev", "level": "debug"},
			raw:      "dev debug",
			expected: []string{"archive", "unrouted"},
		},
		{
			name:     "no env goes to archive and unrouted",
			attrs:    chunk.Attributes{"level": "warn"},
			raw:      "no env warn",
			expected: []string{"archive", "unrouted"},
		},
	}

	// Ingest all test messages.
	for _, tc := range testCases {
		rec := chunk.Record{
			IngestTS: time.Now(),
			Attrs:    tc.attrs,
			Raw:      []byte(tc.raw),
		}
		if err := orch.Ingest(rec); err != nil {
			t.Fatalf("Ingest failed for %s: %v", tc.name, err)
		}
	}

	// Verify each store received the expected messages.
	storeMessages := make(map[string][]string)
	for name, cm := range stores {
		storeMessages[name] = getRecordMessages(t, cm)
	}

	for _, tc := range testCases {
		for _, expectedStore := range tc.expected {
			found := false
			for _, msg := range storeMessages[expectedStore] {
				if msg == tc.raw {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("%s: expected message %q in store %q, but not found (store has: %v)",
					tc.name, tc.raw, expectedStore, storeMessages[expectedStore])
			}
		}

		// Also verify message is NOT in stores not in expected list.
		for storeName, msgs := range storeMessages {
			isExpected := false
			for _, exp := range tc.expected {
				if exp == storeName {
					isExpected = true
					break
				}
			}
			if !isExpected {
				for _, msg := range msgs {
					if msg == tc.raw {
						t.Errorf("%s: message %q should NOT be in store %q",
							tc.name, tc.raw, storeName)
					}
				}
			}
		}
	}
}

func TestRoutingWithIngesters(t *testing.T) {
	orch, stores := newRoutedTestSetup(t)

	// Set up routing: prod gets env=prod, archive is catch-all.
	prodRoute, _ := orchestrator.CompileRoute("prod", "env=prod")
	archiveRoute, _ := orchestrator.CompileRoute("archive", "*")

	router := orchestrator.NewRouter([]*orchestrator.CompiledRoute{prodRoute, archiveRoute})
	orch.SetRouter(router)

	// Create a ingester that emits messages with different attrs.
	recv := newMockIngester([]orchestrator.IngestMessage{
		{Attrs: map[string]string{"env": "prod"}, Raw: []byte("prod msg 1")},
		{Attrs: map[string]string{"env": "prod"}, Raw: []byte("prod msg 2")},
		{Attrs: map[string]string{"env": "staging"}, Raw: []byte("staging msg")},
	})
	orch.RegisterIngester("test", recv)

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	<-recv.started
	time.Sleep(50 * time.Millisecond)

	if err := orch.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Verify routing: prod should have 2 messages, archive should have 3.
	prodMsgs := getRecordMessages(t, stores["prod"])
	archiveMsgs := getRecordMessages(t, stores["archive"])
	stagingMsgs := getRecordMessages(t, stores["staging"])

	if len(prodMsgs) != 2 {
		t.Errorf("prod store: expected 2 messages, got %d: %v", len(prodMsgs), prodMsgs)
	}
	if len(archiveMsgs) != 3 {
		t.Errorf("archive store: expected 3 messages, got %d: %v", len(archiveMsgs), archiveMsgs)
	}
	if len(stagingMsgs) != 0 {
		t.Errorf("staging store: expected 0 messages, got %d: %v", len(stagingMsgs), stagingMsgs)
	}
}

func TestRoutingNoRouterFallback(t *testing.T) {
	orch, stores := newRoutedTestSetup(t)

	// No router set - should fan out to all stores (legacy behavior).
	rec := chunk.Record{
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{"env": "test"},
		Raw:      []byte("fanout message"),
	}
	if err := orch.Ingest(rec); err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// All stores should have the message.
	for name, cm := range stores {
		count := countRecords(t, cm)
		if count != 1 {
			t.Errorf("store %q: expected 1 record (fanout), got %d", name, count)
		}
	}
}

func TestRoutingEmptyRouteReceivesNothing(t *testing.T) {
	orch, stores := newRoutedTestSetup(t)

	// prod has empty route (receives nothing), archive is catch-all.
	prodRoute, _ := orchestrator.CompileRoute("prod", "")
	archiveRoute, _ := orchestrator.CompileRoute("archive", "*")

	router := orchestrator.NewRouter([]*orchestrator.CompiledRoute{prodRoute, archiveRoute})
	orch.SetRouter(router)

	rec := chunk.Record{
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{"env": "prod"},
		Raw:      []byte("should not go to prod"),
	}
	if err := orch.Ingest(rec); err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// prod should have 0, archive should have 1.
	if count := countRecords(t, stores["prod"]); count != 0 {
		t.Errorf("prod store: expected 0 records (empty route), got %d", count)
	}
	if count := countRecords(t, stores["archive"]); count != 1 {
		t.Errorf("archive store: expected 1 record, got %d", count)
	}
}

func TestRoutingComplexExpression(t *testing.T) {
	orch, stores := newRoutedTestSetup(t)

	// prod receives: (env=prod AND level=error) OR (env=prod AND level=critical)
	prodRoute, err := orchestrator.CompileRoute("prod", "(env=prod AND level=error) OR (env=prod AND level=critical)")
	if err != nil {
		t.Fatalf("CompileRoute failed: %v", err)
	}
	archiveRoute, _ := orchestrator.CompileRoute("archive", "*")

	router := orchestrator.NewRouter([]*orchestrator.CompiledRoute{prodRoute, archiveRoute})
	orch.SetRouter(router)

	testCases := []struct {
		attrs        chunk.Attributes
		raw          string
		expectInProd bool
	}{
		{chunk.Attributes{"env": "prod", "level": "error"}, "prod error", true},
		{chunk.Attributes{"env": "prod", "level": "critical"}, "prod critical", true},
		{chunk.Attributes{"env": "prod", "level": "info"}, "prod info", false},
		{chunk.Attributes{"env": "staging", "level": "error"}, "staging error", false},
	}

	for _, tc := range testCases {
		rec := chunk.Record{
			IngestTS: time.Now(),
			Attrs:    tc.attrs,
			Raw:      []byte(tc.raw),
		}
		if err := orch.Ingest(rec); err != nil {
			t.Fatalf("Ingest failed: %v", err)
		}
	}

	prodMsgs := getRecordMessages(t, stores["prod"])

	for _, tc := range testCases {
		found := false
		for _, msg := range prodMsgs {
			if msg == tc.raw {
				found = true
				break
			}
		}
		if found != tc.expectInProd {
			t.Errorf("message %q: expectInProd=%v, found=%v", tc.raw, tc.expectInProd, found)
		}
	}

	// Archive should have all 4 messages.
	if count := countRecords(t, stores["archive"]); count != 4 {
		t.Errorf("archive: expected 4 messages, got %d", count)
	}
}

func TestIngestAckSuccess(t *testing.T) {
	orch, _ := newIngesterTestSetup()

	// Create ack channel and message with ack.
	ackCh := make(chan error, 1)
	msg := orchestrator.IngestMessage{
		Attrs:    map[string]string{"host": "server1"},
		Raw:      []byte("test message with ack"),
		IngestTS: time.Now(),
		Ack:      ackCh,
	}

	// Register ingester before starting.
	recv := &ackTestIngester{
		msg:     msg,
		started: make(chan struct{}),
	}
	orch.RegisterIngester("ack-test", recv)

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer orch.Stop()

	// Wait for ingester to start and send message.
	<-recv.started
	time.Sleep(50 * time.Millisecond)

	// Check that ack was received with nil error (success).
	select {
	case err := <-ackCh:
		if err != nil {
			t.Errorf("expected nil ack, got error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ack")
	}
}

func TestIngestAckNotSentWhenNil(t *testing.T) {
	orch, _ := newIngesterTestSetup()

	// Message without ack channel (fire-and-forget).
	recv := newMockIngester([]orchestrator.IngestMessage{
		{Attrs: map[string]string{"host": "server1"}, Raw: []byte("no ack message")},
	})
	orch.RegisterIngester("no-ack-test", recv)

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer orch.Stop()

	<-recv.started
	time.Sleep(50 * time.Millisecond)

	// If we got here without panic/deadlock, the nil ack channel was handled correctly.
}

// ackTestIngester sends a single message with an ack channel.
type ackTestIngester struct {
	msg     orchestrator.IngestMessage
	started chan struct{}
}

func (r *ackTestIngester) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	close(r.started)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case out <- r.msg:
	}

	<-ctx.Done()
	return ctx.Err()
}
