package orchestrator_test

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/memtest"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"

	"github.com/google/uuid"
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

func newTestSetup(maxRecords int64) (*orchestrator.Orchestrator, chunk.ChunkManager, *trackingIndexManager, uuid.UUID) {
	s, _ := memtest.NewStore(chunkmem.Config{
		RotationPolicy: recordCountPolicy(maxRecords),
	})

	tracker := &trackingIndexManager{IndexManager: s.IM}

	defaultID := uuid.Must(uuid.NewV7())
	orch := orchestrator.New(orchestrator.Config{})
	orch.RegisterStore(orchestrator.NewStore(defaultID, s.CM, tracker, s.QE))

	return orch, s.CM, tracker, defaultID
}

func TestIngestReachesChunkManager(t *testing.T) {
	orch, cm, _, _ := newTestSetup(1 << 20) // Large chunk, no auto-seal

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
	orch, cm, _, _ := newTestSetup(1 << 20)

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
	orch, _, tracker, _ := newTestSetup(2)

	// Ingest 3 records to trigger seal (chunk fills at 2, third causes seal).
	for i := range 3 {
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
	orch, _, tracker, _ := newTestSetup(2)

	// Ingest 3 records to trigger exactly one seal.
	for i := range 3 {
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
	orch, cm, _, defaultID := newTestSetup(1 << 20)

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
	seq, _, err := orch.Search(context.Background(), defaultID, query.Query{}, nil)
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

func TestSearchByUUID(t *testing.T) {
	orch, _, _, defaultID := newTestSetup(1 << 20)

	rec := chunk.Record{IngestTS: t1, Attrs: attrsA, Raw: []byte("test")}
	if err := orch.Ingest(rec); err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Search with the store UUID.
	seq, _, err := orch.Search(context.Background(), defaultID, query.Query{}, nil)
	if err != nil {
		t.Fatalf("Search with store UUID failed: %v", err)
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

	// Zero UUID should return ErrUnknownRegistry.
	_, _, err = orch.Search(context.Background(), uuid.Nil, query.Query{}, nil)
	if err != orchestrator.ErrUnknownRegistry {
		t.Errorf("expected ErrUnknownRegistry for zero UUID, got %v", err)
	}
}

func TestSearchUnknownRegistry(t *testing.T) {
	orch, _, _, _ := newTestSetup(1 << 20)

	_, _, err := orch.Search(context.Background(), uuid.Must(uuid.NewV7()), query.Query{}, nil)
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

	_, _, err := orch.Search(context.Background(), uuid.Must(uuid.NewV7()), query.Query{}, nil)
	if err != orchestrator.ErrNoQueryEngines {
		t.Errorf("expected ErrNoQueryEngines, got %v", err)
	}
}

func TestSearchThenFollowViaOrchestrator(t *testing.T) {
	orch, _, _, defaultID := newTestSetup(1 << 20)

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

	seq, _, err := orch.SearchThenFollow(context.Background(), defaultID, query.Query{
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
	orch, _, _, defaultID := newTestSetup(1 << 20)

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

	seq, _, err := orch.SearchWithContext(context.Background(), defaultID, query.Query{
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
	s, _ := memtest.NewStore(chunkmem.Config{
		RotationPolicy: recordCountPolicy(10000),
	})

	defaultID := uuid.Must(uuid.NewV7())
	orch := orchestrator.New(orchestrator.Config{})
	orch.RegisterStore(orchestrator.NewStore(defaultID, s.CM, s.IM, s.QE))

	return orch, s.CM
}

func TestIngesterMessageReachesChunkManager(t *testing.T) {
	orch, cm := newIngesterTestSetup()

	recv := newMockIngester([]orchestrator.IngestMessage{
		{Attrs: map[string]string{"host": "server1"}, Raw: []byte("test message")},
	})
	orch.RegisterIngester(uuid.Must(uuid.NewV7()), recv)

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
	orch.RegisterIngester(uuid.Must(uuid.NewV7()), recv)

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

	orch.RegisterIngester(uuid.Must(uuid.NewV7()), recv1)
	orch.RegisterIngester(uuid.Must(uuid.NewV7()), recv2)

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
	s, _ := memtest.NewStore(chunkmem.Config{
		RotationPolicy: recordCountPolicy(2), // 2 records per chunk
	})

	tracker := &trackingIndexManager{IndexManager: s.IM}

	defaultID := uuid.Must(uuid.NewV7())
	orch := orchestrator.New(orchestrator.Config{})
	orch.RegisterStore(orchestrator.NewStore(defaultID, s.CM, tracker, s.QE))

	// Create ingester with 3 messages to trigger seal.
	recv := newMockIngester([]orchestrator.IngestMessage{
		{Attrs: map[string]string{"host": "s1"}, Raw: []byte("one")},
		{Attrs: map[string]string{"host": "s1"}, Raw: []byte("two")},
		{Attrs: map[string]string{"host": "s1"}, Raw: []byte("three")},
	})
	orch.RegisterIngester(uuid.Must(uuid.NewV7()), recv)

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
	ingesterID := uuid.Must(uuid.NewV7())
	orch.RegisterIngester(ingesterID, recv)
	orch.UnregisterIngester(ingesterID)

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
	orch.RegisterIngester(uuid.Must(uuid.NewV7()), recv)

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
	orch, cm, _, defaultID := newTestSetup(1 << 20)

	// Get by key.
	got := orch.ChunkManager(defaultID)
	if got != cm {
		t.Error("expected ChunkManager to return registered manager")
	}

	// Unknown key returns nil.
	got = orch.ChunkManager(uuid.Must(uuid.NewV7()))
	if got != nil {
		t.Error("expected nil for unknown key")
	}
}

func TestListStoresAccessor(t *testing.T) {
	orch, _, _, defaultID := newTestSetup(1 << 20)

	keys := orch.ListStores()
	if len(keys) != 1 {
		t.Fatalf("expected 1 key, got %d", len(keys))
	}
	if keys[0] != defaultID {
		t.Errorf("expected %s, got %s", defaultID, keys[0])
	}
}

func TestIndexManagerAccessor(t *testing.T) {
	orch, _, tracker, defaultID := newTestSetup(1 << 20)

	// Get by key.
	got := orch.IndexManager(defaultID)
	if got != tracker {
		t.Error("expected IndexManager to return registered manager")
	}

	// Unknown key returns nil.
	got = orch.IndexManager(uuid.Must(uuid.NewV7()))
	if got != nil {
		t.Error("expected nil for unknown key")
	}
}

func TestListStoresReturnsAllKeys(t *testing.T) {
	orch, _, _, defaultID := newTestSetup(1 << 20)

	keys := orch.ListStores()
	if len(keys) != 1 {
		t.Fatalf("expected 1 key, got %d", len(keys))
	}
	if keys[0] != defaultID {
		t.Errorf("expected %s, got %s", defaultID, keys[0])
	}
}

func TestListIngestersAccessor(t *testing.T) {
	orch, _ := newIngesterTestSetup()

	recv1 := newBlockingIngester()
	recv2 := newBlockingIngester()
	id1 := uuid.Must(uuid.NewV7())
	id2 := uuid.Must(uuid.NewV7())
	orch.RegisterIngester(id1, recv1)
	orch.RegisterIngester(id2, recv2)

	keys := orch.ListIngesters()
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}

	// Keys may be in any order.
	found := make(map[uuid.UUID]bool)
	for _, k := range keys {
		found[k] = true
	}
	if !found[id1] || !found[id2] {
		t.Errorf("expected %s and %s, got %v", id1, id2, keys)
	}
}

func TestIsRunningAccessor(t *testing.T) {
	orch, _ := newIngesterTestSetup()

	if orch.IsRunning() {
		t.Error("expected IsRunning() = false before Start()")
	}

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if !orch.IsRunning() {
		t.Error("expected IsRunning() = true after Start()")
	}

	if err := orch.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if orch.IsRunning() {
		t.Error("expected IsRunning() = false after Stop()")
	}
}

func TestRebuildMissingIndexes(t *testing.T) {
	// Set up with small chunk to seal it.
	s, _ := memtest.NewStore(chunkmem.Config{
		RotationPolicy: recordCountPolicy(2),
	})

	// Append 3 records to seal the first chunk.
	for i := range 3 {
		s.CM.Append(chunk.Record{
			IngestTS: t1.Add(time.Duration(i) * time.Second),
			Attrs:    attrsA,
			Raw:      []byte("record"),
		})
	}

	tracker := &trackingIndexManager{IndexManager: s.IM}

	defaultID := uuid.Must(uuid.NewV7())
	orch := orchestrator.New(orchestrator.Config{})
	orch.RegisterStore(orchestrator.NewStore(defaultID, s.CM, tracker, nil))

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
	orch, _, _, _ := newTestSetup(1 << 20)

	_, _, err := orch.SearchThenFollow(context.Background(), uuid.Must(uuid.NewV7()), query.Query{}, nil)
	if err != orchestrator.ErrUnknownRegistry {
		t.Errorf("expected ErrUnknownRegistry, got %v", err)
	}
}

func TestSearchWithContextUnknownRegistry(t *testing.T) {
	orch, _, _, _ := newTestSetup(1 << 20)

	_, _, err := orch.SearchWithContext(context.Background(), uuid.Must(uuid.NewV7()), query.Query{})
	if err != orchestrator.ErrUnknownRegistry {
		t.Errorf("expected ErrUnknownRegistry, got %v", err)
	}
}

// filteredTestStores holds the store IDs and chunk managers for the filtered test setup.
type filteredTestStores struct {
	prod     uuid.UUID
	staging  uuid.UUID
	archive  uuid.UUID
	unrouted uuid.UUID
	cms      map[uuid.UUID]chunk.ChunkManager
}

// newFilteredTestSetup creates an orchestrator with multiple stores and a filter set.
func newFilteredTestSetup(t *testing.T) (*orchestrator.Orchestrator, filteredTestStores) {
	t.Helper()

	stores := filteredTestStores{
		prod:     uuid.Must(uuid.NewV7()),
		staging:  uuid.Must(uuid.NewV7()),
		archive:  uuid.Must(uuid.NewV7()),
		unrouted: uuid.Must(uuid.NewV7()),
		cms:      make(map[uuid.UUID]chunk.ChunkManager),
	}

	orch := orchestrator.New(orchestrator.Config{})

	for _, id := range []uuid.UUID{stores.prod, stores.staging, stores.archive, stores.unrouted} {
		s := memtest.MustNewStore(t, chunkmem.Config{
			RotationPolicy: recordCountPolicy(10000),
		})
		stores.cms[id] = s.CM

		orch.RegisterStore(orchestrator.NewStore(id, s.CM, s.IM, s.QE))
	}

	return orch, stores
}

// newFilteredTestSetupWithLoader is like newFilteredTestSetup but accepts a
// *fakeConfigLoader and passes it as the ConfigLoader in orchestrator.Config.
func newFilteredTestSetupWithLoader(t *testing.T, loader *fakeConfigLoader) (*orchestrator.Orchestrator, filteredTestStores) {
	t.Helper()

	stores := filteredTestStores{
		prod:     uuid.Must(uuid.NewV7()),
		staging:  uuid.Must(uuid.NewV7()),
		archive:  uuid.Must(uuid.NewV7()),
		unrouted: uuid.Must(uuid.NewV7()),
		cms:      make(map[uuid.UUID]chunk.ChunkManager),
	}

	orch := orchestrator.New(orchestrator.Config{ConfigLoader: loader})

	for _, id := range []uuid.UUID{stores.prod, stores.staging, stores.archive, stores.unrouted} {
		s := memtest.MustNewStore(t, chunkmem.Config{
			RotationPolicy: recordCountPolicy(10000),
		})
		stores.cms[id] = s.CM

		orch.RegisterStore(orchestrator.NewStore(id, s.CM, s.IM, s.QE))
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
	orch, stores := newFilteredTestSetup(t)

	// Compile filters:
	// - prod: receives env=prod messages
	// - staging: receives env=staging messages
	// - archive: catch-all (*)
	// - unrouted: catch-the-rest (+)
	prodFilter, err := orchestrator.CompileFilter(stores.prod, "env=prod")
	if err != nil {
		t.Fatalf("CompileFilter prod failed: %v", err)
	}
	stagingFilter, err := orchestrator.CompileFilter(stores.staging, "env=staging")
	if err != nil {
		t.Fatalf("CompileFilter staging failed: %v", err)
	}
	archiveFilter, err := orchestrator.CompileFilter(stores.archive, "*")
	if err != nil {
		t.Fatalf("CompileFilter archive failed: %v", err)
	}
	unfilteredFilter, err := orchestrator.CompileFilter(stores.unrouted, "+")
	if err != nil {
		t.Fatalf("CompileFilter unrouted failed: %v", err)
	}

	fs := orchestrator.NewFilterSet([]*orchestrator.CompiledFilter{
		prodFilter,
		stagingFilter,
		archiveFilter,
		unfilteredFilter,
	})
	orch.SetFilterSet(fs)

	// Test cases: message attrs -> expected stores
	testCases := []struct {
		name     string
		attrs    chunk.Attributes
		raw      string
		expected []uuid.UUID // stores that should receive the message
	}{
		{
			name:     "prod message goes to prod and archive",
			attrs:    chunk.Attributes{"env": "prod", "level": "error"},
			raw:      "production error",
			expected: []uuid.UUID{stores.prod, stores.archive},
		},
		{
			name:     "staging message goes to staging and archive",
			attrs:    chunk.Attributes{"env": "staging", "level": "info"},
			raw:      "staging info",
			expected: []uuid.UUID{stores.staging, stores.archive},
		},
		{
			name:     "dev message goes to archive and unrouted",
			attrs:    chunk.Attributes{"env": "dev", "level": "debug"},
			raw:      "dev debug",
			expected: []uuid.UUID{stores.archive, stores.unrouted},
		},
		{
			name:     "no env goes to archive and unrouted",
			attrs:    chunk.Attributes{"level": "warn"},
			raw:      "no env warn",
			expected: []uuid.UUID{stores.archive, stores.unrouted},
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
	storeMessages := make(map[uuid.UUID][]string)
	for id, cm := range stores.cms {
		storeMessages[id] = getRecordMessages(t, cm)
	}

	for _, tc := range testCases {
		for _, expectedStore := range tc.expected {
			found := slices.Contains(storeMessages[expectedStore], tc.raw)
			if !found {
				t.Errorf("%s: expected message %q in store %s, but not found (store has: %v)",
					tc.name, tc.raw, expectedStore, storeMessages[expectedStore])
			}
		}

		// Also verify message is NOT in stores not in expected list.
		for storeID, msgs := range storeMessages {
			isExpected := slices.Contains(tc.expected, storeID)
			if !isExpected {
				for _, msg := range msgs {
					if msg == tc.raw {
						t.Errorf("%s: message %q should NOT be in store %s",
							tc.name, tc.raw, storeID)
					}
				}
			}
		}
	}
}

func TestRoutingWithIngesters(t *testing.T) {
	orch, stores := newFilteredTestSetup(t)

	// Set up filtering: prod gets env=prod, archive is catch-all.
	prodFilter, _ := orchestrator.CompileFilter(stores.prod, "env=prod")
	archiveFilter, _ := orchestrator.CompileFilter(stores.archive, "*")

	fs := orchestrator.NewFilterSet([]*orchestrator.CompiledFilter{prodFilter, archiveFilter})
	orch.SetFilterSet(fs)

	// Create a ingester that emits messages with different attrs.
	recv := newMockIngester([]orchestrator.IngestMessage{
		{Attrs: map[string]string{"env": "prod"}, Raw: []byte("prod msg 1")},
		{Attrs: map[string]string{"env": "prod"}, Raw: []byte("prod msg 2")},
		{Attrs: map[string]string{"env": "staging"}, Raw: []byte("staging msg")},
	})
	orch.RegisterIngester(uuid.Must(uuid.NewV7()), recv)

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	<-recv.started
	time.Sleep(50 * time.Millisecond)

	if err := orch.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Verify routing: prod should have 2 messages, archive should have 3.
	prodMsgs := getRecordMessages(t, stores.cms[stores.prod])
	archiveMsgs := getRecordMessages(t, stores.cms[stores.archive])
	stagingMsgs := getRecordMessages(t, stores.cms[stores.staging])

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

func TestRoutingNoFilterSetFallback(t *testing.T) {
	orch, stores := newFilteredTestSetup(t)

	// No filter set - should fan out to all stores (legacy behavior).
	rec := chunk.Record{
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{"env": "test"},
		Raw:      []byte("fanout message"),
	}
	if err := orch.Ingest(rec); err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// All stores should have the message.
	for id, cm := range stores.cms {
		count := countRecords(t, cm)
		if count != 1 {
			t.Errorf("store %s: expected 1 record (fanout), got %d", id, count)
		}
	}
}

func TestRoutingEmptyFilterReceivesNothing(t *testing.T) {
	orch, stores := newFilteredTestSetup(t)

	// prod has empty filter (receives nothing), archive is catch-all.
	prodFilter, _ := orchestrator.CompileFilter(stores.prod, "")
	archiveFilter, _ := orchestrator.CompileFilter(stores.archive, "*")

	fs := orchestrator.NewFilterSet([]*orchestrator.CompiledFilter{prodFilter, archiveFilter})
	orch.SetFilterSet(fs)

	rec := chunk.Record{
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{"env": "prod"},
		Raw:      []byte("should not go to prod"),
	}
	if err := orch.Ingest(rec); err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// prod should have 0, archive should have 1.
	if count := countRecords(t, stores.cms[stores.prod]); count != 0 {
		t.Errorf("prod store: expected 0 records (empty filter), got %d", count)
	}
	if count := countRecords(t, stores.cms[stores.archive]); count != 1 {
		t.Errorf("archive store: expected 1 record, got %d", count)
	}
}

func TestRoutingComplexExpression(t *testing.T) {
	orch, stores := newFilteredTestSetup(t)

	// prod receives: (env=prod AND level=error) OR (env=prod AND level=critical)
	prodFilter, err := orchestrator.CompileFilter(stores.prod, "(env=prod AND level=error) OR (env=prod AND level=critical)")
	if err != nil {
		t.Fatalf("CompileFilter failed: %v", err)
	}
	archiveFilter, _ := orchestrator.CompileFilter(stores.archive, "*")

	fs := orchestrator.NewFilterSet([]*orchestrator.CompiledFilter{prodFilter, archiveFilter})
	orch.SetFilterSet(fs)

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

	prodMsgs := getRecordMessages(t, stores.cms[stores.prod])

	for _, tc := range testCases {
		found := slices.Contains(prodMsgs, tc.raw)
		if found != tc.expectInProd {
			t.Errorf("message %q: expectInProd=%v, found=%v", tc.raw, tc.expectInProd, found)
		}
	}

	// Archive should have all 4 messages.
	if count := countRecords(t, stores.cms[stores.archive]); count != 4 {
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
	orch.RegisterIngester(uuid.Must(uuid.NewV7()), recv)

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
	orch.RegisterIngester(uuid.Must(uuid.NewV7()), recv)

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
