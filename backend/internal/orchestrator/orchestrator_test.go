package orchestrator_test

import (
	"gastrolog/internal/glid"
	"context"
	"fmt"
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

func newTestSetup(t *testing.T, maxRecords int64) (*orchestrator.Orchestrator, chunk.ChunkManager, *trackingIndexManager, glid.GLID) {
	t.Helper()
	s, _ := memtest.NewVault(chunkmem.Config{
		RotationPolicy: recordCountPolicy(maxRecords),
	})

	tracker := &trackingIndexManager{IndexManager: s.IM}

	defaultID := glid.New()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	orch.RegisterVault(orchestrator.NewVaultFromComponents(defaultID, s.CM, tracker, s.QE))

	// Set up a catch-all route so records are delivered to the vault.
	orch.SetFilterSet(orchestrator.NewFilterSet([]*orchestrator.CompiledFilter{
		{VaultID: defaultID, Kind: orchestrator.FilterCatchAll, Expr: "*"},
	}))

	return orch, s.CM, tracker, defaultID
}

func TestIngestReachesChunkManager(t *testing.T) {
	orch, cm, _, _ := newTestSetup(t, 1 << 20) // Large chunk, no auto-seal

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
	orch, cm, _, _ := newTestSetup(t, 1 << 20)

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

func TestSealedChunkTriggersPostSeal(t *testing.T) {
	// Set MaxRecords to 2 so third record triggers seal.
	orch, cm, _, _ := newTestSetup(t, 2)

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

	// Wait for async job to be scheduled.
	time.Sleep(100 * time.Millisecond)

	// Verify the seal happened by checking chunk count.
	metas, err := cm.List()
	if err != nil {
		t.Fatal(err)
	}
	sealed := 0
	for _, m := range metas {
		if m.Sealed {
			sealed++
		}
	}
	if sealed == 0 {
		t.Error("expected at least one sealed chunk")
	}
}

func TestSealTriggeredOncePerChunk(t *testing.T) {
	// Set chunk size to 2 records.
	orch, cm, _, _ := newTestSetup(t, 2)

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

	// Wait for seal.
	time.Sleep(100 * time.Millisecond)

	// Should have exactly 2 chunks: one sealed, one active.
	metas, err := cm.List()
	if err != nil {
		t.Fatal(err)
	}
	sealed := 0
	for _, m := range metas {
		if m.Sealed {
			sealed++
		}
	}
	if sealed != 1 {
		t.Errorf("expected exactly 1 sealed chunk, got %d", sealed)
	}
}

func TestSearchViaOrchestrator(t *testing.T) {
	orch, cm, _, defaultID := newTestSetup(t, 1 << 20)

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
	orch, _, _, defaultID := newTestSetup(t, 1 << 20)

	rec := chunk.Record{IngestTS: t1, Attrs: attrsA, Raw: []byte("test")}
	if err := orch.Ingest(rec); err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Search with the vault UUID.
	seq, _, err := orch.Search(context.Background(), defaultID, query.Query{}, nil)
	if err != nil {
		t.Fatalf("Search with vault UUID failed: %v", err)
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
	_, _, err = orch.Search(context.Background(), glid.Nil, query.Query{}, nil)
	if err != orchestrator.ErrUnknownRegistry {
		t.Errorf("expected ErrUnknownRegistry for zero UUID, got %v", err)
	}
}

func TestSearchUnknownRegistry(t *testing.T) {
	orch, _, _, _ := newTestSetup(t, 1 << 20)

	_, _, err := orch.Search(context.Background(), glid.New(), query.Query{}, nil)
	if err != orchestrator.ErrUnknownRegistry {
		t.Errorf("expected ErrUnknownRegistry, got %v", err)
	}
}

func TestIngestNoChunkManagers(t *testing.T) {
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}

	rec := chunk.Record{IngestTS: t1, Attrs: attrsA, Raw: []byte("test")}
	err = orch.Ingest(rec)
	if err != orchestrator.ErrNoChunkManagers {
		t.Errorf("expected ErrNoChunkManagers, got %v", err)
	}
}

func TestSearchNoQueryEngines(t *testing.T) {
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = orch.Search(context.Background(), glid.New(), query.Query{}, nil)
	if err != orchestrator.ErrNoQueryEngines {
		t.Errorf("expected ErrNoQueryEngines, got %v", err)
	}
}

func TestSearchThenFollowViaOrchestrator(t *testing.T) {
	orch, _, _, defaultID := newTestSetup(t, 1 << 20)

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
	orch, _, _, defaultID := newTestSetup(t, 1 << 20)

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

func newIngesterTestSetup(t *testing.T) (*orchestrator.Orchestrator, chunk.ChunkManager) {
	t.Helper()
	s, _ := memtest.NewVault(chunkmem.Config{
		RotationPolicy: recordCountPolicy(10000),
	})

	defaultID := glid.New()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	orch.RegisterVault(orchestrator.NewVaultFromComponents(defaultID, s.CM, s.IM, s.QE))

	// Set up a catch-all route so records are delivered to the vault.
	orch.SetFilterSet(orchestrator.NewFilterSet([]*orchestrator.CompiledFilter{
		{VaultID: defaultID, Kind: orchestrator.FilterCatchAll, Expr: "*"},
	}))

	return orch, s.CM
}

func TestIngesterMessageReachesChunkManager(t *testing.T) {
	orch, cm := newIngesterTestSetup(t)

	recv := newMockIngester([]orchestrator.IngestMessage{
		{Attrs: map[string]string{"host": "server1"}, Raw: []byte("test message")},
	})
	orch.RegisterIngester(glid.New(), "test", "mock", recv)

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

func TestRouteStatsThroughLifecycle(t *testing.T) {
	t.Parallel()
	orch, _ := newIngesterTestSetup(t)

	msgs := make([]orchestrator.IngestMessage, 5)
	for i := range msgs {
		msgs[i] = orchestrator.IngestMessage{
			Attrs: map[string]string{"host": "server1"},
			Raw:   fmt.Appendf(nil, "msg-%d", i),
		}
	}

	recv := newMockIngester(msgs)
	orch.RegisterIngester(glid.New(), "test", "mock", recv)

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	<-recv.started
	time.Sleep(100 * time.Millisecond)

	if err := orch.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	rs := orch.GetRouteStats()
	if got := rs.Ingested.Load(); got != 5 {
		t.Errorf("Ingested = %d, want 5", got)
	}
	if got := rs.Routed.Load(); got != 5 {
		t.Errorf("Routed = %d, want 5", got)
	}
	if got := rs.Dropped.Load(); got != 0 {
		t.Errorf("Dropped = %d, want 0", got)
	}
	if !orch.IsFilterSetActive() {
		t.Error("expected filterSet active")
	}

	vaultStats := orch.VaultRouteStatsList()
	if len(vaultStats) != 1 {
		t.Fatalf("expected 1 vault stat entry, got %d", len(vaultStats))
	}
	for _, vs := range vaultStats {
		if got := vs.Matched.Load(); got != 5 {
			t.Errorf("Matched = %d, want 5", got)
		}
	}
}

func TestIngesterContextCancellation(t *testing.T) {
	orch, _ := newIngesterTestSetup(t)

	recv := newBlockingIngester()
	orch.RegisterIngester(glid.New(), "test", "mock", recv)

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
	orch, cm := newIngesterTestSetup(t)

	recv1 := newMockIngester([]orchestrator.IngestMessage{
		{Attrs: map[string]string{"source": "recv1"}, Raw: []byte("from recv1")},
	})
	recv2 := newMockIngester([]orchestrator.IngestMessage{
		{Attrs: map[string]string{"source": "recv2"}, Raw: []byte("from recv2")},
	})

	orch.RegisterIngester(glid.New(), "test-1", "mock", recv1)
	orch.RegisterIngester(glid.New(), "test-2", "mock", recv2)

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
	orch, _ := newIngesterTestSetup(t)

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
	orch, _ := newIngesterTestSetup(t)

	err := orch.Stop()
	if err != orchestrator.ErrNotRunning {
		t.Errorf("expected ErrNotRunning, got %v", err)
	}
}

func TestIngesterSealOnChunkFull(t *testing.T) {
	// Set up with small chunk size to trigger seal.
	s, _ := memtest.NewVault(chunkmem.Config{
		RotationPolicy: recordCountPolicy(2), // 2 records per chunk
	})

	defaultID := glid.New()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	orch.RegisterVault(orchestrator.NewVaultFromComponents(defaultID, s.CM, s.IM, s.QE))
	orch.SetFilterSet(orchestrator.NewFilterSet([]*orchestrator.CompiledFilter{
		{VaultID: defaultID, Kind: orchestrator.FilterCatchAll, Expr: "*"},
	}))

	// Create ingester with 3 messages to trigger seal.
	recv := newMockIngester([]orchestrator.IngestMessage{
		{Attrs: map[string]string{"host": "s1"}, Raw: []byte("one")},
		{Attrs: map[string]string{"host": "s1"}, Raw: []byte("two")},
		{Attrs: map[string]string{"host": "s1"}, Raw: []byte("three")},
	})
	orch.RegisterIngester(glid.New(), "test", "mock", recv)

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	<-recv.started
	time.Sleep(100 * time.Millisecond)

	if err := orch.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Should have at least one sealed chunk.
	metas, err := s.CM.List()
	if err != nil {
		t.Fatal(err)
	}
	sealed := 0
	for _, m := range metas {
		if m.Sealed {
			sealed++
		}
	}
	if sealed == 0 {
		t.Error("expected at least one sealed chunk")
	}
}

func TestUnregisterIngester(t *testing.T) {
	orch, _ := newIngesterTestSetup(t)

	recv := newBlockingIngester()
	ingesterID := glid.New()
	orch.RegisterIngester(ingesterID, "test", "mock", recv)
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
	orch, cm := newIngesterTestSetup(t)

	recv := newCountingIngester(100)
	orch.RegisterIngester(glid.New(), "test", "mock", recv)

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
	orch, cm, _, defaultID := newTestSetup(t, 1 << 20)

	// Get by key.
	got := orch.ChunkManager(defaultID)
	if got != cm {
		t.Error("expected ChunkManager to return registered manager")
	}

	// Unknown key returns nil.
	got = orch.ChunkManager(glid.New())
	if got != nil {
		t.Error("expected nil for unknown key")
	}
}

func TestListVaultsAccessor(t *testing.T) {
	orch, _, _, defaultID := newTestSetup(t, 1 << 20)

	keys := orch.ListVaults()
	if len(keys) != 1 {
		t.Fatalf("expected 1 key, got %d", len(keys))
	}
	if keys[0] != defaultID {
		t.Errorf("expected %s, got %s", defaultID, keys[0])
	}
}

func TestIndexManagerAccessor(t *testing.T) {
	orch, _, tracker, defaultID := newTestSetup(t, 1 << 20)

	// Get by key.
	got := orch.IndexManager(defaultID)
	if got != tracker {
		t.Error("expected IndexManager to return registered manager")
	}

	// Unknown key returns nil.
	got = orch.IndexManager(glid.New())
	if got != nil {
		t.Error("expected nil for unknown key")
	}
}

func TestListVaultsReturnsAllKeys(t *testing.T) {
	orch, _, _, defaultID := newTestSetup(t, 1 << 20)

	keys := orch.ListVaults()
	if len(keys) != 1 {
		t.Fatalf("expected 1 key, got %d", len(keys))
	}
	if keys[0] != defaultID {
		t.Errorf("expected %s, got %s", defaultID, keys[0])
	}
}

func TestListIngestersAccessor(t *testing.T) {
	orch, _ := newIngesterTestSetup(t)

	recv1 := newBlockingIngester()
	recv2 := newBlockingIngester()
	id1 := glid.New()
	id2 := glid.New()
	orch.RegisterIngester(id1, "test-1", "mock", recv1)
	orch.RegisterIngester(id2, "test-2", "mock", recv2)

	keys := orch.ListIngesters()
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}

	// Keys may be in any order.
	found := make(map[glid.GLID]bool)
	for _, k := range keys {
		found[k] = true
	}
	if !found[id1] || !found[id2] {
		t.Errorf("expected %s and %s, got %v", id1, id2, keys)
	}
}

func TestIsRunningAccessor(t *testing.T) {
	orch, _ := newIngesterTestSetup(t)

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
	s, _ := memtest.NewVault(chunkmem.Config{
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

	// Wire index builders so HasIndexBuilders() returns true —
	// RebuildMissingIndexes skips vaults without builders.
	s.CM.(chunk.ChunkPostSealProcessor).SetIndexBuilders([]chunk.ChunkIndexBuilder{tracker.BuildAdapter()})

	defaultID := glid.New()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	orch.RegisterVault(orchestrator.NewVaultFromComponents(defaultID, s.CM, tracker, nil))

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

// cloudOverlayCM is a chunk.ChunkManager that delegates everything to an
// embedded ChunkManager but overrides List() to mark every returned meta as
// CloudBacked. Used to simulate a tier whose sealed chunks have already been
// uploaded and pruned from local disk.
type cloudOverlayCM struct {
	chunk.ChunkManager
}

func (c *cloudOverlayCM) List() ([]chunk.ChunkMeta, error) {
	metas, err := c.ChunkManager.List()
	if err != nil {
		return nil, err
	}
	for i := range metas {
		metas[i].CloudBacked = true
	}
	return metas, nil
}

// TestRebuildMissingIndexesCloudBackedWithCompleteIndexes verifies that
// cloud-backed chunks with complete local indexes are NOT rebuilt on restart.
// This is the normal steady-state: uploadToCloud preserves index files, so
// RebuildMissingIndexes has nothing to do.
func TestRebuildMissingIndexesCloudBackedWithCompleteIndexes(t *testing.T) {
	t.Parallel()

	s, _ := memtest.NewVault(chunkmem.Config{
		RotationPolicy: recordCountPolicy(2),
	})
	for i := range 3 {
		s.CM.Append(chunk.Record{
			IngestTS: t1.Add(time.Duration(i) * time.Second),
			Attrs:    attrsA,
			Raw:      []byte("record"),
		})
	}

	// Build indexes for all sealed chunks BEFORE wrapping with the cloud overlay.
	memtest.BuildIndexes(t, s.CM, s.IM)

	tracker := &trackingIndexManager{IndexManager: s.IM}
	s.CM.(chunk.ChunkPostSealProcessor).SetIndexBuilders([]chunk.ChunkIndexBuilder{tracker.BuildAdapter()})
	overlay := &cloudOverlayCM{ChunkManager: s.CM}

	defaultID := glid.New()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	orch.RegisterVault(orchestrator.NewVaultFromComponents(defaultID, overlay, tracker, nil))

	if err := orch.RebuildMissingIndexes(context.Background()); err != nil {
		t.Fatalf("RebuildMissingIndexes failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if got := tracker.buildCount.Load(); got != 0 {
		t.Errorf("expected 0 index builds for cloud-backed chunks with complete indexes, got %d", got)
	}
}

// TestRebuildMissingIndexesCloudBackedWithMissingIndexes verifies that
// cloud-backed chunks whose local index files are missing DO get rebuilt.
// This covers the upgrade scenario: existing deployments where uploadToCloud
// previously deleted the entire chunk directory. On first restart after the
// fix, these chunks need their indexes rebuilt from the cloud blob.
func TestRebuildMissingIndexesCloudBackedWithMissingIndexes(t *testing.T) {
	t.Parallel()

	s, _ := memtest.NewVault(chunkmem.Config{
		RotationPolicy: recordCountPolicy(2),
	})
	for i := range 3 {
		s.CM.Append(chunk.Record{
			IngestTS: t1.Add(time.Duration(i) * time.Second),
			Attrs:    attrsA,
			Raw:      []byte("record"),
		})
	}

	// Do NOT build indexes — simulate a cloud chunk whose local indexes
	// were deleted by the old uploadToCloud code.
	tracker := &trackingIndexManager{IndexManager: s.IM}
	s.CM.(chunk.ChunkPostSealProcessor).SetIndexBuilders([]chunk.ChunkIndexBuilder{tracker.BuildAdapter()})
	overlay := &cloudOverlayCM{ChunkManager: s.CM}

	defaultID := glid.New()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	orch.RegisterVault(orchestrator.NewVaultFromComponents(defaultID, overlay, tracker, nil))

	if err := orch.RebuildMissingIndexes(context.Background()); err != nil {
		t.Fatalf("RebuildMissingIndexes failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if got := tracker.buildCount.Load(); got == 0 {
		t.Error("expected index builds for cloud-backed chunks with missing indexes, got 0")
	}
}

func TestSearchThenFollowUnknownRegistry(t *testing.T) {
	orch, _, _, _ := newTestSetup(t, 1 << 20)

	_, _, err := orch.SearchThenFollow(context.Background(), glid.New(), query.Query{}, nil)
	if err != orchestrator.ErrUnknownRegistry {
		t.Errorf("expected ErrUnknownRegistry, got %v", err)
	}
}

func TestSearchWithContextUnknownRegistry(t *testing.T) {
	orch, _, _, _ := newTestSetup(t, 1 << 20)

	_, _, err := orch.SearchWithContext(context.Background(), glid.New(), query.Query{})
	if err != orchestrator.ErrUnknownRegistry {
		t.Errorf("expected ErrUnknownRegistry, got %v", err)
	}
}

// filteredTestVaults holds the vault IDs and chunk managers for the filtered test setup.
type filteredTestVaults struct {
	prod     glid.GLID
	staging  glid.GLID
	archive  glid.GLID
	catchRest glid.GLID
	cms      map[glid.GLID]chunk.ChunkManager
}

// newFilteredTestSetup creates an orchestrator with multiple vaults and a filter set.
func newFilteredTestSetup(t *testing.T) (*orchestrator.Orchestrator, filteredTestVaults) {
	t.Helper()

	vaults := filteredTestVaults{
		prod:     glid.New(),
		staging:  glid.New(),
		archive:  glid.New(),
		catchRest: glid.New(),
		cms:      make(map[glid.GLID]chunk.ChunkManager),
	}

	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}

	for _, id := range []glid.GLID{vaults.prod, vaults.staging, vaults.archive, vaults.catchRest} {
		s := memtest.MustNewVault(t, chunkmem.Config{
			RotationPolicy: recordCountPolicy(10000),
		})
		vaults.cms[id] = s.CM

		orch.RegisterVault(orchestrator.NewVaultFromComponents(id, s.CM, s.IM, s.QE))
	}

	return orch, vaults
}

// newFilteredTestSetupWithLoader is like newFilteredTestSetup but accepts a
// *fakeSystemLoader and passes it as the SystemLoader in orchestrator.Config.
func newFilteredTestSetupWithLoader(t *testing.T, loader *fakeSystemLoader) (*orchestrator.Orchestrator, filteredTestVaults) {
	t.Helper()

	vaults := filteredTestVaults{
		prod:     glid.New(),
		staging:  glid.New(),
		archive:  glid.New(),
		catchRest: glid.New(),
		cms:      make(map[glid.GLID]chunk.ChunkManager),
	}

	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: loader})
	if err != nil {
		t.Fatal(err)
	}

	for _, id := range []glid.GLID{vaults.prod, vaults.staging, vaults.archive, vaults.catchRest} {
		s := memtest.MustNewVault(t, chunkmem.Config{
			RotationPolicy: recordCountPolicy(10000),
		})
		vaults.cms[id] = s.CM

		orch.RegisterVault(orchestrator.NewVaultFromComponents(id, s.CM, s.IM, s.QE))
	}

	return orch, vaults
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

func TestFilteringIntegration(t *testing.T) {
	orch, vaults := newFilteredTestSetup(t)

	// Compile filters:
	// - prod: receives env=prod messages
	// - staging: receives env=staging messages
	// - archive: catch-all (*)
	// - catchRest: catch-the-rest (+)
	prodFilter, err := orchestrator.CompileFilter(vaults.prod, "env=prod")
	if err != nil {
		t.Fatalf("CompileFilter prod failed: %v", err)
	}
	stagingFilter, err := orchestrator.CompileFilter(vaults.staging, "env=staging")
	if err != nil {
		t.Fatalf("CompileFilter staging failed: %v", err)
	}
	archiveFilter, err := orchestrator.CompileFilter(vaults.archive, "*")
	if err != nil {
		t.Fatalf("CompileFilter archive failed: %v", err)
	}
	unfilteredFilter, err := orchestrator.CompileFilter(vaults.catchRest, "+")
	if err != nil {
		t.Fatalf("CompileFilter catchRest failed: %v", err)
	}

	fs := orchestrator.NewFilterSet([]*orchestrator.CompiledFilter{
		prodFilter,
		stagingFilter,
		archiveFilter,
		unfilteredFilter,
	})
	orch.SetFilterSet(fs)

	// Test cases: message attrs -> expected vaults
	testCases := []struct {
		name     string
		attrs    chunk.Attributes
		raw      string
		expected []glid.GLID // vaults that should receive the message
	}{
		{
			name:     "prod message goes to prod and archive",
			attrs:    chunk.Attributes{"env": "prod", "level": "error"},
			raw:      "production error",
			expected: []glid.GLID{vaults.prod, vaults.archive},
		},
		{
			name:     "staging message goes to staging and archive",
			attrs:    chunk.Attributes{"env": "staging", "level": "info"},
			raw:      "staging info",
			expected: []glid.GLID{vaults.staging, vaults.archive},
		},
		{
			name:     "dev message goes to archive and catchRest",
			attrs:    chunk.Attributes{"env": "dev", "level": "debug"},
			raw:      "dev debug",
			expected: []glid.GLID{vaults.archive, vaults.catchRest},
		},
		{
			name:     "no env goes to archive and catchRest",
			attrs:    chunk.Attributes{"level": "warn"},
			raw:      "no env warn",
			expected: []glid.GLID{vaults.archive, vaults.catchRest},
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

	// Verify each vault received the expected messages.
	vaultMessages := make(map[glid.GLID][]string)
	for id, cm := range vaults.cms {
		vaultMessages[id] = getRecordMessages(t, cm)
	}

	for _, tc := range testCases {
		for _, expectedVault := range tc.expected {
			found := slices.Contains(vaultMessages[expectedVault], tc.raw)
			if !found {
				t.Errorf("%s: expected message %q in vault %s, but not found (vault has: %v)",
					tc.name, tc.raw, expectedVault, vaultMessages[expectedVault])
			}
		}

		// Also verify message is NOT in vaults not in expected list.
		for vaultID, msgs := range vaultMessages {
			isExpected := slices.Contains(tc.expected, vaultID)
			if !isExpected {
				for _, msg := range msgs {
					if msg == tc.raw {
						t.Errorf("%s: message %q should NOT be in vault %s",
							tc.name, tc.raw, vaultID)
					}
				}
			}
		}
	}
}

func TestFilteringWithIngesters(t *testing.T) {
	orch, vaults := newFilteredTestSetup(t)

	// Set up filtering: prod gets env=prod, archive is catch-all.
	prodFilter, _ := orchestrator.CompileFilter(vaults.prod, "env=prod")
	archiveFilter, _ := orchestrator.CompileFilter(vaults.archive, "*")

	fs := orchestrator.NewFilterSet([]*orchestrator.CompiledFilter{prodFilter, archiveFilter})
	orch.SetFilterSet(fs)

	// Create a ingester that emits messages with different attrs.
	recv := newMockIngester([]orchestrator.IngestMessage{
		{Attrs: map[string]string{"env": "prod"}, Raw: []byte("prod msg 1")},
		{Attrs: map[string]string{"env": "prod"}, Raw: []byte("prod msg 2")},
		{Attrs: map[string]string{"env": "staging"}, Raw: []byte("staging msg")},
	})
	orch.RegisterIngester(glid.New(), "test", "mock", recv)

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	<-recv.started
	time.Sleep(50 * time.Millisecond)

	if err := orch.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Verify filtering: prod should have 2 messages, archive should have 3.
	prodMsgs := getRecordMessages(t, vaults.cms[vaults.prod])
	archiveMsgs := getRecordMessages(t, vaults.cms[vaults.archive])
	stagingMsgs := getRecordMessages(t, vaults.cms[vaults.staging])

	if len(prodMsgs) != 2 {
		t.Errorf("prod vault: expected 2 messages, got %d: %v", len(prodMsgs), prodMsgs)
	}
	if len(archiveMsgs) != 3 {
		t.Errorf("archive vault: expected 3 messages, got %d: %v", len(archiveMsgs), archiveMsgs)
	}
	if len(stagingMsgs) != 0 {
		t.Errorf("staging vault: expected 0 messages, got %d: %v", len(stagingMsgs), stagingMsgs)
	}
}

func TestFilteringNoFilterSetDropsRecords(t *testing.T) {
	orch, vaults := newFilteredTestSetup(t)

	// No filter set — records should be silently dropped.
	rec := chunk.Record{
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{"env": "test"},
		Raw:      []byte("dropped message"),
	}
	if err := orch.Ingest(rec); err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// No vault should have the message.
	for id, cm := range vaults.cms {
		count := countRecords(t, cm)
		if count != 0 {
			t.Errorf("vault %s: expected 0 records (no routes), got %d", id, count)
		}
	}
}

func TestFilteringEmptyFilterReceivesNothing(t *testing.T) {
	orch, vaults := newFilteredTestSetup(t)

	// prod has empty filter (receives nothing), archive is catch-all.
	prodFilter, _ := orchestrator.CompileFilter(vaults.prod, "")
	archiveFilter, _ := orchestrator.CompileFilter(vaults.archive, "*")

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
	if count := countRecords(t, vaults.cms[vaults.prod]); count != 0 {
		t.Errorf("prod vault: expected 0 records (empty filter), got %d", count)
	}
	if count := countRecords(t, vaults.cms[vaults.archive]); count != 1 {
		t.Errorf("archive vault: expected 1 record, got %d", count)
	}
}

func TestFilteringComplexExpression(t *testing.T) {
	orch, vaults := newFilteredTestSetup(t)

	// prod receives: (env=prod AND level=error) OR (env=prod AND level=critical)
	prodFilter, err := orchestrator.CompileFilter(vaults.prod, "(env=prod AND level=error) OR (env=prod AND level=critical)")
	if err != nil {
		t.Fatalf("CompileFilter failed: %v", err)
	}
	archiveFilter, _ := orchestrator.CompileFilter(vaults.archive, "*")

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

	prodMsgs := getRecordMessages(t, vaults.cms[vaults.prod])

	for _, tc := range testCases {
		found := slices.Contains(prodMsgs, tc.raw)
		if found != tc.expectInProd {
			t.Errorf("message %q: expectInProd=%v, found=%v", tc.raw, tc.expectInProd, found)
		}
	}

	// Archive should have all 4 messages.
	if count := countRecords(t, vaults.cms[vaults.archive]); count != 4 {
		t.Errorf("archive: expected 4 messages, got %d", count)
	}
}

func TestIngestAckSuccess(t *testing.T) {
	orch, _ := newIngesterTestSetup(t)

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
	orch.RegisterIngester(glid.New(), "test", "mock", recv)

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
	orch, _ := newIngesterTestSetup(t)

	// Message without ack channel (fire-and-forget).
	recv := newMockIngester([]orchestrator.IngestMessage{
		{Attrs: map[string]string{"host": "server1"}, Raw: []byte("no ack message")},
	})
	orch.RegisterIngester(glid.New(), "test", "mock", recv)

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

// slowDigester adds a fixed delay to simulate CPU-bound digestion work.
type slowDigester struct {
	delay time.Duration
}

func (d *slowDigester) Digest(_ *orchestrator.IngestMessage) {
	time.Sleep(d.delay)
}

// slowChunkManager wraps a ChunkManager and adds a fixed delay to Append.
type slowChunkManager struct {
	chunk.ChunkManager
	delay time.Duration
}

func (s *slowChunkManager) Append(rec chunk.Record) (chunk.ChunkID, uint64, error) {
	time.Sleep(s.delay)
	return s.ChunkManager.Append(rec)
}

func TestPipelineOverlap(t *testing.T) {
	const (
		n            = 10
		digestDelay  = 10 * time.Millisecond
		writeDelay   = 10 * time.Millisecond
		perRecordSeq = digestDelay + writeDelay // 20ms sequential per record
	)

	s, _ := memtest.NewVault(chunkmem.Config{
		RotationPolicy: recordCountPolicy(1 << 20),
	})

	slowCM := &slowChunkManager{ChunkManager: s.CM, delay: writeDelay}
	vaultID := glid.New()

	orch, err := orchestrator.New(orchestrator.Config{IngestChannelSize: n})
	if err != nil {
		t.Fatal(err)
	}
	orch.RegisterVault(orchestrator.NewVaultFromComponents(vaultID, slowCM, s.IM, s.QE))
	orch.SetFilterSet(orchestrator.NewFilterSet([]*orchestrator.CompiledFilter{
		{VaultID: vaultID, Kind: orchestrator.FilterCatchAll, Expr: "*"},
	}))
	orch.RegisterDigester(&slowDigester{delay: digestDelay})

	// Ingester that sends n messages then waits for cancellation.
	msgs := make([]orchestrator.IngestMessage, n)
	for i := range n {
		msgs[i] = orchestrator.IngestMessage{
			Attrs:    map[string]string{"i": fmt.Sprintf("%d", i)},
			Raw:      []byte("msg"),
			IngestTS: time.Now(),
		}
	}

	// Use ack on the last message to know when everything is done.
	ackCh := make(chan error, 1)
	msgs[n-1].Ack = ackCh

	recv := newMockIngester(msgs)
	orch.RegisterIngester(glid.New(), "test", "mock", recv)

	start := time.Now()

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for last message to be acked (all records written).
	select {
	case err := <-ackCh:
		if err != nil {
			t.Fatalf("ack error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for pipeline to finish")
	}

	elapsed := time.Since(start)

	if err := orch.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Verify all records were written.
	cursor, err := slowCM.OpenCursor(slowCM.Active().ID)
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
	if count != n {
		t.Fatalf("expected %d records, got %d", n, count)
	}

	// Sequential would take n * (digestDelay + writeDelay) = 200ms.
	// Pipelined should be roughly n * max(digestDelay, writeDelay) + min = ~110ms.
	// Use 80% of sequential as the threshold to prove overlap.
	seqTime := time.Duration(n) * perRecordSeq
	threshold := seqTime * 80 / 100
	t.Logf("elapsed=%v, sequential=%v, threshold=%v", elapsed, seqTime, threshold)

	if elapsed >= threshold {
		t.Errorf("pipeline did not overlap: elapsed %v >= threshold %v (sequential %v)", elapsed, threshold, seqTime)
	}
}

// panickingIngester emits one message then panics.
type panickingIngester struct {
	started chan struct{}
}

func (r *panickingIngester) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	close(r.started)

	out <- orchestrator.IngestMessage{
		Raw:      []byte("before-panic"),
		IngestTS: time.Now(),
	}
	panic("intentional test panic")
}

func TestIngesterPanicRecovery(t *testing.T) {
	orch, cm := newIngesterTestSetup(t)

	panicker := &panickingIngester{started: make(chan struct{})}
	normalMsg := orchestrator.IngestMessage{
		Raw:      []byte("normal-message"),
		IngestTS: time.Now(),
	}
	normal := newMockIngester([]orchestrator.IngestMessage{normalMsg})

	panickerID := glid.New()
	normalID := glid.New()

	if err := orch.AddIngester(panickerID, "panicker", "test", panicker); err != nil {
		t.Fatal(err)
	}
	if err := orch.AddIngester(normalID, "normal", "test", normal); err != nil {
		t.Fatal(err)
	}

	if err := orch.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Wait for both ingesters to start.
	select {
	case <-panicker.started:
	case <-time.After(2 * time.Second):
		t.Fatal("panicking ingester did not start")
	}
	select {
	case <-normal.started:
	case <-time.After(2 * time.Second):
		t.Fatal("normal ingester did not start")
	}

	// Give the pipeline time to process messages.
	time.Sleep(200 * time.Millisecond)

	// Stop should complete without hanging — the panicking ingester's
	// goroutine has already exited via recover().
	if err := orch.Stop(); err != nil {
		t.Fatal(err)
	}

	// The normal ingester's message should have been written.
	records, err := cm.List()
	if err != nil {
		t.Fatal(err)
	}

	totalRecords := int64(0)
	for _, meta := range records {
		totalRecords += meta.RecordCount
	}
	if totalRecords == 0 {
		t.Error("expected at least one record from the normal ingester")
	}
}
