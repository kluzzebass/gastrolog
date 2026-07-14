package orchestrator_test

import (
	"context"
	"errors"
	"gastrolog/internal/glid"
	"path/filepath"
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
	"gastrolog/internal/pipeline/routing"
	"gastrolog/internal/query"
)

// mustNewTestOrch creates an orchestrator for external-package tests with a
// segments directory under t.TempDir(), matching production's home.SegmentsDir().
func mustNewTestOrch(t *testing.T, cfg orchestrator.Config) *orchestrator.Orchestrator {
	t.Helper()
	if cfg.SegmentsDir == "" {
		cfg.SegmentsDir = filepath.Join(t.TempDir(), "segments")
	}
	orch, err := orchestrator.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return orch
}

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
	orch := mustNewTestOrch(t, orchestrator.Config{})
	orch.RegisterVault(orchestrator.NewVaultFromComponents(defaultID, s.CM, tracker, s.QE))

	// Set up a catch-all route so records are delivered to the vault.
	cr, _ := routing.CompileRoute(glid.New(), "all", 0, "*", []glid.GLID{defaultID})
	orch.SetTestRoutingTable(routing.NewTable([]*routing.Route{cr}))

	return orch, s.CM, tracker, defaultID
}

func TestIngestReachesChunkManager(t *testing.T) {
	orch, cm, _, _ := newTestSetup(t, 1<<20) // Large chunk, no auto-seal

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
	orch, cm, _, _ := newTestSetup(t, 1<<20)

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
	orch, cm, _, defaultID := newTestSetup(t, 1<<20)

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
	orch, _, _, defaultID := newTestSetup(t, 1<<20)

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
	orch, _, _, _ := newTestSetup(t, 1<<20)

	_, _, err := orch.Search(context.Background(), glid.New(), query.Query{}, nil)
	if err != orchestrator.ErrUnknownRegistry {
		t.Errorf("expected ErrUnknownRegistry, got %v", err)
	}
}

func TestIngestNoChunkManagers(t *testing.T) {
	orch := mustNewTestOrch(t, orchestrator.Config{})

	rec := chunk.Record{IngestTS: t1, Attrs: attrsA, Raw: []byte("test")}
	err := orch.Ingest(rec)
	if err != orchestrator.ErrNoChunkManagers {
		t.Errorf("expected ErrNoChunkManagers, got %v", err)
	}
}

func TestSearchNoQueryEngines(t *testing.T) {
	orch := mustNewTestOrch(t, orchestrator.Config{})

	_, _, err := orch.Search(context.Background(), glid.New(), query.Query{}, nil)
	if err != orchestrator.ErrNoQueryEngines {
		t.Errorf("expected ErrNoQueryEngines, got %v", err)
	}
}

func TestSearchThenFollowViaOrchestrator(t *testing.T) {
	orch, _, _, defaultID := newTestSetup(t, 1<<20)

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
	orch, _, _, defaultID := newTestSetup(t, 1<<20)

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

// failOnceIngester fails its first run, then blocks until ctx is done — a
// source that recovers on the pipeline's retry.
type failOnceIngester struct {
	attempts atomic.Int32
}

func (f *failOnceIngester) Run(ctx context.Context, _ chan<- orchestrator.IngestMessage) error {
	if f.attempts.Add(1) == 1 {
		return errors.New("source unavailable")
	}
	<-ctx.Done()
	return ctx.Err()
}

// TestIngesterAliveTracksErrorRetry pins the observability chain behind the
// gastrolog-fjwhbr fix: a non-passive ingester whose run returns an error
// drops its alive state (OnIngesterAlive false, IsIngesterRunning false — the
// trigger for the ingester convergence sweep's ingester-not-running alert,
// gastrolog-3mnjlo), and the pipeline retry re-arms the run so the alive state
// comes back up, which is what lets the sweep clear the alert on recovery.
func TestIngesterAliveTracksErrorRetry(t *testing.T) {
	t.Parallel()

	type aliveEvent struct {
		id    glid.GLID
		alive bool
	}
	events := make(chan aliveEvent, 16)
	orch := mustNewTestOrch(t, orchestrator.Config{
		OnIngesterAlive: func(id glid.GLID, alive bool) {
			events <- aliveEvent{id: id, alive: alive}
		},
		IngesterRetryDelay: func(int) time.Duration { return 0 },
	})

	id := glid.New()
	orch.RegisterIngester(id, "flaky", "mock", &failOnceIngester{})

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer orch.Stop()

	next := func(want bool) {
		t.Helper()
		select {
		case ev := <-events:
			if ev.id != id || ev.alive != want {
				t.Fatalf("alive event = %+v, want id=%v alive=%v", ev, id, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for alive=%v event", want)
		}
	}

	next(true)  // first run attempt
	next(false) // run returned an error — the sweep would alert here
	next(true)  // retry re-armed the run — the sweep clears on this
	if !orch.IsIngesterRunning(id) {
		t.Fatal("IsIngesterRunning must report true once the retried run holds")
	}
}

func newIngesterTestSetup(t *testing.T) (*orchestrator.Orchestrator, chunk.ChunkManager) {
	t.Helper()
	s, _ := memtest.NewVault(chunkmem.Config{
		RotationPolicy: recordCountPolicy(10000),
	})

	defaultID := glid.New()
	orch := mustNewTestOrch(t, orchestrator.Config{})
	orch.RegisterVault(orchestrator.NewVaultFromComponents(defaultID, s.CM, s.IM, s.QE))

	// Set up a catch-all route so records are delivered to the vault.
	cr, _ := routing.CompileRoute(glid.New(), "all", 0, "*", []glid.GLID{defaultID})
	orch.SetTestRoutingTable(routing.NewTable([]*routing.Route{cr}))

	return orch, s.CM
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

// Registry accessor tests

func TestChunkManagerAccessor(t *testing.T) {
	orch, cm, _, defaultID := newTestSetup(t, 1<<20)

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
	orch, _, _, defaultID := newTestSetup(t, 1<<20)

	keys := orch.ListVaults()
	if len(keys) != 1 {
		t.Fatalf("expected 1 key, got %d", len(keys))
	}
	if keys[0] != defaultID {
		t.Errorf("expected %s, got %s", defaultID, keys[0])
	}
}

func TestIndexManagerAccessor(t *testing.T) {
	orch, _, tracker, defaultID := newTestSetup(t, 1<<20)

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
	orch, _, _, defaultID := newTestSetup(t, 1<<20)

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
	orch := mustNewTestOrch(t, orchestrator.Config{})
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
// CloudBacked. Used to simulate an instance whose sealed chunks have already been
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
	orch := mustNewTestOrch(t, orchestrator.Config{})
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
	orch := mustNewTestOrch(t, orchestrator.Config{})
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
	orch, _, _, _ := newTestSetup(t, 1<<20)

	_, _, err := orch.SearchThenFollow(context.Background(), glid.New(), query.Query{}, nil)
	if err != orchestrator.ErrUnknownRegistry {
		t.Errorf("expected ErrUnknownRegistry, got %v", err)
	}
}

func TestSearchWithContextUnknownRegistry(t *testing.T) {
	orch, _, _, _ := newTestSetup(t, 1<<20)

	_, _, err := orch.SearchWithContext(context.Background(), glid.New(), query.Query{})
	if err != orchestrator.ErrUnknownRegistry {
		t.Errorf("expected ErrUnknownRegistry, got %v", err)
	}
}

// filteredTestVaults holds the vault IDs and chunk managers for the filtered test setup.
type filteredTestVaults struct {
	prod      glid.GLID
	staging   glid.GLID
	archive   glid.GLID
	catchRest glid.GLID
	cms       map[glid.GLID]chunk.ChunkManager
}

// newFilteredTestSetup creates an orchestrator with multiple vaults and a filter set.
func newFilteredTestSetup(t *testing.T) (*orchestrator.Orchestrator, filteredTestVaults) {
	t.Helper()

	vaults := filteredTestVaults{
		prod:      glid.New(),
		staging:   glid.New(),
		archive:   glid.New(),
		catchRest: glid.New(),
		cms:       make(map[glid.GLID]chunk.ChunkManager),
	}

	orch := mustNewTestOrch(t, orchestrator.Config{})

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
		prod:      glid.New(),
		staging:   glid.New(),
		archive:   glid.New(),
		catchRest: glid.New(),
		cms:       make(map[glid.GLID]chunk.ChunkManager),
	}

	orch := mustNewTestOrch(t, orchestrator.Config{SystemLoader: loader})

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

	// gastrolog-4kkoo (Phase 5): priority-ordered first-match-wins.
	// Specific routes at priority 10 (env=prod, env=staging) fire first;
	// the catch-all sits at priority 100 and absorbs everything else.
	// Each record reaches exactly one vault (no multi-fan-out unless a
	// single route lists multiple destinations).
	prodRoute, err := routing.CompileRoute(glid.New(), "prod", 10, "env=prod", []glid.GLID{vaults.prod})
	if err != nil {
		t.Fatalf("CompileRoute prod: %v", err)
	}
	stagingRoute, err := routing.CompileRoute(glid.New(), "staging", 10, "env=staging", []glid.GLID{vaults.staging})
	if err != nil {
		t.Fatalf("CompileRoute staging: %v", err)
	}
	archiveRoute, err := routing.CompileRoute(glid.New(), "archive", 100, "*", []glid.GLID{vaults.archive})
	if err != nil {
		t.Fatalf("CompileRoute archive: %v", err)
	}

	rs := routing.NewTable([]*routing.Route{
		prodRoute,
		stagingRoute,
		archiveRoute,
	})
	orch.SetTestRoutingTable(rs)

	// Test cases: message attrs -> expected vault (first-match-wins).
	testCases := []struct {
		name     string
		attrs    chunk.Attributes
		raw      string
		expected []glid.GLID // vaults that should receive the message
	}{
		{
			name:     "prod message goes only to prod",
			attrs:    chunk.Attributes{"env": "prod", "level": "error"},
			raw:      "production error",
			expected: []glid.GLID{vaults.prod},
		},
		{
			name:     "staging message goes only to staging",
			attrs:    chunk.Attributes{"env": "staging", "level": "info"},
			raw:      "staging info",
			expected: []glid.GLID{vaults.staging},
		},
		{
			name:     "dev message falls through to archive",
			attrs:    chunk.Attributes{"env": "dev", "level": "debug"},
			raw:      "dev debug",
			expected: []glid.GLID{vaults.archive},
		},
		{
			name:     "no env falls through to archive",
			attrs:    chunk.Attributes{"level": "warn"},
			raw:      "no env warn",
			expected: []glid.GLID{vaults.archive},
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

	// gastrolog-4kkoo (Phase 5): a route with an empty match expression
	// (MatchNone) is enrolled but never fires — useful as a temporary
	// "muted" state. prod is muted at priority 10, archive catches at 100.
	prodRoute, _ := routing.CompileRoute(glid.New(), "prod", 10, "", []glid.GLID{vaults.prod})
	archiveRoute, _ := routing.CompileRoute(glid.New(), "archive", 100, "*", []glid.GLID{vaults.archive})

	rs := routing.NewTable([]*routing.Route{prodRoute, archiveRoute})
	orch.SetTestRoutingTable(rs)

	rec := chunk.Record{
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{"env": "prod"},
		Raw:      []byte("should not go to prod"),
	}
	if err := orch.Ingest(rec); err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// prod's muted route never fires, so the record falls through to archive.
	if count := countRecords(t, vaults.cms[vaults.prod]); count != 0 {
		t.Errorf("prod vault: expected 0 records (muted route), got %d", count)
	}
	if count := countRecords(t, vaults.cms[vaults.archive]); count != 1 {
		t.Errorf("archive vault: expected 1 record, got %d", count)
	}
}

func TestFilteringComplexExpression(t *testing.T) {
	orch, vaults := newFilteredTestSetup(t)

	// gastrolog-4kkoo (Phase 5): prod route at priority 10 with a complex
	// expression; archive catch-all at priority 100.
	prodRoute, err := routing.CompileRoute(glid.New(), "prod", 10,
		"(env=prod AND level=error) OR (env=prod AND level=critical)",
		[]glid.GLID{vaults.prod})
	if err != nil {
		t.Fatalf("CompileRoute failed: %v", err)
	}
	archiveRoute, _ := routing.CompileRoute(glid.New(), "archive", 100, "*", []glid.GLID{vaults.archive})

	rs := routing.NewTable([]*routing.Route{prodRoute, archiveRoute})
	orch.SetTestRoutingTable(rs)

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

	// gastrolog-4kkoo (Phase 5): first-match-wins — archive only catches
	// the records that didn't match the prod route. The fixture has 2
	// matching (prod+error, prod+critical) and 2 falling through (prod+info,
	// staging+error), so archive sees 2.
	if count := countRecords(t, vaults.cms[vaults.archive]); count != 2 {
		t.Errorf("archive: expected 2 fall-through messages, got %d", count)
	}
}
