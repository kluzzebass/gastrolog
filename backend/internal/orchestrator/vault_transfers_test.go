package orchestrator_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"gastrolog/internal/chanwatch"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/chunk/memory"
	"gastrolog/internal/system"
	indexfile "gastrolog/internal/index/file"
	"gastrolog/internal/orchestrator"
)

// sliceIterator adapts a []chunk.Record into a chunk.RecordIterator.
func sliceIterator(records []chunk.Record) chunk.RecordIterator {
	i := 0
	return func() (chunk.Record, error) {
		if i >= len(records) {
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		rec := records[i]
		i++
		return rec, nil
	}
}

// mockTransferrer records calls to TransferRecords.
type mockTransferrer struct {
	calls   []transferCall
	failErr error        // if set, TransferRecords returns this error
	gate    chan struct{} // if non-nil, TransferRecords blocks until closed
}

func (m *mockTransferrer) WaitVaultReady(_ context.Context, _ string, _ uuid.UUID) error {
	return nil // always ready in tests
}

type transferCall struct {
	NodeID  string
	VaultID uuid.UUID
	Records []chunk.Record
}

func (m *mockTransferrer) TransferRecords(_ context.Context, nodeID string, vaultID uuid.UUID, next chunk.RecordIterator) error {
	if m.gate != nil {
		<-m.gate
	}
	if m.failErr != nil {
		return m.failErr
	}
	// Drain iterator into slice for test assertions.
	var records []chunk.Record
	for {
		rec, err := next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			return err
		}
		records = append(records, rec.Copy())
	}
	m.calls = append(m.calls, transferCall{
		NodeID:  nodeID,
		VaultID: vaultID,
		Records: records,
	})
	return nil
}

func (m *mockTransferrer) ForwardAppend(_ context.Context, nodeID string, vaultID uuid.UUID, records []chunk.Record) error {
	if m.failErr != nil {
		return m.failErr
	}
	copied := make([]chunk.Record, len(records))
	for i, r := range records {
		copied[i] = r.Copy()
	}
	m.calls = append(m.calls, transferCall{
		NodeID:  nodeID,
		VaultID: vaultID,
		Records: copied,
	})
	return nil
}

// staticSystemLoader implements orchestrator.SystemLoader for tests.
type staticSystemLoader struct {
	cfg *system.Config
}

func (f *staticSystemLoader) Load(_ context.Context) (*system.System, error) {
	if f.cfg == nil { return nil, nil }; return &system.System{Config: *f.cfg}, nil
}

func newFileVault(t *testing.T) (chunk.ChunkManager, *indexfile.Manager) {
	t.Helper()
	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            dir,
		RotationPolicy: chunk.NewRecordCountPolicy(100),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cm.Close() })
	im := indexfile.NewManager(dir, nil, nil)
	return cm, im
}

func newMemVault(t *testing.T) chunk.ChunkManager {
	t.Helper()
	cm, err := memory.NewManager(memory.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(100),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cm.Close() })
	return cm
}

func seedAndSeal(t *testing.T, orch *orchestrator.Orchestrator, vaultID uuid.UUID, count int) chunk.ChunkID {
	t.Helper()
	for i := range count {
		ts := time.Date(2025, 1, 1, 0, 0, i, 0, time.UTC)
		rec := chunk.Record{
			IngestTS: ts,
			Raw:      []byte("test-msg"),
		}
		if _, _, err := orch.Append(vaultID, rec); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if _, err := orch.SealActive(vaultID, uuid.Nil); err != nil {
		t.Fatalf("seal: %v", err)
	}
	metas, err := orch.ListChunkMetas(vaultID)
	if err != nil {
		t.Fatal(err)
	}
	if len(metas) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(metas))
	}
	return metas[0].ID
}

// TestMoveChunkRemote, TestMoveChunkRemoteMemoryVault, and
// TestMoveChunkRemoteTransferError were removed: they tested the concept of
// NodeID-based remote vault assignment which no longer exists. Remote vault
// transfer will be reintroduced via tier primary election in a future issue.

func TestMoveChunkRemoteNoTransferrer(t *testing.T) {
	t.Parallel()
	srcID := uuid.Must(uuid.NewV7())
	dstID := uuid.Must(uuid.NewV7())

	srcCM, srcIM := newFileVault(t)

	loader := &staticSystemLoader{cfg: &system.Config{
		Vaults: []system.VaultConfig{
			{ID: dstID},
		},
	}}

	orch, err := orchestrator.New(orchestrator.Config{
		SystemLoader: loader,
		LocalNodeID:  "node-A",
	})
	if err != nil {
		t.Fatal(err)
	}
	orch.RegisterVault(orchestrator.NewVaultFromComponents(srcID, srcCM, srcIM, nil))

	chunkID := seedAndSeal(t, orch, srcID, 1)

	err = orch.MoveChunk(context.Background(), chunkID, srcID, dstID)
	if err == nil {
		t.Fatal("expected error when transferrer is nil")
	}
	t.Logf("got expected error: %v", err)
}

func TestMoveChunkLocalImportFallback(t *testing.T) {
	t.Parallel()
	// When both source and dest are memory vaults (no ChunkMover),
	// MoveChunk should use ImportRecords.
	srcID := uuid.Must(uuid.NewV7())
	dstID := uuid.Must(uuid.NewV7())

	srcCM := newMemVault(t)
	dstCM := newMemVault(t)

	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	orch.RegisterVault(orchestrator.NewVaultFromComponents(srcID, srcCM, nil, nil))
	orch.RegisterVault(orchestrator.NewVaultFromComponents(dstID, dstCM, nil, nil))

	chunkID := seedAndSeal(t, orch, srcID, 5)

	if err := orch.MoveChunk(context.Background(), chunkID, srcID, dstID); err != nil {
		t.Fatalf("MoveChunk local import: %v", err)
	}

	// Source should be empty.
	srcMetas, _ := orch.ListChunkMetas(srcID)
	if len(srcMetas) != 0 {
		t.Errorf("source has %d chunks, want 0", len(srcMetas))
	}

	// Destination should have exactly 1 sealed chunk with 5 records.
	dstMetas, _ := orch.ListChunkMetas(dstID)
	if len(dstMetas) != 1 {
		t.Fatalf("destination has %d chunks, want 1", len(dstMetas))
	}
	if !dstMetas[0].Sealed {
		t.Error("destination chunk should be sealed")
	}
	if dstMetas[0].RecordCount != 5 {
		t.Errorf("destination chunk has %d records, want 5", dstMetas[0].RecordCount)
	}

	// Destination active chunk should be unaffected (nil or empty).
	if active := dstCM.Active(); active != nil && active.RecordCount > 0 {
		t.Errorf("destination active chunk has %d records, should be unaffected", active.RecordCount)
	}
}

func TestImportRecordsMemory(t *testing.T) {
	t.Parallel()
	cm := newMemVault(t)

	records := make([]chunk.Record, 5)
	for i := range records {
		records[i] = chunk.Record{
			SourceTS: time.Date(2025, 1, 1, 0, 0, i, 0, time.UTC),
			IngestTS: time.Date(2025, 1, 1, 0, 0, i, 0, time.UTC),
			WriteTS:  time.Date(2025, 1, 1, 0, 0, i, 0, time.UTC),
			Raw:      []byte("test"),
			Attrs:    chunk.Attributes{"key": "val"},
		}
	}

	meta, err := cm.ImportRecords(sliceIterator(records))
	if err != nil {
		t.Fatalf("ImportRecords: %v", err)
	}
	if !meta.Sealed {
		t.Error("imported chunk should be sealed")
	}
	if meta.RecordCount != 5 {
		t.Errorf("record count = %d, want 5", meta.RecordCount)
	}

	// Verify records are readable.
	cursor, err := cm.OpenCursor(meta.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cursor.Close() }()
	count := 0
	for {
		_, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		count++
	}
	if count != 5 {
		t.Errorf("cursor returned %d records, want 5", count)
	}

	// Active chunk should be unaffected.
	if active := cm.Active(); active != nil && active.RecordCount > 0 {
		t.Errorf("active chunk has %d records, should be unaffected", active.RecordCount)
	}
}

func TestImportRecordsFile(t *testing.T) {
	t.Parallel()
	cm, _ := newFileVault(t)

	records := make([]chunk.Record, 5)
	for i := range records {
		records[i] = chunk.Record{
			SourceTS: time.Date(2025, 1, 1, 0, 0, i, 0, time.UTC),
			IngestTS: time.Date(2025, 1, 1, 0, 0, i, 0, time.UTC),
			WriteTS:  time.Date(2025, 1, 1, 0, 0, i, 0, time.UTC),
			Raw:      []byte("test-data"),
			Attrs:    chunk.Attributes{"src": "test"},
		}
	}

	meta, err := cm.ImportRecords(sliceIterator(records))
	if err != nil {
		t.Fatalf("ImportRecords: %v", err)
	}
	if !meta.Sealed {
		t.Error("imported chunk should be sealed")
	}
	if meta.RecordCount != 5 {
		t.Errorf("record count = %d, want 5", meta.RecordCount)
	}

	// Verify records are readable via cursor.
	cursor, err := cm.OpenCursor(meta.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cursor.Close() }()
	count := 0
	for {
		rec, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if string(rec.Raw) != "test-data" {
			t.Errorf("record %d raw = %q, want %q", count, rec.Raw, "test-data")
		}
		count++
	}
	if count != 5 {
		t.Errorf("cursor returned %d records, want 5", count)
	}
}

func TestImportRecordsEmpty(t *testing.T) {
	t.Parallel()
	cm := newMemVault(t)

	meta, err := cm.ImportRecords(sliceIterator(nil))
	if err != nil {
		t.Fatalf("ImportRecords(nil): %v", err)
	}
	if meta.RecordCount != 0 {
		t.Errorf("expected empty meta for nil records, got count=%d", meta.RecordCount)
	}
}

// --- Drain tests ---

// noopForwarder satisfies RecordForwarder for tests that need filter routing
// but don't actually forward anything.
type noopForwarder struct{}

func (noopForwarder) Forward(context.Context, string, uuid.UUID, []chunk.Record) error { return nil }

func (noopForwarder) ForwardSync(context.Context, string, uuid.UUID, []chunk.Record) error {
	return nil
}

func (noopForwarder) RegisterPressureGate(*chanwatch.PressureGate) {
	// No-op: these tests don't exercise forward-path pressure.
}

// waitForJob polls the scheduler until the job completes or the timeout expires.
func waitForJob(t *testing.T, sched *orchestrator.Scheduler, jobID string, timeout time.Duration) orchestrator.JobInfo {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		info, ok := sched.GetJob(jobID)
		if ok {
			snap := info.Snapshot()
			if snap.Progress.Status == orchestrator.JobStatusCompleted || snap.Progress.Status == orchestrator.JobStatusFailed {
				return snap
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %s did not complete within %v", jobID, timeout)
	return orchestrator.JobInfo{}
}

// drainSetup creates an orchestrator with a single vault, routes, and a mock
// transferrer suitable for drain tests. Returns the orchestrator, vault ID,
// mock transferrer, and config loader (for route-based filter reload).
func drainSetup(t *testing.T, recordCount int) (*orchestrator.Orchestrator, uuid.UUID, *mockTransferrer) {
	t.Helper()

	vaultID := uuid.Must(uuid.NewV7())
	filterID := uuid.Must(uuid.NewV7())
	routeID := uuid.Must(uuid.NewV7())

	cm := newMemVault(t)

	loader := &staticSystemLoader{cfg: &system.Config{
		Vaults: []system.VaultConfig{
			{ID: vaultID},
		},
		Filters: []system.FilterConfig{
			{ID: filterID, Expression: "*"},
		},
		Routes: []system.RouteConfig{
			{ID: routeID, FilterID: &filterID, Destinations: []uuid.UUID{vaultID}, Enabled: true},
		},
	}}

	orch, err := orchestrator.New(orchestrator.Config{
		SystemLoader: loader,
		LocalNodeID:  "node-A",
	})
	if err != nil {
		t.Fatal(err)
	}

	orch.SetRecordForwarder(noopForwarder{})

	mock := &mockTransferrer{}
	orch.SetRemoteTransferrer(mock)

	orch.RegisterVault(orchestrator.NewVaultFromComponents(vaultID, cm, nil, nil))

	// Build initial filters from routes.
	if err := orch.ReloadFilters(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Seed records and seal.
	if recordCount > 0 {
		seedAndSeal(t, orch, vaultID, recordCount)
	}

	return orch, vaultID, mock
}

func TestDrainVault_Basic(t *testing.T) {
	t.Parallel()
	orch, vaultID, mock := drainSetup(t, 5)

	// Gate the mock so the worker blocks until we've checked IsDraining.
	mock.gate = make(chan struct{})

	// Start drain.
	if err := orch.DrainVault(context.Background(), vaultID, "node-B"); err != nil {
		t.Fatalf("DrainVault: %v", err)
	}

	if !orch.IsDraining(vaultID) {
		t.Fatal("expected IsDraining to be true")
	}

	// Release the worker so it can complete.
	close(mock.gate)

	// Wait for the drain worker to complete.
	jobs := orch.Scheduler().ListJobs()
	var jobID string
	for _, j := range jobs {
		if j.Name == "drain:"+vaultID.String() {
			jobID = j.ID
			break
		}
	}
	if jobID == "" {
		t.Fatal("drain job not found in scheduler")
	}

	info := waitForJob(t, orch.Scheduler(), jobID, 5*time.Second)
	if info.Progress.Status != orchestrator.JobStatusCompleted {
		t.Fatalf("drain job failed: %s", info.Progress.Error)
	}

	// Verify TransferRecords was called with all sealed chunks.
	if len(mock.calls) != 1 {
		t.Fatalf("expected 1 TransferRecords call, got %d", len(mock.calls))
	}
	call := mock.calls[0]
	if call.NodeID != "node-B" {
		t.Errorf("nodeID = %q, want %q", call.NodeID, "node-B")
	}
	if len(call.Records) != 5 {
		t.Errorf("expected 5 records, got %d", len(call.Records))
	}

	// Vault should be unregistered after drain.
	if orch.VaultExists(vaultID) {
		t.Error("vault should be unregistered after drain completes")
	}
	if orch.IsDraining(vaultID) {
		t.Error("expected IsDraining to be false after drain completes")
	}
}

func TestDrainVault_CancelDrain(t *testing.T) {
	t.Parallel()
	vaultID := uuid.Must(uuid.NewV7())
	filterID := uuid.Must(uuid.NewV7())
	routeID := uuid.Must(uuid.NewV7())

	cm := newMemVault(t)

	loader := &staticSystemLoader{cfg: &system.Config{
		Vaults: []system.VaultConfig{
			{ID: vaultID},
		},
		Filters: []system.FilterConfig{
			{ID: filterID, Expression: "*"},
		},
		Routes: []system.RouteConfig{
			{ID: routeID, FilterID: &filterID, Destinations: []uuid.UUID{vaultID}, Enabled: true},
		},
	}}

	orch, err := orchestrator.New(orchestrator.Config{
		SystemLoader: loader,
		LocalNodeID:  "node-A",
	})
	if err != nil {
		t.Fatal(err)
	}

	orch.SetRecordForwarder(noopForwarder{})

	// Use a transferrer that blocks until context cancellation.
	blockTransfer := &mockTransferrer{failErr: context.Canceled}
	orch.SetRemoteTransferrer(blockTransfer)

	orch.RegisterVault(orchestrator.NewVaultFromComponents(vaultID, cm, nil, nil))

	if err := orch.ReloadFilters(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Seed multiple chunks so drain has work to do.
	seedAndSeal(t, orch, vaultID, 3)

	// Start drain.
	if err := orch.DrainVault(context.Background(), vaultID, "node-B"); err != nil {
		t.Fatalf("DrainVault: %v", err)
	}

	// Cancel immediately.
	if err := orch.CancelDrain(context.Background(), vaultID); err != nil {
		t.Fatalf("CancelDrain: %v", err)
	}

	if orch.IsDraining(vaultID) {
		t.Error("expected IsDraining to be false after cancel")
	}

	// Vault should still be registered (not removed).
	if !orch.VaultExists(vaultID) {
		t.Error("vault should remain registered after cancel")
	}

	// Remaining chunks should still be local.
	metas, err := orch.ListChunkMetas(vaultID)
	if err != nil {
		t.Fatal(err)
	}
	if len(metas) == 0 {
		t.Error("expected at least some chunks to remain after cancel")
	}
}

func TestDrainVault_AlreadyDraining(t *testing.T) {
	t.Parallel()
	orch, vaultID, _ := drainSetup(t, 3)

	// Use a transferrer that blocks so the drain stays in progress.
	blocking := make(chan struct{})
	orch.SetRemoteTransferrer(&mockTransferrer{failErr: context.Canceled})

	if err := orch.DrainVault(context.Background(), vaultID, "node-B"); err != nil {
		t.Fatalf("DrainVault: %v", err)
	}

	// Second drain should error.
	err := orch.DrainVault(context.Background(), vaultID, "node-C")
	if err == nil {
		t.Fatal("expected error for already-draining vault")
	}

	close(blocking)
}

func TestDrainVault_EmptyVault(t *testing.T) {
	t.Parallel()
	orch, vaultID, mock := drainSetup(t, 0)

	if err := orch.DrainVault(context.Background(), vaultID, "node-B"); err != nil {
		t.Fatalf("DrainVault: %v", err)
	}

	// Wait for drain to complete.
	jobs := orch.Scheduler().ListJobs()
	var jobID string
	for _, j := range jobs {
		if j.Name == "drain:"+vaultID.String() {
			jobID = j.ID
			break
		}
	}
	if jobID == "" {
		t.Fatal("drain job not found in scheduler")
	}

	info := waitForJob(t, orch.Scheduler(), jobID, 5*time.Second)
	if info.Progress.Status != orchestrator.JobStatusCompleted {
		t.Fatalf("drain job failed: %s", info.Progress.Error)
	}

	// No transfers should have been called.
	if len(mock.calls) != 0 {
		t.Errorf("expected 0 TransferRecords calls, got %d", len(mock.calls))
	}

	// Vault should be unregistered.
	if orch.VaultExists(vaultID) {
		t.Error("vault should be unregistered after drain")
	}
}

func TestDrainVault_NoTransferrer(t *testing.T) {
	t.Parallel()
	vaultID := uuid.Must(uuid.NewV7())
	filterID := uuid.Must(uuid.NewV7())
	routeID := uuid.Must(uuid.NewV7())

	cm := newMemVault(t)

	loader := &staticSystemLoader{cfg: &system.Config{
		Vaults: []system.VaultConfig{
			{ID: vaultID},
		},
		Filters: []system.FilterConfig{
			{ID: filterID, Expression: "*"},
		},
		Routes: []system.RouteConfig{
			{ID: routeID, FilterID: &filterID, Destinations: []uuid.UUID{vaultID}, Enabled: true},
		},
	}}

	orch, err := orchestrator.New(orchestrator.Config{
		SystemLoader: loader,
		LocalNodeID:  "node-A",
	})
	if err != nil {
		t.Fatal(err)
	}

	orch.SetRecordForwarder(noopForwarder{})
	// Deliberately do NOT set a RemoteTransferrer.

	orch.RegisterVault(orchestrator.NewVaultFromComponents(vaultID, cm, nil, nil))

	if err := orch.ReloadFilters(context.Background()); err != nil {
		t.Fatal(err)
	}

	seedAndSeal(t, orch, vaultID, 2)

	if err := orch.DrainVault(context.Background(), vaultID, "node-B"); err != nil {
		t.Fatalf("DrainVault: %v", err)
	}

	// Wait for the drain worker to fail.
	jobs := orch.Scheduler().ListJobs()
	var jobID string
	for _, j := range jobs {
		if j.Name == "drain:"+vaultID.String() {
			jobID = j.ID
			break
		}
	}
	if jobID == "" {
		t.Fatal("drain job not found in scheduler")
	}

	info := waitForJob(t, orch.Scheduler(), jobID, 5*time.Second)
	if info.Progress.Status != orchestrator.JobStatusFailed {
		t.Fatalf("expected drain job to fail, got status %d", info.Progress.Status)
	}

	// Vault should still be registered (drain failed, data not lost).
	if !orch.VaultExists(vaultID) {
		t.Error("vault should remain registered when drain fails")
	}
}

func (m *mockTransferrer) StreamToTier(_ context.Context, _ string, _, _ uuid.UUID, _ chunk.RecordIterator) error {
	return nil
}

