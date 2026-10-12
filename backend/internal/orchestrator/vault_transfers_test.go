package orchestrator_test

import (
	"context"
	"errors"
	"gastrolog/internal/glid"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/chunk/memory"
	indexfile "gastrolog/internal/index/file"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/system"
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
	failErr error         // if set, TransferRecords returns this error
	gate    chan struct{} // if non-nil, TransferRecords blocks until closed
}

type transferCall struct {
	NodeID  string
	VaultID glid.GLID
	Records []chunk.Record
}

func (m *mockTransferrer) TransferRecords(_ context.Context, nodeID string, vaultID glid.GLID, next chunk.RecordIterator) error {
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

func (m *mockTransferrer) ForwardAppend(_ context.Context, nodeID string, vaultID glid.GLID, records []chunk.Record) error {
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
	if f.cfg == nil {
		return nil, nil
	}
	return &system.System{Config: *f.cfg}, nil
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
	im := indexfile.NewManager(dir, nil, nil, cm)
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

func seedAndSeal(t *testing.T, orch *orchestrator.Orchestrator, vaultID glid.GLID, count int) chunk.ChunkID {
	t.Helper()
	for i := range count {
		ts := time.Date(2025, 1, 1, 0, 0, i, 0, time.UTC)
		rec := chunk.Record{
			IngestTS: ts,
			Raw:      []byte("test-msg"),
		}
		if err := orch.AppendToVault(vaultID, chunk.ChunkID{}, rec); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if _, err := orch.SealActive(vaultID); err != nil {
		t.Fatalf("seal: %v", err)
	}
	metas, err := orch.ListLocalChunkMetas(vaultID)
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
// transfer will be reintroduced via vault leader election in a future issue.

func TestMoveChunkRemoteNoTransferrer(t *testing.T) {
	t.Parallel()
	srcID := glid.New()
	dstID := glid.New()

	srcCM, srcIM := newFileVault(t)

	loader := &staticSystemLoader{cfg: &system.Config{
		Vaults: []system.VaultConfig{
			{ID: dstID},
		},
	}}

	orch := mustNewTestOrch(t, orchestrator.Config{
		SystemLoader: loader,
		LocalNodeID:  "node-A",
	})
	orch.RegisterVault(orchestrator.NewVaultFromComponents(srcID, srcCM, srcIM, nil))

	chunkID := seedAndSeal(t, orch, srcID, 1)

	err := orch.MoveChunk(context.Background(), chunkID, srcID, dstID)
	if err == nil {
		t.Fatal("expected error when transferrer is nil")
	}
	t.Logf("got expected error: %v", err)
}

func TestMoveChunkLocalImportFallback(t *testing.T) {
	t.Parallel()
	// When both source and dest are memory vaults (no ChunkMover),
	// MoveChunk should use ImportRecords.
	srcID := glid.New()
	dstID := glid.New()

	srcCM := newMemVault(t)
	dstCM := newMemVault(t)

	orch := mustNewTestOrch(t, orchestrator.Config{})
	orch.RegisterVault(orchestrator.NewVaultFromComponents(srcID, srcCM, nil, nil))
	orch.RegisterVault(orchestrator.NewVaultFromComponents(dstID, dstCM, nil, nil))

	chunkID := seedAndSeal(t, orch, srcID, 5)

	if err := orch.MoveChunk(context.Background(), chunkID, srcID, dstID); err != nil {
		t.Fatalf("MoveChunk local import: %v", err)
	}

	// Source should be empty.
	srcMetas, _ := orch.ListLocalChunkMetas(srcID)
	if len(srcMetas) != 0 {
		t.Errorf("source has %d chunks, want 0", len(srcMetas))
	}

	// Destination should have exactly 1 sealed chunk with 5 records.
	dstMetas, _ := orch.ListLocalChunkMetas(dstID)
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

	meta, err := cm.ImportRecords(chunk.ChunkID{}, sliceIterator(records))
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

	meta, err := cm.ImportRecords(chunk.ChunkID{}, sliceIterator(records))
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

	meta, err := cm.ImportRecords(chunk.ChunkID{}, sliceIterator(nil))
	if err != nil {
		t.Fatalf("ImportRecords(nil): %v", err)
	}
	if meta.RecordCount != 0 {
		t.Errorf("expected empty meta for nil records, got count=%d", meta.RecordCount)
	}
}
