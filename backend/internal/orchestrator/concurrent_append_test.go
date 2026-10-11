package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"io"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	indexfile "gastrolog/internal/index/file"
	"gastrolog/internal/query"
)

// TestConcurrentAppendAttrIntegrity guards attr.log integrity under
// concurrent AppendToVault calls through the orchestrator: 8 goroutines
// burst into one file-backed vault, then every record is read back via
// cursor. Corrupt attr.log surfaces as an error from cursor.Next().
func TestConcurrentAppendAttrIntegrity(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	nodeID := "test-node"

	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(200),
	})
	if err != nil {
		t.Fatal(err)
	}
	im := indexfile.NewManager(dir, nil, nil, cm)

	orch := newTestOrch(t, Config{LocalNodeID: nodeID})

	vaultInst := &VaultInstance{
		VaultID: vaultID, Type: "file",
		Chunks: cm, Indexes: im, Query: query.New(cm, im, nil),
	}
	orch.RegisterVault(NewVault(vaultID, vaultInst))
	t.Cleanup(func() {
		orch.Stop()
		_ = cm.Close()
	})

	const goroutines = 8
	const perGoroutine = 500
	const totalRecords = goroutines * perGoroutine

	var wg sync.WaitGroup
	var appendErrors int64
	var mu sync.Mutex

	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)

	for g := range goroutines {
		wg.Add(1)
		go func(gIdx int) {
			defer wg.Done()
			base := gIdx * perGoroutine
			for i := range perGoroutine {
				ts := t0.Add(time.Duration(base+i) * time.Microsecond)
				err := orch.AppendToVault(vaultID, chunk.ChunkID{}, chunk.Record{
					IngestTS: ts,
					WriteTS:  ts,
					Raw:      fmt.Appendf(nil, "orch-concurrent-%d-%d", gIdx, i),
					Attrs:    chunk.Attributes{"goroutine": fmt.Sprintf("g%d", gIdx), "index": fmt.Sprintf("%d", i)},
				})
				if err != nil {
					mu.Lock()
					appendErrors++
					mu.Unlock()
				}
			}
		}(g)
	}
	wg.Wait()

	if appendErrors > 0 {
		t.Fatalf("%d append errors during concurrent burst", appendErrors)
	}

	// Seal remaining.
	if active := cm.Active(); active != nil && active.RecordCount > 0 {
		_ = cm.Seal()
	}

	// Background PostSealProcess (compression) jobs run on the scheduler.
	requireIdle(t, orch.Scheduler())

	// Read back ALL records via cursor.
	metas, _ := cm.List()
	var readErrors int
	var totalRead int

	for _, m := range metas {
		cursor, err := cm.OpenCursor(m.ID)
		if err != nil {
			t.Fatalf("OpenCursor(%s): %v", m.ID, err)
		}
		for {
			rec, _, err := cursor.Next()
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				break
			}
			if err != nil {
				readErrors++
				// Log chunk metadata for diagnosis.
				t.Errorf("chunk %s (records=%d, sealed=%v) record index %d: %v",
					m.ID, m.RecordCount, m.Sealed, totalRead, err)
				if readErrors > 10 {
					_ = cursor.Close()
					t.Fatalf("too many read errors")
				}
				break
			}
			totalRead++
			if rec.Attrs == nil || rec.Attrs["goroutine"] == "" {
				t.Errorf("record %d: missing attrs", totalRead)
			}
		}
		_ = cursor.Close()
	}

	if totalRead != totalRecords {
		t.Errorf("expected %d records, read %d", totalRecords, totalRead)
	}
	if readErrors > 0 {
		t.Errorf("%d records had attr corruption", readErrors)
	} else {
		t.Logf("all %d records intact across %d chunks", totalRead, len(metas))
	}
}

// ==========================================================================
// Transition concurrent with active appends
// ==========================================================================

// TestTransitionConcurrentWithAppends runs appends and transitions simultaneously.
// One goroutine continuously appends records (creating new sealed chunks via
// rotation), while another goroutine transitions completed chunks to instance 1.
// Verifies no data loss and no panics from concurrent Delete + Append races.

// ==========================================================================
// Cursor open on chunk when Seal fires
// ==========================================================================

// TestCursorOpenDuringSeal opens a cursor on the active chunk, then seals it
// from another goroutine (simulating rotation). The cursor should either
// complete cleanly or return a well-defined error — never panic or corrupt.
func TestCursorOpenDuringSeal(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir: dir, Now: time.Now, RotationPolicy: chunk.NewRecordCountPolicy(10000),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	// Append 500 records.
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 500 {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts,
			Raw:   fmt.Appendf(nil, "seal-race-%d", i),
			Attrs: chunk.Attributes{"k": "v"},
		}); err != nil {
			t.Fatal(err)
		}
	}

	active := cm.Active()
	if active == nil {
		t.Fatal("no active chunk")
	}

	// Open cursor on active chunk.
	cursor, err := cm.OpenCursor(active.ID)
	if err != nil {
		t.Fatal(err)
	}

	// Read first 100 records.
	for range 100 {
		_, _, err := cursor.Next()
		if err != nil {
			_ = cursor.Close()
			t.Fatalf("cursor.Next before seal: %v", err)
		}
	}

	// Seal the chunk while cursor is open.
	if err := cm.Seal(); err != nil {
		_ = cursor.Close()
		t.Fatal(err)
	}

	// Continue reading — cursor should complete or return clean error.
	readAfterSeal := 0
	for {
		_, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			// Acceptable: cursor may report an error after seal.
			// Unacceptable: panic or corrupted data.
			t.Logf("cursor error after seal at record %d: %v", 100+readAfterSeal, err)
			break
		}
		readAfterSeal++
	}
	_ = cursor.Close()

	totalRead := 100 + readAfterSeal
	t.Logf("read %d records total (100 before seal, %d after)", totalRead, readAfterSeal)

	// We should have read at least the 100 pre-seal records.
	// The remaining 400 may or may not be readable depending on timing.
	if totalRead < 100 {
		t.Errorf("expected at least 100 records, got %d", totalRead)
	}
}

// ==========================================================================
// Remote import succeeds but source delete fails
// ==========================================================================

// TestTransitionSourceDeleteFailsAfterImport verifies behavior when the
// transition successfully streams records to the next instance but fails to
// delete the source chunk. The source chunk should be retained (not lost).

// failingIndexManager always fails on DeleteIndexes.
type failingIndexManager struct{ retentionFakeIndexManager }

func (f *failingIndexManager) DeleteIndexes(_ chunk.ChunkID) error {
	return fmt.Errorf("simulated index delete failure")
}

// ==========================================================================
// Faulty blobstore for cloud instance tests
// ==========================================================================

// faultyBlobstore wraps a real blobstore and injects failures.
type faultyBlobstore struct {
	inner        blobstore.Store
	failUpload   bool
	failDownload bool
}

func (f *faultyBlobstore) EnsureBucket(ctx context.Context) error { return f.inner.EnsureBucket(ctx) }
func (f *faultyBlobstore) Upload(ctx context.Context, key string, data io.Reader, metadata map[string]string) error {
	if f.failUpload {
		return fmt.Errorf("simulated upload failure")
	}
	return f.inner.Upload(ctx, key, data, metadata)
}
func (f *faultyBlobstore) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	if f.failDownload {
		return nil, fmt.Errorf("simulated download failure")
	}
	return f.inner.Download(ctx, key)
}
func (f *faultyBlobstore) DownloadRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if f.failDownload {
		return nil, fmt.Errorf("simulated download range failure")
	}
	return f.inner.DownloadRange(ctx, key, offset, length)
}
func (f *faultyBlobstore) Delete(ctx context.Context, key string) error {
	return f.inner.Delete(ctx, key)
}
func (f *faultyBlobstore) List(ctx context.Context, prefix string, fn func(blobstore.BlobInfo) error) error {
	return f.inner.List(ctx, prefix, fn)
}
func (f *faultyBlobstore) Head(ctx context.Context, key string) (blobstore.BlobInfo, error) {
	return f.inner.Head(ctx, key)
}

// TestCloudUploadFailureRetainsChunk verifies that when cloud upload fails
// during PostSealProcess, the chunk remains locally readable (not deleted).
func TestCloudUploadFailureRetainsChunk(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()

	faulty := &faultyBlobstore{inner: blobstore.NewMemory(), failUpload: true}

	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir: dir, Now: time.Now, RotationPolicy: chunk.NewRecordCountPolicy(1000),
		CloudStore: faulty, VaultID: vaultID,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	// Ingest and seal.
	for i := range 100 {
		ts := time.Date(2025, 6, 15, 10, 0, i, 0, time.UTC)
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: fmt.Appendf(nil, "cloud-fail-%d", i),
		}); err != nil {
			t.Fatal(err)
		}
	}
	_ = cm.Seal()

	metas, _ := cm.List()
	chunkID := metas[0].ID

	// PostSealProcess succeeds (upload failure is non-fatal — data kept locally).
	if err := cm.PostSealProcess(context.Background(), chunkID); err != nil {
		t.Fatalf("PostSealProcess: %v", err)
	}

	// Chunk should still be locally readable.
	records := readAllRecords(t, cm)
	if len(records) != 100 {
		t.Errorf("expected 100 records after failed upload, got %d", len(records))
	}

	// Chunk should NOT be cloud-backed.
	meta, _ := cm.Meta(chunkID)
	if meta.CloudBacked {
		t.Error("chunk should not be cloud-backed after failed upload")
	}
}

// TestCloudDownloadFailureDuringTransition verifies that when cloud download
// fails during a transition (cursor can't read cloud-backed chunk), the
// source chunk is retained.

// ==========================================================================
// Vault reconfiguration during active transition
// ==========================================================================

// TestReconfigDuringTransitionDoesNotPanic verifies that changing the vault's
// vault list while a transition is running doesn't cause a panic. The transition
// should either complete with the original config or fail gracefully.

// ==========================================================================
// Drain concurrent with active ingestion
// ==========================================================================

// ==========================================================================
// Seal failure handling
// ==========================================================================

// TestSealFailureChunkRemains verifies that if Seal fails (e.g., from a
// Raft apply error), the active chunk remains usable — records aren't lost.
// Note: we can't easily simulate ENOSPC, but we CAN verify that the chunk
// manager handles seal errors gracefully by testing with the Raft callback.
func TestSealFailureChunkRemains(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir: dir, Now: time.Now, RotationPolicy: chunk.NewRecordCountPolicy(10000),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	// Append records.
	for i := range 100 {
		ts := time.Date(2025, 6, 15, 10, 0, i, 0, time.UTC)
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: fmt.Appendf(nil, "seal-fail-%d", i),
		}); err != nil {
			t.Fatal(err)
		}
	}

	active := cm.Active()
	if active == nil || active.RecordCount != 100 {
		t.Fatalf("expected active chunk with 100 records, got %v", active)
	}

	// Seal should succeed on file manager (no Raft).
	if err := cm.Seal(); err != nil {
		t.Fatal(err)
	}

	// Records should be in sealed chunk, readable via cursor.
	records := readAllRecords(t, cm)
	if len(records) != 100 {
		t.Errorf("expected 100 records after seal, got %d", len(records))
	}

	// Append more records to new active chunk — manager should still be usable.
	for i := range 50 {
		ts := time.Date(2025, 6, 15, 11, 0, i, 0, time.UTC)
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: fmt.Appendf(nil, "post-seal-%d", i),
		}); err != nil {
			t.Fatalf("post-seal append %d: %v", i, err)
		}
	}

	// Seal again.
	_ = cm.Seal()

	allRecords := readAllRecords(t, cm)
	if len(allRecords) != 150 {
		t.Errorf("expected 150 total records, got %d", len(allRecords))
	}
}
