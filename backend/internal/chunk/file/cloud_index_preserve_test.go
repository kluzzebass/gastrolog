package file

import (
	"context"
	"gastrolog/internal/glid"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	filetoken "gastrolog/internal/index/file/token"
)

// newCloudManagerWithIndexes creates a file-based chunk manager with a cloud
// store AND a file-based token indexer wired in. The index manager writes to
// the same directory as the chunk manager (production layout). Returns the
// manager, its data directory, and the cloud store.
func newCloudManagerWithIndexes(t *testing.T) (*Manager, string, *blobstore.Memory) {
	t.Helper()
	dir := t.TempDir()
	cacheDir := t.TempDir()
	vaultID := glid.New()
	store := blobstore.NewMemory()

	cm, err := NewManager(Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
		CloudStore:     store,
		VaultID:        vaultID,
		CacheDir:       cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Wire file-based token indexer using the same dir as the chunk manager.
	tokenIndexer := filetoken.NewIndexer(dir, cm, nil)
	im := indexfile.NewManager(dir, []index.Indexer{tokenIndexer}, nil)
	cm.SetIndexBuilders([]chunk.ChunkIndexBuilder{im.BuildAdapter()})

	t.Cleanup(func() { _ = cm.Close() })
	return cm, dir, store
}

func appendSealAndUpload(t *testing.T, cm *Manager, n int) chunk.ChunkID {
	t.Helper()
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range n {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: []byte("cloud-idx-test"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := cm.Seal(); err != nil {
		t.Fatal(err)
	}
	metas, _ := cm.List()
	for _, m := range metas {
		if m.Sealed {
			if err := cm.PostSealProcess(context.Background(), m.ID); err != nil {
				t.Fatalf("PostSealProcess: %v", err)
			}
			return m.ID
		}
	}
	t.Fatal("no sealed chunk")
	return chunk.ChunkID{}
}

// TestUploadToCloudPreservesIndexFiles verifies that after a successful cloud
// upload, data files (raw.log, idx.log, attr.log, etc.) are removed but any
// index files in the chunk directory are preserved.
func TestUploadToCloudPreservesIndexFiles(t *testing.T) {
	t.Parallel()
	cm, dir, _ := newCloudManagerWithIndexes(t)

	chunkID := appendSealAndUpload(t, cm, 50)
	chunkDir := filepath.Join(dir, chunkID.String())

	// Chunk directory must still exist.
	if _, err := os.Stat(chunkDir); os.IsNotExist(err) {
		t.Fatal("chunk directory should not be deleted after cloud upload")
	}

	// Data files must be gone.
	for _, name := range dataFileNames {
		path := filepath.Join(chunkDir, name)
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("data file %s should be deleted after cloud upload", name)
		}
	}

	// Directory should have at least one file (index files).
	if !chunkDirHasFiles(chunkDir) {
		t.Error("chunk directory should still contain index files after cloud upload")
	}
}

// TestRemoveLocalDataFiles verifies that removeLocalDataFiles deletes only
// data files and preserves everything else.
func TestRemoveLocalDataFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cm, err := NewManager(Config{
		Dir: dir,
		Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cm.Close() }()

	id := chunk.NewChunkID()
	chunkDir := filepath.Join(dir, id.String())
	if err := os.MkdirAll(chunkDir, 0o750); err != nil {
		t.Fatal(err)
	}

	// Create data files.
	for _, name := range dataFileNames {
		if err := os.WriteFile(filepath.Join(chunkDir, name), []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Create fake index files.
	indexFiles := []string{"token.idx", "json.idx", "attr_keys.idx"}
	for _, name := range indexFiles {
		if err := os.WriteFile(filepath.Join(chunkDir, name), []byte("index"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	if err := cm.removeLocalDataFiles(id); err != nil {
		t.Fatalf("removeLocalDataFiles: %v", err)
	}

	// Data files must be gone.
	for _, name := range dataFileNames {
		path := filepath.Join(chunkDir, name)
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("data file %s should be removed", name)
		}
	}

	// Index files must remain.
	for _, name := range indexFiles {
		path := filepath.Join(chunkDir, name)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("index file %s should be preserved: %v", name, err)
		}
	}
}

// TestRemoveLocalDataFilesMissingFilesOK verifies that removeLocalDataFiles
// does not fail when some data files are already missing.
func TestRemoveLocalDataFilesMissingFilesOK(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cm, err := NewManager(Config{
		Dir: dir,
		Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cm.Close() }()

	id := chunk.NewChunkID()
	chunkDir := filepath.Join(dir, id.String())
	if err := os.MkdirAll(chunkDir, 0o750); err != nil {
		t.Fatal(err)
	}

	// Only create one data file — the rest are missing.
	if err := os.WriteFile(filepath.Join(chunkDir, rawLogFileName), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Should succeed despite missing files.
	if err := cm.removeLocalDataFiles(id); err != nil {
		t.Fatalf("removeLocalDataFiles should tolerate missing files: %v", err)
	}

	if _, err := os.Stat(filepath.Join(chunkDir, rawLogFileName)); !os.IsNotExist(err) {
		t.Error("raw.log should be deleted")
	}
}

// TestLoadExistingPreservesCloudIndexDirs verifies that on restart,
// loadExisting does NOT delete chunk directories that contain index files
// (belonging to cloud-backed chunks).
func TestLoadExistingPreservesCloudIndexDirs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	// Simulate a cloud-backed chunk directory with only index files.
	id := chunk.NewChunkID()
	chunkDir := filepath.Join(dir, id.String())
	if err := os.MkdirAll(chunkDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(chunkDir, "token.idx"), []byte("index-data"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Create a manager — loadExisting runs during construction.
	cm, err := NewManager(Config{
		Dir: dir,
		Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cm.Close() }()

	// The cloud index directory must survive loadExisting.
	if _, err := os.Stat(chunkDir); os.IsNotExist(err) {
		t.Fatal("loadExisting should not delete chunk directories that contain index files")
	}
	if _, err := os.Stat(filepath.Join(chunkDir, "token.idx")); os.IsNotExist(err) {
		t.Fatal("token.idx should survive loadExisting")
	}
}

// TestLoadExistingRemovesEmptyLeftoverDirs verifies that truly empty
// chunk directories (no data files, no index files) are cleaned up
// during loadExisting.
func TestLoadExistingRemovesEmptyLeftoverDirs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	// Simulate an empty leftover chunk directory.
	id := chunk.NewChunkID()
	chunkDir := filepath.Join(dir, id.String())
	if err := os.MkdirAll(chunkDir, 0o750); err != nil {
		t.Fatal(err)
	}

	cm, err := NewManager(Config{
		Dir: dir,
		Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cm.Close() }()

	// The empty directory should be cleaned up.
	if _, err := os.Stat(chunkDir); !os.IsNotExist(err) {
		t.Fatal("loadExisting should remove empty leftover chunk directories")
	}
}

// TestRestartPreservesCloudIndexes is an integration test: append records,
// seal, upload to cloud, close the manager, reopen it, and verify the
// cloud chunk's index directory survived the restart.
func TestRestartPreservesCloudIndexes(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cacheDir := t.TempDir()
	vaultID := glid.New()
	store := blobstore.NewMemory()

	// Phase 1: create, ingest, seal, upload.
	cm, err := NewManager(Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
		CloudStore:     store,
		VaultID:        vaultID,
		CacheDir:       cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	tokenIndexer := filetoken.NewIndexer(dir, cm, nil)
	im := indexfile.NewManager(dir, []index.Indexer{tokenIndexer}, nil)
	cm.SetIndexBuilders([]chunk.ChunkIndexBuilder{im.BuildAdapter()})

	chunkID := appendSealAndUpload(t, cm, 50)
	chunkDir := filepath.Join(dir, chunkID.String())

	// Verify index files exist before close.
	if !chunkDirHasFiles(chunkDir) {
		t.Fatal("index files should exist after upload")
	}

	_ = cm.Close()

	// Phase 2: reopen the manager — loadExisting must not delete index dir.
	cm2, err := NewManager(Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
		CloudStore:     store,
		VaultID:        vaultID,
		CacheDir:       cacheDir,
	})
	if err != nil {
		t.Fatalf("reopen manager: %v", err)
	}
	defer func() { _ = cm2.Close() }()

	// Cloud index directory and its files must survive the restart.
	if _, err := os.Stat(chunkDir); os.IsNotExist(err) {
		t.Fatal("cloud chunk index directory should survive manager restart")
	}
	if !chunkDirHasFiles(chunkDir) {
		t.Fatal("index files should survive manager restart")
	}
}

// TestRegisterCloudChunk verifies that RegisterCloudChunk creates a cloud
// index entry from metadata alone, making the chunk visible in List().
func TestRegisterCloudChunk(t *testing.T) {
	t.Parallel()
	cm, _, _ := newCloudManagerWithIndexes(t)

	id := chunk.NewChunkID()
	info := chunk.CloudChunkInfo{
		WriteStart:  time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC),
		WriteEnd:    time.Date(2025, 6, 1, 0, 1, 0, 0, time.UTC),
		RecordCount: 100,
		Bytes:       50000,
		DiskBytes:   30000,
		NumFrames:   2,
	}

	if err := cm.RegisterCloudChunk(id, info); err != nil {
		t.Fatalf("RegisterCloudChunk: %v", err)
	}

	// Chunk should appear in List().
	metas, err := cm.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var found bool
	for _, m := range metas {
		if m.ID == id {
			found = true
			if !m.CloudBacked {
				t.Error("expected CloudBacked=true")
			}
			if m.RecordCount != 100 {
				t.Errorf("RecordCount = %d, want 100", m.RecordCount)
			}
			break
		}
	}
	if !found {
		t.Error("registered cloud chunk not found in List()")
	}
}

// TestRegisterCloudChunkIdempotent verifies that calling RegisterCloudChunk
// twice for the same chunk ID is a no-op.
func TestRegisterCloudChunkIdempotent(t *testing.T) {
	t.Parallel()
	cm, _, _ := newCloudManagerWithIndexes(t)

	id := chunk.NewChunkID()
	info := chunk.CloudChunkInfo{RecordCount: 50, Bytes: 1000, DiskBytes: 500}

	if err := cm.RegisterCloudChunk(id, info); err != nil {
		t.Fatalf("first RegisterCloudChunk: %v", err)
	}
	if err := cm.RegisterCloudChunk(id, info); err != nil {
		t.Fatalf("second RegisterCloudChunk should be no-op: %v", err)
	}
}

// TestRegisterCloudChunkRequiresCloudStore verifies that RegisterCloudChunk
// fails if the manager has no cloud store configured.
func TestRegisterCloudChunkRequiresCloudStore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cm, err := NewManager(Config{Dir: dir, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cm.Close() }()

	id := chunk.NewChunkID()
	info := chunk.CloudChunkInfo{RecordCount: 10}
	if err := cm.RegisterCloudChunk(id, info); err == nil {
		t.Error("expected error without cloud store, got nil")
	}
}

// TestCloudReadOnlySkipsUpload verifies that PostSealProcess with CloudReadOnly
// does NOT upload to cloud.
func TestCloudReadOnlySkipsUpload(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cacheDir := t.TempDir()
	vaultID := glid.New()
	store := blobstore.NewMemory()

	cm, err := NewManager(Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
		CloudStore:     store,
		VaultID:        vaultID,
		CacheDir:       cacheDir,
		CloudReadOnly:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cm.Close() }()

	// Append, seal, post-seal.
	ts := time.Now()
	for i := range 10 {
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ts.Add(time.Duration(i) * time.Microsecond),
			WriteTS:  ts.Add(time.Duration(i) * time.Microsecond),
			Raw:      []byte("readonly-test"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := cm.Seal(); err != nil {
		t.Fatal(err)
	}

	metas, _ := cm.List()
	var sealedID chunk.ChunkID
	for _, m := range metas {
		if m.Sealed {
			sealedID = m.ID
			break
		}
	}
	if err := cm.PostSealProcess(context.Background(), sealedID); err != nil {
		t.Fatalf("PostSealProcess: %v", err)
	}

	// Chunk should NOT be cloud-backed (upload was skipped).
	meta, err := cm.Meta(sealedID)
	if err != nil {
		t.Fatalf("Meta: %v", err)
	}
	if meta.CloudBacked {
		t.Error("CloudReadOnly manager should NOT upload — chunk should not be cloud-backed")
	}

	// Verify nothing was uploaded to the store.
	var blobCount int
	_ = store.List(context.Background(), "", func(_ blobstore.BlobInfo) error {
		blobCount++
		return nil
	})
	if blobCount > 0 {
		t.Errorf("expected 0 blobs in store, got %d", blobCount)
	}
}

// TestChunkDirHasFiles verifies the helper function.
func TestChunkDirHasFiles(t *testing.T) {
	t.Parallel()

	t.Run("empty dir", func(t *testing.T) {
		dir := t.TempDir()
		if chunkDirHasFiles(dir) {
			t.Error("empty directory should return false")
		}
	})

	t.Run("dir with file", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "test.idx"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if !chunkDirHasFiles(dir) {
			t.Error("directory with a file should return true")
		}
	})

	t.Run("dir with only subdirs", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dir, "subdir"), 0o750); err != nil {
			t.Fatal(err)
		}
		if chunkDirHasFiles(dir) {
			t.Error("directory with only subdirectories should return false")
		}
	})

	t.Run("nonexistent dir", func(t *testing.T) {
		if chunkDirHasFiles("/nonexistent/path") {
			t.Error("nonexistent directory should return false")
		}
	})
}
