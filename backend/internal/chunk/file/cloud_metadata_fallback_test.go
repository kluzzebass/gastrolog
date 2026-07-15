package file

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	filetoken "gastrolog/internal/index/file/token"
)

// openCloudManager builds a cloud-backed manager over a caller-supplied dir and
// store so a test can restart it (unlike newCloudManagerWithIndexes, which owns
// its own dir/store). Mirrors the production wiring (token indexer on the same
// dir).
func openCloudManager(t *testing.T, dir string, vaultID glid.GLID, store blobstore.Store) *Manager {
	t.Helper()
	cm, err := NewManager(Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
		CloudStore:     store,
		VaultID:        vaultID,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	tokenIndexer := filetoken.NewIndexer(dir, cm, nil)
	im := indexfile.NewManager(dir, []index.Indexer{tokenIndexer}, nil, cm)
	cm.SetIndexBuilders([]chunk.ChunkIndexBuilder{im.BuildAdapter()})
	return cm
}

func readBlob(t *testing.T, store blobstore.Store, key string) []byte {
	t.Helper()
	rc, err := store.Download(context.Background(), key)
	if err != nil {
		t.Fatalf("download blob %s: %v", key, err)
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read blob %s: %v", key, err)
	}
	return data
}

// forceCloudRescan deletes the on-disk cloud index so the next manager open
// repopulates it from the store — the only way to make an already-indexed
// chunk flow back through DecodeObjectMetadata.
func forceCloudRescan(t *testing.T, dir string) {
	t.Helper()
	if err := os.Remove(filepath.Join(dir, cloudIndexFile)); err != nil {
		t.Fatalf("remove cloud index: %v", err)
	}
}

func metaForID(t *testing.T, cm *Manager, id chunk.ChunkID) (chunk.ChunkMeta, bool) {
	t.Helper()
	metas, err := cm.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, m := range metas {
		if m.ID == id {
			return m, true
		}
	}
	return chunk.ChunkMeta{}, false
}

// TestCloudMetadataFallback_RecoversFromFooter is the caller-contract unhappy
// path: when a cloud blob's object metadata is mangled (record_count = "abc"),
// the cloud-index scan must NOT index a zero-record ChunkMeta. It falls back to
// the authoritative GLCB footer and recovers the true record count.
func TestCloudMetadataFallback_RecoversFromFooter(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	vaultID := glid.New()
	store := blobstore.NewMemory()

	const nRecords = 50
	cm := openCloudManager(t, dir, vaultID, store)
	chunkID := appendSealAndUpload(t, cm, nRecords)
	key := cm.blobKey(chunkID)

	// Corrupt the object metadata in place: keep the (valid) blob bytes, but
	// replace record_count with garbage. The footer inside the bytes stays
	// authoritative.
	data := readBlob(t, store, key)
	head, err := store.Head(context.Background(), key)
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	corrupt := make(map[string]string, len(head.Metadata))
	for k, v := range head.Metadata {
		corrupt[k] = v
	}
	corrupt["record_count"] = "abc"
	if err := store.Upload(context.Background(), key, byteReader(data), corrupt); err != nil {
		t.Fatalf("re-upload corrupt metadata: %v", err)
	}

	_ = cm.Close()
	forceCloudRescan(t, dir)

	cm2 := openCloudManager(t, dir, vaultID, store)
	defer func() { _ = cm2.Close() }()

	meta, ok := metaForID(t, cm2, chunkID)
	if !ok {
		t.Fatal("chunk missing from index after footer-fallback recovery")
	}
	if meta.RecordCount != nRecords {
		t.Errorf("RecordCount = %d, want %d (should be recovered from footer, not fabricated zero)", meta.RecordCount, nRecords)
	}
	if !meta.CloudBacked {
		t.Error("expected CloudBacked=true")
	}
}

// TestCloudMetadataFallback_SkipsWhenFooterUnreadable proves that when BOTH the
// object metadata and the blob bytes are corrupt (footer fallback also fails),
// the scan skips the chunk entirely rather than indexing a fabricated
// zero-record ChunkMeta. Nothing is lost — the bytes remain in the store — but
// no phantom authoritative zero reaches retention/query.
func TestCloudMetadataFallback_SkipsWhenFooterUnreadable(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	vaultID := glid.New()
	store := blobstore.NewMemory()

	cm := openCloudManager(t, dir, vaultID, store)
	chunkID := appendSealAndUpload(t, cm, 25)
	key := cm.blobKey(chunkID)

	head, err := store.Head(context.Background(), key)
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	corrupt := make(map[string]string, len(head.Metadata))
	for k, v := range head.Metadata {
		corrupt[k] = v
	}
	corrupt["record_count"] = "abc"
	// Replace the blob bytes with garbage too, so the footer read fails.
	if err := store.Upload(context.Background(), key, byteReader([]byte("not a glcb blob")), corrupt); err != nil {
		t.Fatalf("re-upload corrupt blob: %v", err)
	}

	_ = cm.Close()
	forceCloudRescan(t, dir)

	cm2 := openCloudManager(t, dir, vaultID, store)
	defer func() { _ = cm2.Close() }()

	if meta, ok := metaForID(t, cm2, chunkID); ok {
		t.Errorf("chunk should have been skipped, but was indexed as %+v", meta)
	}
}

// byteReader is a tiny io.Reader over a byte slice (avoids importing bytes just
// for one call site).
func byteReader(b []byte) io.Reader { return &sliceReader{b: b} }

type sliceReader struct {
	b   []byte
	off int
}

func (r *sliceReader) Read(p []byte) (int, error) {
	if r.off >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.off:])
	r.off += n
	return n, nil
}
