package glcb

// DownloadAndUnwrap tests: the canonical download-side unwrap against
// the in-memory blob store — the caller contract (byte-identical
// reassembly, dst rewound) and both blob-store sentinel translations.
// Malformed-object handling (truncation, corruption, foreign bytes) is
// covered format-side in transport_test.go. No network, no timing.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// wrappedBlobBytes builds a real blob with the writer and frames it for
// transport, returning both forms.
func wrappedBlobBytes(t *testing.T) (blob []byte, object []byte) {
	t.Helper()
	dir := t.TempDir()
	w, err := NewWriter(chunk.NewChunkID(), glid.New(), dir)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	base := time.Date(2026, 4, 1, 8, 0, 0, 0, time.UTC)
	for i := range 25 {
		ts := base.Add(time.Duration(i) * time.Second)
		if err := w.Add(chunk.Record{
			SourceTS: ts, IngestTS: ts, WriteTS: ts,
			Raw: []byte("remote-reader-payload"),
		}); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	f, err := os.CreateTemp(dir, "blob-*.glcb")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteTo(f); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	blob, err = os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err := WrapForTransport(&buf, f.Name()); err != nil {
		t.Fatalf("WrapForTransport: %v", err)
	}
	return blob, buf.Bytes()
}

// downloadDst returns a destination temp file for DownloadAndUnwrap, the
// same shape callers use before promoting the plain GLCB into place.
func downloadDst(t *testing.T) *os.File {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "data.glcb.*")
	if err != nil {
		t.Fatalf("create dst: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// TestDownloadAndUnwrap_HappyPath uploads a transport-framed blob,
// downloads + unwraps it, and verifies the bytes and that dst is left
// rewound to offset 0 for the caller's promote step.
func TestDownloadAndUnwrap_HappyPath(t *testing.T) {
	t.Parallel()

	blob, object := wrappedBlobBytes(t)
	store := blobstore.NewMemory()
	const key = "vault/chunk/data.glcb.zst"
	if err := store.Upload(context.Background(), key, bytes.NewReader(object), nil); err != nil {
		t.Fatalf("upload: %v", err)
	}

	dst := downloadDst(t)
	if err := DownloadAndUnwrap(context.Background(), store, key, dst); err != nil {
		t.Fatalf("DownloadAndUnwrap: %v", err)
	}

	pos, err := dst.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatalf("query dst offset: %v", err)
	}
	if pos != 0 {
		t.Fatalf("dst offset after unwrap = %d, want 0 (rewound)", pos)
	}
	got, err := io.ReadAll(dst)
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if !bytes.Equal(got, blob) {
		t.Fatalf("unwrapped blob = %d bytes, want %d matching bytes", len(got), len(blob))
	}
}

// TestDownloadAndUnwrap_NotFoundMapsToChunkSuspect pins the sentinel
// translation: a missing blob surfaces as chunk.ErrChunkSuspect so query
// callers never reach into blobstore.
func TestDownloadAndUnwrap_NotFoundMapsToChunkSuspect(t *testing.T) {
	t.Parallel()

	store := blobstore.NewMemory()
	err := DownloadAndUnwrap(context.Background(), store, "missing/data.glcb.zst", downloadDst(t))
	if !errors.Is(err, chunk.ErrChunkSuspect) {
		t.Fatalf("error = %v, want chunk.ErrChunkSuspect", err)
	}
	// The blob-store sentinel stays wrapped for diagnostics.
	if !errors.Is(err, blobstore.ErrBlobNotFound) {
		t.Fatalf("error = %v, want wrapped blobstore.ErrBlobNotFound", err)
	}
}

// TestDownloadAndUnwrap_ArchivedMapsToChunkArchived pins the other
// sentinel translation: an archived blob (Glacier-style storage class,
// simulated by the memory store's Archiver) surfaces as
// chunk.ErrChunkArchived.
func TestDownloadAndUnwrap_ArchivedMapsToChunkArchived(t *testing.T) {
	t.Parallel()

	_, object := wrappedBlobBytes(t)
	store := blobstore.NewMemory()
	const key = "vault/chunk/data.glcb.zst"
	if err := store.Upload(context.Background(), key, bytes.NewReader(object), nil); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := store.Archive(context.Background(), key, "GLACIER"); err != nil {
		t.Fatalf("archive: %v", err)
	}

	err := DownloadAndUnwrap(context.Background(), store, key, downloadDst(t))
	if !errors.Is(err, chunk.ErrChunkArchived) {
		t.Fatalf("error = %v, want chunk.ErrChunkArchived", err)
	}
	if !errors.Is(err, blobstore.ErrBlobArchived) {
		t.Fatalf("error = %v, want wrapped blobstore.ErrBlobArchived", err)
	}
}
