package glcb

// DownloadAndUnwrap tests (gastrolog-5do8sh): the canonical download-side
// unwrap against the in-memory blob store — happy path, both blob-store
// sentinel translations, and corrupt zstd transport payloads. No network,
// no timing.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
)

// zstdWrap compresses payload the way the upload pipeline wraps a GLCB
// for cloud transport.
func zstdWrap(t *testing.T, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatalf("zstd writer: %v", err)
	}
	if _, err := enc.Write(payload); err != nil {
		t.Fatalf("compress payload: %v", err)
	}
	if err := enc.Close(); err != nil {
		t.Fatalf("close zstd writer: %v", err)
	}
	return buf.Bytes()
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

// TestDownloadAndUnwrap_HappyPath uploads a zstd-wrapped payload,
// downloads + unwraps it, and verifies the bytes and that dst is left
// rewound to offset 0 for the caller's promote step.
func TestDownloadAndUnwrap_HappyPath(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("glcb payload bytes "), 64)
	store := blobstore.NewMemory()
	const key = "vault/chunk/data.glcb.zst"
	if err := store.Upload(context.Background(), key, bytes.NewReader(zstdWrap(t, payload)), nil); err != nil {
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
	if !bytes.Equal(got, payload) {
		t.Fatalf("unwrapped payload = %d bytes, want %d matching bytes", len(got), len(payload))
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

	store := blobstore.NewMemory()
	const key = "vault/chunk/data.glcb.zst"
	if err := store.Upload(context.Background(), key, bytes.NewReader(zstdWrap(t, []byte("payload"))), nil); err != nil {
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

// TestDownloadAndUnwrap_TruncatedZstd uploads a valid zstd stream cut in
// half: the frame header still parses, so the failure surfaces from the
// decompress copy loop.
func TestDownloadAndUnwrap_TruncatedZstd(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("truncated transport payload "), 4096)
	wrapped := zstdWrap(t, payload)
	if len(wrapped) < 32 {
		t.Fatalf("compressed stream unexpectedly small (%d bytes); enlarge the payload", len(wrapped))
	}
	store := blobstore.NewMemory()
	const key = "vault/chunk/data.glcb.zst"
	if err := store.Upload(context.Background(), key, bytes.NewReader(wrapped[:len(wrapped)/2]), nil); err != nil {
		t.Fatalf("upload: %v", err)
	}

	err := DownloadAndUnwrap(context.Background(), store, key, downloadDst(t))
	if err == nil {
		t.Fatal("DownloadAndUnwrap on truncated zstd: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "decompress cloud blob") {
		t.Fatalf("error = %q, want mention of decompress cloud blob", err)
	}
}

// TestDownloadAndUnwrap_GarbagePayload uploads bytes that are not a zstd
// stream at all; the unwrap must fail (whichever zstd stage detects it),
// never silently produce an empty GLCB.
func TestDownloadAndUnwrap_GarbagePayload(t *testing.T) {
	t.Parallel()

	store := blobstore.NewMemory()
	const key = "vault/chunk/data.glcb.zst"
	if err := store.Upload(context.Background(), key, strings.NewReader("this is not a zstd stream"), nil); err != nil {
		t.Fatalf("upload: %v", err)
	}

	if err := DownloadAndUnwrap(context.Background(), store, key, downloadDst(t)); err == nil {
		t.Fatal("DownloadAndUnwrap on garbage payload: expected error, got nil")
	}
}
