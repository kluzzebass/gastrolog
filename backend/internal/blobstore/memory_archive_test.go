package blobstore

import (
	"context"
	"errors"
	"io"
	"testing"
)

func TestMemoryArchiveBlocksDownload(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	m := NewMemory()

	_ = m.Upload(ctx, "chunk-1", reader("data"), nil)

	// Readable before archive.
	rc, err := m.Download(ctx, "chunk-1")
	if err != nil {
		t.Fatalf("pre-archive Download: %v", err)
	}
	_ = rc.Close()

	// Archive it.
	if err := m.Archive(ctx, "chunk-1", "GLACIER"); err != nil {
		t.Fatal(err)
	}

	// Download should fail with ErrBlobArchived.
	_, err = m.Download(ctx, "chunk-1")
	if !errors.Is(err, ErrBlobArchived) {
		t.Fatalf("expected ErrBlobArchived, got %v", err)
	}

	// DownloadRange should also fail.
	_, err = m.DownloadRange(ctx, "chunk-1", 0, 4)
	if !errors.Is(err, ErrBlobArchived) {
		t.Fatalf("expected ErrBlobArchived for range, got %v", err)
	}
}

func TestMemoryArchiveHeadStillWorks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	m := NewMemory()

	_ = m.Upload(ctx, "chunk-1", reader("data"), map[string]string{"k": "v"})
	_ = m.Archive(ctx, "chunk-1", "DEEP_ARCHIVE")

	info, err := m.Head(ctx, "chunk-1")
	if err != nil {
		t.Fatalf("Head after archive: %v", err)
	}
	if info.StorageClass != "DEEP_ARCHIVE" {
		t.Errorf("StorageClass=%q, want DEEP_ARCHIVE", info.StorageClass)
	}
	if !info.IsArchived() {
		t.Error("IsArchived should be true")
	}
	if info.Metadata["k"] != "v" {
		t.Error("metadata should be preserved after archive")
	}
}

func TestMemoryArchiveListShowsStorageClass(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	m := NewMemory()

	_ = m.Upload(ctx, "a", reader("1"), nil)
	_ = m.Upload(ctx, "b", reader("2"), nil)
	_ = m.Archive(ctx, "a", "GLACIER")

	var infos []BlobInfo
	_ = m.List(ctx, "", func(info BlobInfo) error {
		infos = append(infos, info)
		return nil
	})

	if len(infos) != 2 {
		t.Fatalf("expected 2 blobs, got %d", len(infos))
	}
	for _, info := range infos {
		if info.Key == "a" && info.StorageClass != "GLACIER" {
			t.Errorf("blob a: StorageClass=%q, want GLACIER", info.StorageClass)
		}
		if info.Key == "b" && info.StorageClass != "" {
			t.Errorf("blob b: StorageClass=%q, want empty", info.StorageClass)
		}
	}
}

func TestMemoryRestoreAllowsDownload(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	m := NewMemory()

	_ = m.Upload(ctx, "chunk-1", reader("glacier-data"), nil)
	_ = m.Archive(ctx, "chunk-1", "GLACIER")

	// Restore it.
	if err := m.Restore(ctx, "chunk-1"); err != nil {
		t.Fatal(err)
	}

	// Should be restoring.
	restoring, err := m.IsRestoring(ctx, "chunk-1")
	if err != nil {
		t.Fatal(err)
	}
	if !restoring {
		t.Error("expected IsRestoring=true after Restore")
	}

	// Download should work now.
	rc, err := m.Download(ctx, "chunk-1")
	if err != nil {
		t.Fatalf("Download after restore: %v", err)
	}
	data, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(data) != "glacier-data" {
		t.Errorf("data=%q, want 'glacier-data'", data)
	}
}

func TestMemoryArchiveRestoreRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	m := NewMemory()

	_ = m.Upload(ctx, "k", reader("payload"), map[string]string{"a": "b"})

	// Standard → GLACIER → DEEP_ARCHIVE → restore → readable.
	_ = m.Archive(ctx, "k", "GLACIER")
	_, err := m.Download(ctx, "k")
	if !errors.Is(err, ErrBlobArchived) {
		t.Fatal("expected archived")
	}

	_ = m.Archive(ctx, "k", "DEEP_ARCHIVE")
	info, _ := m.Head(ctx, "k")
	if info.StorageClass != "DEEP_ARCHIVE" {
		t.Errorf("StorageClass=%q after re-archive", info.StorageClass)
	}

	_ = m.Restore(ctx, "k")
	rc, err := m.Download(ctx, "k")
	if err != nil {
		t.Fatalf("Download after restore: %v", err)
	}
	data, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(data) != "payload" {
		t.Errorf("data=%q", data)
	}
	if info.Metadata["a"] != "b" {
		t.Error("metadata lost")
	}
}

func TestMemoryDeleteArchivedBlob(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	m := NewMemory()

	_ = m.Upload(ctx, "k", reader("x"), nil)
	_ = m.Archive(ctx, "k", "GLACIER")

	// Delete should work even when archived.
	if err := m.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	_, err := m.Head(ctx, "k")
	if err == nil {
		t.Error("Head should fail after delete")
	}
}

func TestMemoryArchiveNotFound(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	m := NewMemory()

	if err := m.Archive(ctx, "nope", "GLACIER"); err == nil {
		t.Error("expected error for missing key")
	}
	if err := m.Restore(ctx, "nope"); err == nil {
		t.Error("expected error for missing key")
	}
}

func reader(s string) io.Reader {
	return io.NopCloser(readerOnly(s))
}

type readerOnly string

func (r readerOnly) Read(p []byte) (int, error) {
	n := copy(p, string(r))
	if n < len(string(r)) {
		return n, nil
	}
	return n, io.EOF
}
