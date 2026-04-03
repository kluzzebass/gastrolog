package blobstore

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
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
	// Instant restore (default) — download works immediately after Restore.
	m := NewMemory()

	_ = m.Upload(ctx, "chunk-1", reader("glacier-data"), nil)
	_ = m.Archive(ctx, "chunk-1", "GLACIER")

	if err := m.Restore(ctx, "chunk-1", "Standard", 7); err != nil {
		t.Fatal(err)
	}

	// With zero delay, IsRestoring is false (restore is instant).
	restoring, err := m.IsRestoring(ctx, "chunk-1")
	if err != nil {
		t.Fatal(err)
	}
	if restoring {
		t.Error("expected IsRestoring=false with zero RestoreDelay")
	}

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

func TestMemoryRestoreWithDelay(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	clock := &now

	m := NewMemoryWithConfig(MemoryConfig{
		Now:          func() time.Time { return *clock },
		RestoreDelay: 5 * time.Second,
	})

	_ = m.Upload(ctx, "k", reader("data"), nil)
	_ = m.Archive(ctx, "k", "GLACIER")

	_ = m.Restore(ctx, "k", "Expedited", 7)

	// During delay: still archived, IsRestoring=true.
	restoring, _ := m.IsRestoring(ctx, "k")
	if !restoring {
		t.Error("expected IsRestoring=true during delay")
	}
	_, err := m.Download(ctx, "k")
	if !errors.Is(err, ErrBlobArchived) {
		t.Fatalf("expected ErrBlobArchived during delay, got %v", err)
	}

	// Advance past delay.
	advanced := now.Add(6 * time.Second)
	clock = &advanced

	restoring, _ = m.IsRestoring(ctx, "k")
	if restoring {
		t.Error("expected IsRestoring=false after delay")
	}
	rc, err := m.Download(ctx, "k")
	if err != nil {
		t.Fatalf("Download after delay: %v", err)
	}
	_ = rc.Close()
}

func TestMemoryRestoreExpiry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	clock := &now

	m := NewMemoryWithConfig(MemoryConfig{
		Now:           func() time.Time { return *clock },
		RestoreDelay:  0,
		RestoreExpiry: 10 * time.Second,
	})

	_ = m.Upload(ctx, "k", reader("data"), nil)
	_ = m.Archive(ctx, "k", "DEEP_ARCHIVE")
	_ = m.Restore(ctx, "k", "Standard", 7)

	// Readable immediately (no delay).
	rc, err := m.Download(ctx, "k")
	if err != nil {
		t.Fatalf("Download after restore: %v", err)
	}
	_ = rc.Close()

	// Advance past expiry — should re-archive.
	advanced := now.Add(11 * time.Second)
	clock = &advanced

	_, err = m.Download(ctx, "k")
	if !errors.Is(err, ErrBlobArchived) {
		t.Fatalf("expected ErrBlobArchived after expiry, got %v", err)
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

	_ = m.Restore(ctx, "k", "Standard", 7)
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
	if err := m.Restore(ctx, "nope", "Standard", 7); err == nil {
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
