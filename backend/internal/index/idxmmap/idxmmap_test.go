package idxmmap_test

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/index/idxmmap"
)

// TestLoad_Decodes verifies the happy path: write some bytes to disk,
// load via mmap, decode them, and confirm the decoded values match.
func TestLoad_Decodes(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "test.bin")

	// Write 8 bytes: a uint64 0xCAFEBABE_DEADBEEF in little-endian
	want := uint64(0xCAFEBABEDEADBEEF)
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, want)
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	got, err := idxmmap.Load(path, func(data []byte) (uint64, error) {
		if len(data) < 8 {
			return 0, errors.New("data too short")
		}
		return binary.LittleEndian.Uint64(data[:8]), nil
	})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != want {
		t.Errorf("Load returned %x, want %x", got, want)
	}
}

// TestLoad_EmptyFile verifies that an empty file returns ErrEmpty rather
// than calling the decoder with a zero-length slice (which would be a
// valid mmap on macOS with size=0).
func TestLoad_EmptyFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "empty.bin")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("write empty file: %v", err)
	}

	called := false
	_, err := idxmmap.Load(path, func(_ []byte) (struct{}, error) {
		called = true
		return struct{}{}, nil
	})
	if !errors.Is(err, idxmmap.ErrEmpty) {
		t.Errorf("expected ErrEmpty, got %v", err)
	}
	if called {
		t.Error("decoder should not be called for empty file")
	}
}

// TestLoad_MissingFile verifies that a missing file returns an os-level
// error wrapping the underlying ENOENT, NOT a panic or generic error.
func TestLoad_MissingFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "does-not-exist.bin")

	called := false
	_, err := idxmmap.Load(path, func(_ []byte) (struct{}, error) {
		called = true
		return struct{}{}, nil
	})
	if err == nil {
		t.Fatal("expected error for missing file")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("expected os.ErrNotExist, got %v", err)
	}
	if called {
		t.Error("decoder should not be called for missing file")
	}
}

// TestLoad_DecoderError verifies that decoder errors are propagated
// unwrapped and the mmap is still released cleanly.
func TestLoad_DecoderError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "test.bin")
	if err := os.WriteFile(path, []byte("hello"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	sentinel := errors.New("decoder bork")
	_, err := idxmmap.Load(path, func(_ []byte) (int, error) {
		return 0, sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error, got %v", err)
	}
}
