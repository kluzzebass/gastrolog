package time

import (
	"os"
	"path/filepath"
	"testing"
	gotime "time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
	"gastrolog/internal/index"
)

func writeIndex(t *testing.T, dir string, chunkID chunk.ChunkID, entries []index.TimeIndexEntry) {
	t.Helper()
	chunkDir := filepath.Join(dir, chunkID.String())
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	data := encodeIndex(entries)
	if err := os.WriteFile(filepath.Join(chunkDir, indexFileName), data, 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
}

func TestOpenAndFindStartRoundTrip(t *testing.T) {
	dir := t.TempDir()
	id := chunk.NewChunkID()
	entries := []index.TimeIndexEntry{
		{Timestamp: gotime.UnixMicro(1000), RecordPos: 0},
		{Timestamp: gotime.UnixMicro(2000), RecordPos: 64},
		{Timestamp: gotime.UnixMicro(3000), RecordPos: 128},
	}
	writeIndex(t, dir, id, entries)

	reader, err := Open(dir, id)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Before all entries.
	ref, ok := reader.FindStart(gotime.UnixMicro(500))
	if ok {
		t.Fatalf("expected ok=false, got ref %+v", ref)
	}

	// Between entries.
	ref, ok = reader.FindStart(gotime.UnixMicro(2500))
	if !ok {
		t.Fatal("expected ok=true")
	}
	if ref.ChunkID != id {
		t.Fatalf("expected chunkID %s, got %s", id, ref.ChunkID)
	}
	if ref.Pos != 64 {
		t.Fatalf("expected pos 64, got %d", ref.Pos)
	}
}

func TestOpenNotFound(t *testing.T) {
	dir := t.TempDir()
	bogusID := chunk.NewChunkID()

	_, err := Open(dir, bogusID)
	if err == nil {
		t.Fatal("expected error opening nonexistent index, got nil")
	}
}

func TestOpenValidatesHeader(t *testing.T) {
	dir := t.TempDir()
	id := chunk.NewChunkID()
	chunkDir := filepath.Join(dir, id.String())
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// Write a file with a bad signature byte.
	bad := make([]byte, headerSize)
	bad[0] = 0xFF
	bad[1] = format.TypeTimeIndex
	bad[2] = currentVersion
	if err := os.WriteFile(filepath.Join(chunkDir, indexFileName), bad, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	_, err := Open(dir, id)
	if err == nil {
		t.Fatal("expected error from bad header, got nil")
	}
}
