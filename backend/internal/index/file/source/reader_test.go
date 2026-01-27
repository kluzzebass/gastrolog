package source

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/kluzzebass/gastrolog/internal/chunk"
	"github.com/kluzzebass/gastrolog/internal/index"
)

func writeIndex(t *testing.T, dir string, chunkID chunk.ChunkID, entries []index.SourceIndexEntry) {
	t.Helper()
	chunkDir := filepath.Join(dir, chunkID.String())
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	data := encodeIndex(chunkID, entries)
	if err := os.WriteFile(filepath.Join(chunkDir, indexFileName), data, 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
}

func TestOpenAndLookupRoundTrip(t *testing.T) {
	dir := t.TempDir()
	id := chunk.NewChunkID()
	src1 := chunk.NewSourceID()
	src2 := chunk.NewSourceID()
	entries := []index.SourceIndexEntry{
		{SourceID: src1, Positions: []uint64{0, 128}},
		{SourceID: src2, Positions: []uint64{64, 192, 256}},
	}
	writeIndex(t, dir, id, entries)

	reader, err := Open(dir, id)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Look up src1.
	pos1, ok := reader.Lookup(src1)
	if !ok {
		t.Fatal("expected to find src1")
	}
	if len(pos1) != 2 || pos1[0] != 0 || pos1[1] != 128 {
		t.Fatalf("src1: expected [0 128], got %v", pos1)
	}

	// Look up src2.
	pos2, ok := reader.Lookup(src2)
	if !ok {
		t.Fatal("expected to find src2")
	}
	if len(pos2) != 3 || pos2[0] != 64 || pos2[1] != 192 || pos2[2] != 256 {
		t.Fatalf("src2: expected [64 192 256], got %v", pos2)
	}

	// Look up missing source.
	_, ok = reader.Lookup(chunk.NewSourceID())
	if ok {
		t.Fatal("expected ok=false for missing source")
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
	bad[1] = typeByte
	bad[2] = versionByte
	if err := os.WriteFile(filepath.Join(chunkDir, indexFileName), bad, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	_, err := Open(dir, id)
	if err == nil {
		t.Fatal("expected error from bad header, got nil")
	}
}
