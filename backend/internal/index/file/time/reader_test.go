package time

import (
	"os"
	"path/filepath"
	"testing"
	gotime "time"

	"github.com/kluzzebass/gastrolog/internal/chunk"
	"github.com/kluzzebass/gastrolog/internal/index"
)

func writeIndex(t *testing.T, dir string, chunkID chunk.ChunkID, entries []index.TimeIndexEntry) {
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

func TestFindStartBeforeAllEntries(t *testing.T) {
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

	ref, ok := reader.FindStart(gotime.UnixMicro(500))
	if ok {
		t.Fatalf("expected ok=false, got ref %+v", ref)
	}
	if ref != (chunk.RecordRef{}) {
		t.Fatalf("expected zero RecordRef, got %+v", ref)
	}
}

func TestFindStartAtExactEntry(t *testing.T) {
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

	ref, ok := reader.FindStart(gotime.UnixMicro(2000))
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

func TestFindStartBetweenEntries(t *testing.T) {
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

	// Between entry 1 (ts=2000) and entry 2 (ts=3000) — should return entry 1.
	ref, ok := reader.FindStart(gotime.UnixMicro(2500))
	if !ok {
		t.Fatal("expected ok=true")
	}
	if ref.Pos != 64 {
		t.Fatalf("expected pos 64, got %d", ref.Pos)
	}
}

func TestFindStartAfterAllEntries(t *testing.T) {
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

	ref, ok := reader.FindStart(gotime.UnixMicro(9999))
	if !ok {
		t.Fatal("expected ok=true")
	}
	if ref.Pos != 128 {
		t.Fatalf("expected pos 128 (last entry), got %d", ref.Pos)
	}
}

func TestFindStartSingleEntry(t *testing.T) {
	dir := t.TempDir()
	id := chunk.NewChunkID()
	entries := []index.TimeIndexEntry{
		{Timestamp: gotime.UnixMicro(5000), RecordPos: 0},
	}
	writeIndex(t, dir, id, entries)

	reader, err := Open(dir, id)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Before the only entry.
	ref, ok := reader.FindStart(gotime.UnixMicro(4000))
	if ok {
		t.Fatalf("expected ok=false for time before single entry, got ref %+v", ref)
	}

	// At the exact entry.
	ref, ok = reader.FindStart(gotime.UnixMicro(5000))
	if !ok {
		t.Fatal("expected ok=true for exact match")
	}
	if ref.Pos != 0 {
		t.Fatalf("expected pos 0, got %d", ref.Pos)
	}

	// After the only entry.
	ref, ok = reader.FindStart(gotime.UnixMicro(6000))
	if !ok {
		t.Fatal("expected ok=true for time after single entry")
	}
	if ref.Pos != 0 {
		t.Fatalf("expected pos 0, got %d", ref.Pos)
	}
}

func TestFindStartEmptyIndex(t *testing.T) {
	dir := t.TempDir()
	id := chunk.NewChunkID()
	writeIndex(t, dir, id, nil)

	reader, err := Open(dir, id)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	ref, ok := reader.FindStart(gotime.UnixMicro(1000))
	if ok {
		t.Fatalf("expected ok=false for empty index, got ref %+v", ref)
	}
	if ref != (chunk.RecordRef{}) {
		t.Fatalf("expected zero RecordRef, got %+v", ref)
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
