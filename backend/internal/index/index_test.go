package index

import (
	"testing"
	"time"

	"github.com/kluzzebass/gastrolog/internal/chunk"
)

func TestNewIndexTimeEntries(t *testing.T) {
	entries := []TimeIndexEntry{
		{Timestamp: time.UnixMicro(1000), RecordPos: 0},
		{Timestamp: time.UnixMicro(2000), RecordPos: 128},
		{Timestamp: time.UnixMicro(3000), RecordPos: 256},
	}

	idx := NewIndex(entries)
	got := idx.Entries()

	if len(got) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(got))
	}
	for i := range entries {
		if !got[i].Timestamp.Equal(entries[i].Timestamp) {
			t.Fatalf("entry %d: expected timestamp %v, got %v", i, entries[i].Timestamp, got[i].Timestamp)
		}
		if got[i].RecordPos != entries[i].RecordPos {
			t.Fatalf("entry %d: expected pos %d, got %d", i, entries[i].RecordPos, got[i].RecordPos)
		}
	}
}

func TestNewIndexSourceEntries(t *testing.T) {
	src := chunk.NewSourceID()
	entries := []SourceIndexEntry{
		{SourceID: src, Positions: []uint64{0, 64, 128}},
	}

	idx := NewIndex(entries)
	got := idx.Entries()

	if len(got) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(got))
	}
	if got[0].SourceID != src {
		t.Fatalf("expected source %s, got %s", src, got[0].SourceID)
	}
	if len(got[0].Positions) != 3 {
		t.Fatalf("expected 3 positions, got %d", len(got[0].Positions))
	}
}

func TestNewIndexEmpty(t *testing.T) {
	idx := NewIndex[TimeIndexEntry](nil)
	got := idx.Entries()
	if got != nil {
		t.Fatalf("expected nil entries, got %v", got)
	}
}

func TestNewIndexEmptySlice(t *testing.T) {
	idx := NewIndex([]TimeIndexEntry{})
	got := idx.Entries()
	if len(got) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(got))
	}
}

func TestNewIndexEntriesReturnsSameSlice(t *testing.T) {
	entries := []TimeIndexEntry{
		{Timestamp: time.UnixMicro(1000), RecordPos: 0},
	}
	idx := NewIndex(entries)

	a := idx.Entries()
	b := idx.Entries()

	// Both calls should return the same backing slice.
	if len(a) != len(b) {
		t.Fatalf("entries length mismatch: %d vs %d", len(a), len(b))
	}
	if !a[0].Timestamp.Equal(b[0].Timestamp) {
		t.Fatal("entries should be identical across calls")
	}
}

func TestNewIndexWithIntType(t *testing.T) {
	// Verify generics work with arbitrary types.
	idx := NewIndex([]int{1, 2, 3})
	got := idx.Entries()
	if len(got) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(got))
	}
	if got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("expected [1 2 3], got %v", got)
	}
}

// TimeIndexReader tests

func TestFindStartBeforeAllEntries(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []TimeIndexEntry{
		{Timestamp: time.UnixMicro(1000), RecordPos: 0},
		{Timestamp: time.UnixMicro(2000), RecordPos: 64},
		{Timestamp: time.UnixMicro(3000), RecordPos: 128},
	}
	reader := NewTimeIndexReader(id, entries)

	ref, ok := reader.FindStart(time.UnixMicro(500))
	if ok {
		t.Fatalf("expected ok=false, got ref %+v", ref)
	}
	if ref != (chunk.RecordRef{}) {
		t.Fatalf("expected zero RecordRef, got %+v", ref)
	}
}

func TestFindStartAtExactEntry(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []TimeIndexEntry{
		{Timestamp: time.UnixMicro(1000), RecordPos: 0},
		{Timestamp: time.UnixMicro(2000), RecordPos: 64},
		{Timestamp: time.UnixMicro(3000), RecordPos: 128},
	}
	reader := NewTimeIndexReader(id, entries)

	ref, ok := reader.FindStart(time.UnixMicro(2000))
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
	id := chunk.NewChunkID()
	entries := []TimeIndexEntry{
		{Timestamp: time.UnixMicro(1000), RecordPos: 0},
		{Timestamp: time.UnixMicro(2000), RecordPos: 64},
		{Timestamp: time.UnixMicro(3000), RecordPos: 128},
	}
	reader := NewTimeIndexReader(id, entries)

	ref, ok := reader.FindStart(time.UnixMicro(2500))
	if !ok {
		t.Fatal("expected ok=true")
	}
	if ref.Pos != 64 {
		t.Fatalf("expected pos 64, got %d", ref.Pos)
	}
}

func TestFindStartAfterAllEntries(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []TimeIndexEntry{
		{Timestamp: time.UnixMicro(1000), RecordPos: 0},
		{Timestamp: time.UnixMicro(2000), RecordPos: 64},
		{Timestamp: time.UnixMicro(3000), RecordPos: 128},
	}
	reader := NewTimeIndexReader(id, entries)

	ref, ok := reader.FindStart(time.UnixMicro(9999))
	if !ok {
		t.Fatal("expected ok=true")
	}
	if ref.Pos != 128 {
		t.Fatalf("expected pos 128, got %d", ref.Pos)
	}
}

func TestFindStartSingleEntry(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []TimeIndexEntry{
		{Timestamp: time.UnixMicro(5000), RecordPos: 0},
	}
	reader := NewTimeIndexReader(id, entries)

	// Before the only entry.
	ref, ok := reader.FindStart(time.UnixMicro(4000))
	if ok {
		t.Fatalf("expected ok=false, got ref %+v", ref)
	}

	// At the exact entry.
	ref, ok = reader.FindStart(time.UnixMicro(5000))
	if !ok {
		t.Fatal("expected ok=true")
	}
	if ref.Pos != 0 {
		t.Fatalf("expected pos 0, got %d", ref.Pos)
	}

	// After the only entry.
	ref, ok = reader.FindStart(time.UnixMicro(6000))
	if !ok {
		t.Fatal("expected ok=true")
	}
	if ref.Pos != 0 {
		t.Fatalf("expected pos 0, got %d", ref.Pos)
	}
}

func TestFindStartEmptyIndex(t *testing.T) {
	id := chunk.NewChunkID()
	reader := NewTimeIndexReader(id, nil)

	ref, ok := reader.FindStart(time.UnixMicro(1000))
	if ok {
		t.Fatalf("expected ok=false, got ref %+v", ref)
	}
	if ref != (chunk.RecordRef{}) {
		t.Fatalf("expected zero RecordRef, got %+v", ref)
	}
}
