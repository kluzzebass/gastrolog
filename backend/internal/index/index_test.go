package index

import (
	"bytes"
	"slices"
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

// SourceIndexReader tests

// sortEntries sorts source index entries by SourceID string, matching indexer output.
func sortEntries(entries []SourceIndexEntry) []SourceIndexEntry {
	sorted := make([]SourceIndexEntry, len(entries))
	copy(sorted, entries)
	slices.SortFunc(sorted, func(a, b SourceIndexEntry) int {
		ab := [16]byte(a.SourceID)
		bb := [16]byte(b.SourceID)
		return bytes.Compare(ab[:], bb[:])
	})
	return sorted
}

func TestLookupFound(t *testing.T) {
	id := chunk.NewChunkID()
	src1 := chunk.NewSourceID()
	src2 := chunk.NewSourceID()
	src3 := chunk.NewSourceID()
	entries := sortEntries([]SourceIndexEntry{
		{SourceID: src1, Positions: []uint64{0, 128}},
		{SourceID: src2, Positions: []uint64{64}},
		{SourceID: src3, Positions: []uint64{192, 256, 320}},
	})

	reader := NewSourceIndexReader(id, entries)

	for _, e := range entries {
		positions, ok := reader.Lookup(e.SourceID)
		if !ok {
			t.Fatalf("expected to find source %s", e.SourceID)
		}
		if len(positions) != len(e.Positions) {
			t.Fatalf("source %s: expected %d positions, got %d", e.SourceID, len(e.Positions), len(positions))
		}
		for i, p := range positions {
			if p != e.Positions[i] {
				t.Fatalf("source %s pos %d: expected %d, got %d", e.SourceID, i, e.Positions[i], p)
			}
		}
	}
}

func TestLookupNotFound(t *testing.T) {
	id := chunk.NewChunkID()
	src1 := chunk.NewSourceID()
	entries := sortEntries([]SourceIndexEntry{
		{SourceID: src1, Positions: []uint64{0}},
	})

	reader := NewSourceIndexReader(id, entries)

	missing := chunk.NewSourceID()
	positions, ok := reader.Lookup(missing)
	if ok {
		t.Fatalf("expected ok=false for missing source, got positions %v", positions)
	}
	if positions != nil {
		t.Fatalf("expected nil positions, got %v", positions)
	}
}

func TestLookupEmptyIndex(t *testing.T) {
	id := chunk.NewChunkID()
	reader := NewSourceIndexReader(id, nil)

	positions, ok := reader.Lookup(chunk.NewSourceID())
	if ok {
		t.Fatalf("expected ok=false for empty index, got positions %v", positions)
	}
	if positions != nil {
		t.Fatalf("expected nil positions, got %v", positions)
	}
}

func TestLookupSingleEntry(t *testing.T) {
	id := chunk.NewChunkID()
	src := chunk.NewSourceID()
	entries := []SourceIndexEntry{
		{SourceID: src, Positions: []uint64{42, 84}},
	}

	reader := NewSourceIndexReader(id, entries)

	positions, ok := reader.Lookup(src)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if len(positions) != 2 || positions[0] != 42 || positions[1] != 84 {
		t.Fatalf("expected [42 84], got %v", positions)
	}
}

func TestLookupCorrectPositions(t *testing.T) {
	id := chunk.NewChunkID()
	src1 := chunk.NewSourceID()
	src2 := chunk.NewSourceID()
	entries := sortEntries([]SourceIndexEntry{
		{SourceID: src1, Positions: []uint64{0, 64, 128}},
		{SourceID: src2, Positions: []uint64{32, 96}},
	})

	reader := NewSourceIndexReader(id, entries)

	pos1, ok := reader.Lookup(src1)
	if !ok {
		t.Fatal("expected to find src1")
	}
	if len(pos1) != 3 || pos1[0] != 0 || pos1[1] != 64 || pos1[2] != 128 {
		t.Fatalf("src1: expected [0 64 128], got %v", pos1)
	}

	pos2, ok := reader.Lookup(src2)
	if !ok {
		t.Fatal("expected to find src2")
	}
	if len(pos2) != 2 || pos2[0] != 32 || pos2[1] != 96 {
		t.Fatalf("src2: expected [32 96], got %v", pos2)
	}
}

// TokenIndexReader tests

func TestTokenLookupFound(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []TokenIndexEntry{
		{Token: "apple", Positions: []uint64{0, 128}},
		{Token: "banana", Positions: []uint64{64}},
		{Token: "cherry", Positions: []uint64{192, 256}},
	}

	reader := NewTokenIndexReader(id, entries)

	for _, e := range entries {
		positions, ok := reader.Lookup(e.Token)
		if !ok {
			t.Fatalf("expected to find token %q", e.Token)
		}
		if len(positions) != len(e.Positions) {
			t.Fatalf("token %q: expected %d positions, got %d", e.Token, len(e.Positions), len(positions))
		}
		for i, p := range positions {
			if p != e.Positions[i] {
				t.Fatalf("token %q pos %d: expected %d, got %d", e.Token, i, e.Positions[i], p)
			}
		}
	}
}

func TestTokenLookupNotFound(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []TokenIndexEntry{
		{Token: "error", Positions: []uint64{0}},
		{Token: "warning", Positions: []uint64{64}},
	}

	reader := NewTokenIndexReader(id, entries)

	positions, ok := reader.Lookup("info")
	if ok {
		t.Fatalf("expected ok=false for missing token, got positions %v", positions)
	}
	if positions != nil {
		t.Fatalf("expected nil positions, got %v", positions)
	}
}

func TestTokenLookupEmptyIndex(t *testing.T) {
	id := chunk.NewChunkID()
	reader := NewTokenIndexReader(id, nil)

	positions, ok := reader.Lookup("anything")
	if ok {
		t.Fatalf("expected ok=false for empty index, got positions %v", positions)
	}
	if positions != nil {
		t.Fatalf("expected nil positions, got %v", positions)
	}
}

func TestTokenLookupSingleEntry(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []TokenIndexEntry{
		{Token: "error", Positions: []uint64{42, 84}},
	}

	reader := NewTokenIndexReader(id, entries)

	positions, ok := reader.Lookup("error")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if len(positions) != 2 || positions[0] != 42 || positions[1] != 84 {
		t.Fatalf("expected [42 84], got %v", positions)
	}
}

func TestTokenLookupFirstEntry(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []TokenIndexEntry{
		{Token: "aaa", Positions: []uint64{0}},
		{Token: "bbb", Positions: []uint64{64}},
		{Token: "ccc", Positions: []uint64{128}},
	}

	reader := NewTokenIndexReader(id, entries)

	positions, ok := reader.Lookup("aaa")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if len(positions) != 1 || positions[0] != 0 {
		t.Fatalf("expected [0], got %v", positions)
	}
}

func TestTokenLookupLastEntry(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []TokenIndexEntry{
		{Token: "aaa", Positions: []uint64{0}},
		{Token: "bbb", Positions: []uint64{64}},
		{Token: "zzz", Positions: []uint64{128}},
	}

	reader := NewTokenIndexReader(id, entries)

	positions, ok := reader.Lookup("zzz")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if len(positions) != 1 || positions[0] != 128 {
		t.Fatalf("expected [128], got %v", positions)
	}
}

func TestTokenLookupCaseSensitive(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []TokenIndexEntry{
		{Token: "error", Positions: []uint64{0}},
	}

	reader := NewTokenIndexReader(id, entries)

	// Tokens are stored lowercase by the indexer, so uppercase lookup should fail.
	_, ok := reader.Lookup("ERROR")
	if ok {
		t.Fatal("expected ok=false for case mismatch")
	}

	// Lowercase should work.
	_, ok = reader.Lookup("error")
	if !ok {
		t.Fatal("expected ok=true for exact match")
	}
}
