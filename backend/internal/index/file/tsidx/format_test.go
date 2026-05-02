package tsidx

import (
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
)

func TestFindStartPosition(t *testing.T) {
	t.Parallel()
	entries := []Entry{
		{TS: 100, Pos: 0},
		{TS: 200, Pos: 1},
		{TS: 200, Pos: 2},
		{TS: 300, Pos: 3},
		{TS: 400, Pos: 4},
	}

	tests := []struct {
		ts       int64
		wantPos  uint64
		wantFound bool
	}{
		{50, 0, true},
		{100, 0, true},
		{150, 1, true},
		{200, 1, true},
		{250, 3, true},
		{400, 4, true},
		{500, 0, false},
	}
	for _, tt := range tests {
		pos, found := FindStartPosition(entries, tt.ts)
		if found != tt.wantFound || pos != tt.wantPos {
			t.Errorf("FindStartPosition(entries, %d): got (%d, %v), want (%d, %v)", tt.ts, pos, found, tt.wantPos, tt.wantFound)
		}
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()
	entries := []Entry{
		{TS: 300, Pos: 2},
		{TS: 100, Pos: 0},
		{TS: 200, Pos: 1},
	}
	data := encodeIndex(entries, format.TypeIngestIndex)
	decoded, err := decodeIndex(data, format.TypeIngestIndex)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Should be sorted by ts.
	if len(decoded) != 3 {
		t.Fatalf("len: got %d, want 3", len(decoded))
	}
	if decoded[0].TS != 100 || decoded[0].Pos != 0 {
		t.Errorf("entry 0: got (%d, %d), want (100, 0)", decoded[0].TS, decoded[0].Pos)
	}
	if decoded[1].TS != 200 || decoded[1].Pos != 1 {
		t.Errorf("entry 1: got (%d, %d), want (200, 1)", decoded[1].TS, decoded[1].Pos)
	}
	if decoded[2].TS != 300 || decoded[2].Pos != 2 {
		t.Errorf("entry 2: got (%d, %d), want (300, 2)", decoded[2].TS, decoded[2].Pos)
	}
}

// TestSearchIngestFileRankVsPos exercises the rank-vs-position distinction on
// a non-monotonic chunk: physical record positions are scattered relative to
// IngestTS-sorted order, so the rank value is the only correct one for
// histogram-style bucket counting. See gastrolog-66b7x.
func TestSearchIngestFileRankVsPos(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	chunkID := chunk.NewChunkID()
	chunkDir := filepath.Join(dir, chunkID.String())
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// Simulate an ImportRecords-built chunk: records were written in
	// source-WriteTS order, so physical positions don't match IngestTS rank.
	// Sorted-by-TS order: TS=100 (pos=4), 200 (pos=2), 300 (pos=0), 400 (pos=3), 500 (pos=1)
	entries := []Entry{
		{TS: 300, Pos: 0},
		{TS: 500, Pos: 1},
		{TS: 200, Pos: 2},
		{TS: 400, Pos: 3},
		{TS: 100, Pos: 4},
	}
	data := encodeIndex(entries, format.TypeIngestIndex)
	if err := os.WriteFile(IngestIndexPath(dir, chunkID), data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	tests := []struct {
		ts       int64
		wantRank uint64
		wantPos  uint64
		wantFound bool
	}{
		{50, 0, 4, true},   // before all → first entry: rank 0, pos=4 (TS=100 lives at physical 4)
		{100, 0, 4, true},  // exact first
		{150, 1, 2, true},  // → TS=200 at rank 1, pos=2
		{250, 2, 0, true},  // → TS=300 at rank 2, pos=0
		{500, 4, 1, true},  // exact last: rank 4, pos=1
		{600, 0, 0, false}, // past end
	}
	for _, tt := range tests {
		gotRank, found, err := SearchIngestFileRank(dir, chunkID, tt.ts)
		if err != nil {
			t.Fatalf("SearchIngestFileRank(%d): %v", tt.ts, err)
		}
		if found != tt.wantFound || gotRank != tt.wantRank {
			t.Errorf("SearchIngestFileRank(%d): got (%d, %v), want (%d, %v)", tt.ts, gotRank, found, tt.wantRank, tt.wantFound)
		}
		gotPos, foundPos, err := SearchIngestFile(dir, chunkID, tt.ts)
		if err != nil {
			t.Fatalf("SearchIngestFile(%d): %v", tt.ts, err)
		}
		if foundPos != tt.wantFound || gotPos != tt.wantPos {
			t.Errorf("SearchIngestFile(%d): got (%d, %v), want (%d, %v)", tt.ts, gotPos, foundPos, tt.wantPos, tt.wantFound)
		}
	}

	// Bucket-count sanity: counting via rank gives correct cardinalities,
	// counting via pos does not. Range [200, 500) covers ranks [1, 4).
	startRank, _, _ := SearchIngestFileRank(dir, chunkID, 200)
	endRank, _, _ := SearchIngestFileRank(dir, chunkID, 500)
	if got := endRank - startRank; got != 3 {
		t.Errorf("rank delta for [200,500): got %d, want 3 (TS=200,300,400)", got)
	}
	// pos arithmetic — wrong on this chunk:
	startPos, _, _ := SearchIngestFile(dir, chunkID, 200)
	endPos, _, _ := SearchIngestFile(dir, chunkID, 500)
	if int64(endPos)-int64(startPos) == 3 {
		t.Errorf("pos arithmetic accidentally correct on non-monotonic chunk; rank fix not actually exercised")
	}
}

func TestLoadIngestIndex(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	chunkID := chunk.NewChunkID()
	chunkDir := filepath.Join(dir, chunkID.String())
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// Write a minimal index.
	entries := []Entry{{TS: 1000, Pos: 0}}
	data := encodeIndex(entries, format.TypeIngestIndex)
	path := IngestIndexPath(dir, chunkID)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	loaded, err := LoadIngestIndex(dir, chunkID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded) != 1 || loaded[0].TS != 1000 || loaded[0].Pos != 0 {
		t.Errorf("loaded: got %v, want [{1000, 0}]", loaded)
	}
}
