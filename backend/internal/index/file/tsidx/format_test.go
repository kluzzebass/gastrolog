package tsidx

import (
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
)

func TestFindStartPosition(t *testing.T) {
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

func TestLoadIngestIndex(t *testing.T) {
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
