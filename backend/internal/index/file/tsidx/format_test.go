package tsidx

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/cloud"
	"gastrolog/internal/glid"

	"github.com/klauspost/compress/zstd"
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
		ts        int64
		wantPos   uint64
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

// TestDecodeRawEntries pins the embedded ITSI/STSI section format —
// raw `[ts:i64][pos:u32]` × N with no header, count derived from
// section size. The writer in chunk/cloud emits this exact layout.
func TestDecodeRawEntries(t *testing.T) {
	t.Parallel()
	// Three entries: (ts=100, pos=5), (ts=200, pos=2), (ts=300, pos=9).
	// Little-endian: ts as u64, pos as u32, no separator, no header.
	raw := []byte{
		100, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0,
		200, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0,
		44, 1, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, // 300 = 0x012C → 44, 1
	}
	entries, err := decodeRawEntries(raw)
	if err != nil {
		t.Fatalf("decodeRawEntries: %v", err)
	}
	want := []Entry{
		{TS: 100, Pos: 5},
		{TS: 200, Pos: 2},
		{TS: 300, Pos: 9},
	}
	if len(entries) != len(want) {
		t.Fatalf("len = %d, want %d", len(entries), len(want))
	}
	for i := range want {
		if entries[i] != want[i] {
			t.Errorf("entry %d = %+v, want %+v", i, entries[i], want[i])
		}
	}
}

// writeTestGLCB writes a 3-record GLCB blob to a tempfile under dir/<chunkID>/
// at the canonical data.glcb location. Used by the GLCB-read tests below.
func writeTestGLCB(t *testing.T, dir string, chunkID chunk.ChunkID) {
	t.Helper()

	chunkDir := filepath.Join(dir, chunkID.String())
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	defer enc.Close()

	w := cloud.NewWriter(chunkID, glid.New(), enc)
	now := time.Unix(0, 0)
	for i, ts := range []time.Duration{100, 200, 300} {
		rec := chunk.Record{
			WriteTS:  now.Add(ts),
			IngestTS: now.Add(ts),
			SourceTS: now.Add(ts),
			Raw:      []byte{byte(i)},
		}
		if err := w.Add(rec); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	var buf bytes.Buffer
	if _, err := w.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if err := os.WriteFile(filepath.Join(chunkDir, cloud.BlobFilename), buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write blob: %v", err)
	}
}

func TestLoadIngestIndexFromGLCB(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	chunkID := chunk.NewChunkID()
	writeTestGLCB(t, dir, chunkID)

	entries, err := LoadIngestIndex(dir, chunkID)
	if err != nil {
		t.Fatalf("LoadIngestIndex: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("len = %d, want 3", len(entries))
	}
	// Sorted by TS ascending; positions match insertion order (0,1,2).
	for i, want := range []int64{100, 200, 300} {
		if entries[i].TS != want {
			t.Errorf("entry %d TS = %d, want %d", i, entries[i].TS, want)
		}
		if entries[i].Pos != uint32(i) {
			t.Errorf("entry %d Pos = %d, want %d", i, entries[i].Pos, i)
		}
	}
}

func TestOpenIngestMmapSearchTS(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	chunkID := chunk.NewChunkID()
	writeTestGLCB(t, dir, chunkID)

	mv, err := OpenIngestMmap(dir, chunkID)
	if err != nil {
		t.Fatalf("OpenIngestMmap: %v", err)
	}
	defer func() { _ = mv.Close() }()

	tests := []struct {
		ts        int64
		wantRank  uint32
		wantPos   uint32
		wantFound bool
	}{
		{50, 0, 0, true},
		{100, 0, 0, true},
		{150, 1, 1, true},
		{300, 2, 2, true},
		{500, 0, 0, false},
	}
	for _, tt := range tests {
		rank, pos, ok := mv.SearchTS(tt.ts)
		if ok != tt.wantFound || rank != tt.wantRank || pos != tt.wantPos {
			t.Errorf("SearchTS(%d) = (rank=%d, pos=%d, ok=%v), want (rank=%d, pos=%d, ok=%v)",
				tt.ts, rank, pos, ok, tt.wantRank, tt.wantPos, tt.wantFound)
		}
	}
}
