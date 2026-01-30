package file

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
)

func TestIdxEntryRoundTrip(t *testing.T) {
	entry := IdxEntry{
		IngestTS:      time.UnixMicro(123456789),
		WriteTS:       time.UnixMicro(987654321),
		SourceLocalID: 42,
		RawOffset:     1000,
		RawSize:       500,
	}

	var buf [IdxEntrySize]byte
	EncodeIdxEntry(entry, buf[:])

	decoded := DecodeIdxEntry(buf[:])

	if !decoded.IngestTS.Equal(entry.IngestTS) {
		t.Errorf("IngestTS: want %v, got %v", entry.IngestTS, decoded.IngestTS)
	}
	if !decoded.WriteTS.Equal(entry.WriteTS) {
		t.Errorf("WriteTS: want %v, got %v", entry.WriteTS, decoded.WriteTS)
	}
	if decoded.SourceLocalID != entry.SourceLocalID {
		t.Errorf("SourceLocalID: want %d, got %d", entry.SourceLocalID, decoded.SourceLocalID)
	}
	if decoded.RawOffset != entry.RawOffset {
		t.Errorf("RawOffset: want %d, got %d", entry.RawOffset, decoded.RawOffset)
	}
	if decoded.RawSize != entry.RawSize {
		t.Errorf("RawSize: want %d, got %d", entry.RawSize, decoded.RawSize)
	}
}

func TestIdxFileOffset(t *testing.T) {
	tests := []struct {
		index    uint64
		expected int64
	}{
		{0, int64(format.HeaderSize)},
		{1, int64(format.HeaderSize) + IdxEntrySize},
		{2, int64(format.HeaderSize) + 2*IdxEntrySize},
		{100, int64(format.HeaderSize) + 100*IdxEntrySize},
	}

	for _, tt := range tests {
		got := IdxFileOffset(tt.index)
		if got != tt.expected {
			t.Errorf("IdxFileOffset(%d): want %d, got %d", tt.index, tt.expected, got)
		}
	}
}

func TestRecordCount(t *testing.T) {
	tests := []struct {
		fileSize int64
		expected uint64
	}{
		{0, 0},
		{int64(format.HeaderSize), 0},
		{int64(format.HeaderSize) + IdxEntrySize, 1},
		{int64(format.HeaderSize) + 2*IdxEntrySize, 2},
		{int64(format.HeaderSize) + 100*IdxEntrySize, 100},
		// Partial entry is not counted
		{int64(format.HeaderSize) + IdxEntrySize + 10, 1},
	}

	for _, tt := range tests {
		got := RecordCount(tt.fileSize)
		if got != tt.expected {
			t.Errorf("RecordCount(%d): want %d, got %d", tt.fileSize, tt.expected, got)
		}
	}
}

func TestManagerAppendAndCursor(t *testing.T) {
	dir := t.TempDir()

	mgr, err := NewManager(Config{
		Dir:           dir,
		MaxChunkBytes: 1 << 20, // 1MB
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: time.UnixMicro(100), SourceID: sourceID, Raw: []byte("first record")},
		{IngestTS: time.UnixMicro(200), SourceID: sourceID, Raw: []byte("second record with more data")},
		{IngestTS: time.UnixMicro(300), SourceID: sourceID, Raw: []byte("third")},
	}

	var chunkID chunk.ChunkID
	var positions []uint64
	for _, rec := range records {
		id, pos, err := mgr.Append(rec)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		chunkID = id
		positions = append(positions, pos)
	}

	// Positions should be record indices (0, 1, 2)
	for i, pos := range positions {
		if pos != uint64(i) {
			t.Errorf("Position %d: want %d, got %d", i, i, pos)
		}
	}

	// Verify files exist
	chunkDir := filepath.Join(dir, chunkID.String())
	if _, err := os.Stat(filepath.Join(chunkDir, rawLogFileName)); err != nil {
		t.Errorf("raw.log not found: %v", err)
	}
	if _, err := os.Stat(filepath.Join(chunkDir, idxLogFileName)); err != nil {
		t.Errorf("idx.log not found: %v", err)
	}
	if _, err := os.Stat(filepath.Join(chunkDir, sourcesFileName)); err != nil {
		t.Errorf("sources.bin not found: %v", err)
	}

	// Open cursor and read records (unsealed chunk uses stdio)
	cursor, err := mgr.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("OpenCursor: %v", err)
	}
	defer cursor.Close()

	for i, want := range records {
		got, ref, err := cursor.Next()
		if err != nil {
			t.Fatalf("Next %d: %v", i, err)
		}
		if ref.Pos != uint64(i) {
			t.Errorf("Record %d position: want %d, got %d", i, i, ref.Pos)
		}
		if !bytes.Equal(got.Raw, want.Raw) {
			t.Errorf("Record %d raw: want %q, got %q", i, want.Raw, got.Raw)
		}
		if got.SourceID != want.SourceID {
			t.Errorf("Record %d sourceID: want %s, got %s", i, want.SourceID, got.SourceID)
		}
	}

	// Next should return ErrNoMoreRecords
	if _, _, err := cursor.Next(); err != chunk.ErrNoMoreRecords {
		t.Errorf("Expected ErrNoMoreRecords, got %v", err)
	}
}

func TestManagerSealAndMmapCursor(t *testing.T) {
	dir := t.TempDir()

	mgr, err := NewManager(Config{
		Dir:           dir,
		MaxChunkBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: time.UnixMicro(100), SourceID: sourceID, Raw: []byte("alpha")},
		{IngestTS: time.UnixMicro(200), SourceID: sourceID, Raw: []byte("beta")},
		{IngestTS: time.UnixMicro(300), SourceID: sourceID, Raw: []byte("gamma")},
	}

	var chunkID chunk.ChunkID
	for _, rec := range records {
		id, _, err := mgr.Append(rec)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		chunkID = id
	}

	// Seal the chunk
	if err := mgr.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	// Verify sealed flag in meta
	meta, err := mgr.Meta(chunkID)
	if err != nil {
		t.Fatalf("Meta: %v", err)
	}
	if !meta.Sealed {
		t.Error("Chunk should be sealed")
	}

	// Open cursor (sealed chunk uses mmap)
	cursor, err := mgr.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("OpenCursor: %v", err)
	}
	defer cursor.Close()

	// Forward iteration
	for i, want := range records {
		got, ref, err := cursor.Next()
		if err != nil {
			t.Fatalf("Next %d: %v", i, err)
		}
		if ref.Pos != uint64(i) {
			t.Errorf("Record %d position: want %d, got %d", i, i, ref.Pos)
		}
		if !bytes.Equal(got.Raw, want.Raw) {
			t.Errorf("Record %d raw: want %q, got %q", i, want.Raw, got.Raw)
		}
	}
}

func TestCursorSeekAndPrev(t *testing.T) {
	dir := t.TempDir()

	mgr, err := NewManager(Config{
		Dir:           dir,
		MaxChunkBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: time.UnixMicro(100), SourceID: sourceID, Raw: []byte("zero")},
		{IngestTS: time.UnixMicro(200), SourceID: sourceID, Raw: []byte("one")},
		{IngestTS: time.UnixMicro(300), SourceID: sourceID, Raw: []byte("two")},
		{IngestTS: time.UnixMicro(400), SourceID: sourceID, Raw: []byte("three")},
	}

	var chunkID chunk.ChunkID
	for _, rec := range records {
		id, _, err := mgr.Append(rec)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		chunkID = id
	}

	if err := mgr.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	cursor, err := mgr.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("OpenCursor: %v", err)
	}
	defer cursor.Close()

	// Seek to record 2
	if err := cursor.Seek(chunk.RecordRef{ChunkID: chunkID, Pos: 2}); err != nil {
		t.Fatalf("Seek: %v", err)
	}

	// Next should return record 2
	got, ref, err := cursor.Next()
	if err != nil {
		t.Fatalf("Next after seek: %v", err)
	}
	if ref.Pos != 2 {
		t.Errorf("Position after seek: want 2, got %d", ref.Pos)
	}
	if !bytes.Equal(got.Raw, records[2].Raw) {
		t.Errorf("Raw after seek: want %q, got %q", records[2].Raw, got.Raw)
	}

	// Seek to end (position 4 = after last record)
	if err := cursor.Seek(chunk.RecordRef{ChunkID: chunkID, Pos: 4}); err != nil {
		t.Fatalf("Seek to end: %v", err)
	}

	// Prev should return record 3
	got, ref, err = cursor.Prev()
	if err != nil {
		t.Fatalf("Prev from end: %v", err)
	}
	if ref.Pos != 3 {
		t.Errorf("Position from Prev: want 3, got %d", ref.Pos)
	}
	if !bytes.Equal(got.Raw, records[3].Raw) {
		t.Errorf("Raw from Prev: want %q, got %q", records[3].Raw, got.Raw)
	}

	// Continue Prev to beginning
	for i := 2; i >= 0; i-- {
		got, ref, err := cursor.Prev()
		if err != nil {
			t.Fatalf("Prev %d: %v", i, err)
		}
		if ref.Pos != uint64(i) {
			t.Errorf("Prev position: want %d, got %d", i, ref.Pos)
		}
		if !bytes.Equal(got.Raw, records[i].Raw) {
			t.Errorf("Prev raw: want %q, got %q", records[i].Raw, got.Raw)
		}
	}

	// Prev at beginning should return ErrNoMoreRecords
	if _, _, err := cursor.Prev(); err != chunk.ErrNoMoreRecords {
		t.Errorf("Expected ErrNoMoreRecords at beginning, got %v", err)
	}
}

func TestEmptyChunkCursor(t *testing.T) {
	dir := t.TempDir()

	mgr, err := NewManager(Config{
		Dir:           dir,
		MaxChunkBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	// Create and seal an empty chunk
	if err := mgr.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	meta := mgr.Active()
	if meta != nil {
		t.Error("Active should be nil after seal")
	}

	// List should have one chunk
	metas, err := mgr.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(metas) != 1 {
		t.Fatalf("Expected 1 chunk, got %d", len(metas))
	}

	cursor, err := mgr.OpenCursor(metas[0].ID)
	if err != nil {
		t.Fatalf("OpenCursor: %v", err)
	}
	defer cursor.Close()

	// Next on empty chunk should return ErrNoMoreRecords
	if _, _, err := cursor.Next(); err != chunk.ErrNoMoreRecords {
		t.Errorf("Expected ErrNoMoreRecords on empty chunk, got %v", err)
	}

	// Prev on empty chunk should return ErrNoMoreRecords
	if _, _, err := cursor.Prev(); err != chunk.ErrNoMoreRecords {
		t.Errorf("Expected ErrNoMoreRecords on empty chunk Prev, got %v", err)
	}
}

func TestManagerReload(t *testing.T) {
	dir := t.TempDir()

	sourceID := chunk.NewSourceID()
	var chunkID chunk.ChunkID

	// Create manager, write records, seal, close
	{
		mgr, err := NewManager(Config{Dir: dir})
		if err != nil {
			t.Fatalf("NewManager: %v", err)
		}

		for i := 0; i < 3; i++ {
			id, _, err := mgr.Append(chunk.Record{
				IngestTS: time.UnixMicro(int64(i * 100)),
				SourceID: sourceID,
				Raw:      []byte("record"),
			})
			if err != nil {
				t.Fatalf("Append: %v", err)
			}
			chunkID = id
		}

		if err := mgr.Seal(); err != nil {
			t.Fatalf("Seal: %v", err)
		}

		if err := mgr.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Reopen manager, verify data
	{
		mgr, err := NewManager(Config{Dir: dir})
		if err != nil {
			t.Fatalf("NewManager (reload): %v", err)
		}
		defer mgr.Close()

		meta, err := mgr.Meta(chunkID)
		if err != nil {
			t.Fatalf("Meta: %v", err)
		}
		if !meta.Sealed {
			t.Error("Chunk should be sealed after reload")
		}

		cursor, err := mgr.OpenCursor(chunkID)
		if err != nil {
			t.Fatalf("OpenCursor: %v", err)
		}
		defer cursor.Close()

		count := 0
		for {
			_, _, err := cursor.Next()
			if err == chunk.ErrNoMoreRecords {
				break
			}
			if err != nil {
				t.Fatalf("Next: %v", err)
			}
			count++
		}
		if count != 3 {
			t.Errorf("Expected 3 records, got %d", count)
		}
	}
}

func TestRotationOnMaxChunkBytes(t *testing.T) {
	dir := t.TempDir()

	// Very small max to force rotation
	mgr, err := NewManager(Config{
		Dir:           dir,
		MaxChunkBytes: 50, // Will fit ~1 record
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	sourceID := chunk.NewSourceID()
	var chunkIDs []chunk.ChunkID

	for i := 0; i < 5; i++ {
		id, _, err := mgr.Append(chunk.Record{
			IngestTS: time.UnixMicro(int64(i * 100)),
			SourceID: sourceID,
			Raw:      []byte("some data here"),
		})
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		if len(chunkIDs) == 0 || chunkIDs[len(chunkIDs)-1] != id {
			chunkIDs = append(chunkIDs, id)
		}
	}

	// Should have multiple chunks due to rotation
	if len(chunkIDs) < 2 {
		t.Errorf("Expected multiple chunks due to rotation, got %d", len(chunkIDs))
	}

	// All but last should be sealed
	metas, err := mgr.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	sealedCount := 0
	for _, m := range metas {
		if m.Sealed {
			sealedCount++
		}
	}
	if sealedCount != len(metas)-1 {
		t.Errorf("Expected %d sealed chunks, got %d", len(metas)-1, sealedCount)
	}
}
