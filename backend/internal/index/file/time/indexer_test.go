package time

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	gotime "time"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/format"
	"gastrolog/internal/index"
)

func setupChunkManager(t *testing.T, records []chunk.Record) (chunk.ChunkManager, chunk.ChunkID) {
	t.Helper()
	dir := t.TempDir()
	callIdx := 0
	manager, err := chunkfile.NewManager(chunkfile.Config{
		Dir: dir,
		Now: func() gotime.Time {
			if callIdx < len(records) {
				ts := records[callIdx].IngestTS
				callIdx++
				return ts
			}
			return gotime.Now()
		},
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	for _, rec := range records {
		if _, _, err := manager.Append(rec); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}
	metas, err := manager.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(metas) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(metas))
	}
	return manager, metas[0].ID
}

func TestIndexerBuild(t *testing.T) {
	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1000), SourceID: sourceID, Raw: []byte("one")},
		{IngestTS: gotime.UnixMicro(2000), SourceID: sourceID, Raw: []byte("two")},
		{IngestTS: gotime.UnixMicro(3000), SourceID: sourceID, Raw: []byte("three")},
		{IngestTS: gotime.UnixMicro(4000), SourceID: sourceID, Raw: []byte("four")},
		{IngestTS: gotime.UnixMicro(5000), SourceID: sourceID, Raw: []byte("five")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, 2)

	if indexer.Name() != "time" {
		t.Fatalf("expected name %q, got %q", "time", indexer.Name())
	}

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	idxPath := filepath.Join(indexDir, chunkID.String(), indexFileName)
	data, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}

	entries, err := decodeIndex(chunkID, data)
	if err != nil {
		t.Fatalf("decode index: %v", err)
	}

	// With sparsity=2 and 5 records: record 0 (always first), record 2, record 4.
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	expectedTS := []gotime.Time{gotime.UnixMicro(1000), gotime.UnixMicro(3000), gotime.UnixMicro(5000)}
	for i, e := range entries {
		if !e.Timestamp.Equal(expectedTS[i]) {
			t.Fatalf("entry %d: expected timestamp %v, got %v", i, expectedTS[i], e.Timestamp)
		}
	}
}

func TestIndexerIdempotent(t *testing.T) {
	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(100), SourceID: sourceID, Raw: []byte("alpha")},
		{IngestTS: gotime.UnixMicro(200), SourceID: sourceID, Raw: []byte("beta")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, 1)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("first build: %v", err)
	}
	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("second build: %v", err)
	}

	idxPath := filepath.Join(indexDir, chunkID.String(), indexFileName)
	data, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}
	entries, err := decodeIndex(chunkID, data)
	if err != nil {
		t.Fatalf("decode index: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
}

func TestIndexerCancelledContext(t *testing.T) {
	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(100), SourceID: sourceID, Raw: []byte("data")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := indexer.Build(ctx, chunkID)
	if err == nil {
		t.Fatal("expected error from cancelled context, got nil")
	}
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestIndexerBuildEmptyChunk(t *testing.T) {
	manager, chunkID := setupChunkManager(t, nil)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, 1)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	idxPath := filepath.Join(indexDir, chunkID.String(), indexFileName)
	data, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}

	entries, err := decodeIndex(chunkID, data)
	if err != nil {
		t.Fatalf("decode index: %v", err)
	}

	if len(entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(entries))
	}
}

func TestIndexerBuildSingleRecord(t *testing.T) {
	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(42), SourceID: sourceID, Raw: []byte("only")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, 10)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	idxPath := filepath.Join(indexDir, chunkID.String(), indexFileName)
	data, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}

	entries, err := decodeIndex(chunkID, data)
	if err != nil {
		t.Fatalf("decode index: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if !entries[0].Timestamp.Equal(gotime.UnixMicro(42)) {
		t.Fatalf("expected timestamp %v, got %v", gotime.UnixMicro(42), entries[0].Timestamp)
	}
	if entries[0].RecordPos != 0 {
		t.Fatalf("expected record pos 0, got %d", entries[0].RecordPos)
	}
}

func TestIndexerBuildSparsityOne(t *testing.T) {
	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(10), SourceID: sourceID, Raw: []byte("a")},
		{IngestTS: gotime.UnixMicro(20), SourceID: sourceID, Raw: []byte("b")},
		{IngestTS: gotime.UnixMicro(30), SourceID: sourceID, Raw: []byte("c")},
		{IngestTS: gotime.UnixMicro(40), SourceID: sourceID, Raw: []byte("d")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, 1)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	idxPath := filepath.Join(indexDir, chunkID.String(), indexFileName)
	data, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}

	entries, err := decodeIndex(chunkID, data)
	if err != nil {
		t.Fatalf("decode index: %v", err)
	}

	if len(entries) != 4 {
		t.Fatalf("expected 4 entries, got %d", len(entries))
	}

	expectedTS := []gotime.Time{gotime.UnixMicro(10), gotime.UnixMicro(20), gotime.UnixMicro(30), gotime.UnixMicro(40)}
	for i, e := range entries {
		if !e.Timestamp.Equal(expectedTS[i]) {
			t.Fatalf("entry %d: expected timestamp %v, got %v", i, expectedTS[i], e.Timestamp)
		}
	}
}

func TestIndexerBuildRecordPos(t *testing.T) {
	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), SourceID: sourceID, Raw: []byte("aaa")},
		{IngestTS: gotime.UnixMicro(2), SourceID: sourceID, Raw: []byte("bbb")},
		{IngestTS: gotime.UnixMicro(3), SourceID: sourceID, Raw: []byte("ccc")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, 1)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	idxPath := filepath.Join(indexDir, chunkID.String(), indexFileName)
	data, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}

	entries, err := decodeIndex(chunkID, data)
	if err != nil {
		t.Fatalf("decode index: %v", err)
	}

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	recordSize, err := chunkfile.RecordSize(len(records[0].Raw))
	if err != nil {
		t.Fatalf("record size: %v", err)
	}
	for i, e := range entries {
		expectedPos := uint64(i) * uint64(recordSize)
		if e.RecordPos != expectedPos {
			t.Fatalf("entry %d: expected pos %d, got %d", i, expectedPos, e.RecordPos)
		}
	}
}

func TestIndexerBuildInvalidChunkID(t *testing.T) {
	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), SourceID: sourceID, Raw: []byte("x")},
	}

	manager, _ := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, 1)

	bogusID := chunk.NewChunkID()
	err := indexer.Build(context.Background(), bogusID)
	if err == nil {
		t.Fatal("expected error for invalid chunk ID, got nil")
	}
}

func TestIndexerBuildReadOnlyDir(t *testing.T) {
	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), SourceID: sourceID, Raw: []byte("x")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	if err := os.Chmod(indexDir, 0o444); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	defer os.Chmod(indexDir, 0o755)

	indexer := NewIndexer(indexDir, manager, 1)
	err := indexer.Build(context.Background(), chunkID)
	if err == nil {
		t.Fatal("expected error writing to read-only dir, got nil")
	}
}

func TestSignature(t *testing.T) {
	testChunkID := chunk.NewChunkID()
	entries := []index.TimeIndexEntry{
		{Timestamp: gotime.UnixMicro(1000), RecordPos: 0},
	}

	data := encodeIndex(testChunkID, entries)
	if data[0] != format.Signature {
		t.Fatalf("expected signature byte 0x%02x, got 0x%02x", format.Signature, data[0])
	}
	if data[1] != format.TypeTimeIndex {
		t.Fatalf("expected type byte '%c', got 0x%02x", format.TypeTimeIndex, data[1])
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	testChunkID := chunk.NewChunkID()
	entries := []index.TimeIndexEntry{
		{Timestamp: gotime.UnixMicro(1000), RecordPos: 0},
		{Timestamp: gotime.UnixMicro(2000), RecordPos: 128},
		{Timestamp: gotime.UnixMicro(3000), RecordPos: 256},
	}

	data := encodeIndex(testChunkID, entries)
	got, err := decodeIndex(testChunkID, data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

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

func TestEncodeDecodeEmpty(t *testing.T) {
	testChunkID := chunk.NewChunkID()
	data := encodeIndex(testChunkID, nil)
	got, err := decodeIndex(testChunkID, data)
	if err != nil {
		t.Fatalf("decode empty: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(got))
	}
}

func TestDecodeErrors(t *testing.T) {
	testChunkID := chunk.NewChunkID()

	// Too small.
	if _, err := decodeIndex(testChunkID, []byte{'i'}); err != ErrIndexTooSmall {
		t.Fatalf("expected ErrIndexTooSmall, got %v", err)
	}

	// Wrong signature.
	bad := make([]byte, headerSize)
	bad[0] = 0xFF
	bad[1] = format.TypeTimeIndex
	bad[2] = currentVersion
	if _, err := decodeIndex(testChunkID, bad); err == nil {
		t.Fatal("expected error for wrong signature, got nil")
	}

	// Wrong type byte.
	bad1b := make([]byte, headerSize)
	bad1b[0] = format.Signature
	bad1b[1] = 'x'
	bad1b[2] = currentVersion
	if _, err := decodeIndex(testChunkID, bad1b); err == nil {
		t.Fatal("expected error for wrong type byte, got nil")
	}

	// Wrong version.
	bad2 := make([]byte, headerSize)
	bad2[0] = format.Signature
	bad2[1] = format.TypeTimeIndex
	bad2[2] = 0xFF
	if _, err := decodeIndex(testChunkID, bad2); err == nil {
		t.Fatal("expected error for wrong version, got nil")
	}

	// Chunk ID mismatch.
	data := encodeIndex(testChunkID, nil)
	wrongChunkID := chunk.NewChunkID()
	if _, err := decodeIndex(wrongChunkID, data); err != ErrChunkIDMismatch {
		t.Fatalf("expected ErrChunkIDMismatch, got %v", err)
	}

	// Count mismatch: header says 1 entry but no entry data.
	bad3 := encodeIndex(testChunkID, nil)
	bad3[format.HeaderSize+chunkIDSize] = 1
	if _, err := decodeIndex(testChunkID, bad3); err != ErrEntrySizeMismatch {
		t.Fatalf("expected ErrEntrySizeMismatch, got %v", err)
	}
}

func TestIndexerBuildUnsealedChunk(t *testing.T) {
	dir := t.TempDir()
	manager, err := chunkfile.NewManager(chunkfile.Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	sourceID := chunk.NewSourceID()
	chunkID, _, err := manager.Append(chunk.Record{IngestTS: gotime.UnixMicro(1), SourceID: sourceID, Raw: []byte("x")})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, 1)

	err = indexer.Build(context.Background(), chunkID)
	if err == nil {
		t.Fatal("expected error building index on unsealed chunk, got nil")
	}
	if err != chunk.ErrChunkNotSealed {
		t.Fatalf("expected ErrChunkNotSealed, got %v", err)
	}
}

func TestLoadIndex(t *testing.T) {
	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1000), SourceID: sourceID, Raw: []byte("one")},
		{IngestTS: gotime.UnixMicro(2000), SourceID: sourceID, Raw: []byte("two")},
		{IngestTS: gotime.UnixMicro(3000), SourceID: sourceID, Raw: []byte("three")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, 1)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	entries, err := LoadIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("load index: %v", err)
	}

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	expectedTS := []gotime.Time{gotime.UnixMicro(1000), gotime.UnixMicro(2000), gotime.UnixMicro(3000)}
	for i, e := range entries {
		if !e.Timestamp.Equal(expectedTS[i]) {
			t.Fatalf("entry %d: expected timestamp %v, got %v", i, expectedTS[i], e.Timestamp)
		}
	}
}

func TestLoadIndexNotFound(t *testing.T) {
	indexDir := t.TempDir()
	bogusID := chunk.NewChunkID()

	_, err := LoadIndex(indexDir, bogusID)
	if err == nil {
		t.Fatal("expected error loading nonexistent index, got nil")
	}
}
