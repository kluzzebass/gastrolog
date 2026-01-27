package file

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	gotime "time"

	"github.com/google/uuid"
	"github.com/kluzzebass/gastrolog/internal/chunk"
	chunkfile "github.com/kluzzebass/gastrolog/internal/chunk/file"
	indexsource "github.com/kluzzebass/gastrolog/internal/index/source"
)

func setupChunkManager(t *testing.T, records []chunk.Record) (chunk.ChunkManager, chunk.ChunkID) {
	t.Helper()
	dir := t.TempDir()
	manager, err := chunkfile.NewManager(chunkfile.Config{Dir: dir})
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

// sortEntries sorts entries by SourceID string for deterministic comparison.
func sortEntries(entries []indexsource.IndexEntry) {
	sort.Slice(entries, func(i, j int) bool {
		a := uuid.UUID(entries[i].SourceID)
		b := uuid.UUID(entries[j].SourceID)
		return a.String() < b.String()
	})
}

func TestSourceIndexerBuild(t *testing.T) {
	src1 := chunk.NewSourceID()
	src2 := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1000), SourceID: src1, Raw: []byte("one")},
		{IngestTS: gotime.UnixMicro(2000), SourceID: src2, Raw: []byte("two")},
		{IngestTS: gotime.UnixMicro(3000), SourceID: src1, Raw: []byte("three")},
		{IngestTS: gotime.UnixMicro(4000), SourceID: src2, Raw: []byte("four")},
		{IngestTS: gotime.UnixMicro(5000), SourceID: src1, Raw: []byte("five")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewSourceIndexer(indexDir, manager)

	if indexer.Name() != "source" {
		t.Fatalf("expected name %q, got %q", "source", indexer.Name())
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

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	// Find entries by source.
	sortEntries(entries)
	entryMap := make(map[chunk.SourceID][]uint64)
	for _, e := range entries {
		entryMap[e.SourceID] = e.Positions
	}

	if len(entryMap[src1]) != 3 {
		t.Fatalf("src1: expected 3 positions, got %d", len(entryMap[src1]))
	}
	if len(entryMap[src2]) != 2 {
		t.Fatalf("src2: expected 2 positions, got %d", len(entryMap[src2]))
	}
}

func TestSourceIndexerIdempotent(t *testing.T) {
	src := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(100), SourceID: src, Raw: []byte("alpha")},
		{IngestTS: gotime.UnixMicro(200), SourceID: src, Raw: []byte("beta")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewSourceIndexer(indexDir, manager)

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

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if len(entries[0].Positions) != 2 {
		t.Fatalf("expected 2 positions, got %d", len(entries[0].Positions))
	}
}

func TestSourceIndexerCancelledContext(t *testing.T) {
	src := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(100), SourceID: src, Raw: []byte("data")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewSourceIndexer(indexDir, manager)

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

func TestSourceIndexerBuildEmptyChunk(t *testing.T) {
	manager, chunkID := setupChunkManager(t, nil)
	indexDir := t.TempDir()
	indexer := NewSourceIndexer(indexDir, manager)

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

func TestSourceIndexerBuildSingleSource(t *testing.T) {
	src := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(10), SourceID: src, Raw: []byte("a")},
		{IngestTS: gotime.UnixMicro(20), SourceID: src, Raw: []byte("b")},
		{IngestTS: gotime.UnixMicro(30), SourceID: src, Raw: []byte("c")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewSourceIndexer(indexDir, manager)

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
	if entries[0].SourceID != src {
		t.Fatalf("expected source %s, got %s", src, entries[0].SourceID)
	}
	if len(entries[0].Positions) != 3 {
		t.Fatalf("expected 3 positions, got %d", len(entries[0].Positions))
	}
}

func TestSourceIndexerBuildSingleRecord(t *testing.T) {
	src := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(42), SourceID: src, Raw: []byte("only")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewSourceIndexer(indexDir, manager)

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
	if len(entries[0].Positions) != 1 {
		t.Fatalf("expected 1 position, got %d", len(entries[0].Positions))
	}
	if entries[0].Positions[0] != 0 {
		t.Fatalf("expected position 0, got %d", entries[0].Positions[0])
	}
}

func TestSourceIndexerBuildRecordPos(t *testing.T) {
	src := chunk.NewSourceID()
	// All payloads same length so record sizes are uniform.
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), SourceID: src, Raw: []byte("aaa")},
		{IngestTS: gotime.UnixMicro(2), SourceID: src, Raw: []byte("bbb")},
		{IngestTS: gotime.UnixMicro(3), SourceID: src, Raw: []byte("ccc")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewSourceIndexer(indexDir, manager)

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

	recordSize, err := chunkfile.RecordSize(len(records[0].Raw))
	if err != nil {
		t.Fatalf("record size: %v", err)
	}
	for i, pos := range entries[0].Positions {
		expectedPos := uint64(i) * uint64(recordSize)
		if pos != expectedPos {
			t.Fatalf("position %d: expected %d, got %d", i, expectedPos, pos)
		}
	}
}

func TestSourceIndexerBuildInvalidChunkID(t *testing.T) {
	src := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), SourceID: src, Raw: []byte("x")},
	}

	manager, _ := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewSourceIndexer(indexDir, manager)

	bogusID := chunk.NewChunkID()
	err := indexer.Build(context.Background(), bogusID)
	if err == nil {
		t.Fatal("expected error for invalid chunk ID, got nil")
	}
}

func TestSourceIndexerBuildReadOnlyDir(t *testing.T) {
	src := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), SourceID: src, Raw: []byte("x")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	if err := os.Chmod(indexDir, 0o444); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	defer os.Chmod(indexDir, 0o755)

	indexer := NewSourceIndexer(indexDir, manager)
	err := indexer.Build(context.Background(), chunkID)
	if err == nil {
		t.Fatal("expected error writing to read-only dir, got nil")
	}
}

func TestSignature(t *testing.T) {
	testChunkID := chunk.NewChunkID()
	src := chunk.NewSourceID()
	entries := []indexsource.IndexEntry{
		{SourceID: src, Positions: []uint64{0}},
	}

	data := encodeIndex(testChunkID, entries)
	if data[0] != signatureByte {
		t.Fatalf("expected signature byte 0x%02x, got 0x%02x", signatureByte, data[0])
	}
	if data[1] != typeByte {
		t.Fatalf("expected type byte '%c', got 0x%02x", typeByte, data[1])
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	testChunkID := chunk.NewChunkID()
	src1 := chunk.NewSourceID()
	src2 := chunk.NewSourceID()
	entries := []indexsource.IndexEntry{
		{SourceID: src1, Positions: []uint64{0, 128, 256}},
		{SourceID: src2, Positions: []uint64{64, 192}},
	}

	data := encodeIndex(testChunkID, entries)
	got, err := decodeIndex(testChunkID, data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(got))
	}

	// Sort both for comparison since encodeIndex sorts internally.
	sortEntries(entries)
	// got is already sorted from decode.

	for i := range entries {
		if got[i].SourceID != entries[i].SourceID {
			t.Fatalf("entry %d: expected source %s, got %s", i, entries[i].SourceID, got[i].SourceID)
		}
		if len(got[i].Positions) != len(entries[i].Positions) {
			t.Fatalf("entry %d: expected %d positions, got %d", i, len(entries[i].Positions), len(got[i].Positions))
		}
		for j := range entries[i].Positions {
			if got[i].Positions[j] != entries[i].Positions[j] {
				t.Fatalf("entry %d pos %d: expected %d, got %d", i, j, entries[i].Positions[j], got[i].Positions[j])
			}
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
	bad[1] = typeByte
	bad[2] = versionByte
	if _, err := decodeIndex(testChunkID, bad); err != ErrSignatureMismatch {
		t.Fatalf("expected ErrSignatureMismatch, got %v", err)
	}

	// Wrong type byte.
	bad1b := make([]byte, headerSize)
	bad1b[0] = signatureByte
	bad1b[1] = 'x'
	bad1b[2] = versionByte
	if _, err := decodeIndex(testChunkID, bad1b); err != ErrSignatureMismatch {
		t.Fatalf("expected ErrSignatureMismatch for wrong type byte, got %v", err)
	}

	// Wrong version.
	bad2 := make([]byte, headerSize)
	bad2[0] = signatureByte
	bad2[1] = typeByte
	bad2[2] = 0xFF
	if _, err := decodeIndex(testChunkID, bad2); err != ErrVersionMismatch {
		t.Fatalf("expected ErrVersionMismatch, got %v", err)
	}

	// Chunk ID mismatch.
	data := encodeIndex(testChunkID, nil)
	wrongChunkID := chunk.NewChunkID()
	if _, err := decodeIndex(wrongChunkID, data); err != ErrChunkIDMismatch {
		t.Fatalf("expected ErrChunkIDMismatch, got %v", err)
	}

	// Key count mismatch: header says 1 key but no key data.
	bad3 := encodeIndex(testChunkID, nil)
	// Patch keyCount to 1 (at offset signatureSize+typeSize+versionSize+flagsSize+chunkIDSize).
	bad3[signatureSize+typeSize+versionSize+flagsSize+chunkIDSize] = 1
	if _, err := decodeIndex(testChunkID, bad3); err != ErrKeySizeMismatch {
		t.Fatalf("expected ErrKeySizeMismatch, got %v", err)
	}

	// Posting size mismatch: valid header+key with offset pointing past end.
	src := chunk.NewSourceID()
	bad4 := encodeIndex(testChunkID, []indexsource.IndexEntry{
		{SourceID: src, Positions: []uint64{0}},
	})
	// Truncate the posting blob so positions are missing.
	bad4 = bad4[:headerSize+keyEntrySize]
	if _, err := decodeIndex(testChunkID, bad4); err != ErrPostingSizeMismatch {
		t.Fatalf("expected ErrPostingSizeMismatch, got %v", err)
	}
}

func TestSourceIndexerConcurrentBuild(t *testing.T) {
	src := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), SourceID: src, Raw: []byte("aaa")},
		{IngestTS: gotime.UnixMicro(2), SourceID: src, Raw: []byte("bbb")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()

	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			indexer := NewSourceIndexer(indexDir, manager)
			errs[idx] = indexer.Build(context.Background(), chunkID)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}

	// Verify the final file is valid.
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
	if len(entries[0].Positions) != 2 {
		t.Fatalf("expected 2 positions, got %d", len(entries[0].Positions))
	}
}

func TestSourceIndexerBuildLargePostingList(t *testing.T) {
	src := chunk.NewSourceID()
	const numRecords = 1000
	records := make([]chunk.Record, numRecords)
	for i := range records {
		records[i] = chunk.Record{
			IngestTS: gotime.UnixMicro(int64(i + 1)),
			SourceID: src,
			Raw:      []byte("payload"),
		}
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewSourceIndexer(indexDir, manager)

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
	if len(entries[0].Positions) != numRecords {
		t.Fatalf("expected %d positions, got %d", numRecords, len(entries[0].Positions))
	}

	// Verify positions are in ascending order.
	for i := 1; i < len(entries[0].Positions); i++ {
		if entries[0].Positions[i] <= entries[0].Positions[i-1] {
			t.Fatalf("positions not ascending at index %d: %d <= %d",
				i, entries[0].Positions[i], entries[0].Positions[i-1])
		}
	}
}

func TestSourceIndexerPositionsAscending(t *testing.T) {
	src1 := chunk.NewSourceID()
	src2 := chunk.NewSourceID()
	// Interleave two sources to ensure positions are in cursor-traversal order per source.
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), SourceID: src1, Raw: []byte("aaa")},
		{IngestTS: gotime.UnixMicro(2), SourceID: src2, Raw: []byte("bbb")},
		{IngestTS: gotime.UnixMicro(3), SourceID: src1, Raw: []byte("ccc")},
		{IngestTS: gotime.UnixMicro(4), SourceID: src2, Raw: []byte("ddd")},
		{IngestTS: gotime.UnixMicro(5), SourceID: src1, Raw: []byte("eee")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewSourceIndexer(indexDir, manager)

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

	for _, entry := range entries {
		for i := 1; i < len(entry.Positions); i++ {
			if entry.Positions[i] <= entry.Positions[i-1] {
				t.Fatalf("source %s: positions not ascending at index %d: %d <= %d",
					entry.SourceID, i, entry.Positions[i], entry.Positions[i-1])
			}
		}
	}
}

func TestDecodeExtraTrailingBytes(t *testing.T) {
	testChunkID := chunk.NewChunkID()
	src := chunk.NewSourceID()
	entries := []indexsource.IndexEntry{
		{SourceID: src, Positions: []uint64{0, 64}},
	}

	data := encodeIndex(testChunkID, entries)
	// Append extra trailing bytes.
	data = append(data, 0xDE, 0xAD, 0xBE, 0xEF)

	got, err := decodeIndex(testChunkID, data)
	if err != nil {
		t.Fatalf("decode with trailing bytes: %v", err)
	}

	// The decoder should still produce valid entries; trailing bytes are ignored.
	if len(got) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(got))
	}
	if len(got[0].Positions) != 2 {
		t.Fatalf("expected 2 positions, got %d", len(got[0].Positions))
	}
}

func TestSourceIndexerZeroUUID(t *testing.T) {
	zeroSource := chunk.SourceID(uuid.UUID{})
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), SourceID: zeroSource, Raw: []byte("zero")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewSourceIndexer(indexDir, manager)

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
	if entries[0].SourceID != zeroSource {
		t.Fatalf("expected zero UUID source, got %s", entries[0].SourceID)
	}
	if len(entries[0].Positions) != 1 {
		t.Fatalf("expected 1 position, got %d", len(entries[0].Positions))
	}
}

func TestSourceIndexerMaxUUID(t *testing.T) {
	maxSource := chunk.SourceID(uuid.UUID{
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	})
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), SourceID: maxSource, Raw: []byte("max")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewSourceIndexer(indexDir, manager)

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
	if entries[0].SourceID != maxSource {
		t.Fatalf("expected max UUID source, got %s", entries[0].SourceID)
	}
}

func TestSourceIndexerZeroAndMaxUUIDSortOrder(t *testing.T) {
	zeroSource := chunk.SourceID(uuid.UUID{})
	maxSource := chunk.SourceID(uuid.UUID{
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	})
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), SourceID: maxSource, Raw: []byte("max")},
		{IngestTS: gotime.UnixMicro(2), SourceID: zeroSource, Raw: []byte("zero")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewSourceIndexer(indexDir, manager)

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

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	// The zero UUID should sort before the max UUID (lexicographic string order).
	zeroStr := uuid.UUID(zeroSource).String()
	firstStr := uuid.UUID(entries[0].SourceID).String()
	maxStr := uuid.UUID(maxSource).String()
	lastStr := uuid.UUID(entries[1].SourceID).String()

	if firstStr != zeroStr {
		t.Fatalf("expected zero UUID first, got %s", firstStr)
	}
	if lastStr != maxStr {
		t.Fatalf("expected max UUID last, got %s", lastStr)
	}
}

func TestSourceIndexerBuildUnsealedChunk(t *testing.T) {
	dir := t.TempDir()
	manager, err := chunkfile.NewManager(chunkfile.Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	src := chunk.NewSourceID()
	chunkID, _, err := manager.Append(chunk.Record{IngestTS: gotime.UnixMicro(1), SourceID: src, Raw: []byte("x")})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// Do not seal — Build should reject.
	indexDir := t.TempDir()
	indexer := NewSourceIndexer(indexDir, manager)

	err = indexer.Build(context.Background(), chunkID)
	if err == nil {
		t.Fatal("expected error building index on unsealed chunk, got nil")
	}
	if err != chunk.ErrChunkNotSealed {
		t.Fatalf("expected ErrChunkNotSealed, got %v", err)
	}
}
