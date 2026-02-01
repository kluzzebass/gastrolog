package token

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
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

func TestIndexerBuild(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1000), Attrs: attrs, Raw: []byte("connecting to server")},
		{IngestTS: gotime.UnixMicro(2000), Attrs: attrs, Raw: []byte("connection established")},
		{IngestTS: gotime.UnixMicro(3000), Attrs: attrs, Raw: []byte("server timeout error")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if indexer.Name() != "token" {
		t.Fatalf("expected name %q, got %q", "token", indexer.Name())
	}

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	idxPath := filepath.Join(indexDir, chunkID.String(), indexFileName)
	data, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}

	entries, err := decodeIndex(data)
	if err != nil {
		t.Fatalf("decode index: %v", err)
	}

	// Should have: connecting, to, server (x2), connection, established, timeout, error
	entryMap := make(map[string][]uint64)
	for _, e := range entries {
		entryMap[e.Token] = e.Positions
	}

	if len(entryMap["server"]) != 2 {
		t.Fatalf("server: expected 2 positions, got %d", len(entryMap["server"]))
	}
	if len(entryMap["connecting"]) != 1 {
		t.Fatalf("connecting: expected 1 position, got %d", len(entryMap["connecting"]))
	}
	if len(entryMap["timeout"]) != 1 {
		t.Fatalf("timeout: expected 1 position, got %d", len(entryMap["timeout"]))
	}
}

func TestIndexerIdempotent(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(100), Attrs: attrs, Raw: []byte("alpha beta")},
		{IngestTS: gotime.UnixMicro(200), Attrs: attrs, Raw: []byte("gamma delta")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

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
	entries, err := decodeIndex(data)
	if err != nil {
		t.Fatalf("decode index: %v", err)
	}

	if len(entries) != 4 {
		t.Fatalf("expected 4 entries, got %d", len(entries))
	}
}

func TestIndexerCancelledContext(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(100), Attrs: attrs, Raw: []byte("data")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := indexer.Build(ctx, chunkID)
	if err == nil {
		t.Fatal("expected error from cancelled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestIndexerBuildEmptyChunk(t *testing.T) {
	manager, chunkID := setupChunkManager(t, nil)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	idxPath := filepath.Join(indexDir, chunkID.String(), indexFileName)
	data, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}

	entries, err := decodeIndex(data)
	if err != nil {
		t.Fatalf("decode index: %v", err)
	}

	if len(entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(entries))
	}
}

func TestIndexerBuildSingleToken(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(10), Attrs: attrs, Raw: []byte("hello")},
		{IngestTS: gotime.UnixMicro(20), Attrs: attrs, Raw: []byte("hello")},
		{IngestTS: gotime.UnixMicro(30), Attrs: attrs, Raw: []byte("hello")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	idxPath := filepath.Join(indexDir, chunkID.String(), indexFileName)
	data, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}

	entries, err := decodeIndex(data)
	if err != nil {
		t.Fatalf("decode index: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Token != "hello" {
		t.Fatalf("expected token %q, got %q", "hello", entries[0].Token)
	}
	if len(entries[0].Positions) != 3 {
		t.Fatalf("expected 3 positions, got %d", len(entries[0].Positions))
	}
}

func TestIndexerBuildSingleRecord(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(42), Attrs: attrs, Raw: []byte("only")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	idxPath := filepath.Join(indexDir, chunkID.String(), indexFileName)
	data, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}

	entries, err := decodeIndex(data)
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

func TestIndexerBuildInvalidChunkID(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), Attrs: attrs, Raw: []byte("test")},
	}

	manager, _ := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	bogusID := chunk.NewChunkID()
	err := indexer.Build(context.Background(), bogusID)
	if err == nil {
		t.Fatal("expected error for invalid chunk ID, got nil")
	}
}

func TestIndexerBuildReadOnlyDir(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), Attrs: attrs, Raw: []byte("test")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	if err := os.Chmod(indexDir, 0o444); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	defer os.Chmod(indexDir, 0o755)

	indexer := NewIndexer(indexDir, manager, nil)
	err := indexer.Build(context.Background(), chunkID)
	if err == nil {
		t.Fatal("expected error writing to read-only dir, got nil")
	}
}

func TestSignature(t *testing.T) {
	entries := []index.TokenIndexEntry{
		{Token: "test", Positions: []uint64{0}},
	}

	data := encodeIndex(entries)
	if data[0] != format.Signature {
		t.Fatalf("expected signature byte 0x%02x, got 0x%02x", format.Signature, data[0])
	}
	if data[1] != format.TypeTokenIndex {
		t.Fatalf("expected type byte '%c', got 0x%02x", format.TypeTokenIndex, data[1])
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	entries := []index.TokenIndexEntry{
		{Token: "alpha", Positions: []uint64{0, 128, 256}},
		{Token: "beta", Positions: []uint64{64, 192}},
	}

	data := encodeIndex(entries)
	got, err := decodeIndex(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(got))
	}

	for i := range entries {
		if got[i].Token != entries[i].Token {
			t.Fatalf("entry %d: expected token %q, got %q", i, entries[i].Token, got[i].Token)
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
	data := encodeIndex(nil)
	got, err := decodeIndex(data)
	if err != nil {
		t.Fatalf("decode empty: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(got))
	}
}

func TestDecodeErrors(t *testing.T) {
	// Too small.
	if _, err := decodeIndex([]byte{'i'}); err != ErrIndexTooSmall {
		t.Fatalf("expected ErrIndexTooSmall, got %v", err)
	}

	// Wrong signature.
	bad := make([]byte, headerSize)
	bad[0] = 0xFF
	bad[1] = format.TypeTokenIndex
	bad[2] = currentVersion
	if _, err := decodeIndex(bad); err == nil {
		t.Fatal("expected error for wrong signature, got nil")
	}

	// Wrong type byte.
	bad1b := make([]byte, headerSize)
	bad1b[0] = format.Signature
	bad1b[1] = 'x'
	bad1b[2] = currentVersion
	if _, err := decodeIndex(bad1b); err == nil {
		t.Fatal("expected error for wrong type byte, got nil")
	}

	// Wrong version.
	bad2 := make([]byte, headerSize)
	bad2[0] = format.Signature
	bad2[1] = format.TypeTokenIndex
	bad2[2] = 0xFF
	if _, err := decodeIndex(bad2); err == nil {
		t.Fatal("expected error for wrong version, got nil")
	}

	// Key size mismatch: header says 1 key but no key data.
	bad3 := encodeIndex(nil)
	bad3[format.HeaderSize] = 1
	if _, err := decodeIndex(bad3); err != ErrKeySizeMismatch {
		t.Fatalf("expected ErrKeySizeMismatch, got %v", err)
	}

	// Posting size mismatch: valid header+key with truncated postings.
	bad4 := encodeIndex([]index.TokenIndexEntry{
		{Token: "test", Positions: []uint64{0, 64}},
	})
	// Truncate to remove posting data
	keyStart := headerSize
	tokenLen := 4 // "test"
	keyEntrySize := tokenLenSize + tokenLen + postingOffsetSize + postingCountSize
	bad4 = bad4[:keyStart+keyEntrySize]
	if _, err := decodeIndex(bad4); err != ErrPostingSizeMismatch {
		t.Fatalf("expected ErrPostingSizeMismatch, got %v", err)
	}
}

func TestIndexerConcurrentBuild(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), Attrs: attrs, Raw: []byte("alpha beta")},
		{IngestTS: gotime.UnixMicro(2), Attrs: attrs, Raw: []byte("gamma delta")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()

	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Go(func() {
			indexer := NewIndexer(indexDir, manager, nil)
			errs[i] = indexer.Build(context.Background(), chunkID)
		})
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}

	idxPath := filepath.Join(indexDir, chunkID.String(), indexFileName)
	data, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}
	entries, err := decodeIndex(data)
	if err != nil {
		t.Fatalf("decode index: %v", err)
	}
	if len(entries) != 4 {
		t.Fatalf("expected 4 entries, got %d", len(entries))
	}
}

func TestIndexerBuildLargePostingList(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	const numRecords = 1000
	records := make([]chunk.Record, numRecords)
	for i := range records {
		records[i] = chunk.Record{
			IngestTS: gotime.UnixMicro(int64(i + 1)),
			Attrs:    attrs,
			Raw:      []byte("payload"),
		}
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	idxPath := filepath.Join(indexDir, chunkID.String(), indexFileName)
	data, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}
	entries, err := decodeIndex(data)
	if err != nil {
		t.Fatalf("decode index: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Token != "payload" {
		t.Fatalf("expected token %q, got %q", "payload", entries[0].Token)
	}
	if len(entries[0].Positions) != numRecords {
		t.Fatalf("expected %d positions, got %d", numRecords, len(entries[0].Positions))
	}

	for i := 1; i < len(entries[0].Positions); i++ {
		if entries[0].Positions[i] <= entries[0].Positions[i-1] {
			t.Fatalf("positions not ascending at index %d: %d <= %d",
				i, entries[0].Positions[i], entries[0].Positions[i-1])
		}
	}
}

func TestIndexerPositionsAscending(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), Attrs: attrs, Raw: []byte("word aaa")},
		{IngestTS: gotime.UnixMicro(2), Attrs: attrs, Raw: []byte("bbb ccc")},
		{IngestTS: gotime.UnixMicro(3), Attrs: attrs, Raw: []byte("word ddd")},
		{IngestTS: gotime.UnixMicro(4), Attrs: attrs, Raw: []byte("eee fff")},
		{IngestTS: gotime.UnixMicro(5), Attrs: attrs, Raw: []byte("word ggg")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	idxPath := filepath.Join(indexDir, chunkID.String(), indexFileName)
	data, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}
	entries, err := decodeIndex(data)
	if err != nil {
		t.Fatalf("decode index: %v", err)
	}

	for _, entry := range entries {
		for i := 1; i < len(entry.Positions); i++ {
			if entry.Positions[i] <= entry.Positions[i-1] {
				t.Fatalf("token %s: positions not ascending at index %d: %d <= %d",
					entry.Token, i, entry.Positions[i], entry.Positions[i-1])
			}
		}
	}
}

func TestDecodeExtraTrailingBytes(t *testing.T) {
	entries := []index.TokenIndexEntry{
		{Token: "test", Positions: []uint64{0, 64}},
	}

	data := encodeIndex(entries)
	data = append(data, 0xDE, 0xAD, 0xBE, 0xEF)

	got, err := decodeIndex(data)
	if err != nil {
		t.Fatalf("decode with trailing bytes: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(got))
	}
	if len(got[0].Positions) != 2 {
		t.Fatalf("expected 2 positions, got %d", len(got[0].Positions))
	}
}

func TestIndexerTokensSorted(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), Attrs: attrs, Raw: []byte("zebra alpha mango")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	idxPath := filepath.Join(indexDir, chunkID.String(), indexFileName)
	data, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}
	entries, err := decodeIndex(data)
	if err != nil {
		t.Fatalf("decode index: %v", err)
	}

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	for i := 1; i < len(entries); i++ {
		if entries[i].Token <= entries[i-1].Token {
			t.Fatalf("tokens not sorted at index %d: %q <= %q",
				i, entries[i].Token, entries[i-1].Token)
		}
	}
}

func TestIndexerBuildUnsealedChunk(t *testing.T) {
	dir := t.TempDir()
	manager, err := chunkfile.NewManager(chunkfile.Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	attrs := chunk.Attributes{"source": "test"}
	chunkID, _, err := manager.Append(chunk.Record{IngestTS: gotime.UnixMicro(1), Attrs: attrs, Raw: []byte("test")})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	err = indexer.Build(context.Background(), chunkID)
	if err == nil {
		t.Fatal("expected error building index on unsealed chunk, got nil")
	}
	if err != chunk.ErrChunkNotSealed {
		t.Fatalf("expected ErrChunkNotSealed, got %v", err)
	}
}

func TestLoadIndex(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), Attrs: attrs, Raw: []byte("one two")},
		{IngestTS: gotime.UnixMicro(2), Attrs: attrs, Raw: []byte("two three")},
		{IngestTS: gotime.UnixMicro(3), Attrs: attrs, Raw: []byte("one three")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

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

	entryMap := make(map[string][]uint64)
	for _, e := range entries {
		entryMap[e.Token] = e.Positions
	}
	if len(entryMap["one"]) != 2 {
		t.Fatalf("one: expected 2 positions, got %d", len(entryMap["one"]))
	}
	if len(entryMap["two"]) != 2 {
		t.Fatalf("two: expected 2 positions, got %d", len(entryMap["two"]))
	}
	if len(entryMap["three"]) != 2 {
		t.Fatalf("three: expected 2 positions, got %d", len(entryMap["three"]))
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

func TestIndexerTokenDeduplication(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), Attrs: attrs, Raw: []byte("error error error")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	idxPath := filepath.Join(indexDir, chunkID.String(), indexFileName)
	data, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}

	entries, err := decodeIndex(data)
	if err != nil {
		t.Fatalf("decode index: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	// Same token in same record should only appear once
	if len(entries[0].Positions) != 1 {
		t.Fatalf("expected 1 position (deduplicated), got %d", len(entries[0].Positions))
	}
}

func TestIndexerCaseFolding(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), Attrs: attrs, Raw: []byte("ERROR Error error")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	idxPath := filepath.Join(indexDir, chunkID.String(), indexFileName)
	data, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}

	entries, err := decodeIndex(data)
	if err != nil {
		t.Fatalf("decode index: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Token != "error" {
		t.Fatalf("expected token %q, got %q", "error", entries[0].Token)
	}
}

func TestIndexerShortTokensSkipped(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		// "a" is 1 char (skipped), "zz" and "zzz" are 2+ chars and not hex
		{IngestTS: gotime.UnixMicro(1), Attrs: attrs, Raw: []byte("a zz zzz")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	idxPath := filepath.Join(indexDir, chunkID.String(), indexFileName)
	data, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}

	entries, err := decodeIndex(data)
	if err != nil {
		t.Fatalf("decode index: %v", err)
	}

	// "a" should be skipped (1 char), "zz" and "zzz" kept
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
}

func TestOpenReader(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), Attrs: attrs, Raw: []byte("alpha beta gamma")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	reader, err := Open(indexDir, chunkID)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	positions, found := reader.Lookup("beta")
	if !found {
		t.Fatal("expected to find token 'beta'")
	}
	if len(positions) != 1 {
		t.Fatalf("expected 1 position, got %d", len(positions))
	}

	_, found = reader.Lookup("notfound")
	if found {
		t.Fatal("expected not to find token 'notfound'")
	}
}

func TestIndexerHighBytesAreDelimiters(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	// High bytes (UTF-8) are now delimiters, so "über" becomes "ber" and "größe" becomes "gr" and "e"
	// Only ASCII alphanumeric, underscore, and hyphen are token characters
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), Attrs: attrs, Raw: []byte("über größe")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	entries, err := LoadIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("load index: %v", err)
	}

	// "über" -> "ber" (3 chars, 'g' not hex -> kept)
	// "größe" -> "gr" (2 chars, 'g' not hex -> kept), "e" (1 char -> skipped)
	// So "ber" and "gr" should be indexed
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d: %v", len(entries), entries)
	}

	// Verify we can look them up via reader
	reader, err := Open(indexDir, chunkID)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	_, found := reader.Lookup("ber")
	if !found {
		t.Fatal("expected to find token 'ber'")
	}
	_, found = reader.Lookup("gr")
	if !found {
		t.Fatal("expected to find token 'gr'")
	}
}

func TestIndexerLongTokenTruncated(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	// Create a long token (200 chars) using 'z' (not hex)
	// Max token length is 16, so it should be truncated
	longInput := ""
	for i := 0; i < 200; i++ {
		longInput += "z"
	}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), Attrs: attrs, Raw: []byte(longInput)},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	entries, err := LoadIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("load index: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	// Token should be truncated to 16 characters
	if len(entries[0].Token) != 16 {
		t.Fatalf("expected token length 16, got %d", len(entries[0].Token))
	}
}

func TestOpenReaderLookupFirstLast(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), Attrs: attrs, Raw: []byte("aardvark middle zebra")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	reader, err := Open(indexDir, chunkID)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// First entry (alphabetically)
	_, found := reader.Lookup("aardvark")
	if !found {
		t.Fatal("expected to find first token 'aardvark'")
	}

	// Last entry
	_, found = reader.Lookup("zebra")
	if !found {
		t.Fatal("expected to find last token 'zebra'")
	}

	// Middle entry
	_, found = reader.Lookup("middle")
	if !found {
		t.Fatal("expected to find middle token 'middle'")
	}
}
