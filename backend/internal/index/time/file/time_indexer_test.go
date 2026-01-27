package file

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	gotime "time"

	"github.com/kluzzebass/gastrolog/internal/chunk"
	chunkfile "github.com/kluzzebass/gastrolog/internal/chunk/file"
	indextime "github.com/kluzzebass/gastrolog/internal/index/time"
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

func TestTimeIndexerBuild(t *testing.T) {
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
	indexer := NewTimeIndexer(indexDir, manager, 2)

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

	entries, err := decodeIndex(data)
	if err != nil {
		t.Fatalf("decode index: %v", err)
	}

	// With sparsity=2 and 5 records: record 0 (always first), record 2, record 4.
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	expectedTS := []int64{1000, 3000, 5000}
	for i, e := range entries {
		if e.TimestampUS != expectedTS[i] {
			t.Fatalf("entry %d: expected timestamp %d, got %d", i, expectedTS[i], e.TimestampUS)
		}
	}
}

func TestTimeIndexerIdempotent(t *testing.T) {
	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(100), SourceID: sourceID, Raw: []byte("alpha")},
		{IngestTS: gotime.UnixMicro(200), SourceID: sourceID, Raw: []byte("beta")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewTimeIndexer(indexDir, manager, 1)

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

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
}

func TestTimeIndexerCancelledContext(t *testing.T) {
	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(100), SourceID: sourceID, Raw: []byte("data")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewTimeIndexer(indexDir, manager, 1)

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

func TestTimeIndexerBuildEmptyChunk(t *testing.T) {
	manager, chunkID := setupChunkManager(t, nil)
	indexDir := t.TempDir()
	indexer := NewTimeIndexer(indexDir, manager, 1)

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

func TestTimeIndexerBuildSingleRecord(t *testing.T) {
	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(42), SourceID: sourceID, Raw: []byte("only")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewTimeIndexer(indexDir, manager, 10)

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
	if entries[0].TimestampUS != 42 {
		t.Fatalf("expected timestamp 42, got %d", entries[0].TimestampUS)
	}
	if entries[0].RecordPos != 0 {
		t.Fatalf("expected record pos 0, got %d", entries[0].RecordPos)
	}
}

func TestTimeIndexerBuildSparsityOne(t *testing.T) {
	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(10), SourceID: sourceID, Raw: []byte("a")},
		{IngestTS: gotime.UnixMicro(20), SourceID: sourceID, Raw: []byte("b")},
		{IngestTS: gotime.UnixMicro(30), SourceID: sourceID, Raw: []byte("c")},
		{IngestTS: gotime.UnixMicro(40), SourceID: sourceID, Raw: []byte("d")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewTimeIndexer(indexDir, manager, 1)

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

	if len(entries) != 4 {
		t.Fatalf("expected 4 entries, got %d", len(entries))
	}

	expectedTS := []int64{10, 20, 30, 40}
	for i, e := range entries {
		if e.TimestampUS != expectedTS[i] {
			t.Fatalf("entry %d: expected timestamp %d, got %d", i, expectedTS[i], e.TimestampUS)
		}
	}
}

func TestTimeIndexerBuildRecordPos(t *testing.T) {
	sourceID := chunk.NewSourceID()
	// All payloads same length so record sizes are uniform.
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), SourceID: sourceID, Raw: []byte("aaa")},
		{IngestTS: gotime.UnixMicro(2), SourceID: sourceID, Raw: []byte("bbb")},
		{IngestTS: gotime.UnixMicro(3), SourceID: sourceID, Raw: []byte("ccc")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	// sparsity=1 to capture every record's position.
	indexer := NewTimeIndexer(indexDir, manager, 1)

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

func TestTimeIndexerBuildInvalidChunkID(t *testing.T) {
	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), SourceID: sourceID, Raw: []byte("x")},
	}

	manager, _ := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewTimeIndexer(indexDir, manager, 1)

	bogusID := chunk.NewChunkID()
	err := indexer.Build(context.Background(), bogusID)
	if err == nil {
		t.Fatal("expected error for invalid chunk ID, got nil")
	}
}

func TestTimeIndexerBuildReadOnlyDir(t *testing.T) {
	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), SourceID: sourceID, Raw: []byte("x")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	// Make the directory read-only so MkdirAll or WriteFile fails.
	if err := os.Chmod(indexDir, 0o444); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	defer os.Chmod(indexDir, 0o755)

	indexer := NewTimeIndexer(indexDir, manager, 1)
	err := indexer.Build(context.Background(), chunkID)
	if err == nil {
		t.Fatal("expected error writing to read-only dir, got nil")
	}
}

func TestSignature(t *testing.T) {
	entries := []indextime.IndexEntry{
		{TimestampUS: 1000, RecordPos: 0},
	}

	data := encodeIndex(entries)
	if data[0] != signatureByte {
		t.Fatalf("expected signature byte 0x%02x, got 0x%02x", signatureByte, data[0])
	}
	if data[1] != typeByte {
		t.Fatalf("expected type byte '%c', got 0x%02x", typeByte, data[1])
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	entries := []indextime.IndexEntry{
		{TimestampUS: 1000, RecordPos: 0},
		{TimestampUS: 2000, RecordPos: 128},
		{TimestampUS: 3000, RecordPos: 256},
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
		if got[i] != entries[i] {
			t.Fatalf("entry %d: expected %+v, got %+v", i, entries[i], got[i])
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
	bad[1] = typeByte
	bad[2] = versionByte
	if _, err := decodeIndex(bad); err != ErrSignatureMismatch {
		t.Fatalf("expected ErrSignatureMismatch, got %v", err)
	}

	// Wrong type byte.
	bad1b := make([]byte, headerSize)
	bad1b[0] = signatureByte
	bad1b[1] = 'x'
	bad1b[2] = versionByte
	if _, err := decodeIndex(bad1b); err != ErrSignatureMismatch {
		t.Fatalf("expected ErrSignatureMismatch for wrong type byte, got %v", err)
	}

	// Wrong version.
	bad2 := make([]byte, headerSize)
	bad2[0] = signatureByte
	bad2[1] = typeByte
	bad2[2] = 0xFF
	if _, err := decodeIndex(bad2); err != ErrVersionMismatch {
		t.Fatalf("expected ErrVersionMismatch, got %v", err)
	}

	// Count mismatch: header says 1 entry but no entry data.
	bad3 := make([]byte, headerSize)
	bad3[0] = signatureByte
	bad3[1] = typeByte
	bad3[2] = versionByte
	bad3[3] = flagsByte
	bad3[4] = 1 // entry_count = 1
	if _, err := decodeIndex(bad3); err != ErrEntrySizeMismatch {
		t.Fatalf("expected ErrEntrySizeMismatch, got %v", err)
	}
}
