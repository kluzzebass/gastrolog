package time

import (
	"context"
	"testing"
	gotime "time"

	"gastrolog/internal/chunk"
	chunkmemory "gastrolog/internal/chunk/memory"
)

func setupChunkManager(t *testing.T, records []chunk.Record) (chunk.ChunkManager, chunk.ChunkID) {
	t.Helper()
	callIdx := 0
	manager, err := chunkmemory.NewManager(chunkmemory.Config{
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
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1000), Attrs: attrs, Raw: []byte("one")},
		{IngestTS: gotime.UnixMicro(2000), Attrs: attrs, Raw: []byte("two")},
		{IngestTS: gotime.UnixMicro(3000), Attrs: attrs, Raw: []byte("three")},
		{IngestTS: gotime.UnixMicro(4000), Attrs: attrs, Raw: []byte("four")},
		{IngestTS: gotime.UnixMicro(5000), Attrs: attrs, Raw: []byte("five")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager, 2)

	if indexer.Name() != "time" {
		t.Fatalf("expected name %q, got %q", "time", indexer.Name())
	}

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	entries, ok := indexer.Get(chunkID)
	if !ok {
		t.Fatal("expected index to exist after build")
	}

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
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(100), Attrs: attrs, Raw: []byte("alpha")},
		{IngestTS: gotime.UnixMicro(200), Attrs: attrs, Raw: []byte("beta")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager, 1)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("first build: %v", err)
	}
	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("second build: %v", err)
	}

	entries, ok := indexer.Get(chunkID)
	if !ok {
		t.Fatal("expected index to exist after build")
	}

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
}

func TestIndexerCancelledContext(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(100), Attrs: attrs, Raw: []byte("data")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager, 1)

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

func TestIndexerGetUnbuilt(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(100), Attrs: attrs, Raw: []byte("data")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager, 1)

	_, ok := indexer.Get(chunkID)
	if ok {
		t.Fatal("expected Get to return false for unbuilt chunk")
	}
}

func TestIndexerBuildEmptyChunk(t *testing.T) {
	manager, chunkID := setupChunkManager(t, nil)
	indexer := NewIndexer(manager, 1)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	entries, ok := indexer.Get(chunkID)
	if !ok {
		t.Fatal("expected index to exist after build")
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(entries))
	}
}

func TestIndexerBuildSingleRecord(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(42), Attrs: attrs, Raw: []byte("only")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager, 10)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	entries, ok := indexer.Get(chunkID)
	if !ok {
		t.Fatal("expected index to exist after build")
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

func TestIndexerBuildRecordPos(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), Attrs: attrs, Raw: []byte("aaa")},
		{IngestTS: gotime.UnixMicro(2), Attrs: attrs, Raw: []byte("bbb")},
		{IngestTS: gotime.UnixMicro(3), Attrs: attrs, Raw: []byte("ccc")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager, 1)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	entries, ok := indexer.Get(chunkID)
	if !ok {
		t.Fatal("expected index to exist after build")
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	// Memory manager uses slice indices: 0, 1, 2.
	for i, e := range entries {
		if e.RecordPos != uint64(i) {
			t.Fatalf("entry %d: expected pos %d, got %d", i, i, e.RecordPos)
		}
	}
}

func TestIndexerBuildInvalidChunkID(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), Attrs: attrs, Raw: []byte("x")},
	}

	manager, _ := setupChunkManager(t, records)
	indexer := NewIndexer(manager, 1)

	bogusID := chunk.NewChunkID()
	err := indexer.Build(context.Background(), bogusID)
	if err == nil {
		t.Fatal("expected error for invalid chunk ID, got nil")
	}
}

func TestIndexerCancelledContextNoPartialData(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(100), Attrs: attrs, Raw: []byte("data")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager, 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_ = indexer.Build(ctx, chunkID)

	_, ok := indexer.Get(chunkID)
	if ok {
		t.Fatal("expected no index after failed build")
	}
}

func TestIndexerMultipleChunks(t *testing.T) {
	writeTSValues := []gotime.Time{
		gotime.UnixMicro(100),
		gotime.UnixMicro(200),
		gotime.UnixMicro(300),
	}
	callIdx := 0
	manager, err := chunkmemory.NewManager(chunkmemory.Config{
		Now: func() gotime.Time {
			ts := writeTSValues[callIdx]
			callIdx++
			return ts
		},
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	attrs := chunk.Attributes{"source": "test"}

	// First chunk.
	id1, _, err := manager.Append(chunk.Record{IngestTS: gotime.UnixMicro(100), Attrs: attrs, Raw: []byte("a")})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Second chunk.
	id2, _, err := manager.Append(chunk.Record{IngestTS: gotime.UnixMicro(200), Attrs: attrs, Raw: []byte("b")})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	_, _, err = manager.Append(chunk.Record{IngestTS: gotime.UnixMicro(300), Attrs: attrs, Raw: []byte("c")})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	indexer := NewIndexer(manager, 1)

	if err := indexer.Build(context.Background(), id1); err != nil {
		t.Fatalf("build chunk 1: %v", err)
	}
	if err := indexer.Build(context.Background(), id2); err != nil {
		t.Fatalf("build chunk 2: %v", err)
	}

	entries1, ok := indexer.Get(id1)
	if !ok {
		t.Fatal("expected index for chunk 1")
	}
	if len(entries1) != 1 {
		t.Fatalf("chunk 1: expected 1 entry, got %d", len(entries1))
	}
	if !entries1[0].Timestamp.Equal(gotime.UnixMicro(100)) {
		t.Fatalf("chunk 1: expected timestamp %v, got %v", gotime.UnixMicro(100), entries1[0].Timestamp)
	}

	entries2, ok := indexer.Get(id2)
	if !ok {
		t.Fatal("expected index for chunk 2")
	}
	if len(entries2) != 2 {
		t.Fatalf("chunk 2: expected 2 entries, got %d", len(entries2))
	}
	if !entries2[0].Timestamp.Equal(gotime.UnixMicro(200)) {
		t.Fatalf("chunk 2 entry 0: expected timestamp %v, got %v", gotime.UnixMicro(200), entries2[0].Timestamp)
	}
	if !entries2[1].Timestamp.Equal(gotime.UnixMicro(300)) {
		t.Fatalf("chunk 2 entry 1: expected timestamp %v, got %v", gotime.UnixMicro(300), entries2[1].Timestamp)
	}
}

func TestIndexerBuildUnsealedChunk(t *testing.T) {
	callIdx := 0
	writeTSValues := []gotime.Time{gotime.UnixMicro(1)}
	manager, err := chunkmemory.NewManager(chunkmemory.Config{
		Now: func() gotime.Time {
			ts := writeTSValues[callIdx]
			callIdx++
			return ts
		},
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	attrs := chunk.Attributes{"source": "test"}
	chunkID, _, err := manager.Append(chunk.Record{IngestTS: gotime.UnixMicro(1), Attrs: attrs, Raw: []byte("x")})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	indexer := NewIndexer(manager, 1)

	err = indexer.Build(context.Background(), chunkID)
	if err == nil {
		t.Fatal("expected error building index on unsealed chunk, got nil")
	}
	if err != chunk.ErrChunkNotSealed {
		t.Fatalf("expected ErrChunkNotSealed, got %v", err)
	}
}
