package source

import (
	"context"
	"testing"
	gotime "time"

	"gastrolog/internal/chunk"
	chunkmemory "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
)

func setupReaderTest(t *testing.T, records []chunk.Record) (*Indexer, chunk.ChunkID) {
	t.Helper()
	manager, err := chunkmemory.NewManager(chunkmemory.Config{})
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
	chunkID := metas[0].ID

	indexer := NewIndexer(manager)
	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}
	return indexer, chunkID
}

func TestOpenAndLookupRoundTrip(t *testing.T) {
	src1 := chunk.NewSourceID()
	src2 := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1000), SourceID: src1, Raw: []byte("a")},
		{IngestTS: gotime.UnixMicro(2000), SourceID: src2, Raw: []byte("b")},
		{IngestTS: gotime.UnixMicro(3000), SourceID: src1, Raw: []byte("c")},
	}
	indexer, chunkID := setupReaderTest(t, records)

	reader, err := Open(indexer, chunkID)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// src1 has 2 records.
	pos1, ok := reader.Lookup(src1)
	if !ok {
		t.Fatal("expected to find src1")
	}
	if len(pos1) != 2 {
		t.Fatalf("src1: expected 2 positions, got %d", len(pos1))
	}

	// src2 has 1 record.
	pos2, ok := reader.Lookup(src2)
	if !ok {
		t.Fatal("expected to find src2")
	}
	if len(pos2) != 1 {
		t.Fatalf("src2: expected 1 position, got %d", len(pos2))
	}

	// Missing source.
	_, ok = reader.Lookup(chunk.NewSourceID())
	if ok {
		t.Fatal("expected ok=false for missing source")
	}
}

func TestOpenNotBuilt(t *testing.T) {
	manager, err := chunkmemory.NewManager(chunkmemory.Config{})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	indexer := NewIndexer(manager)
	bogusID := chunk.NewChunkID()

	_, err = Open(indexer, bogusID)
	if err != index.ErrIndexNotFound {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}
}

func TestOpenEmptyChunk(t *testing.T) {
	indexer, chunkID := setupReaderTest(t, nil)

	reader, err := Open(indexer, chunkID)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	_, ok := reader.Lookup(chunk.NewSourceID())
	if ok {
		t.Fatal("expected ok=false for empty index")
	}
}
