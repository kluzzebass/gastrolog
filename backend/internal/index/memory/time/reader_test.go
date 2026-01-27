package time

import (
	"context"
	"testing"
	gotime "time"

	"github.com/kluzzebass/gastrolog/internal/chunk"
	chunkmemory "github.com/kluzzebass/gastrolog/internal/chunk/memory"
	"github.com/kluzzebass/gastrolog/internal/index"
)

func setupReaderTest(t *testing.T, records []chunk.Record, sparsity int) (*Indexer, chunk.ChunkID) {
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

	indexer := NewIndexer(manager, sparsity)
	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}
	return indexer, chunkID
}

func TestOpenAndFindStartRoundTrip(t *testing.T) {
	src := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1000), SourceID: src, Raw: []byte("a")},
		{IngestTS: gotime.UnixMicro(2000), SourceID: src, Raw: []byte("b")},
		{IngestTS: gotime.UnixMicro(3000), SourceID: src, Raw: []byte("c")},
	}
	indexer, chunkID := setupReaderTest(t, records, 1)

	reader, err := Open(indexer, chunkID)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Before all entries.
	ref, ok := reader.FindStart(gotime.UnixMicro(500))
	if ok {
		t.Fatalf("expected ok=false, got ref %+v", ref)
	}

	// Between entries.
	ref, ok = reader.FindStart(gotime.UnixMicro(2500))
	if !ok {
		t.Fatal("expected ok=true")
	}
	if ref.ChunkID != chunkID {
		t.Fatalf("expected chunkID %s, got %s", chunkID, ref.ChunkID)
	}

	// After all entries.
	ref, ok = reader.FindStart(gotime.UnixMicro(9999))
	if !ok {
		t.Fatal("expected ok=true")
	}
}

func TestOpenNotBuilt(t *testing.T) {
	manager, err := chunkmemory.NewManager(chunkmemory.Config{})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	indexer := NewIndexer(manager, 1)
	bogusID := chunk.NewChunkID()

	_, err = Open(indexer, bogusID)
	if err != index.ErrIndexNotFound {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}
}

func TestOpenEmptyChunk(t *testing.T) {
	indexer, chunkID := setupReaderTest(t, nil, 1)

	reader, err := Open(indexer, chunkID)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	ref, ok := reader.FindStart(gotime.UnixMicro(1000))
	if ok {
		t.Fatalf("expected ok=false for empty index, got ref %+v", ref)
	}
	if ref != (chunk.RecordRef{}) {
		t.Fatalf("expected zero RecordRef, got %+v", ref)
	}
}
