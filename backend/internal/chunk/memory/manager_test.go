package memory

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

func TestMemoryChunkManagerAppendSealOpenReader(t *testing.T) {
	manager, err := NewManager(Config{})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	attrs := chunk.Attributes{"source": "test"}
	record := chunk.Record{IngestTS: time.UnixMicro(100), Attrs: attrs, Raw: []byte("alpha")}
	chunkID, offset, err := manager.Append(record)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if offset != 0 {
		t.Fatalf("expected offset 0, got %d", offset)
	}

	// Cursor on unsealed chunk should work.
	unsealedCursor, err := manager.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("open cursor on unsealed chunk: %v", err)
	}
	gotUnsealed, _, err := unsealedCursor.Next()
	if err != nil {
		t.Fatalf("next on unsealed cursor: %v", err)
	}
	if gotUnsealed.Attrs["source"] != record.Attrs["source"] {
		t.Fatalf("unsealed: expected attrs %v got %v", record.Attrs, gotUnsealed.Attrs)
	}
	unsealedCursor.Close()

	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	reader, err := manager.OpenCursor(chunkID)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	got, _, err := reader.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if got.Attrs["source"] != record.Attrs["source"] {
		t.Fatalf("expected attrs %v got %v", record.Attrs, got.Attrs)
	}
	if _, _, err := reader.Next(); err != chunk.ErrNoMoreRecords {
		t.Fatalf("expected end of records, got %v", err)
	}
}

func TestMemoryChunkManagerEmptyChunk(t *testing.T) {
	manager, err := NewManager(Config{})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	// Seal with no prior append creates an empty sealed chunk.
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
	meta := metas[0]
	if !meta.Sealed {
		t.Fatal("expected chunk to be sealed")
	}

	cursor, err := manager.OpenCursor(meta.ID)
	if err != nil {
		t.Fatalf("open cursor: %v", err)
	}
	defer cursor.Close()

	if _, _, err := cursor.Next(); err != chunk.ErrNoMoreRecords {
		t.Fatalf("expected ErrNoMoreRecords, got %v", err)
	}

	if _, _, err := cursor.Prev(); err != chunk.ErrNoMoreRecords {
		t.Fatalf("expected ErrNoMoreRecords from Prev, got %v", err)
	}
}
