package memory

import (
	"testing"
	"time"

	"github.com/kluzzebass/gastrolog/internal/chunk"
)

func TestMemoryChunkManagerAppendSealOpenReader(t *testing.T) {
	manager, err := NewManager(Config{})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	sourceID := chunk.NewSourceID()
	record := chunk.Record{IngestTS: time.UnixMicro(100), SourceID: sourceID, Raw: []byte("alpha")}
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
	if gotUnsealed.SourceID != record.SourceID {
		t.Fatalf("unsealed: expected source id %s got %s", record.SourceID.String(), gotUnsealed.SourceID.String())
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
	if got.SourceID != record.SourceID {
		t.Fatalf("expected source id %s got %s", record.SourceID.String(), got.SourceID.String())
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
