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

	if _, err := manager.OpenCursor(chunkID); err != chunk.ErrChunkNotSealed {
		t.Fatalf("expected not sealed error, got %v", err)
	}

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
