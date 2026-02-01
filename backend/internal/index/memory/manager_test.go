package memory

import (
	"context"
	"testing"
	gotime "time"

	"gastrolog/internal/chunk"
	chunkmemory "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	memorytime "gastrolog/internal/index/memory/time"
)

func setupChunkManager(t *testing.T, records []chunk.Record) (chunk.ChunkManager, chunk.ChunkID) {
	t.Helper()
	// recordIdx tracks which record's timestamp to return for WriteTS.
	// skipNext handles the initial Now() call for createdAt (before first Append).
	recordIdx := 0
	skipNext := true
	manager, err := chunkmemory.NewManager(chunkmemory.Config{
		Now: func() gotime.Time {
			if skipNext {
				skipNext = false
				return gotime.UnixMicro(0) // createdAt value (ignored by tests)
			}
			if recordIdx < len(records) {
				ts := records[recordIdx].IngestTS
				recordIdx++
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

func setupManager(t *testing.T, records []chunk.Record) (*Manager, chunk.ChunkManager, chunk.ChunkID) {
	t.Helper()
	chunkMgr, chunkID := setupChunkManager(t, records)
	timeIndexer := memorytime.NewIndexer(chunkMgr, 1)
	mgr := NewManager(
		[]index.Indexer{timeIndexer},
		timeIndexer,
		nil,
		nil,
		nil,
		nil,
	)
	return mgr, chunkMgr, chunkID
}

func testRecords() []chunk.Record {
	attrs1 := chunk.Attributes{"source": "src1"}
	attrs2 := chunk.Attributes{"source": "src2"}
	return []chunk.Record{
		{IngestTS: gotime.UnixMicro(1000), Attrs: attrs1, Raw: []byte("one")},
		{IngestTS: gotime.UnixMicro(2000), Attrs: attrs2, Raw: []byte("two")},
		{IngestTS: gotime.UnixMicro(3000), Attrs: attrs1, Raw: []byte("three")},
	}
}

func TestBuildIndexes(t *testing.T) {
	mgr, _, chunkID := setupManager(t, testRecords())

	if err := mgr.BuildIndexes(context.Background(), chunkID); err != nil {
		t.Fatalf("build indexes: %v", err)
	}
}

func TestBuildIndexesCancelledContext(t *testing.T) {
	mgr, _, chunkID := setupManager(t, testRecords())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := mgr.BuildIndexes(ctx, chunkID)
	if err == nil {
		t.Fatal("expected error from cancelled context, got nil")
	}
}

func TestBuildIndexesUnsealedChunk(t *testing.T) {
	chunkMgr, err := chunkmemory.NewManager(chunkmemory.Config{})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	attrs := chunk.Attributes{"source": "test"}
	chunkID, _, err := chunkMgr.Append(chunk.Record{IngestTS: gotime.UnixMicro(1), Attrs: attrs, Raw: []byte("x")})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	timeIndexer := memorytime.NewIndexer(chunkMgr, 1)
	mgr := NewManager(
		[]index.Indexer{timeIndexer},
		timeIndexer,
		nil,
		nil,
		nil,
		nil,
	)

	err = mgr.BuildIndexes(context.Background(), chunkID)
	if err == nil {
		t.Fatal("expected error building indexes on unsealed chunk, got nil")
	}
}

func TestOpenTimeIndex(t *testing.T) {
	mgr, _, chunkID := setupManager(t, testRecords())

	if err := mgr.BuildIndexes(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	idx, err := mgr.OpenTimeIndex(chunkID)
	if err != nil {
		t.Fatalf("open time index: %v", err)
	}

	entries := idx.Entries()
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	expectedTS := []gotime.Time{gotime.UnixMicro(1000), gotime.UnixMicro(2000), gotime.UnixMicro(3000)}
	for i, e := range entries {
		if !e.Timestamp.Equal(expectedTS[i]) {
			t.Fatalf("entry %d: expected %v, got %v", i, expectedTS[i], e.Timestamp)
		}
	}
}

func TestOpenTimeIndexNotBuilt(t *testing.T) {
	chunkMgr, err := chunkmemory.NewManager(chunkmemory.Config{})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	timeIndexer := memorytime.NewIndexer(chunkMgr, 1)
	mgr := NewManager(
		[]index.Indexer{timeIndexer},
		timeIndexer,
		nil,
		nil,
		nil,
		nil,
	)

	_, err = mgr.OpenTimeIndex(chunk.NewChunkID())
	if err != index.ErrIndexNotFound {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}
}

func TestBuildAndOpenRoundTrip(t *testing.T) {
	records := testRecords()
	mgr, _, chunkID := setupManager(t, records)

	if err := mgr.BuildIndexes(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	// Time index round-trip.
	timeIdx, err := mgr.OpenTimeIndex(chunkID)
	if err != nil {
		t.Fatalf("open time index: %v", err)
	}
	timeEntries := timeIdx.Entries()
	if len(timeEntries) != 3 {
		t.Fatalf("time: expected 3 entries, got %d", len(timeEntries))
	}
	for i := 1; i < len(timeEntries); i++ {
		if !timeEntries[i].Timestamp.After(timeEntries[i-1].Timestamp) {
			t.Fatalf("time entries not in order at index %d", i)
		}
	}
}
