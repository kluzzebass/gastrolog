package memory

import (
	"context"
	"testing"
	gotime "time"

	"github.com/kluzzebass/gastrolog/internal/chunk"
	chunkmemory "github.com/kluzzebass/gastrolog/internal/chunk/memory"
	"github.com/kluzzebass/gastrolog/internal/index"
	memorysource "github.com/kluzzebass/gastrolog/internal/index/memory/source"
	memorytime "github.com/kluzzebass/gastrolog/internal/index/memory/time"
)

func setupChunkManager(t *testing.T, records []chunk.Record) (chunk.ChunkManager, chunk.ChunkID) {
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
	return manager, metas[0].ID
}

func setupManager(t *testing.T, records []chunk.Record) (*Manager, chunk.ChunkManager, chunk.ChunkID) {
	t.Helper()
	chunkMgr, chunkID := setupChunkManager(t, records)
	timeIndexer := memorytime.NewIndexer(chunkMgr, 1)
	sourceIndexer := memorysource.NewIndexer(chunkMgr)
	mgr := NewManager(
		[]index.Indexer{timeIndexer, sourceIndexer},
		timeIndexer,
		sourceIndexer,
	)
	return mgr, chunkMgr, chunkID
}

func testRecords() []chunk.Record {
	src1 := chunk.NewSourceID()
	src2 := chunk.NewSourceID()
	return []chunk.Record{
		{IngestTS: gotime.UnixMicro(1000), SourceID: src1, Raw: []byte("one")},
		{IngestTS: gotime.UnixMicro(2000), SourceID: src2, Raw: []byte("two")},
		{IngestTS: gotime.UnixMicro(3000), SourceID: src1, Raw: []byte("three")},
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

	src := chunk.NewSourceID()
	chunkID, _, err := chunkMgr.Append(chunk.Record{IngestTS: gotime.UnixMicro(1), SourceID: src, Raw: []byte("x")})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	timeIndexer := memorytime.NewIndexer(chunkMgr, 1)
	sourceIndexer := memorysource.NewIndexer(chunkMgr)
	mgr := NewManager(
		[]index.Indexer{timeIndexer, sourceIndexer},
		timeIndexer,
		sourceIndexer,
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
	sourceIndexer := memorysource.NewIndexer(chunkMgr)
	mgr := NewManager(
		[]index.Indexer{timeIndexer, sourceIndexer},
		timeIndexer,
		sourceIndexer,
	)

	_, err = mgr.OpenTimeIndex(chunk.NewChunkID())
	if err != index.ErrIndexNotFound {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}
}

func TestOpenSourceIndex(t *testing.T) {
	mgr, _, chunkID := setupManager(t, testRecords())

	if err := mgr.BuildIndexes(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	idx, err := mgr.OpenSourceIndex(chunkID)
	if err != nil {
		t.Fatalf("open source index: %v", err)
	}

	entries := idx.Entries()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
}

func TestOpenSourceIndexNotBuilt(t *testing.T) {
	chunkMgr, err := chunkmemory.NewManager(chunkmemory.Config{})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	timeIndexer := memorytime.NewIndexer(chunkMgr, 1)
	sourceIndexer := memorysource.NewIndexer(chunkMgr)
	mgr := NewManager(
		[]index.Indexer{timeIndexer, sourceIndexer},
		timeIndexer,
		sourceIndexer,
	)

	_, err = mgr.OpenSourceIndex(chunk.NewChunkID())
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

	// Source index round-trip.
	sourceIdx, err := mgr.OpenSourceIndex(chunkID)
	if err != nil {
		t.Fatalf("open source index: %v", err)
	}
	sourceEntries := sourceIdx.Entries()
	if len(sourceEntries) != 2 {
		t.Fatalf("source: expected 2 entries, got %d", len(sourceEntries))
	}
	totalPositions := 0
	for _, e := range sourceEntries {
		totalPositions += len(e.Positions)
	}
	if totalPositions != 3 {
		t.Fatalf("source: expected 3 total positions, got %d", totalPositions)
	}
}
