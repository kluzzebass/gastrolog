package source

import (
	"context"
	"sync"
	"testing"
	gotime "time"

	"github.com/google/uuid"
	"github.com/kluzzebass/gastrolog/internal/chunk"
	chunkmemory "github.com/kluzzebass/gastrolog/internal/chunk/memory"
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

func TestIndexerBuild(t *testing.T) {
	src1 := chunk.NewSourceID()
	src2 := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1000), SourceID: src1, Raw: []byte("one")},
		{IngestTS: gotime.UnixMicro(2000), SourceID: src2, Raw: []byte("two")},
		{IngestTS: gotime.UnixMicro(3000), SourceID: src1, Raw: []byte("three")},
		{IngestTS: gotime.UnixMicro(4000), SourceID: src2, Raw: []byte("four")},
		{IngestTS: gotime.UnixMicro(5000), SourceID: src1, Raw: []byte("five")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	if indexer.Name() != "source" {
		t.Fatalf("expected name %q, got %q", "source", indexer.Name())
	}

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	entries, ok := indexer.Get(chunkID)
	if !ok {
		t.Fatal("expected index to exist after build")
	}

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	entryMap := make(map[chunk.SourceID][]uint64)
	for _, e := range entries {
		entryMap[e.SourceID] = e.Positions
	}

	if len(entryMap[src1]) != 3 {
		t.Fatalf("src1: expected 3 positions, got %d", len(entryMap[src1]))
	}
	if len(entryMap[src2]) != 2 {
		t.Fatalf("src2: expected 2 positions, got %d", len(entryMap[src2]))
	}
}

func TestIndexerIdempotent(t *testing.T) {
	src := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(100), SourceID: src, Raw: []byte("alpha")},
		{IngestTS: gotime.UnixMicro(200), SourceID: src, Raw: []byte("beta")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

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

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if len(entries[0].Positions) != 2 {
		t.Fatalf("expected 2 positions, got %d", len(entries[0].Positions))
	}
}

func TestIndexerCancelledContext(t *testing.T) {
	src := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(100), SourceID: src, Raw: []byte("data")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

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

func TestIndexerCancelledContextNoPartialData(t *testing.T) {
	src := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(100), SourceID: src, Raw: []byte("data")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_ = indexer.Build(ctx, chunkID)

	_, ok := indexer.Get(chunkID)
	if ok {
		t.Fatal("expected no index after failed build")
	}
}

func TestIndexerGetUnbuilt(t *testing.T) {
	src := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(100), SourceID: src, Raw: []byte("data")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	_, ok := indexer.Get(chunkID)
	if ok {
		t.Fatal("expected Get to return false for unbuilt chunk")
	}
	_ = chunkID
}

func TestIndexerBuildEmptyChunk(t *testing.T) {
	manager, chunkID := setupChunkManager(t, nil)
	indexer := NewIndexer(manager)

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

func TestIndexerBuildSingleSource(t *testing.T) {
	src := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(10), SourceID: src, Raw: []byte("a")},
		{IngestTS: gotime.UnixMicro(20), SourceID: src, Raw: []byte("b")},
		{IngestTS: gotime.UnixMicro(30), SourceID: src, Raw: []byte("c")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

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
	if entries[0].SourceID != src {
		t.Fatalf("expected source %s, got %s", src, entries[0].SourceID)
	}
	if len(entries[0].Positions) != 3 {
		t.Fatalf("expected 3 positions, got %d", len(entries[0].Positions))
	}
}

func TestIndexerBuildSingleRecord(t *testing.T) {
	src := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(42), SourceID: src, Raw: []byte("only")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

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
	if len(entries[0].Positions) != 1 {
		t.Fatalf("expected 1 position, got %d", len(entries[0].Positions))
	}
	if entries[0].Positions[0] != 0 {
		t.Fatalf("expected position 0, got %d", entries[0].Positions[0])
	}
}

func TestIndexerBuildInvalidChunkID(t *testing.T) {
	src := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), SourceID: src, Raw: []byte("x")},
	}

	manager, _ := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	bogusID := chunk.NewChunkID()
	err := indexer.Build(context.Background(), bogusID)
	if err == nil {
		t.Fatal("expected error for invalid chunk ID, got nil")
	}
}

func TestIndexerBuildRecordPos(t *testing.T) {
	src := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), SourceID: src, Raw: []byte("aaa")},
		{IngestTS: gotime.UnixMicro(2), SourceID: src, Raw: []byte("bbb")},
		{IngestTS: gotime.UnixMicro(3), SourceID: src, Raw: []byte("ccc")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

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

	// Memory manager uses slice indices: 0, 1, 2.
	for i, pos := range entries[0].Positions {
		if pos != uint64(i) {
			t.Fatalf("position %d: expected %d, got %d", i, i, pos)
		}
	}
}

func TestIndexerMultipleChunks(t *testing.T) {
	manager, err := chunkmemory.NewManager(chunkmemory.Config{})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	src := chunk.NewSourceID()

	// First chunk.
	id1, _, err := manager.Append(chunk.Record{IngestTS: gotime.UnixMicro(100), SourceID: src, Raw: []byte("a")})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Second chunk.
	id2, _, err := manager.Append(chunk.Record{IngestTS: gotime.UnixMicro(200), SourceID: src, Raw: []byte("b")})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	_, _, err = manager.Append(chunk.Record{IngestTS: gotime.UnixMicro(300), SourceID: src, Raw: []byte("c")})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	indexer := NewIndexer(manager)

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
	if len(entries1[0].Positions) != 1 {
		t.Fatalf("chunk 1: expected 1 position, got %d", len(entries1[0].Positions))
	}

	entries2, ok := indexer.Get(id2)
	if !ok {
		t.Fatal("expected index for chunk 2")
	}
	if len(entries2) != 1 {
		t.Fatalf("chunk 2: expected 1 entry, got %d", len(entries2))
	}
	if len(entries2[0].Positions) != 2 {
		t.Fatalf("chunk 2: expected 2 positions, got %d", len(entries2[0].Positions))
	}
}

func TestIndexerConcurrentBuildAndGet(t *testing.T) {
	src := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), SourceID: src, Raw: []byte("aaa")},
		{IngestTS: gotime.UnixMicro(2), SourceID: src, Raw: []byte("bbb")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	// First build so Get has something to read.
	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("initial build: %v", err)
	}

	var wg sync.WaitGroup

	buildErrs := make([]error, 8)
	for i := range buildErrs {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			buildErrs[idx] = indexer.Build(context.Background(), chunkID)
		}(i)
	}

	getResults := make([]bool, 8)
	for i := range getResults {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, ok := indexer.Get(chunkID)
			getResults[idx] = ok
		}(i)
	}

	wg.Wait()

	for i, err := range buildErrs {
		if err != nil {
			t.Fatalf("build goroutine %d: %v", i, err)
		}
	}

	for i, ok := range getResults {
		if !ok {
			t.Fatalf("get goroutine %d: expected index to exist", i)
		}
	}

	entries, ok := indexer.Get(chunkID)
	if !ok {
		t.Fatal("expected index to exist after concurrent operations")
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if len(entries[0].Positions) != 2 {
		t.Fatalf("expected 2 positions, got %d", len(entries[0].Positions))
	}
}

func TestIndexerBuildLargePostingList(t *testing.T) {
	src := chunk.NewSourceID()
	const numRecords = 1000
	records := make([]chunk.Record, numRecords)
	for i := range records {
		records[i] = chunk.Record{
			IngestTS: gotime.UnixMicro(int64(i + 1)),
			SourceID: src,
			Raw:      []byte("payload"),
		}
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	entries, ok := indexer.Get(chunkID)
	if !ok {
		t.Fatal("expected index to exist")
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
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
	src1 := chunk.NewSourceID()
	src2 := chunk.NewSourceID()
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), SourceID: src1, Raw: []byte("a")},
		{IngestTS: gotime.UnixMicro(2), SourceID: src2, Raw: []byte("b")},
		{IngestTS: gotime.UnixMicro(3), SourceID: src1, Raw: []byte("c")},
		{IngestTS: gotime.UnixMicro(4), SourceID: src2, Raw: []byte("d")},
		{IngestTS: gotime.UnixMicro(5), SourceID: src1, Raw: []byte("e")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	entries, ok := indexer.Get(chunkID)
	if !ok {
		t.Fatal("expected index to exist")
	}

	for _, entry := range entries {
		for i := 1; i < len(entry.Positions); i++ {
			if entry.Positions[i] <= entry.Positions[i-1] {
				t.Fatalf("source %s: positions not ascending at index %d: %d <= %d",
					entry.SourceID, i, entry.Positions[i], entry.Positions[i-1])
			}
		}
	}
}

func TestIndexerMultipleSourcesMultipleChunks(t *testing.T) {
	manager, err := chunkmemory.NewManager(chunkmemory.Config{})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	srcA := chunk.NewSourceID()
	srcB := chunk.NewSourceID()

	// First chunk: srcA only.
	id1, _, err := manager.Append(chunk.Record{IngestTS: gotime.UnixMicro(100), SourceID: srcA, Raw: []byte("a1")})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, _, err := manager.Append(chunk.Record{IngestTS: gotime.UnixMicro(200), SourceID: srcA, Raw: []byte("a2")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Second chunk: srcB only.
	id2, _, err := manager.Append(chunk.Record{IngestTS: gotime.UnixMicro(300), SourceID: srcB, Raw: []byte("b1")})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, _, err := manager.Append(chunk.Record{IngestTS: gotime.UnixMicro(400), SourceID: srcB, Raw: []byte("b2")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, _, err := manager.Append(chunk.Record{IngestTS: gotime.UnixMicro(500), SourceID: srcB, Raw: []byte("b3")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := manager.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	indexer := NewIndexer(manager)

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
	if entries1[0].SourceID != srcA {
		t.Fatalf("chunk 1: expected source %s, got %s", srcA, entries1[0].SourceID)
	}
	if len(entries1[0].Positions) != 2 {
		t.Fatalf("chunk 1: expected 2 positions, got %d", len(entries1[0].Positions))
	}

	entries2, ok := indexer.Get(id2)
	if !ok {
		t.Fatal("expected index for chunk 2")
	}
	if len(entries2) != 1 {
		t.Fatalf("chunk 2: expected 1 entry, got %d", len(entries2))
	}
	if entries2[0].SourceID != srcB {
		t.Fatalf("chunk 2: expected source %s, got %s", srcB, entries2[0].SourceID)
	}
	if len(entries2[0].Positions) != 3 {
		t.Fatalf("chunk 2: expected 3 positions, got %d", len(entries2[0].Positions))
	}
}

func TestIndexerZeroUUID(t *testing.T) {
	zeroSource := chunk.SourceID(uuid.UUID{})
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), SourceID: zeroSource, Raw: []byte("zero")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	entries, ok := indexer.Get(chunkID)
	if !ok {
		t.Fatal("expected index to exist")
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].SourceID != zeroSource {
		t.Fatalf("expected zero UUID source, got %s", entries[0].SourceID)
	}
	if len(entries[0].Positions) != 1 {
		t.Fatalf("expected 1 position, got %d", len(entries[0].Positions))
	}
}

func TestIndexerMaxUUID(t *testing.T) {
	maxSource := chunk.SourceID(uuid.UUID{
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	})
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), SourceID: maxSource, Raw: []byte("max")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	entries, ok := indexer.Get(chunkID)
	if !ok {
		t.Fatal("expected index to exist")
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].SourceID != maxSource {
		t.Fatalf("expected max UUID source, got %s", entries[0].SourceID)
	}
}

func TestIndexerZeroAndMaxUUIDSortOrder(t *testing.T) {
	zeroSource := chunk.SourceID(uuid.UUID{})
	maxSource := chunk.SourceID(uuid.UUID{
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	})
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), SourceID: maxSource, Raw: []byte("max")},
		{IngestTS: gotime.UnixMicro(2), SourceID: zeroSource, Raw: []byte("zero")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	entries, ok := indexer.Get(chunkID)
	if !ok {
		t.Fatal("expected index to exist")
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	zeroStr := uuid.UUID(zeroSource).String()
	firstStr := uuid.UUID(entries[0].SourceID).String()
	maxStr := uuid.UUID(maxSource).String()
	lastStr := uuid.UUID(entries[1].SourceID).String()

	if firstStr != zeroStr {
		t.Fatalf("expected zero UUID first, got %s", firstStr)
	}
	if lastStr != maxStr {
		t.Fatalf("expected max UUID last, got %s", lastStr)
	}
}

func TestIndexerBuildUnsealedChunk(t *testing.T) {
	manager, err := chunkmemory.NewManager(chunkmemory.Config{})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	src := chunk.NewSourceID()
	chunkID, _, err := manager.Append(chunk.Record{IngestTS: gotime.UnixMicro(1), SourceID: src, Raw: []byte("x")})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	indexer := NewIndexer(manager)

	err = indexer.Build(context.Background(), chunkID)
	if err == nil {
		t.Fatal("expected error building index on unsealed chunk, got nil")
	}
	if err != chunk.ErrChunkNotSealed {
		t.Fatalf("expected ErrChunkNotSealed, got %v", err)
	}
}
