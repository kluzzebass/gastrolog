package token

import (
	"context"
	"testing"

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
	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{SourceID: sourceID, Raw: []byte("error connecting to database")},
		{SourceID: sourceID, Raw: []byte("warning: slow query detected")},
		{SourceID: sourceID, Raw: []byte("error: connection timeout")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	if indexer.Name() != "token" {
		t.Fatalf("expected name %q, got %q", "token", indexer.Name())
	}

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	entries, ok := indexer.Get(chunkID)
	if !ok {
		t.Fatal("expected index to exist after build")
	}

	// Check that we have tokens.
	if len(entries) == 0 {
		t.Fatal("expected at least one token entry")
	}

	// Check specific tokens exist with correct positions.
	tokenMap := make(map[string][]uint64)
	for _, e := range entries {
		tokenMap[e.Token] = e.Positions
	}

	// "error" appears in records 0 and 2
	if positions, ok := tokenMap["error"]; !ok {
		t.Error("expected 'error' token")
	} else if len(positions) != 2 {
		t.Errorf("expected 'error' in 2 records, got %d", len(positions))
	}

	// "warning" appears in record 1 only
	if positions, ok := tokenMap["warning"]; !ok {
		t.Error("expected 'warning' token")
	} else if len(positions) != 1 {
		t.Errorf("expected 'warning' in 1 record, got %d", len(positions))
	}

	// "connecting" appears in record 0 only
	if positions, ok := tokenMap["connecting"]; !ok {
		t.Error("expected 'connecting' token")
	} else if len(positions) != 1 {
		t.Errorf("expected 'connecting' in 1 record, got %d", len(positions))
	}

	// "timeout" appears in record 2 only
	if positions, ok := tokenMap["timeout"]; !ok {
		t.Error("expected 'timeout' token")
	} else if len(positions) != 1 {
		t.Errorf("expected 'timeout' in 1 record, got %d", len(positions))
	}
}

func TestIndexerBuildDedupeWithinRecord(t *testing.T) {
	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{SourceID: sourceID, Raw: []byte("error error error multiple errors")},
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

	// Find "error" token.
	var errorPositions []uint64
	for _, e := range entries {
		if e.Token == "error" {
			errorPositions = e.Positions
			break
		}
	}

	// Should only appear once per record, not multiple times.
	if len(errorPositions) != 1 {
		t.Errorf("expected 'error' to appear in 1 position (deduped), got %d", len(errorPositions))
	}
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

func TestIndexerBuildNoTokensInRecord(t *testing.T) {
	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{SourceID: sourceID, Raw: []byte("... --- ...")}, // only delimiters and single chars
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
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(entries))
	}
}

func TestIndexerBuildSorted(t *testing.T) {
	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{SourceID: sourceID, Raw: []byte("zebra apple mango")},
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

	// Check sorted order.
	for i := 1; i < len(entries); i++ {
		if entries[i].Token < entries[i-1].Token {
			t.Errorf("entries not sorted: %q comes after %q", entries[i].Token, entries[i-1].Token)
		}
	}

	// Verify expected order.
	expected := []string{"apple", "mango", "zebra"}
	if len(entries) != len(expected) {
		t.Fatalf("expected %d entries, got %d", len(expected), len(entries))
	}
	for i, e := range entries {
		if e.Token != expected[i] {
			t.Errorf("entry %d: expected %q, got %q", i, expected[i], e.Token)
		}
	}
}

func TestIndexerBuildUnsealedChunk(t *testing.T) {
	manager, err := chunkmemory.NewManager(chunkmemory.Config{})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	sourceID := chunk.NewSourceID()
	chunkID, _, err := manager.Append(chunk.Record{SourceID: sourceID, Raw: []byte("test")})
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

func TestIndexerBuildCancelledContext(t *testing.T) {
	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{SourceID: sourceID, Raw: []byte("test data")},
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

func TestIndexerGetUnbuilt(t *testing.T) {
	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{SourceID: sourceID, Raw: []byte("test")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	_, ok := indexer.Get(chunkID)
	if ok {
		t.Fatal("expected Get to return false for unbuilt chunk")
	}
}

func TestIndexerBuildInvalidChunkID(t *testing.T) {
	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{SourceID: sourceID, Raw: []byte("test")},
	}

	manager, _ := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	bogusID := chunk.NewChunkID()
	err := indexer.Build(context.Background(), bogusID)
	if err == nil {
		t.Fatal("expected error for invalid chunk ID, got nil")
	}
}

func TestIndexerIdempotent(t *testing.T) {
	sourceID := chunk.NewSourceID()
	records := []chunk.Record{
		{SourceID: sourceID, Raw: []byte("hello world")},
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
	if len(entries) != 2 { // "hello", "world"
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
}
