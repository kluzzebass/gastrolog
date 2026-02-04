package memory

import (
	"context"
	"testing"
	gotime "time"

	"gastrolog/internal/chunk"
	chunkmemory "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	memtoken "gastrolog/internal/index/memory/token"
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
	tokenIndexer := memtoken.NewIndexer(chunkMgr)
	mgr := NewManager(
		[]index.Indexer{tokenIndexer},
		tokenIndexer,
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
		{IngestTS: gotime.UnixMicro(1000), Attrs: attrs1, Raw: []byte("error message one")},
		{IngestTS: gotime.UnixMicro(2000), Attrs: attrs2, Raw: []byte("warning message two")},
		{IngestTS: gotime.UnixMicro(3000), Attrs: attrs1, Raw: []byte("error message three")},
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

	tokenIndexer := memtoken.NewIndexer(chunkMgr)
	mgr := NewManager(
		[]index.Indexer{tokenIndexer},
		tokenIndexer,
		nil,
		nil,
		nil,
	)

	err = mgr.BuildIndexes(context.Background(), chunkID)
	if err == nil {
		t.Fatal("expected error building indexes on unsealed chunk, got nil")
	}
}

func TestOpenTokenIndex(t *testing.T) {
	mgr, _, chunkID := setupManager(t, testRecords())

	if err := mgr.BuildIndexes(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	idx, err := mgr.OpenTokenIndex(chunkID)
	if err != nil {
		t.Fatalf("open token index: %v", err)
	}

	entries := idx.Entries()
	if len(entries) == 0 {
		t.Fatal("expected at least some token entries")
	}

	// Check that expected tokens are present
	tokenSet := make(map[string]bool)
	for _, e := range entries {
		tokenSet[e.Token] = true
	}

	expectedTokens := []string{"error", "message", "warning"}
	for _, tok := range expectedTokens {
		if !tokenSet[tok] {
			t.Errorf("expected token %q not found in index", tok)
		}
	}
}

func TestOpenTokenIndexNotBuilt(t *testing.T) {
	chunkMgr, err := chunkmemory.NewManager(chunkmemory.Config{})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	tokenIndexer := memtoken.NewIndexer(chunkMgr)
	mgr := NewManager(
		[]index.Indexer{tokenIndexer},
		tokenIndexer,
		nil,
		nil,
		nil,
	)

	_, err = mgr.OpenTokenIndex(chunk.NewChunkID())
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

	// Token index round-trip.
	tokenIdx, err := mgr.OpenTokenIndex(chunkID)
	if err != nil {
		t.Fatalf("open token index: %v", err)
	}
	tokenEntries := tokenIdx.Entries()
	if len(tokenEntries) == 0 {
		t.Fatal("token: expected some entries")
	}

	// Verify entries are sorted by token
	for i := 1; i < len(tokenEntries); i++ {
		if tokenEntries[i].Token < tokenEntries[i-1].Token {
			t.Fatalf("token entries not sorted at index %d", i)
		}
	}
}
