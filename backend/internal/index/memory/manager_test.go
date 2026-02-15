package memory

import (
	"context"
	"errors"
	"testing"
	gotime "time"

	"gastrolog/internal/chunk"
	chunkmemory "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	memattr "gastrolog/internal/index/memory/attr"
	"gastrolog/internal/index/memory/kv"
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

// setupFullManager creates a Manager with all three indexers (token, attr, kv)
// wired up, builds indexes, and returns everything needed for testing.
func setupFullManager(t *testing.T, records []chunk.Record) (*Manager, chunk.ChunkID) {
	t.Helper()
	chunkMgr, chunkID := setupChunkManager(t, records)
	tokIdx := memtoken.NewIndexer(chunkMgr)
	attrIdx := memattr.NewIndexer(chunkMgr)
	kvIdx := kv.NewIndexer(chunkMgr)
	mgr := NewManager(
		[]index.Indexer{tokIdx, attrIdx, kvIdx},
		tokIdx,
		attrIdx,
		kvIdx,
		nil,
	)
	if err := mgr.BuildIndexes(context.Background(), chunkID); err != nil {
		t.Fatalf("build indexes: %v", err)
	}
	return mgr, chunkID
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

// testRecordsWithKV returns records that contain key=value pairs in the raw text,
// as well as attributes, to exercise both attr and kv indexers.
func testRecordsWithKV() []chunk.Record {
	return []chunk.Record{
		{
			IngestTS: gotime.UnixMicro(1000),
			Attrs:    chunk.Attributes{"host": "web01", "env": "prod"},
			Raw:      []byte("level=error status=500 request failed"),
		},
		{
			IngestTS: gotime.UnixMicro(2000),
			Attrs:    chunk.Attributes{"host": "web02", "env": "staging"},
			Raw:      []byte("level=warn status=429 rate limited"),
		},
		{
			IngestTS: gotime.UnixMicro(3000),
			Attrs:    chunk.Attributes{"host": "web01", "env": "prod"},
			Raw:      []byte("level=info status=200 request success"),
		},
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

// --- Tests for OpenAttrKeyIndex ---

func TestOpenAttrKeyIndex(t *testing.T) {
	mgr, chunkID := setupFullManager(t, testRecordsWithKV())

	idx, err := mgr.OpenAttrKeyIndex(chunkID)
	if err != nil {
		t.Fatalf("open attr key index: %v", err)
	}

	entries := idx.Entries()
	if len(entries) == 0 {
		t.Fatal("expected at least some attr key entries")
	}

	keySet := make(map[string]bool)
	for _, e := range entries {
		keySet[e.Key] = true
	}
	// Attrs are lowercased by the attr indexer.
	for _, expected := range []string{"host", "env"} {
		if !keySet[expected] {
			t.Errorf("expected attr key %q not found", expected)
		}
	}
}

func TestOpenAttrKeyIndex_NilStore(t *testing.T) {
	mgr := NewManager(nil, nil, nil, nil, nil)
	_, err := mgr.OpenAttrKeyIndex(chunk.NewChunkID())
	if !errors.Is(err, index.ErrIndexNotFound) {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}
}

func TestOpenAttrKeyIndex_NotBuilt(t *testing.T) {
	chunkMgr, _ := chunkmemory.NewManager(chunkmemory.Config{})
	attrIdx := memattr.NewIndexer(chunkMgr)
	mgr := NewManager(nil, nil, attrIdx, nil, nil)

	_, err := mgr.OpenAttrKeyIndex(chunk.NewChunkID())
	if !errors.Is(err, index.ErrIndexNotFound) {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}
}

// --- Tests for OpenAttrValueIndex ---

func TestOpenAttrValueIndex(t *testing.T) {
	mgr, chunkID := setupFullManager(t, testRecordsWithKV())

	idx, err := mgr.OpenAttrValueIndex(chunkID)
	if err != nil {
		t.Fatalf("open attr value index: %v", err)
	}

	entries := idx.Entries()
	if len(entries) == 0 {
		t.Fatal("expected at least some attr value entries")
	}

	valSet := make(map[string]bool)
	for _, e := range entries {
		valSet[e.Value] = true
	}
	for _, expected := range []string{"web01", "web02", "prod", "staging"} {
		if !valSet[expected] {
			t.Errorf("expected attr value %q not found", expected)
		}
	}
}

func TestOpenAttrValueIndex_NilStore(t *testing.T) {
	mgr := NewManager(nil, nil, nil, nil, nil)
	_, err := mgr.OpenAttrValueIndex(chunk.NewChunkID())
	if !errors.Is(err, index.ErrIndexNotFound) {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}
}

func TestOpenAttrValueIndex_NotBuilt(t *testing.T) {
	chunkMgr, _ := chunkmemory.NewManager(chunkmemory.Config{})
	attrIdx := memattr.NewIndexer(chunkMgr)
	mgr := NewManager(nil, nil, attrIdx, nil, nil)

	_, err := mgr.OpenAttrValueIndex(chunk.NewChunkID())
	if !errors.Is(err, index.ErrIndexNotFound) {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}
}

// --- Tests for OpenAttrKVIndex ---

func TestOpenAttrKVIndex(t *testing.T) {
	mgr, chunkID := setupFullManager(t, testRecordsWithKV())

	idx, err := mgr.OpenAttrKVIndex(chunkID)
	if err != nil {
		t.Fatalf("open attr kv index: %v", err)
	}

	entries := idx.Entries()
	if len(entries) == 0 {
		t.Fatal("expected at least some attr kv entries")
	}

	// Check that host=web01 pair is present.
	found := false
	for _, e := range entries {
		if e.Key == "host" && e.Value == "web01" {
			found = true
			// web01 appears in records 0 and 2.
			if len(e.Positions) != 2 {
				t.Errorf("expected 2 positions for host=web01, got %d", len(e.Positions))
			}
			break
		}
	}
	if !found {
		t.Error("expected attr kv entry host=web01 not found")
	}
}

func TestOpenAttrKVIndex_NilStore(t *testing.T) {
	mgr := NewManager(nil, nil, nil, nil, nil)
	_, err := mgr.OpenAttrKVIndex(chunk.NewChunkID())
	if !errors.Is(err, index.ErrIndexNotFound) {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}
}

func TestOpenAttrKVIndex_NotBuilt(t *testing.T) {
	chunkMgr, _ := chunkmemory.NewManager(chunkmemory.Config{})
	attrIdx := memattr.NewIndexer(chunkMgr)
	mgr := NewManager(nil, nil, attrIdx, nil, nil)

	_, err := mgr.OpenAttrKVIndex(chunk.NewChunkID())
	if !errors.Is(err, index.ErrIndexNotFound) {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}
}

// --- Tests for OpenKVKeyIndex ---

func TestOpenKVKeyIndex(t *testing.T) {
	mgr, chunkID := setupFullManager(t, testRecordsWithKV())

	idx, status, err := mgr.OpenKVKeyIndex(chunkID)
	if err != nil {
		t.Fatalf("open kv key index: %v", err)
	}
	if status != index.KVComplete {
		t.Errorf("expected KVComplete status, got %v", status)
	}

	entries := idx.Entries()
	if len(entries) == 0 {
		t.Fatal("expected at least some kv key entries")
	}

	keySet := make(map[string]bool)
	for _, e := range entries {
		keySet[e.Key] = true
	}
	for _, expected := range []string{"level", "status"} {
		if !keySet[expected] {
			t.Errorf("expected kv key %q not found", expected)
		}
	}
}

func TestOpenKVKeyIndex_NilStore(t *testing.T) {
	mgr := NewManager(nil, nil, nil, nil, nil)
	_, _, err := mgr.OpenKVKeyIndex(chunk.NewChunkID())
	if !errors.Is(err, index.ErrIndexNotFound) {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}
}

func TestOpenKVKeyIndex_NotBuilt(t *testing.T) {
	chunkMgr, _ := chunkmemory.NewManager(chunkmemory.Config{})
	kvIdx := kv.NewIndexer(chunkMgr)
	mgr := NewManager(nil, nil, nil, kvIdx, nil)

	_, _, err := mgr.OpenKVKeyIndex(chunk.NewChunkID())
	if !errors.Is(err, index.ErrIndexNotFound) {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}
}

// --- Tests for OpenKVValueIndex ---

func TestOpenKVValueIndex(t *testing.T) {
	mgr, chunkID := setupFullManager(t, testRecordsWithKV())

	idx, status, err := mgr.OpenKVValueIndex(chunkID)
	if err != nil {
		t.Fatalf("open kv value index: %v", err)
	}
	if status != index.KVComplete {
		t.Errorf("expected KVComplete status, got %v", status)
	}

	entries := idx.Entries()
	if len(entries) == 0 {
		t.Fatal("expected at least some kv value entries")
	}

	valSet := make(map[string]bool)
	for _, e := range entries {
		valSet[e.Value] = true
	}
	for _, expected := range []string{"error", "warn", "info", "500", "429", "200"} {
		if !valSet[expected] {
			t.Errorf("expected kv value %q not found", expected)
		}
	}
}

func TestOpenKVValueIndex_NilStore(t *testing.T) {
	mgr := NewManager(nil, nil, nil, nil, nil)
	_, _, err := mgr.OpenKVValueIndex(chunk.NewChunkID())
	if !errors.Is(err, index.ErrIndexNotFound) {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}
}

func TestOpenKVValueIndex_NotBuilt(t *testing.T) {
	chunkMgr, _ := chunkmemory.NewManager(chunkmemory.Config{})
	kvIdx := kv.NewIndexer(chunkMgr)
	mgr := NewManager(nil, nil, nil, kvIdx, nil)

	_, _, err := mgr.OpenKVValueIndex(chunk.NewChunkID())
	if !errors.Is(err, index.ErrIndexNotFound) {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}
}

// --- Tests for OpenKVIndex ---

func TestOpenKVIndex(t *testing.T) {
	mgr, chunkID := setupFullManager(t, testRecordsWithKV())

	idx, status, err := mgr.OpenKVIndex(chunkID)
	if err != nil {
		t.Fatalf("open kv index: %v", err)
	}
	if status != index.KVComplete {
		t.Errorf("expected KVComplete status, got %v", status)
	}

	entries := idx.Entries()
	if len(entries) == 0 {
		t.Fatal("expected at least some kv entries")
	}

	// Check that level=error pair is present.
	found := false
	for _, e := range entries {
		if e.Key == "level" && e.Value == "error" {
			found = true
			if len(e.Positions) != 1 {
				t.Errorf("expected 1 position for level=error, got %d", len(e.Positions))
			}
			break
		}
	}
	if !found {
		t.Error("expected kv entry level=error not found")
	}
}

func TestOpenKVIndex_NilStore(t *testing.T) {
	mgr := NewManager(nil, nil, nil, nil, nil)
	_, _, err := mgr.OpenKVIndex(chunk.NewChunkID())
	if !errors.Is(err, index.ErrIndexNotFound) {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}
}

func TestOpenKVIndex_NotBuilt(t *testing.T) {
	chunkMgr, _ := chunkmemory.NewManager(chunkmemory.Config{})
	kvIdx := kv.NewIndexer(chunkMgr)
	mgr := NewManager(nil, nil, nil, kvIdx, nil)

	_, _, err := mgr.OpenKVIndex(chunk.NewChunkID())
	if !errors.Is(err, index.ErrIndexNotFound) {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}
}

// --- Tests for DeleteIndexes ---

func TestDeleteIndexes(t *testing.T) {
	mgr, chunkID := setupFullManager(t, testRecordsWithKV())

	// Verify all indexes are accessible before deletion.
	if _, err := mgr.OpenTokenIndex(chunkID); err != nil {
		t.Fatalf("token index should exist before delete: %v", err)
	}
	if _, err := mgr.OpenAttrKeyIndex(chunkID); err != nil {
		t.Fatalf("attr key index should exist before delete: %v", err)
	}
	if _, _, err := mgr.OpenKVIndex(chunkID); err != nil {
		t.Fatalf("kv index should exist before delete: %v", err)
	}

	// Delete all indexes.
	if err := mgr.DeleteIndexes(chunkID); err != nil {
		t.Fatalf("delete indexes: %v", err)
	}

	// Verify all indexes are gone after deletion.
	if _, err := mgr.OpenTokenIndex(chunkID); !errors.Is(err, index.ErrIndexNotFound) {
		t.Errorf("token index should be gone after delete, got %v", err)
	}
	if _, err := mgr.OpenAttrKeyIndex(chunkID); !errors.Is(err, index.ErrIndexNotFound) {
		t.Errorf("attr key index should be gone after delete, got %v", err)
	}
	if _, err := mgr.OpenAttrValueIndex(chunkID); !errors.Is(err, index.ErrIndexNotFound) {
		t.Errorf("attr value index should be gone after delete, got %v", err)
	}
	if _, err := mgr.OpenAttrKVIndex(chunkID); !errors.Is(err, index.ErrIndexNotFound) {
		t.Errorf("attr kv index should be gone after delete, got %v", err)
	}
	if _, _, err := mgr.OpenKVKeyIndex(chunkID); !errors.Is(err, index.ErrIndexNotFound) {
		t.Errorf("kv key index should be gone after delete, got %v", err)
	}
	if _, _, err := mgr.OpenKVValueIndex(chunkID); !errors.Is(err, index.ErrIndexNotFound) {
		t.Errorf("kv value index should be gone after delete, got %v", err)
	}
	if _, _, err := mgr.OpenKVIndex(chunkID); !errors.Is(err, index.ErrIndexNotFound) {
		t.Errorf("kv index should be gone after delete, got %v", err)
	}
}

func TestDeleteIndexes_NilStores(t *testing.T) {
	// Manager with all nil stores should not panic.
	mgr := NewManager(nil, nil, nil, nil, nil)
	if err := mgr.DeleteIndexes(chunk.NewChunkID()); err != nil {
		t.Fatalf("delete indexes with nil stores: %v", err)
	}
}

func TestDeleteIndexes_NonexistentChunk(t *testing.T) {
	mgr, _ := setupFullManager(t, testRecordsWithKV())

	// Deleting indexes for a chunk that does not exist should not error.
	if err := mgr.DeleteIndexes(chunk.NewChunkID()); err != nil {
		t.Fatalf("delete indexes for nonexistent chunk: %v", err)
	}
}

// --- Tests for FindIngestStartPosition ---

func TestFindIngestStartPosition(t *testing.T) {
	mgr, chunkID := setupFullManager(t, testRecordsWithKV())

	_, _, err := mgr.FindIngestStartPosition(chunkID, gotime.Now())
	if !errors.Is(err, index.ErrIndexNotFound) {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}
}

// --- Tests for FindSourceStartPosition ---

func TestFindSourceStartPosition(t *testing.T) {
	mgr, chunkID := setupFullManager(t, testRecordsWithKV())

	_, _, err := mgr.FindSourceStartPosition(chunkID, gotime.Now())
	if !errors.Is(err, index.ErrIndexNotFound) {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}
}

// --- Tests for IndexSizes ---

func TestIndexSizes(t *testing.T) {
	mgr, chunkID := setupFullManager(t, testRecordsWithKV())

	sizes := mgr.IndexSizes(chunkID)

	// With all three indexers wired, we expect all index types to have entries.
	expectedKeys := []string{"token", "attr_key", "attr_val", "attr_kv", "kv_key", "kv_val", "kv_kv"}
	for _, key := range expectedKeys {
		size, ok := sizes[key]
		if !ok {
			t.Errorf("expected size entry for %q, not found", key)
			continue
		}
		if size <= 0 {
			t.Errorf("expected positive size for %q, got %d", key, size)
		}
	}
}

func TestIndexSizes_NilStores(t *testing.T) {
	mgr := NewManager(nil, nil, nil, nil, nil)
	sizes := mgr.IndexSizes(chunk.NewChunkID())
	if len(sizes) != 0 {
		t.Errorf("expected empty sizes map for nil stores, got %v", sizes)
	}
}

func TestIndexSizes_NoIndexesBuilt(t *testing.T) {
	chunkMgr, _ := chunkmemory.NewManager(chunkmemory.Config{})
	tokIdx := memtoken.NewIndexer(chunkMgr)
	attrIdx := memattr.NewIndexer(chunkMgr)
	kvIdx := kv.NewIndexer(chunkMgr)
	mgr := NewManager(nil, tokIdx, attrIdx, kvIdx, nil)

	// No indexes built, so sizes should be empty for a random chunk ID.
	sizes := mgr.IndexSizes(chunk.NewChunkID())
	if len(sizes) != 0 {
		t.Errorf("expected empty sizes map when no indexes built, got %v", sizes)
	}
}

// --- Tests for IndexesComplete ---

func TestIndexesComplete(t *testing.T) {
	mgr, chunkID := setupFullManager(t, testRecordsWithKV())

	complete, err := mgr.IndexesComplete(chunkID)
	if err != nil {
		t.Fatalf("indexes complete: %v", err)
	}
	if !complete {
		t.Error("expected indexes to be complete after building all indexes")
	}
}

func TestIndexesComplete_NilStores(t *testing.T) {
	mgr := NewManager(nil, nil, nil, nil, nil)
	complete, err := mgr.IndexesComplete(chunk.NewChunkID())
	if err != nil {
		t.Fatalf("indexes complete with nil stores: %v", err)
	}
	if !complete {
		t.Error("expected complete=true with nil stores (nothing to check)")
	}
}

func TestIndexesComplete_MissingTokenIndex(t *testing.T) {
	chunkMgr, chunkID := setupChunkManager(t, testRecordsWithKV())
	tokIdx := memtoken.NewIndexer(chunkMgr)
	attrIdx := memattr.NewIndexer(chunkMgr)
	kvIdx := kv.NewIndexer(chunkMgr)

	// Build only attr and kv, skip token.
	if err := attrIdx.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build attr: %v", err)
	}
	if err := kvIdx.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build kv: %v", err)
	}

	mgr := NewManager(nil, tokIdx, attrIdx, kvIdx, nil)

	complete, err := mgr.IndexesComplete(chunkID)
	if err != nil {
		t.Fatalf("indexes complete: %v", err)
	}
	if complete {
		t.Error("expected indexes incomplete when token index is missing")
	}
}

func TestIndexesComplete_MissingAttrIndex(t *testing.T) {
	chunkMgr, chunkID := setupChunkManager(t, testRecordsWithKV())
	tokIdx := memtoken.NewIndexer(chunkMgr)
	attrIdx := memattr.NewIndexer(chunkMgr)
	kvIdx := kv.NewIndexer(chunkMgr)

	// Build only token and kv, skip attr.
	if err := tokIdx.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build token: %v", err)
	}
	if err := kvIdx.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build kv: %v", err)
	}

	mgr := NewManager(nil, tokIdx, attrIdx, kvIdx, nil)

	complete, err := mgr.IndexesComplete(chunkID)
	if err != nil {
		t.Fatalf("indexes complete: %v", err)
	}
	if complete {
		t.Error("expected indexes incomplete when attr index is missing")
	}
}

func TestIndexesComplete_MissingKVIndex(t *testing.T) {
	chunkMgr, chunkID := setupChunkManager(t, testRecordsWithKV())
	tokIdx := memtoken.NewIndexer(chunkMgr)
	attrIdx := memattr.NewIndexer(chunkMgr)
	kvIdx := kv.NewIndexer(chunkMgr)

	// Build only token and attr, skip kv.
	if err := tokIdx.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build token: %v", err)
	}
	if err := attrIdx.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build attr: %v", err)
	}

	mgr := NewManager(nil, tokIdx, attrIdx, kvIdx, nil)

	complete, err := mgr.IndexesComplete(chunkID)
	if err != nil {
		t.Fatalf("indexes complete: %v", err)
	}
	if complete {
		t.Error("expected indexes incomplete when kv index is missing")
	}
}

func TestDeleteThenIndexesComplete(t *testing.T) {
	mgr, chunkID := setupFullManager(t, testRecordsWithKV())

	// Indexes are complete after build.
	complete, err := mgr.IndexesComplete(chunkID)
	if err != nil {
		t.Fatalf("indexes complete: %v", err)
	}
	if !complete {
		t.Fatal("expected indexes complete after build")
	}

	// Delete all indexes.
	if err := mgr.DeleteIndexes(chunkID); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Indexes should now be incomplete.
	complete, err = mgr.IndexesComplete(chunkID)
	if err != nil {
		t.Fatalf("indexes complete after delete: %v", err)
	}
	if complete {
		t.Error("expected indexes incomplete after delete")
	}
}
