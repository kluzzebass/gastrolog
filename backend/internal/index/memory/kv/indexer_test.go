package kv

import (
	"context"
	"fmt"
	"testing"

	"gastrolog/internal/chunk"
	chunkmemory "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
)

// Verify constants are accessible for testing.
var _ = MaxUniqueKeys

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
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{Attrs: attrs, Raw: []byte("level=error host=server1 msg=failed")},
		{Attrs: attrs, Raw: []byte("level=info host=server2 msg=started")},
		{Attrs: attrs, Raw: []byte("level=error host=server3 msg=timeout")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	if indexer.Name() != "kv" {
		t.Fatalf("expected name %q, got %q", "kv", indexer.Name())
	}

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	// Check key index.
	keyEntries, status, ok := indexer.GetKey(chunkID)
	if !ok {
		t.Fatal("expected key index to exist after build")
	}
	if status != index.KVComplete {
		t.Fatalf("expected status Complete, got %v", status)
	}
	if len(keyEntries) == 0 {
		t.Fatal("expected at least one key entry")
	}

	keyMap := make(map[string][]uint64)
	for _, e := range keyEntries {
		keyMap[e.Key] = e.Positions
	}

	// "level" appears in all 3 records
	if positions, ok := keyMap["level"]; !ok {
		t.Error("expected 'level' key")
	} else if len(positions) != 3 {
		t.Errorf("expected 'level' in 3 records, got %d", len(positions))
	}

	// "host" appears in all 3 records
	if positions, ok := keyMap["host"]; !ok {
		t.Error("expected 'host' key")
	} else if len(positions) != 3 {
		t.Errorf("expected 'host' in 3 records, got %d", len(positions))
	}

	// Check value index.
	valEntries, status, ok := indexer.GetValue(chunkID)
	if !ok {
		t.Fatal("expected value index to exist after build")
	}
	if status != index.KVComplete {
		t.Fatalf("expected status Complete, got %v", status)
	}

	valMap := make(map[string][]uint64)
	for _, e := range valEntries {
		valMap[e.Value] = e.Positions
	}

	// "error" appears in records 0 and 2
	if positions, ok := valMap["error"]; !ok {
		t.Error("expected 'error' value")
	} else if len(positions) != 2 {
		t.Errorf("expected 'error' in 2 records, got %d", len(positions))
	}

	// "info" appears in record 1 only
	if positions, ok := valMap["info"]; !ok {
		t.Error("expected 'info' value")
	} else if len(positions) != 1 {
		t.Errorf("expected 'info' in 1 record, got %d", len(positions))
	}

	// Check KV index.
	kvEntries, status, ok := indexer.GetKV(chunkID)
	if !ok {
		t.Fatal("expected kv index to exist after build")
	}
	if status != index.KVComplete {
		t.Fatalf("expected status Complete, got %v", status)
	}

	kvMap := make(map[string][]uint64)
	for _, e := range kvEntries {
		kvMap[e.Key+"\x00"+e.Value] = e.Positions
	}

	// "level=error" appears in records 0 and 2
	if positions, ok := kvMap["level\x00error"]; !ok {
		t.Error("expected 'level=error' pair")
	} else if len(positions) != 2 {
		t.Errorf("expected 'level=error' in 2 records, got %d", len(positions))
	}

	// "level=info" appears in record 1 only
	if positions, ok := kvMap["level\x00info"]; !ok {
		t.Error("expected 'level=info' pair")
	} else if len(positions) != 1 {
		t.Errorf("expected 'level=info' in 1 record, got %d", len(positions))
	}
}

func TestIndexerCaseFolding(t *testing.T) {
	// kv extraction lowercases both keys and values.
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{Attrs: attrs, Raw: []byte("Level=ERROR")},
		{Attrs: attrs, Raw: []byte("level=error")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	// Key index should have single "level" entry (both are lowercased).
	keyEntries, _, ok := indexer.GetKey(chunkID)
	if !ok {
		t.Fatal("expected key index to exist")
	}

	if len(keyEntries) != 1 {
		t.Fatalf("expected 1 key entry (case folded), got %d", len(keyEntries))
	}
	if keyEntries[0].Key != "level" {
		t.Errorf("expected key 'level', got %q", keyEntries[0].Key)
	}
	if len(keyEntries[0].Positions) != 2 {
		t.Errorf("expected 2 positions, got %d", len(keyEntries[0].Positions))
	}

	// Value index should have single "error" entry (both are lowercased).
	valEntries, _, ok := indexer.GetValue(chunkID)
	if !ok {
		t.Fatal("expected value index to exist")
	}

	if len(valEntries) != 1 {
		t.Fatalf("expected 1 value entry (case folded), got %d", len(valEntries))
	}
	if valEntries[0].Value != "error" {
		t.Errorf("expected value 'error', got %q", valEntries[0].Value)
	}
	if len(valEntries[0].Positions) != 2 {
		t.Errorf("expected 2 positions, got %d", len(valEntries[0].Positions))
	}
}

func TestIndexerDeduplication(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		// Same key=value repeated in message
		{Attrs: attrs, Raw: []byte("level=error level=error level=error")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	// "level" key should appear only once per record (deduped).
	keyEntries, _, ok := indexer.GetKey(chunkID)
	if !ok {
		t.Fatal("expected key index to exist")
	}

	var levelPositions []uint64
	for _, e := range keyEntries {
		if e.Key == "level" {
			levelPositions = e.Positions
			break
		}
	}

	if len(levelPositions) != 1 {
		t.Errorf("expected 'level' in 1 position (deduped), got %d", len(levelPositions))
	}
}

func TestIndexerBuildEmptyChunk(t *testing.T) {
	manager, chunkID := setupChunkManager(t, nil)
	indexer := NewIndexer(manager)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	keyEntries, status, ok := indexer.GetKey(chunkID)
	if !ok {
		t.Fatal("expected key index to exist after build")
	}
	if status != index.KVComplete {
		t.Fatalf("expected status Complete, got %v", status)
	}
	if len(keyEntries) != 0 {
		t.Fatalf("expected 0 key entries, got %d", len(keyEntries))
	}
}

func TestIndexerBuildNoKeyValues(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{Attrs: attrs, Raw: []byte("just plain text without any key value pairs")},
		{Attrs: attrs, Raw: []byte("more plain text here")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	keyEntries, status, ok := indexer.GetKey(chunkID)
	if !ok {
		t.Fatal("expected key index to exist after build")
	}
	if status != index.KVComplete {
		t.Fatalf("expected status Complete, got %v", status)
	}
	if len(keyEntries) != 0 {
		t.Fatalf("expected 0 key entries, got %d", len(keyEntries))
	}
}

func TestIndexerBuildSorted(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{Attrs: attrs, Raw: []byte("zebra=z apple=a mango=m")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	// Check key entries are sorted.
	keyEntries, _, ok := indexer.GetKey(chunkID)
	if !ok {
		t.Fatal("expected key index to exist")
	}

	expectedKeys := []string{"apple", "mango", "zebra"}
	if len(keyEntries) != len(expectedKeys) {
		t.Fatalf("expected %d key entries, got %d", len(expectedKeys), len(keyEntries))
	}
	for i, e := range keyEntries {
		if e.Key != expectedKeys[i] {
			t.Errorf("key entry %d: expected %q, got %q", i, expectedKeys[i], e.Key)
		}
	}

	// Check value entries are sorted.
	valEntries, _, ok := indexer.GetValue(chunkID)
	if !ok {
		t.Fatal("expected value index to exist")
	}

	expectedVals := []string{"a", "m", "z"}
	if len(valEntries) != len(expectedVals) {
		t.Fatalf("expected %d value entries, got %d", len(expectedVals), len(valEntries))
	}
	for i, e := range valEntries {
		if e.Value != expectedVals[i] {
			t.Errorf("value entry %d: expected %q, got %q", i, expectedVals[i], e.Value)
		}
	}

	// Check kv entries are sorted by key then value.
	kvEntries, _, ok := indexer.GetKV(chunkID)
	if !ok {
		t.Fatal("expected kv index to exist")
	}

	expectedKVs := []struct{ k, v string }{
		{"apple", "a"},
		{"mango", "m"},
		{"zebra", "z"},
	}
	if len(kvEntries) != len(expectedKVs) {
		t.Fatalf("expected %d kv entries, got %d", len(expectedKVs), len(kvEntries))
	}
	for i, e := range kvEntries {
		if e.Key != expectedKVs[i].k || e.Value != expectedKVs[i].v {
			t.Errorf("kv entry %d: expected %q=%q, got %q=%q", i, expectedKVs[i].k, expectedKVs[i].v, e.Key, e.Value)
		}
	}
}

func TestIndexerBuildDottedKeys(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{Attrs: attrs, Raw: []byte("log.level=error app.name=myapp")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	keyEntries, _, ok := indexer.GetKey(chunkID)
	if !ok {
		t.Fatal("expected key index to exist")
	}

	keySet := make(map[string]struct{})
	for _, e := range keyEntries {
		keySet[e.Key] = struct{}{}
	}

	if _, ok := keySet["log.level"]; !ok {
		t.Error("expected 'log.level' dotted key")
	}
	if _, ok := keySet["app.name"]; !ok {
		t.Error("expected 'app.name' dotted key")
	}
}

func TestIndexerBuildUnsealedChunk(t *testing.T) {
	manager, err := chunkmemory.NewManager(chunkmemory.Config{})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	attrs := chunk.Attributes{"source": "test"}
	chunkID, _, err := manager.Append(chunk.Record{Attrs: attrs, Raw: []byte("level=error")})
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
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{Attrs: attrs, Raw: []byte("level=error")},
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
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{Attrs: attrs, Raw: []byte("level=error")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	if _, _, ok := indexer.GetKey(chunkID); ok {
		t.Fatal("expected GetKey to return false for unbuilt chunk")
	}
	if _, _, ok := indexer.GetValue(chunkID); ok {
		t.Fatal("expected GetValue to return false for unbuilt chunk")
	}
	if _, _, ok := indexer.GetKV(chunkID); ok {
		t.Fatal("expected GetKV to return false for unbuilt chunk")
	}
}

func TestIndexerBuildInvalidChunkID(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{Attrs: attrs, Raw: []byte("level=error")},
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
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{Attrs: attrs, Raw: []byte("level=error host=server1")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("first build: %v", err)
	}
	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("second build: %v", err)
	}

	keyEntries, status, ok := indexer.GetKey(chunkID)
	if !ok {
		t.Fatal("expected key index to exist after build")
	}
	if status != index.KVComplete {
		t.Fatalf("expected status Complete, got %v", status)
	}
	if len(keyEntries) != 2 { // "level", "host"
		t.Fatalf("expected 2 key entries, got %d", len(keyEntries))
	}
}

func TestIndexerCapped(t *testing.T) {
	// Create enough unique keys to exceed MaxUniqueKeys.
	// We need to generate records that will cause capping.
	// This test verifies the capping logic works correctly.

	// Use a large rotation policy to keep all records in one chunk.
	manager, err := chunkmemory.NewManager(chunkmemory.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(uint64(MaxUniqueKeys + 200)),
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	attrs := chunk.Attributes{"source": "test"}

	// Generate records with many unique keys.
	// MaxUniqueKeys is 10000, so we need more than that.
	for i := range MaxUniqueKeys + 100 {
		_, _, err := manager.Append(chunk.Record{
			Attrs: attrs,
			Raw:   fmt.Appendf(nil, "key%d=value%d", i, i),
		})
		if err != nil {
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

	indexer := NewIndexer(manager)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	// Should be capped.
	_, status, ok := indexer.GetKey(chunkID)
	if !ok {
		t.Fatal("expected key index to exist after build (even if capped)")
	}
	if status != index.KVCapped {
		t.Fatalf("expected status Capped, got %v", status)
	}

	// Same for value and kv indexes.
	_, status, ok = indexer.GetValue(chunkID)
	if !ok {
		t.Fatal("expected value index to exist")
	}
	if status != index.KVCapped {
		t.Fatalf("expected value status Capped, got %v", status)
	}

	_, status, ok = indexer.GetKV(chunkID)
	if !ok {
		t.Fatal("expected kv index to exist")
	}
	if status != index.KVCapped {
		t.Fatalf("expected kv status Capped, got %v", status)
	}
}
