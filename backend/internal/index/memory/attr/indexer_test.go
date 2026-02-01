package attr

import (
	"context"
	"testing"

	"gastrolog/internal/chunk"
	chunkmemory "gastrolog/internal/chunk/memory"
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
	records := []chunk.Record{
		{Attrs: chunk.Attributes{"env": "prod", "host": "server1"}, Raw: []byte("log1")},
		{Attrs: chunk.Attributes{"env": "dev", "host": "server2"}, Raw: []byte("log2")},
		{Attrs: chunk.Attributes{"env": "prod", "host": "server3"}, Raw: []byte("log3")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	if indexer.Name() != "attr" {
		t.Fatalf("expected name %q, got %q", "attr", indexer.Name())
	}

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	// Check key index.
	keyEntries, ok := indexer.GetKey(chunkID)
	if !ok {
		t.Fatal("expected key index to exist after build")
	}
	if len(keyEntries) == 0 {
		t.Fatal("expected at least one key entry")
	}

	keyMap := make(map[string][]uint64)
	for _, e := range keyEntries {
		keyMap[e.Key] = e.Positions
	}

	// "env" appears in all 3 records
	if positions, ok := keyMap["env"]; !ok {
		t.Error("expected 'env' key")
	} else if len(positions) != 3 {
		t.Errorf("expected 'env' in 3 records, got %d", len(positions))
	}

	// "host" appears in all 3 records
	if positions, ok := keyMap["host"]; !ok {
		t.Error("expected 'host' key")
	} else if len(positions) != 3 {
		t.Errorf("expected 'host' in 3 records, got %d", len(positions))
	}

	// Check value index.
	valEntries, ok := indexer.GetValue(chunkID)
	if !ok {
		t.Fatal("expected value index to exist after build")
	}

	valMap := make(map[string][]uint64)
	for _, e := range valEntries {
		valMap[e.Value] = e.Positions
	}

	// "prod" appears in records 0 and 2
	if positions, ok := valMap["prod"]; !ok {
		t.Error("expected 'prod' value")
	} else if len(positions) != 2 {
		t.Errorf("expected 'prod' in 2 records, got %d", len(positions))
	}

	// "dev" appears in record 1 only
	if positions, ok := valMap["dev"]; !ok {
		t.Error("expected 'dev' value")
	} else if len(positions) != 1 {
		t.Errorf("expected 'dev' in 1 record, got %d", len(positions))
	}

	// Check KV index.
	kvEntries, ok := indexer.GetKV(chunkID)
	if !ok {
		t.Fatal("expected kv index to exist after build")
	}

	kvMap := make(map[string][]uint64)
	for _, e := range kvEntries {
		kvMap[e.Key+"\x00"+e.Value] = e.Positions
	}

	// "env=prod" appears in records 0 and 2
	if positions, ok := kvMap["env\x00prod"]; !ok {
		t.Error("expected 'env=prod' pair")
	} else if len(positions) != 2 {
		t.Errorf("expected 'env=prod' in 2 records, got %d", len(positions))
	}

	// "env=dev" appears in record 1 only
	if positions, ok := kvMap["env\x00dev"]; !ok {
		t.Error("expected 'env=dev' pair")
	} else if len(positions) != 1 {
		t.Errorf("expected 'env=dev' in 1 record, got %d", len(positions))
	}
}

func TestIndexerCaseFolding(t *testing.T) {
	records := []chunk.Record{
		{Attrs: chunk.Attributes{"ENV": "PROD"}, Raw: []byte("log1")},
		{Attrs: chunk.Attributes{"env": "prod"}, Raw: []byte("log2")},
		{Attrs: chunk.Attributes{"Env": "Prod"}, Raw: []byte("log3")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	// Key index should have single "env" entry with 3 positions.
	keyEntries, ok := indexer.GetKey(chunkID)
	if !ok {
		t.Fatal("expected key index to exist")
	}
	if len(keyEntries) != 1 {
		t.Fatalf("expected 1 key entry (case folded), got %d", len(keyEntries))
	}
	if keyEntries[0].Key != "env" {
		t.Errorf("expected key 'env', got %q", keyEntries[0].Key)
	}
	if len(keyEntries[0].Positions) != 3 {
		t.Errorf("expected 3 positions, got %d", len(keyEntries[0].Positions))
	}

	// Value index should have single "prod" entry with 3 positions.
	valEntries, ok := indexer.GetValue(chunkID)
	if !ok {
		t.Fatal("expected value index to exist")
	}
	if len(valEntries) != 1 {
		t.Fatalf("expected 1 value entry (case folded), got %d", len(valEntries))
	}
	if valEntries[0].Value != "prod" {
		t.Errorf("expected value 'prod', got %q", valEntries[0].Value)
	}
	if len(valEntries[0].Positions) != 3 {
		t.Errorf("expected 3 positions, got %d", len(valEntries[0].Positions))
	}

	// KV index should have single "env=prod" entry with 3 positions.
	kvEntries, ok := indexer.GetKV(chunkID)
	if !ok {
		t.Fatal("expected kv index to exist")
	}
	if len(kvEntries) != 1 {
		t.Fatalf("expected 1 kv entry (case folded), got %d", len(kvEntries))
	}
	if kvEntries[0].Key != "env" || kvEntries[0].Value != "prod" {
		t.Errorf("expected kv 'env=prod', got %q=%q", kvEntries[0].Key, kvEntries[0].Value)
	}
}

func TestIndexerDeduplication(t *testing.T) {
	// Record with duplicate key-value pairs (shouldn't happen, but test dedup).
	records := []chunk.Record{
		{Attrs: chunk.Attributes{"env": "prod", "environment": "prod"}, Raw: []byte("log1")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	// "prod" value should appear only once per record (deduped).
	valEntries, ok := indexer.GetValue(chunkID)
	if !ok {
		t.Fatal("expected value index to exist")
	}

	var prodPositions []uint64
	for _, e := range valEntries {
		if e.Value == "prod" {
			prodPositions = e.Positions
			break
		}
	}

	if len(prodPositions) != 1 {
		t.Errorf("expected 'prod' in 1 position (deduped), got %d", len(prodPositions))
	}
}

func TestIndexerBuildEmptyChunk(t *testing.T) {
	manager, chunkID := setupChunkManager(t, nil)
	indexer := NewIndexer(manager)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	keyEntries, ok := indexer.GetKey(chunkID)
	if !ok {
		t.Fatal("expected key index to exist after build")
	}
	if len(keyEntries) != 0 {
		t.Fatalf("expected 0 key entries, got %d", len(keyEntries))
	}

	valEntries, ok := indexer.GetValue(chunkID)
	if !ok {
		t.Fatal("expected value index to exist after build")
	}
	if len(valEntries) != 0 {
		t.Fatalf("expected 0 value entries, got %d", len(valEntries))
	}

	kvEntries, ok := indexer.GetKV(chunkID)
	if !ok {
		t.Fatal("expected kv index to exist after build")
	}
	if len(kvEntries) != 0 {
		t.Fatalf("expected 0 kv entries, got %d", len(kvEntries))
	}
}

func TestIndexerBuildNoAttrs(t *testing.T) {
	records := []chunk.Record{
		{Attrs: nil, Raw: []byte("log without attrs")},
		{Attrs: chunk.Attributes{}, Raw: []byte("log with empty attrs")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	keyEntries, ok := indexer.GetKey(chunkID)
	if !ok {
		t.Fatal("expected key index to exist after build")
	}
	if len(keyEntries) != 0 {
		t.Fatalf("expected 0 key entries, got %d", len(keyEntries))
	}
}

func TestIndexerBuildSorted(t *testing.T) {
	records := []chunk.Record{
		{Attrs: chunk.Attributes{"zebra": "z", "apple": "a", "mango": "m"}, Raw: []byte("log")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	// Check key entries are sorted.
	keyEntries, ok := indexer.GetKey(chunkID)
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
	valEntries, ok := indexer.GetValue(chunkID)
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
	kvEntries, ok := indexer.GetKV(chunkID)
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

func TestIndexerBuildUnsealedChunk(t *testing.T) {
	manager, err := chunkmemory.NewManager(chunkmemory.Config{})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	chunkID, _, err := manager.Append(chunk.Record{Attrs: chunk.Attributes{"k": "v"}, Raw: []byte("test")})
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
	records := []chunk.Record{
		{Attrs: chunk.Attributes{"k": "v"}, Raw: []byte("test")},
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
	records := []chunk.Record{
		{Attrs: chunk.Attributes{"k": "v"}, Raw: []byte("test")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	if _, ok := indexer.GetKey(chunkID); ok {
		t.Fatal("expected GetKey to return false for unbuilt chunk")
	}
	if _, ok := indexer.GetValue(chunkID); ok {
		t.Fatal("expected GetValue to return false for unbuilt chunk")
	}
	if _, ok := indexer.GetKV(chunkID); ok {
		t.Fatal("expected GetKV to return false for unbuilt chunk")
	}
}

func TestIndexerBuildInvalidChunkID(t *testing.T) {
	records := []chunk.Record{
		{Attrs: chunk.Attributes{"k": "v"}, Raw: []byte("test")},
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
	records := []chunk.Record{
		{Attrs: chunk.Attributes{"k": "v"}, Raw: []byte("test")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexer := NewIndexer(manager)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("first build: %v", err)
	}
	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("second build: %v", err)
	}

	keyEntries, ok := indexer.GetKey(chunkID)
	if !ok {
		t.Fatal("expected key index to exist after build")
	}
	if len(keyEntries) != 1 {
		t.Fatalf("expected 1 key entry, got %d", len(keyEntries))
	}
}
