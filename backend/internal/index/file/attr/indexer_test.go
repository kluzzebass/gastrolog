package attr

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	gotime "time"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/format"
	"gastrolog/internal/index"
	"gastrolog/internal/index/inverted"
)

func setupChunkManager(t *testing.T, records []chunk.Record) (chunk.ChunkManager, chunk.ChunkID) {
	t.Helper()
	dir := t.TempDir()
	manager, err := chunkfile.NewManager(chunkfile.Config{Dir: dir})
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
		{IngestTS: gotime.UnixMicro(1000), Attrs: chunk.Attributes{"env": "prod", "host": "srv1"}, Raw: []byte("msg1")},
		{IngestTS: gotime.UnixMicro(2000), Attrs: chunk.Attributes{"env": "prod", "host": "srv2"}, Raw: []byte("msg2")},
		{IngestTS: gotime.UnixMicro(3000), Attrs: chunk.Attributes{"env": "dev", "host": "srv1"}, Raw: []byte("msg3")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if indexer.Name() != "attr" {
		t.Fatalf("expected name %q, got %q", "attr", indexer.Name())
	}

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	// Verify all three index files exist
	keyPath := filepath.Join(indexDir, chunkID.String(), keyIndexFileName)
	valuePath := filepath.Join(indexDir, chunkID.String(), valueIndexFileName)
	kvPath := filepath.Join(indexDir, chunkID.String(), kvIndexFileName)

	for _, path := range []string{keyPath, valuePath, kvPath} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("index file not created: %s", path)
		}
	}

	// Load and verify key index
	keyEntries, err := LoadKeyIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("load key index: %v", err)
	}

	keyMap := make(map[string][]uint64)
	for _, e := range keyEntries {
		keyMap[e.Key] = e.Positions
	}

	// "env" appears in all 3, "host" appears in all 3
	if len(keyMap["env"]) != 3 {
		t.Fatalf("env: expected 3 positions, got %d", len(keyMap["env"]))
	}
	if len(keyMap["host"]) != 3 {
		t.Fatalf("host: expected 3 positions, got %d", len(keyMap["host"]))
	}

	// Load and verify value index
	valueEntries, err := LoadValueIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("load value index: %v", err)
	}

	valueMap := make(map[string][]uint64)
	for _, e := range valueEntries {
		valueMap[e.Value] = e.Positions
	}

	// "prod" appears in 2, "dev" in 1, "srv1" in 2, "srv2" in 1
	if len(valueMap["prod"]) != 2 {
		t.Fatalf("prod: expected 2 positions, got %d", len(valueMap["prod"]))
	}
	if len(valueMap["dev"]) != 1 {
		t.Fatalf("dev: expected 1 position, got %d", len(valueMap["dev"]))
	}
	if len(valueMap["srv1"]) != 2 {
		t.Fatalf("srv1: expected 2 positions, got %d", len(valueMap["srv1"]))
	}

	// Load and verify kv index
	kvEntries, err := LoadKVIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("load kv index: %v", err)
	}

	kvMap := make(map[string][]uint64)
	for _, e := range kvEntries {
		kvMap[e.Key+":"+e.Value] = e.Positions
	}

	// env=prod appears 2x, env=dev 1x, host=srv1 2x, host=srv2 1x
	if len(kvMap["env:prod"]) != 2 {
		t.Fatalf("env:prod: expected 2 positions, got %d", len(kvMap["env:prod"]))
	}
	if len(kvMap["env:dev"]) != 1 {
		t.Fatalf("env:dev: expected 1 position, got %d", len(kvMap["env:dev"]))
	}
}

func TestIndexerIdempotent(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(100), Attrs: chunk.Attributes{"k": "v"}, Raw: []byte("data")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("first build: %v", err)
	}
	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("second build: %v", err)
	}

	keyEntries, err := LoadKeyIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("load key index: %v", err)
	}
	if len(keyEntries) != 1 {
		t.Fatalf("expected 1 key entry, got %d", len(keyEntries))
	}
}

func TestIndexerCancelledContext(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(100), Attrs: chunk.Attributes{"k": "v"}, Raw: []byte("data")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := indexer.Build(ctx, chunkID)
	if err == nil {
		t.Fatal("expected error from cancelled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestIndexerBuildEmptyChunk(t *testing.T) {
	manager, chunkID := setupChunkManager(t, nil)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	keyEntries, err := LoadKeyIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("load key index: %v", err)
	}
	if len(keyEntries) != 0 {
		t.Fatalf("expected 0 key entries, got %d", len(keyEntries))
	}
}

func TestIndexerBuildUnsealedChunk(t *testing.T) {
	dir := t.TempDir()
	manager, err := chunkfile.NewManager(chunkfile.Config{Dir: dir})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	chunkID, _, err := manager.Append(chunk.Record{
		IngestTS: gotime.UnixMicro(1),
		Attrs:    chunk.Attributes{"k": "v"},
		Raw:      []byte("test"),
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	err = indexer.Build(context.Background(), chunkID)
	if err == nil {
		t.Fatal("expected error building index on unsealed chunk, got nil")
	}
	if err != chunk.ErrChunkNotSealed {
		t.Fatalf("expected ErrChunkNotSealed, got %v", err)
	}
}

func TestIndexerCaseFolding(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), Attrs: chunk.Attributes{"ENV": "PROD"}, Raw: []byte("msg1")},
		{IngestTS: gotime.UnixMicro(2), Attrs: chunk.Attributes{"Env": "Prod"}, Raw: []byte("msg2")},
		{IngestTS: gotime.UnixMicro(3), Attrs: chunk.Attributes{"env": "prod"}, Raw: []byte("msg3")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	// All should be normalized to lowercase
	keyEntries, err := LoadKeyIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("load key index: %v", err)
	}
	if len(keyEntries) != 1 {
		t.Fatalf("expected 1 key entry (case-folded), got %d", len(keyEntries))
	}
	if keyEntries[0].Key != "env" {
		t.Fatalf("expected key %q, got %q", "env", keyEntries[0].Key)
	}
	if len(keyEntries[0].Positions) != 3 {
		t.Fatalf("expected 3 positions, got %d", len(keyEntries[0].Positions))
	}

	valueEntries, err := LoadValueIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("load value index: %v", err)
	}
	if len(valueEntries) != 1 {
		t.Fatalf("expected 1 value entry (case-folded), got %d", len(valueEntries))
	}
	if valueEntries[0].Value != "prod" {
		t.Fatalf("expected value %q, got %q", "prod", valueEntries[0].Value)
	}
}

func TestIndexerDeduplication(t *testing.T) {
	// Same key=value multiple times in one record should only record position once
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), Attrs: chunk.Attributes{"k1": "v1", "k2": "v1"}, Raw: []byte("msg")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	// Value "v1" appears twice in attrs but should only have 1 position (deduped within record)
	valueEntries, err := LoadValueIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("load value index: %v", err)
	}
	if len(valueEntries) != 1 {
		t.Fatalf("expected 1 value entry, got %d", len(valueEntries))
	}
	if len(valueEntries[0].Positions) != 1 {
		t.Fatalf("expected 1 position (deduped), got %d", len(valueEntries[0].Positions))
	}
}

func TestEncodeDecodeKeyIndexRoundTrip(t *testing.T) {
	entries := []index.AttrKeyIndexEntry{
		{Key: "alpha", Positions: []uint64{0, 128, 256}},
		{Key: "beta", Positions: []uint64{64, 192}},
	}

	data := encodeKeyIndex(entries)
	got, err := decodeKeyIndex(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(got))
	}

	for i := range entries {
		if got[i].Key != entries[i].Key {
			t.Fatalf("entry %d: expected key %q, got %q", i, entries[i].Key, got[i].Key)
		}
		if len(got[i].Positions) != len(entries[i].Positions) {
			t.Fatalf("entry %d: expected %d positions, got %d", i, len(entries[i].Positions), len(got[i].Positions))
		}
	}
}

func TestEncodeDecodeValueIndexRoundTrip(t *testing.T) {
	entries := []index.AttrValueIndexEntry{
		{Value: "prod", Positions: []uint64{0, 64}},
		{Value: "dev", Positions: []uint64{128}},
	}

	data := encodeValueIndex(entries)
	got, err := decodeValueIndex(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(got))
	}

	for i := range entries {
		if got[i].Value != entries[i].Value {
			t.Fatalf("entry %d: expected value %q, got %q", i, entries[i].Value, got[i].Value)
		}
	}
}

func TestEncodeDecodeKVIndexRoundTrip(t *testing.T) {
	entries := []index.AttrKVIndexEntry{
		{Key: "env", Value: "prod", Positions: []uint64{0, 64}},
		{Key: "env", Value: "dev", Positions: []uint64{128}},
	}

	data := encodeKVIndex(entries)
	got, err := decodeKVIndex(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(got))
	}

	for i := range entries {
		if got[i].Key != entries[i].Key || got[i].Value != entries[i].Value {
			t.Fatalf("entry %d: expected %s=%s, got %s=%s", i, entries[i].Key, entries[i].Value, got[i].Key, got[i].Value)
		}
	}
}

func TestEncodeDecodeEmpty(t *testing.T) {
	keyData := encodeKeyIndex(nil)
	keyGot, err := decodeKeyIndex(keyData)
	if err != nil {
		t.Fatalf("decode empty key index: %v", err)
	}
	if len(keyGot) != 0 {
		t.Fatalf("expected 0 key entries, got %d", len(keyGot))
	}

	valueData := encodeValueIndex(nil)
	valueGot, err := decodeValueIndex(valueData)
	if err != nil {
		t.Fatalf("decode empty value index: %v", err)
	}
	if len(valueGot) != 0 {
		t.Fatalf("expected 0 value entries, got %d", len(valueGot))
	}

	kvData := encodeKVIndex(nil)
	kvGot, err := decodeKVIndex(kvData)
	if err != nil {
		t.Fatalf("decode empty kv index: %v", err)
	}
	if len(kvGot) != 0 {
		t.Fatalf("expected 0 kv entries, got %d", len(kvGot))
	}
}

func TestDecodeErrors(t *testing.T) {
	// Too small
	if _, err := decodeKeyIndex([]byte{'i'}); err != inverted.ErrIndexTooSmall {
		t.Fatalf("expected ErrIndexTooSmall, got %v", err)
	}

	// Wrong signature
	bad := make([]byte, headerSize)
	bad[0] = 0xFF
	bad[1] = format.TypeAttrKeyIndex
	bad[2] = currentVersion
	if _, err := decodeKeyIndex(bad); err == nil {
		t.Fatal("expected error for wrong signature, got nil")
	}

	// Wrong type byte
	bad2 := make([]byte, headerSize)
	bad2[0] = format.Signature
	bad2[1] = 'x'
	bad2[2] = currentVersion
	if _, err := decodeKeyIndex(bad2); err == nil {
		t.Fatal("expected error for wrong type byte, got nil")
	}

	// Wrong version
	bad3 := make([]byte, headerSize)
	bad3[0] = format.Signature
	bad3[1] = format.TypeAttrKeyIndex
	bad3[2] = 0xFF
	if _, err := decodeKeyIndex(bad3); err == nil {
		t.Fatal("expected error for wrong version, got nil")
	}
}

func TestLoadIndexNotFound(t *testing.T) {
	indexDir := t.TempDir()
	bogusID := chunk.NewChunkID()

	_, err := LoadKeyIndex(indexDir, bogusID)
	if err == nil {
		t.Fatal("expected error loading nonexistent key index, got nil")
	}

	_, err = LoadValueIndex(indexDir, bogusID)
	if err == nil {
		t.Fatal("expected error loading nonexistent value index, got nil")
	}

	_, err = LoadKVIndex(indexDir, bogusID)
	if err == nil {
		t.Fatal("expected error loading nonexistent kv index, got nil")
	}
}

func TestIndexerEntriesSorted(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), Attrs: chunk.Attributes{"zebra": "zoo", "alpha": "ant"}, Raw: []byte("msg")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	keyEntries, err := LoadKeyIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("load key index: %v", err)
	}

	// Keys should be sorted alphabetically
	for i := 1; i < len(keyEntries); i++ {
		if keyEntries[i].Key <= keyEntries[i-1].Key {
			t.Fatalf("keys not sorted at index %d: %q <= %q", i, keyEntries[i].Key, keyEntries[i-1].Key)
		}
	}

	valueEntries, err := LoadValueIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("load value index: %v", err)
	}

	// Values should be sorted alphabetically
	for i := 1; i < len(valueEntries); i++ {
		if valueEntries[i].Value <= valueEntries[i-1].Value {
			t.Fatalf("values not sorted at index %d: %q <= %q", i, valueEntries[i].Value, valueEntries[i-1].Value)
		}
	}
}

func TestOpenReaders(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), Attrs: chunk.Attributes{"env": "prod", "region": "us"}, Raw: []byte("msg1")},
		{IngestTS: gotime.UnixMicro(2), Attrs: chunk.Attributes{"env": "dev", "region": "eu"}, Raw: []byte("msg2")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	// Test key reader
	keyReader, err := OpenKeyIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("open key index: %v", err)
	}

	positions, found := keyReader.Lookup("env")
	if !found {
		t.Fatal("expected to find key 'env'")
	}
	if len(positions) != 2 {
		t.Fatalf("expected 2 positions for 'env', got %d", len(positions))
	}

	_, found = keyReader.Lookup("notfound")
	if found {
		t.Fatal("expected not to find key 'notfound'")
	}

	// Test value reader
	valueReader, err := OpenValueIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("open value index: %v", err)
	}

	positions, found = valueReader.Lookup("prod")
	if !found {
		t.Fatal("expected to find value 'prod'")
	}
	if len(positions) != 1 {
		t.Fatalf("expected 1 position for 'prod', got %d", len(positions))
	}

	// Test kv reader
	kvReader, err := OpenKVIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("open kv index: %v", err)
	}

	positions, found = kvReader.Lookup("env", "prod")
	if !found {
		t.Fatal("expected to find kv 'env=prod'")
	}
	if len(positions) != 1 {
		t.Fatalf("expected 1 position for 'env=prod', got %d", len(positions))
	}

	_, found = kvReader.Lookup("env", "staging")
	if found {
		t.Fatal("expected not to find kv 'env=staging'")
	}
}
