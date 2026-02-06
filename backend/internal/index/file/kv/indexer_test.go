package kv

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
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1000), Attrs: attrs, Raw: []byte("status=500 method=GET")},
		{IngestTS: gotime.UnixMicro(2000), Attrs: attrs, Raw: []byte("status=200 method=POST")},
		{IngestTS: gotime.UnixMicro(3000), Attrs: attrs, Raw: []byte("status=500 method=PUT")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if indexer.Name() != "kv" {
		t.Fatalf("expected name %q, got %q", "kv", indexer.Name())
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
	keyEntries, status, err := LoadKeyIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("load key index: %v", err)
	}
	if status != index.KVComplete {
		t.Fatalf("expected complete status, got %v", status)
	}

	keyMap := make(map[string][]uint64)
	for _, e := range keyEntries {
		keyMap[e.Key] = e.Positions
	}

	// "status" appears in all 3, "method" appears in all 3
	if len(keyMap["status"]) != 3 {
		t.Fatalf("status: expected 3 positions, got %d", len(keyMap["status"]))
	}
	if len(keyMap["method"]) != 3 {
		t.Fatalf("method: expected 3 positions, got %d", len(keyMap["method"]))
	}

	// Load and verify value index
	valueEntries, status, err := LoadValueIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("load value index: %v", err)
	}
	if status != index.KVComplete {
		t.Fatalf("expected complete status, got %v", status)
	}

	valueMap := make(map[string][]uint64)
	for _, e := range valueEntries {
		valueMap[e.Value] = e.Positions
	}

	// "500" in 2 records, "200" in 1, "GET" in 1, "POST" in 1, "PUT" in 1
	if len(valueMap["500"]) != 2 {
		t.Fatalf("500: expected 2 positions, got %d", len(valueMap["500"]))
	}
	if len(valueMap["200"]) != 1 {
		t.Fatalf("200: expected 1 position, got %d", len(valueMap["200"]))
	}

	// Load and verify kv index
	kvEntries, status, err := LoadKVIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("load kv index: %v", err)
	}
	if status != index.KVComplete {
		t.Fatalf("expected complete status, got %v", status)
	}

	kvMap := make(map[string][]uint64)
	for _, e := range kvEntries {
		kvMap[e.Key+":"+e.Value] = e.Positions
	}

	if len(kvMap["status:500"]) != 2 {
		t.Fatalf("status:500: expected 2 positions, got %d", len(kvMap["status:500"]))
	}
	if len(kvMap["method:get"]) != 1 {
		t.Fatalf("method:get: expected 1 position, got %d", len(kvMap["method:get"]))
	}
}

func TestIndexerIdempotent(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(100), Attrs: attrs, Raw: []byte("key=value")},
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

	keyEntries, _, err := LoadKeyIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("load key index: %v", err)
	}
	if len(keyEntries) != 1 {
		t.Fatalf("expected 1 key entry, got %d", len(keyEntries))
	}
}

func TestIndexerCancelledContext(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(100), Attrs: attrs, Raw: []byte("key=value")},
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

	keyEntries, status, err := LoadKeyIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("load key index: %v", err)
	}
	if status != index.KVComplete {
		t.Fatalf("expected complete status, got %v", status)
	}
	if len(keyEntries) != 0 {
		t.Fatalf("expected 0 key entries, got %d", len(keyEntries))
	}
}

func TestIndexerBuildNoKeyValues(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(100), Attrs: attrs, Raw: []byte("just plain text no key values")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	keyEntries, _, err := LoadKeyIndex(indexDir, chunkID)
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

	attrs := chunk.Attributes{"source": "test"}
	chunkID, _, err := manager.Append(chunk.Record{
		IngestTS: gotime.UnixMicro(1),
		Attrs:    attrs,
		Raw:      []byte("key=value"),
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
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), Attrs: attrs, Raw: []byte("STATUS=500")},
		{IngestTS: gotime.UnixMicro(2), Attrs: attrs, Raw: []byte("Status=500")},
		{IngestTS: gotime.UnixMicro(3), Attrs: attrs, Raw: []byte("status=500")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	// Keys should be normalized to lowercase
	keyEntries, _, err := LoadKeyIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("load key index: %v", err)
	}
	if len(keyEntries) != 1 {
		t.Fatalf("expected 1 key entry (case-folded), got %d", len(keyEntries))
	}
	if keyEntries[0].Key != "status" {
		t.Fatalf("expected key %q, got %q", "status", keyEntries[0].Key)
	}
	if len(keyEntries[0].Positions) != 3 {
		t.Fatalf("expected 3 positions, got %d", len(keyEntries[0].Positions))
	}
}

func TestIndexerDeduplication(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	// Same key=value pair multiple times in one message
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), Attrs: attrs, Raw: []byte("status=500 status=500 status=500")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	kvEntries, _, err := LoadKVIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("load kv index: %v", err)
	}
	if len(kvEntries) != 1 {
		t.Fatalf("expected 1 kv entry, got %d", len(kvEntries))
	}
	// Should only have 1 position despite appearing 3 times (deduped within record)
	if len(kvEntries[0].Positions) != 1 {
		t.Fatalf("expected 1 position (deduped), got %d", len(kvEntries[0].Positions))
	}
}

func TestIndexerDottedKeys(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), Attrs: attrs, Raw: []byte("http.status=200 http.method=GET")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	keyEntries, _, err := LoadKeyIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("load key index: %v", err)
	}

	keyMap := make(map[string]bool)
	for _, e := range keyEntries {
		keyMap[e.Key] = true
	}

	if !keyMap["http.status"] {
		t.Fatal("expected to find key 'http.status'")
	}
	if !keyMap["http.method"] {
		t.Fatal("expected to find key 'http.method'")
	}
}

func TestEncodeDecodeKeyIndexRoundTrip(t *testing.T) {
	entries := []index.KVKeyIndexEntry{
		{Key: "alpha", Positions: []uint64{0, 128, 256}},
		{Key: "beta", Positions: []uint64{64, 192}},
	}

	data := encodeKeyIndex(entries, index.KVComplete)
	got, status, err := decodeKeyIndex(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if status != index.KVComplete {
		t.Fatalf("expected complete status, got %v", status)
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
	entries := []index.KVValueIndexEntry{
		{Value: "500", Positions: []uint64{0, 64}},
		{Value: "200", Positions: []uint64{128}},
	}

	data := encodeValueIndex(entries, index.KVComplete)
	got, status, err := decodeValueIndex(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if status != index.KVComplete {
		t.Fatalf("expected complete status, got %v", status)
	}

	if len(got) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(got))
	}
}

func TestEncodeDecodeKVIndexRoundTrip(t *testing.T) {
	entries := []index.KVIndexEntry{
		{Key: "status", Value: "500", Positions: []uint64{0, 64}},
		{Key: "status", Value: "200", Positions: []uint64{128}},
	}

	data := encodeKVIndex(entries, index.KVComplete)
	got, status, err := decodeKVIndex(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if status != index.KVComplete {
		t.Fatalf("expected complete status, got %v", status)
	}

	if len(got) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(got))
	}
}

func TestEncodeDecodeCappedStatus(t *testing.T) {
	// Test that capped status is preserved
	data := encodeKeyIndex(nil, index.KVCapped)
	_, status, err := decodeKeyIndex(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if status != index.KVCapped {
		t.Fatalf("expected capped status, got %v", status)
	}
}

func TestEncodeDecodeEmpty(t *testing.T) {
	keyData := encodeKeyIndex(nil, index.KVComplete)
	keyGot, _, err := decodeKeyIndex(keyData)
	if err != nil {
		t.Fatalf("decode empty key index: %v", err)
	}
	if len(keyGot) != 0 {
		t.Fatalf("expected 0 key entries, got %d", len(keyGot))
	}

	valueData := encodeValueIndex(nil, index.KVComplete)
	valueGot, _, err := decodeValueIndex(valueData)
	if err != nil {
		t.Fatalf("decode empty value index: %v", err)
	}
	if len(valueGot) != 0 {
		t.Fatalf("expected 0 value entries, got %d", len(valueGot))
	}

	kvData := encodeKVIndex(nil, index.KVComplete)
	kvGot, _, err := decodeKVIndex(kvData)
	if err != nil {
		t.Fatalf("decode empty kv index: %v", err)
	}
	if len(kvGot) != 0 {
		t.Fatalf("expected 0 kv entries, got %d", len(kvGot))
	}
}

func TestDecodeErrors(t *testing.T) {
	// Too small
	if _, _, err := decodeKeyIndex([]byte{'i'}); err != inverted.ErrIndexTooSmall {
		t.Fatalf("expected ErrIndexTooSmall, got %v", err)
	}

	// Wrong signature
	bad := make([]byte, headerSize)
	bad[0] = 0xFF
	bad[1] = format.TypeKVKeyIndex
	bad[2] = currentVersion
	if _, _, err := decodeKeyIndex(bad); err == nil {
		t.Fatal("expected error for wrong signature, got nil")
	}

	// Wrong type byte
	bad2 := make([]byte, headerSize)
	bad2[0] = format.Signature
	bad2[1] = 'x'
	bad2[2] = currentVersion
	if _, _, err := decodeKeyIndex(bad2); err == nil {
		t.Fatal("expected error for wrong type byte, got nil")
	}

	// Wrong version
	bad3 := make([]byte, headerSize)
	bad3[0] = format.Signature
	bad3[1] = format.TypeKVKeyIndex
	bad3[2] = 0xFF
	if _, _, err := decodeKeyIndex(bad3); err == nil {
		t.Fatal("expected error for wrong version, got nil")
	}
}

func TestLoadIndexNotFound(t *testing.T) {
	indexDir := t.TempDir()
	bogusID := chunk.NewChunkID()

	_, _, err := LoadKeyIndex(indexDir, bogusID)
	if err == nil {
		t.Fatal("expected error loading nonexistent key index, got nil")
	}

	_, _, err = LoadValueIndex(indexDir, bogusID)
	if err == nil {
		t.Fatal("expected error loading nonexistent value index, got nil")
	}

	_, _, err = LoadKVIndex(indexDir, bogusID)
	if err == nil {
		t.Fatal("expected error loading nonexistent kv index, got nil")
	}
}

func TestIndexerEntriesSorted(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), Attrs: attrs, Raw: []byte("zebra=zoo alpha=ant")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	keyEntries, _, err := LoadKeyIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("load key index: %v", err)
	}

	// Keys should be sorted alphabetically
	for i := 1; i < len(keyEntries); i++ {
		if keyEntries[i].Key <= keyEntries[i-1].Key {
			t.Fatalf("keys not sorted at index %d: %q <= %q", i, keyEntries[i].Key, keyEntries[i-1].Key)
		}
	}
}

func TestOpenReaders(t *testing.T) {
	attrs := chunk.Attributes{"source": "test"}
	records := []chunk.Record{
		{IngestTS: gotime.UnixMicro(1), Attrs: attrs, Raw: []byte("status=500 method=GET")},
		{IngestTS: gotime.UnixMicro(2), Attrs: attrs, Raw: []byte("status=200 method=POST")},
	}

	manager, chunkID := setupChunkManager(t, records)
	indexDir := t.TempDir()
	indexer := NewIndexer(indexDir, manager, nil)

	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	// Test key reader
	keyReader, status, err := OpenKeyIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("open key index: %v", err)
	}
	if status != index.KVComplete {
		t.Fatalf("expected complete status, got %v", status)
	}

	positions, found := keyReader.Lookup("status")
	if !found {
		t.Fatal("expected to find key 'status'")
	}
	if len(positions) != 2 {
		t.Fatalf("expected 2 positions for 'status', got %d", len(positions))
	}

	_, found = keyReader.Lookup("notfound")
	if found {
		t.Fatal("expected not to find key 'notfound'")
	}

	// Test value reader
	valueReader, status, err := OpenValueIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("open value index: %v", err)
	}
	if status != index.KVComplete {
		t.Fatalf("expected complete status, got %v", status)
	}

	positions, found = valueReader.Lookup("500")
	if !found {
		t.Fatal("expected to find value '500'")
	}
	if len(positions) != 1 {
		t.Fatalf("expected 1 position for '500', got %d", len(positions))
	}

	// Test kv reader
	kvReader, status, err := OpenKVIndex(indexDir, chunkID)
	if err != nil {
		t.Fatalf("open kv index: %v", err)
	}
	if status != index.KVComplete {
		t.Fatalf("expected complete status, got %v", status)
	}

	positions, found = kvReader.Lookup("status", "500")
	if !found {
		t.Fatal("expected to find kv 'status=500'")
	}
	if len(positions) != 1 {
		t.Fatalf("expected 1 position for 'status=500', got %d", len(positions))
	}

	_, found = kvReader.Lookup("status", "404")
	if found {
		t.Fatal("expected not to find kv 'status=404'")
	}
}
