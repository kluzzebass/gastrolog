package json

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
)

func setupChunk(t *testing.T, records []string) (chunk.ChunkManager, chunk.ChunkID) {
	t.Helper()
	cm, err := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(100000),
	})
	if err != nil {
		t.Fatalf("create chunk manager: %v", err)
	}
	for _, r := range records {
		if _, _, err := cm.Append(chunk.Record{Raw: []byte(r)}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	meta := cm.Active()
	if meta == nil {
		t.Fatal("no active chunk")
	}
	chunkID := meta.ID
	if err := cm.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}
	return cm, chunkID
}

func TestIndexer_BuildAndRead(t *testing.T) {
	records := []string{
		`{"level":"error","service":"gateway","http":{"status":500}}`,
		`{"level":"info","service":"gateway","http":{"status":200}}`,
		`{"level":"error","service":"auth","tags":["login","failed"]}`,
		`{"level":"info","service":"auth","message":"success"}`,
	}
	cm, chunkID := setupChunk(t, records)

	dir := t.TempDir()
	indexer := NewIndexer(dir, cm, nil)
	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	pathEntries, pvEntries, status, err := LoadIndex(dir, chunkID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if status != index.JSONComplete {
		t.Errorf("status = %v, want JSONComplete", status)
	}

	reader := index.NewJSONIndexReader(chunkID, pathEntries, status, pvEntries, status)

	// "level" should be in all 4 records.
	positions, found := reader.LookupPath("level")
	if !found {
		t.Fatal("LookupPath(level) not found")
	}
	if len(positions) != 4 {
		t.Errorf("LookupPath(level) = %d positions, want 4", len(positions))
	}

	// "http\x00status" should be in records 0,1.
	positions, found = reader.LookupPath("http\x00status")
	if !found {
		t.Fatal("LookupPath(http\\x00status) not found")
	}
	if len(positions) != 2 {
		t.Errorf("LookupPath(http\\x00status) = %d positions, want 2", len(positions))
	}

	// level=error should be in records 0,2.
	positions, found = reader.LookupPathValue("level", "error")
	if !found {
		t.Fatal("LookupPathValue(level, error) not found")
	}
	if len(positions) != 2 {
		t.Errorf("LookupPathValue(level, error) = %d positions, want 2", len(positions))
	}

	// service=gateway should be in records 0,1.
	positions, found = reader.LookupPathValue("service", "gateway")
	if !found {
		t.Fatal("LookupPathValue(service, gateway) not found")
	}
	if len(positions) != 2 {
		t.Errorf("LookupPathValue(service, gateway) = %d positions, want 2", len(positions))
	}

	// tags\x00[*] should exist.
	positions, found = reader.LookupPath("tags\x00[*]")
	if !found {
		t.Fatal("LookupPath(tags\\x00[*]) not found")
	}
	if len(positions) != 1 {
		t.Errorf("LookupPath(tags\\x00[*]) = %d positions, want 1", len(positions))
	}

	// Non-existent path.
	_, found = reader.LookupPath("nonexistent")
	if found {
		t.Error("LookupPath(nonexistent) should not be found")
	}
}

func TestIndexer_NonJSON(t *testing.T) {
	var records []string
	for range 5 {
		records = append(records, "plain text log message")
	}
	cm, chunkID := setupChunk(t, records)

	dir := t.TempDir()
	indexer := NewIndexer(dir, cm, nil)
	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	pathEntries, pvEntries, status, err := LoadIndex(dir, chunkID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if status != index.JSONComplete {
		t.Errorf("status = %v, want JSONComplete", status)
	}
	if len(pathEntries) != 0 {
		t.Errorf("pathEntries = %d, want 0", len(pathEntries))
	}
	if len(pvEntries) != 0 {
		t.Errorf("pvEntries = %d, want 0", len(pvEntries))
	}
}

func TestIndexer_PathPrefix(t *testing.T) {
	records := []string{
		`{"http":{"method":"GET","status":200}}`,
		`{"http":{"method":"POST","status":201}}`,
		`{"service":"web"}`,
	}
	cm, chunkID := setupChunk(t, records)

	dir := t.TempDir()
	indexer := NewIndexer(dir, cm, nil)
	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	pathEntries, pvEntries, status, err := LoadIndex(dir, chunkID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	reader := index.NewJSONIndexReader(chunkID, pathEntries, status, pvEntries, status)

	// LookupPathPrefix("http") should find records with http.
	positions, found := reader.LookupPathPrefix("http")
	if !found {
		t.Fatal("LookupPathPrefix(http) not found")
	}
	if len(positions) != 2 {
		t.Errorf("LookupPathPrefix(http) = %d positions, want 2", len(positions))
	}
}

func TestIndexer_CaseInsensitive(t *testing.T) {
	records := []string{
		`{"Level":"ERROR","Service":"MyApp"}`,
	}
	cm, chunkID := setupChunk(t, records)

	dir := t.TempDir()
	indexer := NewIndexer(dir, cm, nil)
	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	pathEntries, pvEntries, status, err := LoadIndex(dir, chunkID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	reader := index.NewJSONIndexReader(chunkID, pathEntries, status, pvEntries, status)

	_, found := reader.LookupPath("level")
	if !found {
		t.Error("LookupPath(level) should find lowercased path")
	}

	_, found = reader.LookupPathValue("level", "error")
	if !found {
		t.Error("LookupPathValue(level, error) should find lowercased value")
	}
}

func TestIndexer_Budget(t *testing.T) {
	var records []string
	for i := range 100 {
		record := fmt.Sprintf(`{"key%d":"value%d"}`, i, i)
		records = append(records, record)
	}
	cm, chunkID := setupChunk(t, records)

	dir := t.TempDir()
	indexer := NewIndexerWithConfig(dir, cm, nil, Config{Budget: 200})
	if err := indexer.Build(context.Background(), chunkID); err != nil {
		t.Fatalf("build: %v", err)
	}

	pathEntries, pvEntries, status, err := LoadIndex(dir, chunkID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if status != index.JSONCapped {
		t.Errorf("status = %v, want JSONCapped", status)
	}

	// Paths should still be present.
	if len(pathEntries) == 0 {
		t.Error("pathEntries should not be empty even when capped")
	}

	// PV entries should be fewer than total unique pairs.
	if len(pvEntries) >= 100 {
		t.Errorf("pvEntries = %d, expected fewer than 100 due to budget", len(pvEntries))
	}
}

func TestIndexer_FormatRoundTrip(t *testing.T) {
	dict := []string{"error", "level", "service", "web"}
	pathTable := []pathTableEntry{
		{dictID: 1, blobOffset: 0, count: 2},
		{dictID: 2, blobOffset: 8, count: 1},
	}
	pvTable := []pvTableEntry{
		{pathID: 1, valueID: 0, blobOffset: 12, count: 1},
		{pathID: 2, valueID: 3, blobOffset: 16, count: 1},
	}

	postingBlob := make([]byte, 20)
	binary.LittleEndian.PutUint32(postingBlob[0:], 0)
	binary.LittleEndian.PutUint32(postingBlob[4:], 2)
	binary.LittleEndian.PutUint32(postingBlob[8:], 1)
	binary.LittleEndian.PutUint32(postingBlob[12:], 0)
	binary.LittleEndian.PutUint32(postingBlob[16:], 3)

	data := encodeIndex(dict, pathTable, pvTable, postingBlob, index.JSONComplete)
	pathEntries, pvEntries, status, err := decodeIndex(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if status != index.JSONComplete {
		t.Errorf("status = %v, want JSONComplete", status)
	}

	if len(pathEntries) != 2 {
		t.Fatalf("pathEntries = %d, want 2", len(pathEntries))
	}
	if pathEntries[0].Path != "level" {
		t.Errorf("pathEntries[0].Path = %q, want %q", pathEntries[0].Path, "level")
	}
	if !slices.Equal(pathEntries[0].Positions, []uint64{0, 2}) {
		t.Errorf("pathEntries[0].Positions = %v, want [0, 2]", pathEntries[0].Positions)
	}

	if len(pvEntries) != 2 {
		t.Fatalf("pvEntries = %d, want 2", len(pvEntries))
	}
	if pvEntries[0].Path != "level" || pvEntries[0].Value != "error" {
		t.Errorf("pvEntries[0] = %q=%q, want level=error", pvEntries[0].Path, pvEntries[0].Value)
	}
	if !slices.Equal(pvEntries[0].Positions, []uint64{0}) {
		t.Errorf("pvEntries[0].Positions = %v, want [0]", pvEntries[0].Positions)
	}
}
