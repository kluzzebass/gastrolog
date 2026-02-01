package analyzer

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	indexmem "gastrolog/internal/index/memory"
	memattr "gastrolog/internal/index/memory/attr"
	memkv "gastrolog/internal/index/memory/kv"
	memtime "gastrolog/internal/index/memory/time"
	memtoken "gastrolog/internal/index/memory/token"
)

func setupTestSystem(t *testing.T, records []chunk.Record) (chunk.ChunkManager, index.IndexManager, chunk.ChunkID) {
	t.Helper()

	cm, err := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})
	if err != nil {
		t.Fatalf("create chunk manager: %v", err)
	}

	// Append records
	for _, rec := range records {
		if _, _, err := cm.Append(rec); err != nil {
			t.Fatalf("append record: %v", err)
		}
	}

	// Get active chunk and seal it
	meta := cm.Active()
	if meta == nil {
		t.Fatal("no active chunk")
	}
	chunkID := meta.ID

	if err := cm.Seal(); err != nil {
		t.Fatalf("seal chunk: %v", err)
	}

	// Create indexers
	timeIdx := memtime.NewIndexer(cm, 1) // Sample every record
	tokenIdx := memtoken.NewIndexer(cm)
	attrIdx := memattr.NewIndexer(cm)
	kvIdx := memkv.NewIndexer(cm)

	im := indexmem.NewManager(
		[]index.Indexer{timeIdx, tokenIdx, attrIdx, kvIdx},
		timeIdx, tokenIdx, attrIdx, kvIdx, nil,
	)

	// Build indexes
	if err := im.BuildIndexes(t.Context(), chunkID); err != nil {
		t.Fatalf("build indexes: %v", err)
	}

	return cm, im, chunkID
}

func TestAnalyzeChunk_Basic(t *testing.T) {
	now := time.Now()
	records := []chunk.Record{
		{
			IngestTS: now,
			WriteTS:  now,
			Attrs:    chunk.Attributes{"host": "server1", "level": "info"},
			Raw:      []byte("user=alice action=login status=success"),
		},
		{
			IngestTS: now.Add(time.Second),
			WriteTS:  now.Add(time.Second),
			Attrs:    chunk.Attributes{"host": "server1", "level": "error"},
			Raw:      []byte("user=bob action=login status=failure"),
		},
		{
			IngestTS: now.Add(2 * time.Second),
			WriteTS:  now.Add(2 * time.Second),
			Attrs:    chunk.Attributes{"host": "server2", "level": "info"},
			Raw:      []byte("user=alice action=logout"),
		},
	}

	cm, im, chunkID := setupTestSystem(t, records)
	analyzer := New(cm, im)

	analysis, err := analyzer.AnalyzeChunk(chunkID)
	if err != nil {
		t.Fatalf("analyze chunk: %v", err)
	}

	// Basic checks
	if analysis.ChunkID != chunkID {
		t.Errorf("chunk ID mismatch: got %v, want %v", analysis.ChunkID, chunkID)
	}
	if analysis.ChunkRecords != 3 {
		t.Errorf("chunk records: got %d, want 3", analysis.ChunkRecords)
	}
	if !analysis.Sealed {
		t.Error("expected chunk to be sealed")
	}

	// Should have 4 summaries (time, token, attr_kv, kv)
	if len(analysis.Summaries) != 4 {
		t.Errorf("summaries count: got %d, want 4", len(analysis.Summaries))
	}

	// All indexes should be enabled
	for _, s := range analysis.Summaries {
		if s.Status != StatusEnabled {
			t.Errorf("index %s status: got %s, want %s", s.IndexType, s.Status, StatusEnabled)
		}
	}
}

func TestAnalyzeChunk_TimeIndex(t *testing.T) {
	now := time.Now()
	records := make([]chunk.Record, 100)
	for i := range records {
		records[i] = chunk.Record{
			IngestTS: now.Add(time.Duration(i) * time.Second),
			WriteTS:  now.Add(time.Duration(i) * time.Second),
			Raw:      []byte("test message"),
		}
	}

	cm, im, chunkID := setupTestSystem(t, records)
	analyzer := New(cm, im)

	analysis, err := analyzer.AnalyzeChunk(chunkID)
	if err != nil {
		t.Fatalf("analyze chunk: %v", err)
	}

	stats := analysis.TimeStats
	if stats == nil {
		t.Fatal("time stats is nil")
	}

	if stats.EntriesCount != 100 {
		t.Errorf("entries count: got %d, want 100", stats.EntriesCount)
	}
	if stats.EarliestTimestamp.IsZero() {
		t.Error("earliest timestamp is zero")
	}
	if stats.LatestTimestamp.IsZero() {
		t.Error("latest timestamp is zero")
	}
	if stats.IndexBytes <= 0 {
		t.Errorf("index bytes should be positive: got %d", stats.IndexBytes)
	}
}

func TestAnalyzeChunk_TokenIndex(t *testing.T) {
	now := time.Now()
	records := []chunk.Record{
		{IngestTS: now, WriteTS: now, Raw: []byte("error in module foo")},
		{IngestTS: now, WriteTS: now, Raw: []byte("warning in module bar")},
		{IngestTS: now, WriteTS: now, Raw: []byte("error in module bar")},
	}

	cm, im, chunkID := setupTestSystem(t, records)
	analyzer := New(cm, im)

	analysis, err := analyzer.AnalyzeChunk(chunkID)
	if err != nil {
		t.Fatalf("analyze chunk: %v", err)
	}

	stats := analysis.TokenStats
	if stats == nil {
		t.Fatal("token stats is nil")
	}

	if stats.UniqueTokens == 0 {
		t.Error("unique tokens should be > 0")
	}
	if stats.TotalTokenOccurrences == 0 {
		t.Error("total token occurrences should be > 0")
	}
	if stats.MaxTokenFrequency == 0 {
		t.Error("max token frequency should be > 0")
	}

	// Check top tokens exist
	if len(stats.TopTokensByFrequency) == 0 {
		t.Error("top tokens by frequency should not be empty")
	}
}

func TestAnalyzeChunk_AttrKVIndex(t *testing.T) {
	now := time.Now()
	records := []chunk.Record{
		{
			IngestTS: now,
			WriteTS:  now,
			Attrs:    chunk.Attributes{"host": "server1", "env": "prod"},
			Raw:      []byte("test"),
		},
		{
			IngestTS: now,
			WriteTS:  now,
			Attrs:    chunk.Attributes{"host": "server2", "env": "prod"},
			Raw:      []byte("test"),
		},
		{
			IngestTS: now,
			WriteTS:  now,
			Attrs:    chunk.Attributes{"host": "server1", "env": "staging"},
			Raw:      []byte("test"),
		},
	}

	cm, im, chunkID := setupTestSystem(t, records)
	analyzer := New(cm, im)

	analysis, err := analyzer.AnalyzeChunk(chunkID)
	if err != nil {
		t.Fatalf("analyze chunk: %v", err)
	}

	stats := analysis.AttrKVStats
	if stats == nil {
		t.Fatal("attr kv stats is nil")
	}

	// 2 unique keys: host, env
	if stats.UniqueKeys != 2 {
		t.Errorf("unique keys: got %d, want 2", stats.UniqueKeys)
	}

	// 4 unique values: server1, server2, prod, staging
	if stats.UniqueValues != 4 {
		t.Errorf("unique values: got %d, want 4", stats.UniqueValues)
	}

	// Check coverage
	if stats.RecordsWithAttributes != 3 {
		t.Errorf("records with attributes: got %d, want 3", stats.RecordsWithAttributes)
	}
}

func TestAnalyzeChunk_KVIndex(t *testing.T) {
	now := time.Now()
	records := []chunk.Record{
		{IngestTS: now, WriteTS: now, Raw: []byte("user=alice action=login")},
		{IngestTS: now, WriteTS: now, Raw: []byte("user=bob action=login")},
		{IngestTS: now, WriteTS: now, Raw: []byte("user=alice action=logout")},
	}

	cm, im, chunkID := setupTestSystem(t, records)
	analyzer := New(cm, im)

	analysis, err := analyzer.AnalyzeChunk(chunkID)
	if err != nil {
		t.Fatalf("analyze chunk: %v", err)
	}

	stats := analysis.KVStats
	if stats == nil {
		t.Fatal("kv stats is nil")
	}

	if stats.KeysIndexed == 0 {
		t.Error("keys indexed should be > 0")
	}
	if stats.ValuesIndexed == 0 {
		t.Error("values indexed should be > 0")
	}
	if stats.PairsIndexed == 0 {
		t.Error("pairs indexed should be > 0")
	}

	// Should not be budget exhausted with small data
	if stats.BudgetExhausted {
		t.Error("budget should not be exhausted for small dataset")
	}
}

func TestAnalyzeAll(t *testing.T) {
	now := time.Now()

	cm, err := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(10),
	})
	if err != nil {
		t.Fatalf("create chunk manager: %v", err)
	}

	// Create multiple chunks by appending more than rotation threshold
	for i := 0; i < 30; i++ {
		rec := chunk.Record{
			IngestTS: now.Add(time.Duration(i) * time.Second),
			WriteTS:  now.Add(time.Duration(i) * time.Second),
			Attrs:    chunk.Attributes{"idx": string(rune('0' + i%10))},
			Raw:      []byte("test message"),
		}
		if _, _, err := cm.Append(rec); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// Seal all chunks
	if err := cm.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	chunks, _ := cm.List()

	// Create indexers and build indexes
	timeIdx := memtime.NewIndexer(cm, 1)
	tokenIdx := memtoken.NewIndexer(cm)
	attrIdx := memattr.NewIndexer(cm)
	kvIdx := memkv.NewIndexer(cm)

	im := indexmem.NewManager(
		[]index.Indexer{timeIdx, tokenIdx, attrIdx, kvIdx},
		timeIdx, tokenIdx, attrIdx, kvIdx, nil,
	)

	for _, meta := range chunks {
		if err := im.BuildIndexes(t.Context(), meta.ID); err != nil {
			t.Fatalf("build indexes: %v", err)
		}
	}

	analyzer := New(cm, im)
	agg, err := analyzer.AnalyzeAll()
	if err != nil {
		t.Fatalf("analyze all: %v", err)
	}

	if agg.ChunksAnalyzed == 0 {
		t.Error("chunks analyzed should be > 0")
	}
	if len(agg.Chunks) != int(agg.ChunksAnalyzed) {
		t.Errorf("chunks in result mismatch: %d vs %d", len(agg.Chunks), agg.ChunksAnalyzed)
	}

	// Should have bytes for each index type
	for _, typ := range []IndexType{IndexTypeTime, IndexTypeToken, IndexTypeAttrKV, IndexTypeKV} {
		if agg.BytesByIndexType[typ] == 0 {
			t.Errorf("bytes for %s should be > 0", typ)
		}
	}

	// No errors expected
	if agg.ChunksWithErrors != 0 {
		t.Errorf("chunks with errors: got %d, want 0", agg.ChunksWithErrors)
	}
}

func TestAnalyzeChunk_MissingIndexes(t *testing.T) {
	now := time.Now()
	records := []chunk.Record{
		{IngestTS: now, WriteTS: now, Raw: []byte("test")},
	}

	cm, err := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(100),
	})
	if err != nil {
		t.Fatalf("create chunk manager: %v", err)
	}

	for _, rec := range records {
		if _, _, err := cm.Append(rec); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	meta := cm.Active()
	chunkID := meta.ID
	if err := cm.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// Create index manager but don't build indexes
	timeIdx := memtime.NewIndexer(cm, 1)
	tokenIdx := memtoken.NewIndexer(cm)
	attrIdx := memattr.NewIndexer(cm)
	kvIdx := memkv.NewIndexer(cm)

	im := indexmem.NewManager(
		[]index.Indexer{timeIdx, tokenIdx, attrIdx, kvIdx},
		timeIdx, tokenIdx, attrIdx, kvIdx, nil,
	)

	analyzer := New(cm, im)
	analysis, err := analyzer.AnalyzeChunk(chunkID)
	if err != nil {
		t.Fatalf("analyze chunk: %v", err)
	}

	// All indexes should be disabled (not built)
	for _, s := range analysis.Summaries {
		if s.Status != StatusDisabled {
			t.Errorf("index %s status: got %s, want %s", s.IndexType, s.Status, StatusDisabled)
		}
	}
}

func TestPercentile(t *testing.T) {
	tests := []struct {
		sorted []int64
		p      int
		want   int64
	}{
		{[]int64{1, 2, 3, 4, 5}, 50, 3},
		{[]int64{1, 2, 3, 4, 5}, 100, 5},
		{[]int64{1, 2, 3, 4, 5}, 0, 1},
		{[]int64{1}, 50, 1},
		{[]int64{}, 50, 0},
	}

	for _, tc := range tests {
		got := percentile(tc.sorted, tc.p)
		if got != tc.want {
			t.Errorf("percentile(%v, %d) = %d, want %d", tc.sorted, tc.p, got, tc.want)
		}
	}
}

func TestSafePercent(t *testing.T) {
	tests := []struct {
		part, whole int64
		want        float64
	}{
		{50, 100, 0.5},
		{0, 100, 0.0},
		{100, 0, 0.0}, // Avoid division by zero
		{0, 0, 0.0},
	}

	for _, tc := range tests {
		got := safePercent(tc.part, tc.whole)
		if got != tc.want {
			t.Errorf("safePercent(%d, %d) = %f, want %f", tc.part, tc.whole, got, tc.want)
		}
	}
}
