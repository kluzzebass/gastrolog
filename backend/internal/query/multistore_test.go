package query_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	indexmem "gastrolog/internal/index/memory"
	memattr "gastrolog/internal/index/memory/attr"
	memkv "gastrolog/internal/index/memory/kv"
	memtoken "gastrolog/internal/index/memory/token"
	"gastrolog/internal/query"
)

type testRegistry struct {
	stores map[string]struct {
		cm chunk.ChunkManager
		im index.IndexManager
	}
}

func (r *testRegistry) ListStores() []string {
	var keys []string
	for k := range r.stores {
		keys = append(keys, k)
	}
	return keys
}

func (r *testRegistry) ChunkManager(storeID string) chunk.ChunkManager {
	if s, ok := r.stores[storeID]; ok {
		return s.cm
	}
	return nil
}

func (r *testRegistry) IndexManager(storeID string) index.IndexManager {
	if s, ok := r.stores[storeID]; ok {
		return s.im
	}
	return nil
}

func TestMultiStoreSearch(t *testing.T) {
	reg := &testRegistry{
		stores: make(map[string]struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}),
	}

	// Create two stores
	for _, storeName := range []string{"store1", "store2"} {
		cm, _ := chunkmem.NewManager(chunkmem.Config{
			RotationPolicy: chunk.NewRecordCountPolicy(1000),
		})
		tokIdx := memtoken.NewIndexer(cm)
		attrIdx := memattr.NewIndexer(cm)
		kvIdx := memkv.NewIndexer(cm)
		im := indexmem.NewManager([]index.Indexer{tokIdx, attrIdx, kvIdx}, tokIdx, attrIdx, kvIdx, nil)

		// Add some records
		t0 := time.Now()
		for i := 0; i < 5; i++ {
			cm.Append(chunk.Record{
				IngestTS: t0.Add(time.Duration(i) * time.Second),
				Raw:      []byte(fmt.Sprintf("%s-record-%d", storeName, i)),
			})
		}
		cm.Seal()

		reg.stores[storeName] = struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}{cm, im}
	}

	// Create multi-store engine
	eng := query.NewWithRegistry(reg, nil)

	// Run query
	t.Log("Running query...")
	iter, _ := eng.Search(context.Background(), query.Query{}, nil)

	count := 0
	for rec, err := range iter {
		if err != nil {
			t.Fatalf("Error: %v", err)
		}
		t.Logf("Record: %s", rec.Raw)
		count++
	}
	t.Logf("Total: %d records", count)
	
	if count != 10 {
		t.Errorf("expected 10 records, got %d", count)
	}
}
