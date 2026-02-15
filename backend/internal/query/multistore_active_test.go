package query_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	"gastrolog/internal/memtest"
	"gastrolog/internal/query"

	"github.com/google/uuid"
)

func TestMultiStoreSearchActiveChunks(t *testing.T) {
	reg := &testRegistry{
		stores: make(map[uuid.UUID]struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}),
	}

	// Create two stores with ACTIVE (unsealed) chunks
	for range 2 {
		storeID := uuid.Must(uuid.NewV7())
		s := memtest.MustNewStore(t, chunkmem.Config{
			RotationPolicy: chunk.NewRecordCountPolicy(1000),
		})

		// Add some records - DO NOT SEAL
		t0 := time.Now()
		for i := range 5 {
			s.CM.Append(chunk.Record{
				IngestTS: t0.Add(time.Duration(i) * time.Second),
				Raw:      fmt.Appendf(nil, "store-%s-record-%d", storeID, i),
			})
		}
		// NOT calling cm.Seal() - keep chunks active

		reg.stores[storeID] = struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}{s.CM, s.IM}
	}

	// Create multi-store engine
	eng := query.NewWithRegistry(reg, nil)

	// Run query
	t.Log("Running query with active chunks...")
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
