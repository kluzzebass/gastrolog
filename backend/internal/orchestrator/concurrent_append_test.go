package orchestrator

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	indexfile "gastrolog/internal/index/file"
	"gastrolog/internal/query"

	"github.com/google/uuid"
)

// TestConcurrentAppendToTierAttrIntegrity reproduces gastrolog-4dd48:
// concurrent AppendToTier calls through the orchestrator corrupt attr.log.
func TestConcurrentAppendToTierAttrIntegrity(t *testing.T) {
	t.Parallel()

	vaultID := uuid.Must(uuid.NewV7())
	tierID := uuid.Must(uuid.NewV7())
	nodeID := "test-node"

	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(200),
	})
	if err != nil {
		t.Fatal(err)
	}
	im := indexfile.NewManager(dir, nil, nil)

	orch, err := New(Config{LocalNodeID: nodeID})
	if err != nil {
		t.Fatal(err)
	}

	tier := &TierInstance{
		TierID: tierID, Type: "file",
		Chunks: cm, Indexes: im, Query: query.New(cm, im, nil),
	}
	orch.RegisterVault(NewVault(vaultID, tier))
	t.Cleanup(func() {
		orch.Stop()
		_ = cm.Close()
	})

	const goroutines = 8
	const perGoroutine = 500
	const totalRecords = goroutines * perGoroutine

	var wg sync.WaitGroup
	var appendErrors int64
	var mu sync.Mutex

	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)

	for g := range goroutines {
		wg.Add(1)
		go func(gIdx int) {
			defer wg.Done()
			base := gIdx * perGoroutine
			for i := range perGoroutine {
				ts := t0.Add(time.Duration(base+i) * time.Microsecond)
				err := orch.AppendToTier(vaultID, tierID, chunk.ChunkID{}, chunk.Record{
					IngestTS: ts,
					WriteTS:  ts,
					Raw:      fmt.Appendf(nil, "orch-concurrent-%d-%d", gIdx, i),
					Attrs:    chunk.Attributes{"goroutine": fmt.Sprintf("g%d", gIdx), "index": fmt.Sprintf("%d", i)},
				})
				if err != nil {
					mu.Lock()
					appendErrors++
					mu.Unlock()
				}
			}
		}(g)
	}
	wg.Wait()

	if appendErrors > 0 {
		t.Fatalf("%d append errors during concurrent burst", appendErrors)
	}

	// Seal remaining.
	if active := cm.Active(); active != nil && active.RecordCount > 0 {
		_ = cm.Seal()
	}

	// Wait for background PostSealProcess (compression) to complete.
	// Jobs run immediately via scheduler; 1s is ample for 20 chunks.
	time.Sleep(1 * time.Second)


	// Read back ALL records via cursor.
	metas, _ := cm.List()
	var readErrors int
	var totalRead int

	for _, m := range metas {
		cursor, err := cm.OpenCursor(m.ID)
		if err != nil {
			t.Fatalf("OpenCursor(%s): %v", m.ID, err)
		}
		for {
			rec, _, err := cursor.Next()
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				break
			}
			if err != nil {
				readErrors++
				// Log chunk metadata for diagnosis.
				t.Errorf("chunk %s (records=%d, sealed=%v) record index %d: %v",
					m.ID, m.RecordCount, m.Sealed, totalRead, err)
				if readErrors > 10 {
					_ = cursor.Close()
					t.Fatalf("too many read errors")
				}
				break
			}
			totalRead++
			if rec.Attrs == nil || rec.Attrs["goroutine"] == "" {
				t.Errorf("record %d: missing attrs", totalRead)
			}
		}
		_ = cursor.Close()
	}

	if totalRead != totalRecords {
		t.Errorf("expected %d records, read %d", totalRecords, totalRead)
	}
	if readErrors > 0 {
		t.Errorf("%d records had attr corruption", readErrors)
	} else {
		t.Logf("all %d records intact across %d chunks", totalRead, len(metas))
	}
}
