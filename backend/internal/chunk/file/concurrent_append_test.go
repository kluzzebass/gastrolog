package file

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// TestConcurrentAppendAttrIntegrity is a focused reproducer for gastrolog-4dd48:
// concurrent Append calls corrupt attr.log data, causing "invalid attributes data"
// errors when reading back via cursor.
func TestConcurrentAppendAttrIntegrity(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cm, err := NewManager(Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(200),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

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
				// Simulate AppendToVault: call Active() before and after Append,
				// just like the orchestrator does.
				_ = cm.Active()
				_, _, err := cm.Append(chunk.Record{
					IngestTS: ts,
					WriteTS:  ts,
					Raw:      fmt.Appendf(nil, "concurrent-%d-%d", gIdx, i),
					Attrs:    chunk.Attributes{"goroutine": fmt.Sprintf("g%d", gIdx), "index": fmt.Sprintf("%d", i)},
				})
				_ = cm.Active()
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

	// Seal the last active chunk.
	if active := cm.Active(); active != nil && active.RecordCount > 0 {
		if err := cm.Seal(); err != nil {
			t.Fatal(err)
		}
	}

	// Now read back ALL records via cursor and verify no corruption.
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
				t.Errorf("chunk %s record %d: cursor.Next error: %v", m.ID, totalRead, err)
				if readErrors > 10 {
					_ = cursor.Close()
					t.Fatalf("too many read errors, stopping")
				}
				break
			}
			totalRead++

			// Verify attributes are non-empty and have expected keys.
			if rec.Attrs == nil {
				t.Errorf("record %d: nil attrs", totalRead)
			} else {
				if _, ok := rec.Attrs["goroutine"]; !ok {
					t.Errorf("record %d: missing 'goroutine' attr", totalRead)
				}
				if _, ok := rec.Attrs["index"]; !ok {
					t.Errorf("record %d: missing 'index' attr", totalRead)
				}
			}
		}
		_ = cursor.Close()
	}

	if totalRead != totalRecords {
		t.Errorf("expected %d total records, cursor read %d", totalRecords, totalRead)
	}
	if readErrors > 0 {
		t.Errorf("%d records had read errors (attr corruption)", readErrors)
	}
}
