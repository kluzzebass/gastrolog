package file

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// TestIndexBuildRaceWithCompressionAndDelete is the regression test for
// gastrolog-26zu1: a SIGBUS in the JSON indexer's pass1 loop, observed
// in production when index Build was running its cursor over a sealed
// chunk while compression and retention were concurrently mutating the
// chunk's files. The fault address was inside an mmap'd region of
// idx.log that had been invalidated by a rename or unlink.
//
// gastrolog-2mnv8 (commit dde301b8) closed the cursor-construction
// race — a paths-vs-content TOCTOU in openDataFile. That fix prevents
// the SIGBUS-on-open path. This test pins the *post-construction*
// race: the cursor opens cleanly, then concurrent CompressChunk +
// DeleteSilent mutate the underlying files, and a subsequent
// cursor.Next() faults on a now-invalid mmap region.
//
// The test simulates the JSON indexer's pass1 pattern: open cursor,
// iterate records via Next(), do some "work" between iterations.
// Concurrently it kicks off CompressChunk and DeleteSilent on the
// same chunk in flight. With per-chunk lifecycle protection
// (read/write lock that pins the chunk's file lifetime against
// concurrent mutation), every cursor read either completes
// successfully OR the cursor open returns ErrChunkNotFound — never
// SIGBUS, never partial-read corruption.
func TestIndexBuildRaceWithCompressionAndDelete(t *testing.T) {
	t.Parallel()

	const iterations = 20
	const indexersPerIter = 4
	const recordsPerChunk = 200

	for iter := range iterations {
		runIndexBuildRaceIteration(t, iter, indexersPerIter, recordsPerChunk)
	}
}

func runIndexBuildRaceIteration(t *testing.T, iter, indexersPerIter, recordsPerChunk int) {
	t.Helper()
	dir := t.TempDir()

	cm, err := NewManager(Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(uint64(recordsPerChunk * 10)),
	})
	if err != nil {
		t.Fatalf("iter %d: NewManager: %v", iter, err)
	}
	defer func() { _ = cm.Close() }()

	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range recordsPerChunk {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Attrs:    chunk.Attributes{"key": fmt.Sprintf("val-%d", i)},
			Raw:      []byte(fmt.Sprintf(`{"k":"%d","msg":"payload-data"}`, i)),
		}); err != nil {
			t.Fatalf("iter %d: Append %d: %v", iter, i, err)
		}
	}

	active := cm.Active()
	if active == nil {
		t.Fatalf("iter %d: no active chunk after appends", iter)
	}
	chunkID := active.ID
	if err := cm.Seal(); err != nil {
		t.Fatalf("iter %d: Seal: %v", iter, err)
	}

	var wg sync.WaitGroup
	var raceFailures atomic.Int32
	var sigbusOrPanic atomic.Int32
	start := make(chan struct{})

	// Goroutine: trigger compression — atomic temp-file rename onto raw.log
	// and attr.log. This is the first axis of mutation racing the indexer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		if err := cm.CompressChunk(chunkID); err != nil && !errors.Is(err, chunk.ErrChunkNotFound) {
			t.Errorf("iter %d: CompressChunk: %v", iter, err)
		}
	}()

	// Goroutine: trigger retention deletion — os.RemoveAll on the chunk
	// directory. The second axis of mutation. This forces the lifecycle
	// guards to handle delete-during-iterate, not just compress-during-iterate.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		// Tiny stagger so compression has a chance to start its rename
		// before delete starts os.RemoveAll. Without this, delete almost
		// always wins the race and the cursor sees ErrChunkNotFound at
		// open — the more interesting case (cursor open, then delete
		// mid-iterate) is the one that exposed the SIGBUS.
		time.Sleep(time.Microsecond * 50)
		if err := cm.DeleteSilent(chunkID); err != nil &&
			!errors.Is(err, chunk.ErrChunkNotFound) &&
			!errors.Is(err, chunk.ErrActiveChunk) {
			t.Errorf("iter %d: DeleteSilent: %v", iter, err)
		}
	}()

	// Goroutines: simulate JSON indexer Build pass1 — open cursor, iterate
	// records, do trivial CPU work between iterations to widen the race
	// window. Each cursor lifetime that overlaps with compress+delete is
	// what produced the SIGBUS in production.
	for range indexersPerIter {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					// A panic here is the SIGBUS the issue is about,
					// promoted via runtime.sigpanic. Record it as a
					// distinct failure so the test output is unambiguous.
					sigbusOrPanic.Add(1)
					t.Errorf("iter %d: cursor goroutine panicked (likely SIGBUS): %v", iter, r)
				}
			}()
			<-start
			cursor, err := cm.OpenCursor(chunkID)
			if err != nil {
				if errors.Is(err, chunk.ErrChunkNotFound) {
					return // delete won the race; clean error
				}
				t.Errorf("iter %d: OpenCursor: %v", iter, err)
				return
			}
			defer func() { _ = cursor.Close() }()

			for {
				if err := context.Background().Err(); err != nil {
					return
				}
				rec, _, rerr := cursor.Next()
				if errors.Is(rerr, chunk.ErrNoMoreRecords) {
					return
				}
				if rerr != nil {
					// "exceeds mmap size" is the dde301b8 symptom; if it
					// fires we've regressed that fix. Other errors are
					// best-effort tolerated as long as they're clean
					// (i.e., the cursor.Next contract is honored).
					if strings.Contains(rerr.Error(), "exceeds mmap size") ||
						strings.Contains(rerr.Error(), "invalid attributes data") {
						raceFailures.Add(1)
						t.Errorf("iter %d: cursor.Next race: %v", iter, rerr)
					}
					return
				}
				// Trivial work to widen the race window between Next() calls.
				_ = len(rec.Raw)
			}
		}()
	}

	close(start)
	wg.Wait()

	if raceFailures.Load() > 0 {
		t.Fatalf("iter %d: %d race failures observed", iter, raceFailures.Load())
	}
	if sigbusOrPanic.Load() > 0 {
		t.Fatalf("iter %d: %d SIGBUS/panic events", iter, sigbusOrPanic.Load())
	}
}
