package file

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// TestCursorRaceWithCompression is the regression test for gastrolog-2mnv8.
//
// Before the fix, openDataFile() called isCompressed(path) — which opened
// the file, read its header, and closed it — and THEN called os.Open(path)
// as a separate operation. If the post-seal compression pipeline atomically
// swapped raw.log between those two calls, the cursor would mmap the
// compressed bytes as if they were uncompressed, then index into them with
// the original (larger) uncompressed offsets stored in idx.log. The visible
// symptom was "raw range [N:M] exceeds mmap size K" where K was the
// compressed file size — much smaller than the uncompressed offsets.
//
// This test seals a chunk, then races concurrent cursor opens against the
// post-seal compression pass. With the fix, every cursor open either reads
// the uncompressed version (its fd pins it to the original inode regardless
// of any subsequent path swap) or sees a compressed file from the start
// (and uses the seekable reader). Neither path can produce a partial mmap.
func TestCursorRaceWithCompression(t *testing.T) {
	t.Parallel()

	const iterations = 30
	const cursorsPerIter = 8
	const recordsPerChunk = 100

	for iter := range iterations {
		runCursorCompressIteration(t, iter, cursorsPerIter, recordsPerChunk)
	}
}

func runCursorCompressIteration(t *testing.T, iter, cursorsPerIter, recordsPerChunk int) {
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
			Attrs:    chunk.Attributes{"k": "v"},
			Raw:      []byte("payload-data"),
		}); err != nil {
			t.Fatalf("iter %d: Append %d: %v", iter, i, err)
		}
	}

	// Seal the active chunk so its raw.log/attr.log become eligible for
	// post-seal compression. Capture the chunk ID before sealing because
	// Active() returns nil after seal.
	active := cm.Active()
	if active == nil {
		t.Fatalf("iter %d: no active chunk after appends", iter)
	}
	chunkID := active.ID
	if err := cm.Seal(); err != nil {
		t.Fatalf("iter %d: Seal: %v", iter, err)
	}

	// Race: kick off the post-seal compression pass and a flock of cursor
	// opens at roughly the same moment. If the openDataFile fix is in
	// place, no cursor read can return "exceeds mmap size".
	var wg sync.WaitGroup
	var raceFailures atomic.Int32
	start := make(chan struct{})

	// Goroutine 1: trigger compression (the path swap).
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		if err := cm.PostSealProcess(context.Background(), chunkID); err != nil {
			t.Errorf("iter %d: PostSealProcess: %v", iter, err)
		}
	}()

	// Goroutines 2..N: open cursors in a tight loop and try to read all
	// records. Each iteration that hits the race window has a chance to
	// observe inconsistent state without the fix.
	for range cursorsPerIter {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range 50 {
				cur, err := cm.OpenCursor(chunkID)
				if err != nil {
					if errors.Is(err, chunk.ErrChunkNotFound) {
						return // chunk got removed by compression cleanup
					}
					continue
				}
				for {
					_, _, rerr := cur.Next()
					if errors.Is(rerr, chunk.ErrNoMoreRecords) {
						break
					}
					if rerr != nil {
						// "exceeds mmap size" is the specific symptom of
						// the race we're regression-testing. Any other
						// error (e.g. ErrChunkNotFound mid-read) is
						// incidental noise from the parallel compress.
						if strings.Contains(rerr.Error(), "exceeds mmap size") {
							t.Errorf("iter %d: cursor.Next exceeds mmap size: %v", iter, rerr)
							raceFailures.Add(1)
						}
						break
					}
				}
				_ = cur.Close()
			}
		}()
	}

	close(start)
	wg.Wait()

	if raceFailures.Load() > 0 {
		t.Fatalf("iter %d: %d cursor reads observed exceeds-mmap-size race",
			iter, raceFailures.Load())
	}
}
