package cloud

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

func writeFanOutTestGLCB(t *testing.T, recordCount int) string {
	t.Helper()
	chunkID := chunk.NewChunkID()
	vaultID := glid.New()
	now := time.Now().Truncate(time.Nanosecond)

	path := filepath.Join(t.TempDir(), "data.glcb")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := OpenWriter(f, chunkID, vaultID)
	if err != nil {
		t.Fatal(err)
	}
	for i := range recordCount {
		rec := chunk.Record{
			IngestTS: now,
			WriteTS:  now,
			Raw:      []byte{byte('a' + i)},
		}
		if err := w.Add(rec); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestGLCBCursorFanOutSource(t *testing.T) {
	t.Parallel()

	const recordCount = 8
	path := writeFanOutTestGLCB(t, recordCount)
	blob, err := OpenMappedBlob(path)
	if err != nil {
		t.Fatalf("OpenMappedBlob: %v", err)
	}
	defer blob.Close()
	rd, err := blob.Reader()
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer rd.Close()

	cur := NewSeekableCursorWithClose(rd, chunk.ChunkID{}, nil)
	fanout, ok := cur.(chunk.RecordFanOutSource)
	if !ok {
		t.Fatal("glcbCursor should implement RecordFanOutSource")
	}
	if got := fanout.RecordCount(); got != recordCount {
		t.Fatalf("RecordCount = %d, want %d", got, recordCount)
	}

	var wg sync.WaitGroup
	const workers = 4
	got := make([]chunk.Record, recordCount)
	errs := make(chan error, workers)
	for w := range workers {
		start := uint64(w) * recordCount / workers
		end := uint64(w+1) * recordCount / workers
		wg.Add(1)
		go func(start, end uint64) {
			defer wg.Done()
			for pos := start; pos < end; pos++ {
				rec, err := fanout.ReadFanOutRecord(uint32(pos)) //nolint:gosec // G115: test bounds
				if err != nil {
					errs <- err
					return
				}
				got[pos] = rec
			}
		}(start, end)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("ReadFanOutRecord: %v", err)
		}
	}
	for i, rec := range got {
		if len(rec.Raw) == 0 {
			t.Fatalf("record %d: empty raw", i)
		}
	}

	batchCur, ok := cur.(chunk.RecordBatchReader)
	if !ok {
		t.Fatal("glcbCursor should implement RecordBatchReader")
	}
	if err := batchCur.Seek(chunk.RecordRef{Pos: 0}); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	batch, err := batchCur.NextBatch(3)
	if err != nil {
		t.Fatalf("NextBatch: %v", err)
	}
	if len(batch) != 3 {
		t.Fatalf("NextBatch len = %d, want 3", len(batch))
	}
}
