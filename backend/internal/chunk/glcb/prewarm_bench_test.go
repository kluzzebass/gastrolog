package glcb_test

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/glid"
)

// buildLargeBlob writes a GLCB of roughly targetBytes to a temp file and
// returns its path and actual on-disk size. Raw payloads are pseudo-random so
// zstd cannot shrink the blob away — the mapping we drain is the size we ask
// for, which is what makes the page-cache warm meaningful to measure.
func buildLargeBlob(tb testing.TB, targetBytes int64) (string, int64) {
	tb.Helper()
	const rawBytes = 2048
	path := filepath.Join(tb.TempDir(), "large.glcb")
	f, err := os.Create(path)
	if err != nil {
		tb.Fatal(err)
	}
	w, err := glcb.OpenWriter(f, chunk.NewChunkID(), glid.New())
	if err != nil {
		tb.Fatal(err)
	}
	now := time.Now()
	ing := glid.New()
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // deterministic bench payloads, not security
	raw := make([]byte, rawBytes)
	n := int(targetBytes / rawBytes)
	for i := 0; i < n; i++ {
		rng.Read(raw)
		payload := make([]byte, rawBytes)
		copy(payload, raw)
		rec := chunk.Record{
			SourceTS: now,
			IngestTS: now,
			WriteTS:  now,
			EventID:  chunk.EventID{IngesterID: ing, IngestTS: now, IngestSeq: uint32(i)}, //nolint:gosec // G115: bounded by n
			Attrs:    chunk.Attributes{"host": "web-1", "level": "info"},
			Raw:      payload,
		}
		if err := w.Add(rec); err != nil {
			tb.Fatal(err)
		}
	}
	if _, err := w.Finish(); err != nil {
		tb.Fatal(err)
	}
	if err := f.Close(); err != nil {
		tb.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		tb.Fatal(err)
	}
	return path, info.Size()
}

// drainAll reads every record front-to-back, the access pattern the retention
// fan-out drain produces and the one PrewarmSequential targets.
func drainAll(tb testing.TB, rd *glcb.Reader, count uint32) {
	for pos := uint32(0); pos < count; pos++ {
		if _, err := rd.ReadFanOutRecord(pos); err != nil {
			tb.Fatalf("ReadFanOutRecord(%d): %v", pos, err)
		}
	}
}

// BenchmarkPrewarmDrain compares a sequential full-chunk drain with and without
// the madvise prewarm.
//
// COLD-CACHE CAVEAT: there is no root-free way to purge the OS page cache on
// macOS (`purge` needs root; Linux drop_caches needs root too). Each iteration
// re-opens the same file, so after the first pass the blob is fully resident —
// this measures the WARM-cache drain. On a dev Mac with fast NVMe and a warm
// cache the prewarm delta is expected to be noise (madvise on already-resident
// pages is nearly free; there are no major faults to eliminate). The benchmark
// exists to (a) prove the wired warm adds no meaningful overhead and (b) give a
// real seam to re-measure on a disk-saturated, cold-cache host — where 1io54g's
// major-fault stalls actually occur. Interpret single-host numbers accordingly.
func BenchmarkPrewarmDrain(b *testing.B) {
	const target = 128 << 20 // 128 MiB
	path, size := buildLargeBlob(b, target)
	b.Logf("GLCB on-disk size: %d bytes (%.1f MiB)", size, float64(size)/(1<<20))

	run := func(b *testing.B, prewarm bool) {
		b.SetBytes(size)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			blob, err := glcb.OpenMappedBlob(path)
			if err != nil {
				b.Fatal(err)
			}
			rd, err := blob.Reader()
			if err != nil {
				b.Fatal(err)
			}
			if prewarm {
				rd.PrewarmSequential()
			}
			drainAll(b, rd, rd.Meta().RecordCount)
			_ = rd.Close()
			_ = blob.Close()
		}
	}

	b.Run("NoPrewarm", func(b *testing.B) { run(b, false) })
	b.Run("Prewarm", func(b *testing.B) { run(b, true) })
}
