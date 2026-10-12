// Package glcbtest writes real sealed GLCB blobs for tests in other packages,
// so they can exercise the production read path over many chunks without
// running the chunking pipeline.
package glcbtest

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/glid"
)

// Records returns n records spaced one millisecond apart from base, each
// carrying a distinct EventID under the given ingester.
func Records(n int, base time.Time, ingester glid.GLID, payload string) []chunk.Record {
	out := make([]chunk.Record, 0, n)
	for i := range n {
		ts := base.Add(time.Duration(i) * time.Millisecond)
		out = append(out, chunk.Record{
			SourceTS: ts,
			IngestTS: ts,
			WriteTS:  ts,
			EventID:  chunk.EventID{IngesterID: ingester, IngestTS: ts, IngestSeq: uint32(i + 1)},
			Attrs:    chunk.Attributes{"n": strconv.Itoa(i)},
			Raw:      []byte(payload),
		})
	}
	return out
}

// WriteSealedBlob builds <root>/<chunkID>/data.glcb from records — the layout
// the chunking pipeline produces at a vault's ChunkRoot — and returns the
// blob path with the ExternalGLCBInfo a chunk manager needs to register it.
func WriteSealedBlob(t *testing.T, root string, chunkID chunk.ChunkID, vaultID glid.GLID, records []chunk.Record) (string, chunk.ExternalGLCBInfo) {
	t.Helper()
	dir := filepath.Join(root, chunkID.String())
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	w, err := glcb.NewWriter(chunkID, vaultID, dir)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for i, rec := range records {
		if err := w.Add(rec); err != nil {
			t.Fatalf("Add record %d: %v", i, err)
		}
	}
	path := filepath.Join(dir, glcb.BlobFilename)
	f, err := os.Create(filepath.Clean(path))
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	written, err := w.WriteTo(f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		t.Fatalf("WriteTo %s: %v", path, err)
	}
	return path, externalInfo(records, w.TOC(), written, w.IngestTSMonotonic())
}

func externalInfo(records []chunk.Record, toc glcb.BlobTOC, diskBytes int64, ingestTSMonotonic bool) chunk.ExternalGLCBInfo {
	info := chunk.ExternalGLCBInfo{RecordCount: int64(len(records)), DiskBytes: diskBytes, IngestTSMonotonic: ingestTSMonotonic}
	for i, rec := range records {
		info.Bytes += int64(len(rec.Raw))
		if i == 0 {
			info.WriteStart, info.WriteEnd = rec.WriteTS, rec.WriteTS
			info.IngestStart, info.IngestEnd = rec.IngestTS, rec.IngestTS
		}
		widen(&info.WriteStart, &info.WriteEnd, rec.WriteTS)
		widen(&info.IngestStart, &info.IngestEnd, rec.IngestTS)
		if rec.SourceTS.IsZero() {
			continue
		}
		if info.SourceStart.IsZero() {
			info.SourceStart, info.SourceEnd = rec.SourceTS, rec.SourceTS
		}
		widen(&info.SourceStart, &info.SourceEnd, rec.SourceTS)
	}
	if e, ok := toc.Find(glcb.SectionIngestTSIndex); ok {
		info.IngestIdxOffset, info.IngestIdxSize = e.Offset, e.Size
	}
	if e, ok := toc.Find(glcb.SectionSourceTSIndex); ok {
		info.SourceIdxOffset, info.SourceIdxSize = e.Offset, e.Size
	}
	return info
}

func widen(lo, hi *time.Time, ts time.Time) {
	if ts.Before(*lo) {
		*lo = ts
	}
	if ts.After(*hi) {
		*hi = ts
	}
}
