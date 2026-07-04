package cloud_test

// IngestTSMonotonic is a build-time fact persisted in the blob layout meta
// (gastrolog-699s7p) — the old design re-derived it after the fact by
// touching the ingestTS of every record frame, minutes per large blob on
// slow volumes. These tests pin the round-trip for both values.

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/cloud"
	"gastrolog/internal/glid"
)

func writeBlobWithIngestOrder(t *testing.T, ingestOffsets []time.Duration) string {
	t.Helper()
	chunkID := chunk.NewChunkID()
	vaultID := glid.New()
	ingesterID := glid.New()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)

	path := filepath.Join(t.TempDir(), "data.glcb")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := cloud.OpenWriter(f, chunkID, vaultID)
	if err != nil {
		t.Fatal(err)
	}
	for i, off := range ingestOffsets {
		ts := base.Add(off)
		rec := chunk.Record{
			SourceTS: ts.Add(-time.Millisecond),
			IngestTS: ts,
			WriteTS:  ts.Add(time.Millisecond),
			EventID:  chunk.EventID{IngesterID: ingesterID, IngestTS: ts, IngestSeq: uint32(i)}, //nolint:gosec // G115: test loop index
			Attrs:    chunk.Attributes{"k": "v"},
			Raw:      []byte("r"),
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

func TestIngestMonotonicFlagPersistsInLayoutMeta(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		offsets []time.Duration
		want    bool
	}{
		{"monotonic", []time.Duration{0, time.Second, 2 * time.Second}, true},
		{"regression mid-blob", []time.Duration{0, 2 * time.Second, time.Second}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := writeBlobWithIngestOrder(t, tc.offsets)
			blob, err := cloud.OpenMappedBlob(path)
			if err != nil {
				t.Fatal(err)
			}
			defer blob.Close()
			if got := blob.Meta().IngestTSMonotonic; got != tc.want {
				t.Fatalf("Meta().IngestTSMonotonic = %v, want %v (flag must round-trip through the layout meta, not the frames)", got, tc.want)
			}
		})
	}
}
