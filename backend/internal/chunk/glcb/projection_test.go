package glcb_test

import (
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/glid"
)

// writeBlobPath writes a GLCB blob and returns its on-disk path. Unlike
// writeBlobToTempFile it takes testing.TB so benchmarks can build a blob.
func writeBlobPath(tb testing.TB, chunkID chunk.ChunkID, vaultID glid.GLID, records []chunk.Record) string {
	tb.Helper()
	dir := tb.TempDir()
	w, err := glcb.NewWriter(chunkID, vaultID, dir)
	if err != nil {
		tb.Fatalf("NewWriter: %v", err)
	}
	for _, rec := range records {
		if err := w.Add(rec); err != nil {
			tb.Fatalf("Add: %v", err)
		}
	}
	tmp, err := os.CreateTemp(dir, "glcb-bench-*")
	if err != nil {
		tb.Fatalf("create temp: %v", err)
	}
	if _, err := w.WriteTo(tmp); err != nil {
		tb.Fatalf("WriteTo: %v", err)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		tb.Fatalf("seek: %v", err)
	}
	if err := tmp.Close(); err != nil {
		tb.Fatalf("close blob: %v", err)
	}
	return tmp.Name()
}

// projectionRecords builds a record set that exercises every attrs-projection
// case the histogram sampler can hit: records with and without attrs,
// dict-encoded (repeated) attrs, a max-size attr value, and a many-attr record.
func projectionRecords() (chunk.ChunkID, glid.GLID, []chunk.Record) {
	chunkID := chunk.NewChunkID()
	vaultID := glid.New()
	ingesterID := glid.New()
	base := time.Now().Truncate(time.Nanosecond)

	manyAttrs := chunk.Attributes{}
	for i := range 512 {
		manyAttrs["k"+string(rune('a'+i%26))+string(rune('0'+i%10))+strings.Repeat("x", i%7)] =
			"v" + strings.Repeat("y", i%11)
	}

	records := []chunk.Record{
		{
			// Multiple attrs.
			SourceTS: base.Add(-2 * time.Second),
			IngestTS: base.Add(-1 * time.Second),
			WriteTS:  base,
			EventID:  chunk.EventID{IngesterID: ingesterID, IngestTS: base.Add(-1 * time.Second), IngestSeq: 1},
			Attrs:    chunk.Attributes{"host": "web-1", "level": "info", "region": "eu-west"},
			Raw:      []byte("first log message"),
		},
		{
			// No attrs at all.
			IngestTS: base,
			WriteTS:  base.Add(1 * time.Millisecond),
			EventID:  chunk.EventID{IngesterID: ingesterID, IngestTS: base, IngestSeq: 2},
			Attrs:    nil,
			Raw:      []byte("second message, no attrs"),
		},
		{
			// Same keys/values as record 0 — exercises dict-ID reuse so the
			// projection decodes attrs that were dictionary-deduplicated.
			IngestTS: base.Add(1 * time.Second),
			WriteTS:  base.Add(2 * time.Millisecond),
			EventID:  chunk.EventID{IngesterID: ingesterID, IngestTS: base.Add(1 * time.Second), IngestSeq: 3},
			Attrs:    chunk.Attributes{"host": "web-1", "level": "info", "region": "eu-west"},
			Raw:      []byte("third message, dict-reused attrs"),
		},
		{
			// Max-size attr value: dictionary string length is a u16, so a
			// value just under 64 KiB is the boundary the projection must
			// still decode identically to the full path.
			IngestTS: base.Add(2 * time.Second),
			WriteTS:  base.Add(3 * time.Millisecond),
			EventID:  chunk.EventID{IngesterID: ingesterID, IngestTS: base.Add(2 * time.Second), IngestSeq: 4},
			Attrs:    chunk.Attributes{"payload": strings.Repeat("z", 65000)},
			Raw:      []byte("fourth message, max-size attr value"),
		},
		{
			// Many attrs in one record.
			IngestTS: base.Add(3 * time.Second),
			WriteTS:  base.Add(4 * time.Millisecond),
			EventID:  chunk.EventID{IngesterID: ingesterID, IngestTS: base.Add(3 * time.Second), IngestSeq: 5},
			Attrs:    manyAttrs,
			Raw:      []byte("fifth message, many attrs"),
		},
	}
	return chunkID, vaultID, records
}

// assertProjectionMatchesFull checks that Reader.ProjectAttrs yields exactly
// the (writeTS, attrs) the full record decode does, for every position.
func assertProjectionMatchesFull(t *testing.T, rd *glcb.Reader, records []chunk.Record) {
	t.Helper()
	for i := range records {
		full, err := rd.ReadRecord(uint32(i))
		if err != nil {
			t.Fatalf("ReadRecord[%d]: %v", i, err)
		}
		writeTS, attrs, err := rd.ProjectAttrs(uint32(i))
		if err != nil {
			t.Fatalf("ProjectAttrs[%d]: %v", i, err)
		}
		if !writeTS.Equal(full.WriteTS) {
			t.Errorf("[%d] projected writeTS = %v, want %v", i, writeTS, full.WriteTS)
		}
		if len(attrs) != len(full.Attrs) {
			t.Errorf("[%d] projected attr count = %d, want %d", i, len(attrs), len(full.Attrs))
		}
		for k, v := range full.Attrs {
			if attrs[k] != v {
				t.Errorf("[%d] projected attrs[%q] = %q, want %q", i, k, attrs[k], v)
			}
		}
	}
}

func TestProjectAttrsEquivalence(t *testing.T) {
	chunkID, vaultID, records := projectionRecords()
	tmp := writeBlobToTempFile(t, chunkID, vaultID, records)
	rd := openBlobReader(t, tmp)
	defer rd.Close()

	assertProjectionMatchesFull(t, rd, records)

	// Past-end position mirrors ReadRecord's ErrNoMoreRecords.
	if _, _, err := rd.ProjectAttrs(uint32(len(records))); err != chunk.ErrNoMoreRecords {
		t.Errorf("ProjectAttrs past end = %v, want ErrNoMoreRecords", err)
	}
}

func TestProjectAttrsSingleRecord(t *testing.T) {
	chunkID := chunk.NewChunkID()
	vaultID := glid.New()
	now := time.Now().Truncate(time.Nanosecond)
	records := []chunk.Record{{
		IngestTS: now,
		WriteTS:  now.Add(time.Millisecond),
		EventID:  chunk.EventID{IngesterID: glid.New(), IngestTS: now, IngestSeq: 1},
		Attrs:    chunk.Attributes{"only": "one"},
		Raw:      []byte("sole record"),
	}}
	tmp := writeBlobToTempFile(t, chunkID, vaultID, records)
	rd := openBlobReader(t, tmp)
	defer rd.Close()

	assertProjectionMatchesFull(t, rd, records)
}

func TestProjectAttrsEmptyChunk(t *testing.T) {
	chunkID := chunk.NewChunkID()
	vaultID := glid.New()
	tmp := writeBlobToTempFile(t, chunkID, vaultID, nil)
	rd := openBlobReader(t, tmp)
	defer rd.Close()

	if rd.Meta().RecordCount != 0 {
		t.Fatalf("expected empty chunk, got %d records", rd.Meta().RecordCount)
	}
	if _, _, err := rd.ProjectAttrs(0); err != chunk.ErrNoMoreRecords {
		t.Errorf("ProjectAttrs on empty chunk = %v, want ErrNoMoreRecords", err)
	}
}

// TestProjectAttrsViaCursor checks the AttrsProjectionSource seam the manager
// drives: the GLCB cursor exposes ProjectAttrs + RecordCount, and iterating
// positions yields the same (writeTS, attrs) as full-record decode.
func TestProjectAttrsViaCursor(t *testing.T) {
	chunkID, vaultID, records := projectionRecords()
	tmp := writeBlobToTempFile(t, chunkID, vaultID, records)
	rd := openBlobReader(t, tmp)

	cursor := glcb.NewGLCBCursor(rd, chunkID, nil)
	defer cursor.Close()

	proj, ok := cursor.(chunk.AttrsProjectionSource)
	if !ok {
		t.Fatal("GLCB cursor does not implement chunk.AttrsProjectionSource")
	}
	if got := proj.RecordCount(); got != uint64(len(records)) {
		t.Fatalf("RecordCount = %d, want %d", got, len(records))
	}
	for i := range records {
		writeTS, attrs, err := proj.ProjectAttrs(uint32(i))
		if err != nil {
			t.Fatalf("cursor ProjectAttrs[%d]: %v", i, err)
		}
		if !writeTS.Equal(records[i].WriteTS) {
			t.Errorf("[%d] writeTS = %v, want %v", i, writeTS, records[i].WriteTS)
		}
		if len(attrs) != len(records[i].Attrs) {
			t.Errorf("[%d] attr count = %d, want %d", i, len(attrs), len(records[i].Attrs))
		}
		for k, v := range records[i].Attrs {
			if attrs[k] != v {
				t.Errorf("[%d] attrs[%q] = %q, want %q", i, k, attrs[k], v)
			}
		}
	}
}

// Sinks defeat dead-store elimination so the measured allocations are real.
var (
	sinkRec   chunk.Record
	sinkAttrs chunk.Attributes
)

// BenchmarkProjectAttrsAllocs pins the whole point of the projection: the full
// record decode clones each raw payload off the mapping (cloneMmapRecord), and
// the projection does not touch [rawLen][raw] at all. Both paths run the same
// attrs decode, so the allocation difference is exactly the raw-payload clone.
// The assertion is on allocs/op via testing.AllocsPerRun — never on timing.
func BenchmarkProjectAttrsAllocs(b *testing.B) {
	chunkID := chunk.NewChunkID()
	vaultID := glid.New()
	now := time.Now().Truncate(time.Nanosecond)
	// A single record with a substantial raw payload and no attrs, so the only
	// allocation difference between the two paths is the raw-payload clone.
	records := []chunk.Record{{
		IngestTS: now,
		WriteTS:  now.Add(time.Millisecond),
		EventID:  chunk.EventID{IngesterID: glid.New(), IngestTS: now, IngestSeq: 1},
		Attrs:    nil,
		Raw:      make([]byte, 8192),
	}}

	path := writeBlobPath(b, chunkID, vaultID, records)
	blob, err := glcb.OpenMappedBlob(path)
	if err != nil {
		b.Fatalf("OpenMappedBlob: %v", err)
	}
	defer func() { _ = blob.Close() }()
	rd, err := blob.Reader()
	if err != nil {
		b.Fatalf("Reader: %v", err)
	}
	defer rd.Close()

	fullAllocs := testing.AllocsPerRun(1000, func() {
		rec, err := rd.ReadFanOutRecord(0)
		if err != nil {
			b.Fatalf("ReadFanOutRecord: %v", err)
		}
		sinkRec = rec
	})
	projAllocs := testing.AllocsPerRun(1000, func() {
		_, attrs, err := rd.ProjectAttrs(0)
		if err != nil {
			b.Fatalf("ProjectAttrs: %v", err)
		}
		sinkAttrs = attrs
	})

	b.Logf("full decode allocs/op = %.0f, projection allocs/op = %.0f", fullAllocs, projAllocs)
	b.ReportMetric(fullAllocs, "full-allocs/op")
	b.ReportMetric(projAllocs, "proj-allocs/op")

	if projAllocs >= fullAllocs {
		b.Fatalf("projection allocs/op %.0f not below full decode %.0f — raw-payload allocation not eliminated", projAllocs, fullAllocs)
	}
	if diff := fullAllocs - projAllocs; diff != 1 {
		b.Fatalf("full-minus-projection allocs/op = %.0f, want exactly 1 (the raw-payload clone)", diff)
	}
}
