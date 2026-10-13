package glcb_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/glid"
	"gastrolog/internal/tsindex"
)

func writeMappedBlob(t *testing.T, records []chunk.Record) *glcb.MappedBlob {
	t.Helper()
	path := filepath.Join(t.TempDir(), glcb.BlobFilename)
	f, err := os.Create(path) //nolint:gosec // G304: test temp path
	if err != nil {
		t.Fatal(err)
	}
	w, err := glcb.OpenWriter(f, chunk.NewChunkID(), glid.New())
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range records {
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
	blob, err := glcb.OpenMappedBlob(path)
	if err != nil {
		t.Fatalf("OpenMappedBlob: %v", err)
	}
	t.Cleanup(func() { _ = blob.Close() })
	return blob
}

// The source-time index holds every record, a record without a source
// timestamp keyed by its ingest timestamp, while the record itself and the
// blob's source bounds keep saying it has none.
func TestSourceIndexKeysRecordsWithoutSourceTSByIngestTS(t *testing.T) {
	t.Parallel()
	base := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	records := []chunk.Record{
		{IngestTS: base.Add(5 * time.Second), Raw: []byte("a")},
		{SourceTS: base.Add(9 * time.Second), IngestTS: base.Add(6 * time.Second), Raw: []byte("b")},
		{IngestTS: base.Add(1 * time.Second), Raw: []byte("c")},
		{SourceTS: base.Add(3 * time.Second), IngestTS: base.Add(7 * time.Second), Raw: []byte("d")},
	}
	for i := range records {
		records[i].WriteTS = base.Add(time.Duration(i) * time.Millisecond)
	}
	blob := writeMappedBlob(t, records)

	entry, section, ok := blob.Section(glcb.SectionSourceTSIndex)
	if !ok {
		t.Fatal("no STSI section")
	}
	view, err := glcb.DefaultRegistry().NewView(entry, section)
	if err != nil {
		t.Fatal(err)
	}
	stsi := view.(tsindex.View)
	if int(stsi.Len()) != len(records) {
		t.Fatalf("STSI holds %d entries, want one per record (%d)", stsi.Len(), len(records))
	}
	wantPos := []uint32{2, 3, 0, 1} // keys 1s, 3s, 5s, 9s
	for rank, pos := range wantPos {
		e := stsi.EntryAt(uint32(rank)) //nolint:gosec // G115: small test rank
		rec := records[pos]
		key := rec.SourceTS
		if key.IsZero() {
			key = rec.IngestTS
		}
		if e.Pos != pos || e.TS != key.UnixNano() {
			t.Errorf("STSI rank %d = (ts %d, pos %d), want (ts %d, pos %d)", rank, e.TS, e.Pos, key.UnixNano(), pos)
		}
	}

	rd, err := blob.Reader()
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range records {
		got, err := rd.ReadRecord(uint32(i)) //nolint:gosec // G115: small test index
		if err != nil {
			t.Fatal(err)
		}
		if !got.SourceTS.Equal(want.SourceTS) {
			t.Errorf("record %d SourceTS = %v, stored %v", i, got.SourceTS, want.SourceTS)
		}
	}
	meta := blob.Meta()
	if !meta.SourceStart.Equal(base.Add(3*time.Second)) || !meta.SourceEnd.Equal(base.Add(9*time.Second)) {
		t.Errorf("blob source bounds = [%v, %v], want the stored source timestamps' [3s, 9s]", meta.SourceStart, meta.SourceEnd)
	}
}

// A section the TOC lists with no bytes is present and empty; a section the
// TOC does not list is absent. The two must never read alike.
func TestEmptySectionIsPresentNotAbsent(t *testing.T) {
	t.Parallel()
	blob := writeMappedBlob(t, nil)
	for _, ty := range []byte{glcb.SectionIngestTSIndex, glcb.SectionSourceTSIndex} {
		_, data, ok := blob.Section(ty)
		if !ok || len(data) != 0 {
			t.Errorf("Section(0x%02x) of a zero-record blob = (%d bytes, ok=%v), want (0 bytes, ok=true)", ty, len(data), ok)
		}
		entry, data, closer, err := glcb.MapSection(blob.Path(), ty)
		if err != nil {
			t.Fatalf("MapSection(0x%02x) of a zero-record blob: %v", ty, err)
		}
		if entry.Type != ty || len(data) != 0 {
			t.Errorf("MapSection(0x%02x) = (type 0x%02x, %d bytes), want (0x%02x, 0 bytes)", ty, entry.Type, len(data), ty)
		}
		if err := closer(); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, ok := blob.Section(glcb.SectionTokenIndex); ok {
		t.Error("Section of a type the TOC does not list reported present")
	}
}
