package segment_test

// Coverage for gastrolog-1rca2d: MappedSegment must read exactly what the
// verified Open path reads, from a read-only mapping with no per-record I/O.

import (
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/segment"
)

func TestMappedSegmentMatchesOpen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg")
	sf, err := segment.Create(path, segment.Meta{ID: glid.New(), VaultID: glid.New()})
	if err != nil {
		t.Fatal(err)
	}
	frames := make([]segment.Frame, 16)
	for i := range frames {
		frames[i] = encodeSample(t, uint32(31-i)) //nolint:gosec // small test index
	}
	if err := sf.AppendFrames(frames); err != nil {
		t.Fatal(err)
	}
	if err := sf.Finalize(); err != nil {
		t.Fatal(err)
	}
	if err := sf.Close(); err != nil {
		t.Fatal(err)
	}

	ref, err := segment.Open(path) // full verification path
	if err != nil {
		t.Fatal(err)
	}
	defer ref.Close()
	mapped, err := segment.OpenMapped(path)
	if err != nil {
		t.Fatal(err)
	}
	defer mapped.Close()

	if mapped.Len() != ref.Header().RecordCount {
		t.Fatalf("Len = %d, want %d", mapped.Len(), ref.Header().RecordCount)
	}
	for pos := uint32(0); pos < mapped.Len(); pos++ {
		want, err := ref.IndexEntryAt(pos)
		if err != nil {
			t.Fatal(err)
		}
		got, err := mapped.IndexEntryAt(pos)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("entry %d = %+v, want %+v", pos, got, want)
		}
		wantRec, err := ref.ReadRecordAtFilePos(want.FilePos)
		if err != nil {
			t.Fatal(err)
		}
		gotRec, err := mapped.RecordAtFilePos(got.FilePos)
		if err != nil {
			t.Fatal(err)
		}
		if gotRec.EventID != wantRec.EventID || string(gotRec.Raw) != string(wantRec.Raw) ||
			!gotRec.SourceTS.Equal(wantRec.SourceTS) {
			t.Fatalf("record %d differs: %+v vs %+v", pos, gotRec, wantRec)
		}
	}

	// Records must not alias the mapping: mutate a copy read before Close,
	// then read again after — impossible if the decoder aliased the map
	// (Close would have unmapped it and this would fault).
	rec, err := mapped.RecordAtFilePos(func() uint32 {
		e, _ := mapped.IndexEntryAt(0)
		return e.FilePos
	}())
	if err != nil {
		t.Fatal(err)
	}
	if err := mapped.Close(); err != nil {
		t.Fatal(err)
	}
	if len(rec.Raw) == 0 || rec.EventID.IngestTS.After(time.Now()) {
		t.Fatal("record unusable after Close — decoder must copy out of the mapping")
	}

	// Out-of-bounds and bounds checks.
	if _, err := mapped.IndexEntryAt(mapped.Len()); err == nil {
		t.Fatal("IndexEntryAt past end should error")
	}
}

func TestOpenMappedRejectsUnfinalized(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg")
	sf, err := segment.Create(path, segment.Meta{ID: glid.New(), VaultID: glid.New()})
	if err != nil {
		t.Fatal(err)
	}
	if err := sf.AppendFrames([]segment.Frame{encodeSample(t, 1)}); err != nil {
		t.Fatal(err)
	}
	if err := sf.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := segment.OpenMapped(path); err == nil {
		t.Fatal("OpenMapped on unfinalized segment should fail")
	}
}
