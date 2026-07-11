package chunking_test

import (
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/record"
)

type indexedSegRec struct {
	seq        uint32
	sourceZero bool
	payload    string
}

func writeIndexedSegment(t *testing.T, home string, segID, vaultID glid.GLID, recs []indexedSegRec) {
	t.Helper()
	if err := paths.EnsureHeadDir(home); err != nil {
		t.Fatal(err)
	}
	path := paths.HeadSegment(home, segID)
	sf, err := segment.Create(path, segment.Meta{ID: segID, VaultID: vaultID})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	for _, r := range recs {
		ingest := base.Add(time.Duration(r.seq) * time.Second)
		source := ingest
		if r.sourceZero {
			source = time.Time{}
		}
		rec := &record.Record{
			SourceTS: source,
			IngestTS: ingest,
			EventID: record.EventID{
				IngesterID: glid.New(),
				NodeID:     glid.New(),
				IngestTS:   ingest,
				IngestSeq:  r.seq,
			},
			Attrs: record.Attributes{"k": "v"},
			Raw:   []byte(r.payload),
		}
		if err := sf.Append(rec, base); err != nil {
			t.Fatal(err)
		}
	}
	if err := sf.Finalize(); err != nil {
		t.Fatal(err)
	}
	if err := sf.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTrimSpanForSourceStart(t *testing.T) {
	t.Parallel()
	home := t.TempDir()
	segID := glid.New()
	vaultID := glid.New()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	writeIndexedSegment(t, home, segID, vaultID, []indexedSegRec{
		{0, false, "a"},
		{1, true, "skip"},
		{2, false, "c"},
	})

	path := paths.HeadSegment(home, segID)
	idx, err := chunking.BuildOrderedIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	span := chunking.Span{SegmentID: segID, Start: 0, Count: 3}
	trimmed, err := chunking.TrimSpanForSourceStart(span, base.Add(1500*time.Millisecond), idx)
	if err != nil {
		t.Fatal(err)
	}
	if trimmed.Start != 2 || trimmed.Count != 1 {
		t.Fatalf("trimmed = %+v, want start=2 count=1", trimmed)
	}

	got, err := chunking.MergeRecords([]chunking.SpanRef{{Path: path, Span: trimmed}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || string(got[0].Raw) != "c" {
		t.Fatalf("merge = %+v", got)
	}
}

// TestFrameByteLenAtMatchesPerRecordReads pins the bulk frame-length
// derivation: frames are appended back-to-back, so lengths computed from one
// bulk index read (gap to the next frame offset) must equal the per-record
// pread path (index entry + frame length prefix) for every record — including
// varying payload sizes, so adjacent frames differ.
func TestFrameByteLenAtMatchesPerRecordReads(t *testing.T) {
	t.Parallel()
	home := t.TempDir()
	segID, vaultID := glid.New(), glid.New()
	recs := make([]indexedSegRec, 0, 50)
	payload := ""
	for i := range 50 {
		payload += "x"
		if i%7 == 0 {
			payload += "yyyyyyyyyyyyyyyyy"
		}
		recs = append(recs, indexedSegRec{seq: uint32(i), payload: payload}) //nolint:gosec // small test loop
	}
	writeIndexedSegment(t, home, segID, vaultID, recs)

	idx, err := chunking.BuildOrderedIndex(paths.HeadSegment(home, segID))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = idx.Close() }()

	sf, err := segment.Open(paths.HeadSegment(home, segID))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sf.Close() }()

	for pos := uint32(0); pos < idx.Len(); pos++ {
		bulk, err := idx.FrameByteLenAt(pos)
		if err != nil {
			t.Fatalf("bulk FrameByteLenAt(%d): %v", pos, err)
		}
		entry, err := sf.IndexEntryAt(pos)
		if err != nil {
			t.Fatal(err)
		}
		perRecord, err := sf.FrameByteLen(entry.FilePos)
		if err != nil {
			t.Fatal(err)
		}
		if bulk != uint64(perRecord) {
			t.Fatalf("pos %d: bulk length %d != per-record length %d", pos, bulk, perRecord)
		}
	}

	// Out of range errors instead of panicking.
	if _, err := idx.FrameByteLenAt(idx.Len()); err == nil {
		t.Fatal("out-of-range position must error")
	}
}
