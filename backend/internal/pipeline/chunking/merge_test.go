package chunking_test

import (
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/record"
)

func eventID(seq uint32, ts time.Time) record.EventID {
	return record.EventID{
		IngesterID: glid.New(),
		NodeID:     glid.New(),
		IngestTS:   ts,
		IngestSeq:  seq,
	}
}

func makeRecord(seq uint32, ts time.Time, raw string) record.Record {
	id := eventID(seq, ts)
	return record.Record{
		EventID:  id,
		SourceTS: ts.Add(-time.Millisecond),
		IngestTS: id.IngestTS,
		Attrs:    record.Attributes{"k": "v"},
		Raw:      []byte(raw),
	}
}

func writeSegment(t *testing.T, segID, vaultID glid.GLID, recs []record.Record) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), segID.String())

	sf, err := segment.Create(path, segment.Meta{ID: segID, VaultID: vaultID})
	if err != nil {
		t.Fatal(err)
	}
	writeTS := time.Date(2024, 8, 1, 12, 1, 0, 0, time.UTC)
	for i := range recs {
		if err := sf.Append(&recs[i], writeTS); err != nil {
			t.Fatal(err)
		}
	}
	if err := sf.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := sf.MarkComplete(); err != nil {
		t.Fatal(err)
	}
	if err := sf.Close(); err != nil {
		t.Fatal(err)
	}

	sf2, err := segment.Open(path)
	if err != nil {
		t.Fatalf("verify open: %v", err)
	}
	if _, err := sf2.ReadAll(); err != nil {
		_ = sf2.Close()
		t.Fatalf("verify read: %v", err)
	}
	_ = sf2.Close()
	return path
}

func TestBuildOrderedIndexSortsByEventID(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()

	// Append in reverse EventID order on disk.
	recs := []record.Record{
		makeRecord(2, base.Add(2*time.Second), "c"),
		makeRecord(0, base, "a"),
		makeRecord(1, base.Add(time.Second), "b"),
	}
	path := writeSegment(t, segID, vaultID, recs)

	idx, err := chunking.BuildOrderedIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	if idx.Len() != 3 {
		t.Fatalf("len = %d, want 3", idx.Len())
	}
	for i, want := range []byte{'a', 'b', 'c'} {
		rec, err := idx.RecordAt(uint32(i))
		if err != nil {
			t.Fatal(err)
		}
		if rec.Raw[0] != want {
			t.Fatalf("RecordAt(%d) = %q, want %c", i, rec.Raw, want)
		}
	}
}

func TestMergeSingleSegmentFullSpan(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	recs := []record.Record{
		makeRecord(0, base, "a"),
		makeRecord(1, base.Add(time.Second), "b"),
	}
	path := writeSegment(t, segID, vaultID, recs)

	got, err := chunking.MergeRecords([]chunking.SpanRef{{
		Path: path,
		Span: chunking.Span{SegmentID: segID, Start: 0, Count: 2},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !chunking.IsSortedByEventID(got) || len(got) != 2 {
		t.Fatalf("merge = %+v", got)
	}
}

func TestMergeTwoSegmentsKWay(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	vaultID := glid.New()

	segA := glid.New()
	pathA := writeSegment(t, segA, vaultID, []record.Record{
		makeRecord(0, base.Add(3*time.Second), "a3"),
		makeRecord(0, base, "a0"),
	})
	segB := glid.New()
	pathB := writeSegment(t, segB, vaultID, []record.Record{
		makeRecord(0, base.Add(2*time.Second), "b2"),
		makeRecord(0, base.Add(time.Second), "b1"),
	})

	got, err := chunking.MergeRecords([]chunking.SpanRef{
		{Path: pathA, Span: chunking.Span{SegmentID: segA, Start: 0, Count: 2}},
		{Path: pathB, Span: chunking.Span{SegmentID: segB, Start: 0, Count: 2}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("len = %d, want 4", len(got))
	}
	if !chunking.IsSortedByEventID(got) {
		t.Fatalf("not sorted: %+v", eventIDs(got))
	}
	want := []string{"a0", "b1", "b2", "a3"}
	for i, rec := range got {
		if string(rec.Raw) != want[i] {
			t.Fatalf("position %d = %q, want %q", i, rec.Raw, want[i])
		}
	}
}

func TestMergePartialSpans(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	vaultID := glid.New()
	segID := glid.New()

	path := writeSegment(t, segID, vaultID, []record.Record{
		makeRecord(0, base, "r0"),
		makeRecord(1, base.Add(time.Second), "r1"),
		makeRecord(2, base.Add(2*time.Second), "r2"),
		makeRecord(3, base.Add(3*time.Second), "r3"),
	})

	got, err := chunking.MergeRecords([]chunking.SpanRef{{
		Path: path,
		Span: chunking.Span{SegmentID: segID, Start: 1, Count: 2},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || string(got[0].Raw) != "r1" || string(got[1].Raw) != "r2" {
		t.Fatalf("merge = %+v", got)
	}
}

func TestMergeSpanBoundsRejected(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	path := writeSegment(t, segID, vaultID, []record.Record{
		makeRecord(0, base, "x"),
	})

	_, err := chunking.MergeRecords([]chunking.SpanRef{{
		Path: path,
		Span: chunking.Span{SegmentID: segID, Start: 0, Count: 2},
	}})
	if !errors.Is(err, chunking.ErrSpanBounds) {
		t.Fatalf("MergeRecords() = %v, want ErrSpanBounds", err)
	}
}

func TestMergeEmptySpanRejected(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	path := writeSegment(t, segID, vaultID, []record.Record{
		makeRecord(0, base, "x"),
	})

	_, err := chunking.MergeRecords([]chunking.SpanRef{{
		Path: path,
		Span: chunking.Span{SegmentID: segID, Start: 0, Count: 0},
	}})
	if !errors.Is(err, chunking.ErrEmptySpan) {
		t.Fatalf("MergeRecords() = %v, want ErrEmptySpan", err)
	}
}

func TestMergeDeterministicAcrossSpanOrder(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	vaultID := glid.New()
	segA, segB := glid.New(), glid.New()

	pathA := writeSegment(t, segA, vaultID, []record.Record{
		makeRecord(0, base, "a"),
	})
	pathB := writeSegment(t, segB, vaultID, []record.Record{
		makeRecord(0, base.Add(time.Second), "b"),
	})

	refsAB := []chunking.SpanRef{
		{Path: pathA, Span: chunking.Span{SegmentID: segA, Start: 0, Count: 1}},
		{Path: pathB, Span: chunking.Span{SegmentID: segB, Start: 0, Count: 1}},
	}
	refsBA := slices.Clone(refsAB)
	slices.Reverse(refsBA)

	gotAB, err := chunking.MergeRecords(refsAB)
	if err != nil {
		t.Fatal(err)
	}
	gotBA, err := chunking.MergeRecords(refsBA)
	if err != nil {
		t.Fatal(err)
	}
	if len(gotAB) != 2 || len(gotBA) != 2 {
		t.Fatalf("AB=%d BA=%d", len(gotAB), len(gotBA))
	}
	for i := range gotAB {
		if gotAB[i].EventID != gotBA[i].EventID {
			t.Fatalf("order differs at %d: AB=%+v BA=%+v", i, gotAB[i].EventID, gotBA[i].EventID)
		}
	}
}

func eventIDs(recs []record.Record) []record.EventID {
	out := make([]record.EventID, len(recs))
	for i, r := range recs {
		out[i] = r.EventID
	}
	return out
}
