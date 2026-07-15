package chunking_test

import (
	"slices"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

type mapLocator map[glid.GLID]string

func (m mapLocator) SegmentPath(id glid.GLID) (string, bool) {
	path, ok := m[id]
	return path, ok
}

func TestQueryOpenChunkReturnsManifestRecords(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{
		{0, base, "a"},
		{1, base.Add(time.Second), "b"},
	})

	chunkID := chunk.NewChunkID()
	openedAt := base
	manifest := &vaultctlfsm.OpenChunkManifest{
		ChunkID:  chunkID,
		OpenedAt: openedAt,
		Refs: []vaultctlfsm.OpenChunkSegmentRef{{
			SegmentID:         segID,
			FirstRecordNumber: 0,
			LastRecordNumber:  1,
			RefAddedAt:        openedAt,
		}},
	}

	got, report, err := chunking.CollectOpenChunk(chunking.OpenChunkQueryInput{
		Manifest: manifest,
		Locate:   mapLocator{segID: paths.HeadSegment(home, segID)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.MissingSegments) != 0 {
		t.Fatalf("missing = %v", report.MissingSegments)
	}
	if len(got) != 2 {
		t.Fatalf("records = %d, want 2", len(got))
	}
	if !chunking.IsSortedByEventID(got) {
		t.Fatal("not sorted by EventID")
	}
	for _, rec := range got {
		if string(rec.Raw) != "a" && string(rec.Raw) != "b" {
			t.Fatalf("unexpected payload %q", rec.Raw)
		}
	}
}

func TestQueryOpenChunkPartialRef(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{
		{0, base, "a"},
		{1, base.Add(time.Second), "b"},
		{2, base.Add(2 * time.Second), "c"},
	})

	manifest := &vaultctlfsm.OpenChunkManifest{
		ChunkID: chunk.NewChunkID(),
		Refs: []vaultctlfsm.OpenChunkSegmentRef{{
			SegmentID:         segID,
			FirstRecordNumber: 1,
			LastRecordNumber:  2,
		}},
	}

	got, _, err := chunking.CollectOpenChunk(chunking.OpenChunkQueryInput{
		Manifest: manifest,
		Locate:   mapLocator{segID: paths.HeadSegment(home, segID)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("records = %d, want 2", len(got))
	}
	if string(got[0].Raw) != "b" || string(got[1].Raw) != "c" {
		t.Fatalf("payloads = %q, %q", got[0].Raw, got[1].Raw)
	}
}

func TestQueryOpenChunkMissingLocalSegment(t *testing.T) {
	t.Parallel()
	present := glid.New()
	missing := glid.New()
	vaultID := glid.New()
	home := t.TempDir()
	writeHeadSegment(t, home, present, vaultID, []recordForSeg{{0, time.Unix(0, 0).UTC(), "ok"}})

	manifest := &vaultctlfsm.OpenChunkManifest{
		ChunkID: chunk.NewChunkID(),
		Refs: []vaultctlfsm.OpenChunkSegmentRef{
			{SegmentID: present, FirstRecordNumber: 0, LastRecordNumber: 0},
			{SegmentID: missing, FirstRecordNumber: 0, LastRecordNumber: 0},
		},
	}

	got, report, err := chunking.CollectOpenChunk(chunking.OpenChunkQueryInput{
		Manifest: manifest,
		Locate:   mapLocator{present: paths.HeadSegment(home, present)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || string(got[0].Raw) != "ok" {
		t.Fatalf("got = %+v", got)
	}
	if len(report.MissingSegments) != 1 || report.MissingSegments[0] != missing {
		t.Fatalf("missing = %v, want [%s]", report.MissingSegments, missing)
	}
}

func TestQueryOpenChunkMultiSegmentMerge(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segA := glid.New()
	segB := glid.New()
	vaultID := glid.New()
	home := t.TempDir()
	writeHeadSegment(t, home, segA, vaultID, []recordForSeg{{0, base, "a0"}})
	writeHeadSegment(t, home, segB, vaultID, []recordForSeg{{0, base.Add(time.Second), "b0"}})

	manifest := &vaultctlfsm.OpenChunkManifest{
		ChunkID: chunk.NewChunkID(),
		Refs: []vaultctlfsm.OpenChunkSegmentRef{
			{SegmentID: segB, FirstRecordNumber: 0, LastRecordNumber: 0},
			{SegmentID: segA, FirstRecordNumber: 0, LastRecordNumber: 0},
		},
	}

	got, _, err := chunking.CollectOpenChunk(chunking.OpenChunkQueryInput{
		Manifest: manifest,
		Locate: mapLocator{
			segA: paths.HeadSegment(home, segA),
			segB: paths.HeadSegment(home, segB),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("records = %d, want 2", len(got))
	}
	if !chunking.IsSortedByEventID(got) {
		t.Fatal("merge order wrong")
	}
	if string(got[0].Raw) != "a0" || string(got[1].Raw) != "b0" {
		t.Fatalf("payloads = %q, %q", got[0].Raw, got[1].Raw)
	}
}

func TestQueryOpenChunkDedupSealedRefs(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{
		{0, base, "dup"},
		{1, base.Add(time.Second), "unique"},
	})

	path := paths.HeadSegment(home, segID)
	manifest := &vaultctlfsm.OpenChunkManifest{
		ChunkID: chunk.NewChunkID(),
		Refs: []vaultctlfsm.OpenChunkSegmentRef{{
			SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 1,
		}},
	}

	got, _, err := chunking.CollectOpenChunk(chunking.OpenChunkQueryInput{
		Manifest: manifest,
		Locate:   mapLocator{segID: path},
		SealedRefs: []chunking.SpanRef{{
			Path: path,
			Span: chunking.Span{SegmentID: segID, Start: 0, Count: 1},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("records = %d, want 2 after dedup", len(got))
	}
	if !slices.Equal([]string{string(got[0].Raw), string(got[1].Raw)}, []string{"dup", "unique"}) {
		t.Fatalf("payloads = %q, %q", got[0].Raw, got[1].Raw)
	}
}

func TestQueryOpenChunkNilManifest(t *testing.T) {
	t.Parallel()
	_, _, err := chunking.CollectOpenChunk(chunking.OpenChunkQueryInput{
		Locate: mapLocator{},
	})
	if err != chunking.ErrNoOpenChunkManifest {
		t.Fatalf("err = %v, want ErrNoOpenChunkManifest", err)
	}
}

func TestOpenChunkReaderReadAt(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{
		{0, base, "a"},
		{1, base.Add(time.Second), "b"},
	})

	manifest := &vaultctlfsm.OpenChunkManifest{
		ChunkID:      chunk.NewChunkID(),
		TotalRecords: 2,
		Refs: []vaultctlfsm.OpenChunkSegmentRef{
			{SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 1},
		},
	}
	locate := mapLocator{segID: paths.HeadSegment(home, segID)}

	reader, report, err := chunking.NewOpenChunkReader(chunking.OpenChunkQueryInput{
		Manifest: manifest,
		Locate:   locate,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	if len(report.MissingSegments) != 0 {
		t.Fatalf("missing = %v", report.MissingSegments)
	}
	if reader.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", reader.Len())
	}
	last, err := reader.ReadAt(2)
	if err != nil {
		t.Fatal(err)
	}
	if string(last.Raw) != "b" {
		t.Fatalf("last record = %q, want b", last.Raw)
	}
	first, err := reader.ReadAt(1)
	if err != nil {
		t.Fatal(err)
	}
	if string(first.Raw) != "a" {
		t.Fatalf("first record = %q, want a", first.Raw)
	}
	for _, pos := range []uint64{0, 3} {
		if _, err := reader.ReadAt(pos); err != chunking.ErrManifestRecordOutOfRange {
			t.Fatalf("ReadAt(%d) err = %v, want ErrManifestRecordOutOfRange", pos, err)
		}
	}
}

// TestOpenChunkReaderMatchesForwardOrder proves the positional reader and the
// forward stream resolve the same record at the same position — across an
// interleaved multi-segment merge, an overlapping (EventID-deduplicated)
// sealed ref, and a missing local segment (gastrolog-54mjat).
func TestOpenChunkReaderMatchesForwardOrder(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segA := glid.New()
	segB := glid.New()
	missing := glid.New()
	vaultID := glid.New()
	home := t.TempDir()
	// Interleave EventIDs across segments so merge order != ref concatenation.
	writeHeadSegment(t, home, segA, vaultID, []recordForSeg{
		{0, base, "a0"},
		{1, base.Add(2 * time.Second), "a1"},
	})
	writeHeadSegment(t, home, segB, vaultID, []recordForSeg{
		{0, base.Add(time.Second), "b0"},
		{1, base.Add(3 * time.Second), "b1"},
	})

	pathA := paths.HeadSegment(home, segA)
	manifest := &vaultctlfsm.OpenChunkManifest{
		ChunkID:      chunk.NewChunkID(),
		TotalRecords: 5,
		Refs: []vaultctlfsm.OpenChunkSegmentRef{
			{SegmentID: segA, FirstRecordNumber: 0, LastRecordNumber: 1},
			{SegmentID: missing, FirstRecordNumber: 0, LastRecordNumber: 0},
			{SegmentID: segB, FirstRecordNumber: 0, LastRecordNumber: 1},
		},
	}
	in := chunking.OpenChunkQueryInput{
		Manifest: manifest,
		Locate: mapLocator{
			segA: pathA,
			segB: paths.HeadSegment(home, segB),
		},
		// Overlapping copy of a0: dedup must drop one copy from BOTH views.
		SealedRefs: []chunking.SpanRef{{
			Path: pathA,
			Span: chunking.Span{SegmentID: segA, Start: 0, Count: 1},
		}},
	}

	forward, report, err := chunking.CollectOpenChunk(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.MissingSegments) != 1 || report.MissingSegments[0] != missing {
		t.Fatalf("forward missing = %v, want [%s]", report.MissingSegments, missing)
	}
	wantOrder := []string{"a0", "b0", "a1", "b1"}
	if len(forward) != len(wantOrder) {
		t.Fatalf("forward records = %d, want %d", len(forward), len(wantOrder))
	}
	for i, rec := range forward {
		if string(rec.Raw) != wantOrder[i] {
			t.Fatalf("forward[%d] = %q, want %q", i, rec.Raw, wantOrder[i])
		}
	}

	reader, report, err := chunking.NewOpenChunkReader(in)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	if len(report.MissingSegments) != 1 || report.MissingSegments[0] != missing {
		t.Fatalf("reader missing = %v, want [%s]", report.MissingSegments, missing)
	}
	if reader.Len() != uint64(len(forward)) {
		t.Fatalf("reader.Len() = %d, want %d", reader.Len(), len(forward))
	}
	for i, want := range forward {
		got, err := reader.ReadAt(uint64(i) + 1)
		if err != nil {
			t.Fatalf("ReadAt(%d): %v", i+1, err)
		}
		if got.EventID.Compare(want.EventID) != 0 || string(got.Raw) != string(want.Raw) {
			t.Fatalf("ReadAt(%d) = %v %q, forward = %v %q",
				i+1, got.EventID, got.Raw, want.EventID, want.Raw)
		}
	}
}

// TestOpenChunkReaderOpenCounts proves positional reads do not re-open or
// re-verify segments per record: one mapped open per distinct segment for the
// reader's lifetime and zero full-verify opens, however many reads happen
// (gastrolog-54mjat). Not parallel: it reads process-wide open counters.
func TestOpenChunkReaderOpenCounts(t *testing.T) {
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segA := glid.New()
	segB := glid.New()
	vaultID := glid.New()
	home := t.TempDir()
	writeHeadSegment(t, home, segA, vaultID, []recordForSeg{
		{0, base, "a0"},
		{1, base.Add(2 * time.Second), "a1"},
	})
	writeHeadSegment(t, home, segB, vaultID, []recordForSeg{
		{0, base.Add(time.Second), "b0"},
		{1, base.Add(3 * time.Second), "b1"},
	})

	manifest := &vaultctlfsm.OpenChunkManifest{
		ChunkID:      chunk.NewChunkID(),
		TotalRecords: 4,
		Refs: []vaultctlfsm.OpenChunkSegmentRef{
			{SegmentID: segA, FirstRecordNumber: 0, LastRecordNumber: 1},
			{SegmentID: segB, FirstRecordNumber: 0, LastRecordNumber: 1},
		},
	}
	in := chunking.OpenChunkQueryInput{
		Manifest: manifest,
		Locate: mapLocator{
			segA: paths.HeadSegment(home, segA),
			segB: paths.HeadSegment(home, segB),
		},
	}

	opensBefore := segment.Opens()
	mappedBefore := segment.MappedOpens()

	reader, _, err := chunking.NewOpenChunkReader(in)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	// Reverse scan plus a repeat pass: 8 positional reads over 2 segments.
	for pass := 0; pass < 2; pass++ {
		for pos := reader.Len(); pos >= 1; pos-- {
			if _, err := reader.ReadAt(pos); err != nil {
				t.Fatalf("ReadAt(%d): %v", pos, err)
			}
		}
	}

	if delta := segment.Opens() - opensBefore; delta != 0 {
		t.Fatalf("segment.Open (full-verify) calls = %d, want 0", delta)
	}
	if delta := segment.MappedOpens() - mappedBefore; delta != 2 {
		t.Fatalf("OpenMapped calls = %d, want 2 (one per distinct segment)", delta)
	}
}
