package chunking_test

import (
	"slices"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/paths"
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
