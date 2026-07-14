package chunking_test

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/record"
)

func TestRefToSpanPartialSlice(t *testing.T) {
	t.Parallel()
	segID := glid.New()
	span, err := chunking.RefToSpan(chunking.ManifestRef{
		SegmentID:         segID,
		FirstRecordNumber: 10,
		LastRecordNumber:  24,
	})
	if err != nil {
		t.Fatal(err)
	}
	if span.Start != 10 || span.Count != 15 || span.SegmentID != segID {
		t.Fatalf("span = %+v", span)
	}
}

func TestBuildSealedChunkIdenticalDigestAcrossHomes(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segA := glid.New()
	segB := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()

	recsA := []recordForSeg{
		{0, base, "a0"},
		{2, base.Add(2 * time.Second), "a2"},
	}
	recsB := []recordForSeg{
		{1, base.Add(time.Second), "b1"},
		{3, base.Add(3 * time.Second), "b3"},
	}

	homeA := t.TempDir()
	homeB := t.TempDir()
	writeHeadSegment(t, homeA, segA, vaultID, recsA)
	writeHeadSegment(t, homeA, segB, vaultID, recsB)
	writeHeadSegment(t, homeB, segA, vaultID, recsA)
	writeHeadSegment(t, homeB, segB, vaultID, recsB)

	manifest := chunking.SealedManifest{
		ChunkID: chunkID,
		Refs: []chunking.ManifestRef{
			{SegmentID: segA, FirstRecordNumber: 0, LastRecordNumber: 1},
			{SegmentID: segB, FirstRecordNumber: 0, LastRecordNumber: 1},
		},
		SealedAt: base.Add(time.Minute),
	}

	resultA, err := chunking.BuildSealedChunk(chunking.BuildInput{
		Manifest:  manifest,
		VaultID:   vaultID,
		ChunkRoot: filepath.Join(homeA, "chunks"),
		Locate:    chunking.HeadSegmentLocator{Root: homeA},
	})
	if err != nil {
		t.Fatalf("home A build: %v", err)
	}
	resultB, err := chunking.BuildSealedChunk(chunking.BuildInput{
		Manifest:  manifest,
		VaultID:   vaultID,
		ChunkRoot: filepath.Join(homeB, "chunks"),
		Locate:    chunking.HeadSegmentLocator{Root: homeB},
	})
	if err != nil {
		t.Fatalf("home B build: %v", err)
	}
	if resultA.BlobDigest != resultB.BlobDigest {
		t.Fatalf("digest mismatch: A=%x B=%x", resultA.BlobDigest, resultB.BlobDigest)
	}
	if resultA.RecordCount != 4 || resultB.RecordCount != 4 {
		t.Fatalf("record counts = %d / %d, want 4", resultA.RecordCount, resultB.RecordCount)
	}
}

func TestBuildSealedChunkMissingSegment(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	present := glid.New()
	missing := glid.New()
	vaultID := glid.New()
	home := t.TempDir()
	writeHeadSegment(t, home, present, vaultID, []recordForSeg{{0, base, "ok"}})

	_, err := chunking.BuildSealedChunk(chunking.BuildInput{
		Manifest: chunking.SealedManifest{
			ChunkID: chunk.NewChunkID(),
			Refs: []chunking.ManifestRef{
				{SegmentID: present, FirstRecordNumber: 0, LastRecordNumber: 0},
				{SegmentID: missing, FirstRecordNumber: 0, LastRecordNumber: 0},
			},
		},
		VaultID:   vaultID,
		ChunkRoot: filepath.Join(home, "chunks"),
		Locate:    chunking.HeadSegmentLocator{Root: home},
	})
	if !errors.Is(err, chunking.ErrMissingSegments) {
		t.Fatalf("err = %v, want ErrMissingSegments", err)
	}
	var missingErr *chunking.MissingSegmentsError
	if !errors.As(err, &missingErr) {
		t.Fatal("expected MissingSegmentsError")
	}
	if !slices.Contains(missingErr.SegmentIDs, missing) {
		t.Fatalf("missing IDs = %v, want %s", missingErr.SegmentIDs, missing)
	}
	if missingErr.Error() == "" || !errors.Is(missingErr, chunking.ErrMissingSegments) {
		t.Fatalf("Error() = %q", missingErr.Error())
	}
	if _, err := os.Stat(filepath.Join(home, "chunks")); !os.IsNotExist(err) {
		t.Fatal("GLCB must not be written when segments are missing")
	}
}

type recordForSeg struct {
	seq uint32
	ts  time.Time
	raw string
}

func writeHeadSegment(t *testing.T, homeRoot string, segID, vaultID glid.GLID, recs []recordForSeg) {
	t.Helper()
	if err := paths.EnsureHeadDir(homeRoot); err != nil {
		t.Fatal(err)
	}
	records := make([]record.Record, len(recs))
	for i, r := range recs {
		records[i] = makeRecordForSeg(segID, r.seq, r.ts, r.raw)
	}
	src := writeSegment(t, segID, vaultID, records)
	dest := paths.HeadSegment(homeRoot, segID)
	if err := os.Rename(src, dest); err != nil {
		t.Fatal(err)
	}
}

func makeRecordForSeg(segID glid.GLID, seq uint32, ts time.Time, raw string) record.Record {
	id := record.EventID{
		IngesterID: segID,
		NodeID:     segID,
		IngestTS:   ts,
		IngestSeq:  seq,
	}
	return record.Record{
		EventID:  id,
		SourceTS: ts.Add(-time.Millisecond),
		IngestTS: id.IngestTS,
		Attrs:    record.Attributes{"k": "v"},
		Raw:      []byte(raw),
	}
}
