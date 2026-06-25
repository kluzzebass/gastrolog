package chunking_test

import (
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
)

func TestBuildSealedChunkUsesInlineMetaWithoutRescan(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segA := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()
	writeHeadSegment(t, home, segA, vaultID, []recordForSeg{{0, base, "one"}})

	result, err := chunking.BuildSealedChunk(chunking.BuildInput{
		Manifest: chunking.SealedManifest{
			ChunkID: chunkID,
			Refs: []chunking.ManifestRefEntry{
				{SegmentID: segA, FirstRecordNumber: 0, LastRecordNumber: 0},
			},
			SealedAt: base.Add(time.Minute),
		},
		VaultID:   vaultID,
		ChunkRoot: filepath.Join(home, "chunks"),
		Locate:    chunking.HeadSegmentLocator{Root: home},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.RecordCount != 1 {
		t.Fatalf("RecordCount = %d, want 1", result.RecordCount)
	}
	if result.Bytes <= 0 {
		t.Fatalf("Bytes = %d, want > 0", result.Bytes)
	}
	if !result.IngestTSMonotonic {
		t.Fatal("expected monotonic ingest for single record")
	}
}
