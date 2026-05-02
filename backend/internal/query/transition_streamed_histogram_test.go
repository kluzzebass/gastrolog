package query_test

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	"gastrolog/internal/memtest"
	"gastrolog/internal/query"
)

// TestHistogram_SkipsTransitionStreamedChunks is the regression for
// gastrolog-4xusf. During a tier transition, the source chunk is
// marked TransitionStreamed=true after its records have been streamed
// to the destination tier — but the source chunk stays on disk until
// retention confirms the destination receipt. In that window, both
// the source and destination tiers expose the chunk via cm.List(), so
// the histogram (which counts via chunk manifest record counts) would
// double the bucket totals.
//
// The fix surfaces the per-vault TransitionStreamed set on the
// manifest.VaultRegistry interface and skips those chunks in every histogram
// enumeration path. This test models a transition mid-flight: vault A
// (the "source" tier in our two-vault setup) holds a chunk that has
// been marked TransitionStreamed; vault B (the "destination") holds an
// equivalent chunk that has NOT been marked streamed. The histogram
// must count the records exactly once.
func TestHistogram_SkipsTransitionStreamedChunks(t *testing.T) {
	t.Parallel()

	const recordsPerChunk = 10
	t0 := time.Now().Add(-1 * time.Hour) // safely in the past

	// Build a vault and seal a chunk into it.
	makeVaultWithSealedChunk := func(label string) (glid.GLID, chunk.ChunkManager, index.IndexManager, chunk.ChunkID) {
		s := memtest.MustNewVault(t, chunkmem.Config{
			RotationPolicy: chunk.NewRecordCountPolicy(1000),
		})
		for i := range recordsPerChunk {
			s.CM.Append(chunk.Record{
				IngestTS: t0.Add(time.Duration(i) * time.Second),
				Raw:      []byte(label),
			})
		}
		_ = s.CM.Seal()
		metas, _ := s.CM.List()
		if len(metas) != 1 {
			t.Fatalf("%s: expected 1 chunk, got %d", label, len(metas))
		}
		return glid.New(), s.CM, s.IM, metas[0].ID
	}

	srcVaultID, srcCM, srcIM, srcChunkID := makeVaultWithSealedChunk("src")
	dstVaultID, dstCM, dstIM, _ := makeVaultWithSealedChunk("dst")

	reg := &testRegistry{
		vaults: map[glid.GLID]struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}{
			srcVaultID: {srcCM, srcIM},
			dstVaultID: {dstCM, dstIM},
		},
		// Source vault has the chunk marked streamed; destination does not.
		// Histogram enumeration must see the source chunk and skip it.
		streamed: map[glid.GLID]map[chunk.ChunkID]bool{
			srcVaultID: {srcChunkID: true},
		},
	}
	eng := query.NewWithRegistry(reg, nil)

	q := query.Query{
		Start: t0.Add(-1 * time.Minute),
		End:   t0.Add(time.Duration(recordsPerChunk) * time.Second),
	}
	buckets := eng.ComputeHistogram(context.Background(), q, 50)

	var total int64
	for _, b := range buckets {
		total += b.Count
	}

	// Without the filter: 2 × recordsPerChunk (source + destination both counted).
	// With the filter: recordsPerChunk (destination only).
	if total != recordsPerChunk {
		t.Errorf("histogram total = %d; want %d (transition-streamed chunk should be skipped at the source, counted only at the destination)", total, recordsPerChunk)
	}
}

// TestHistogram_NoStreamed_BaselineUnchanged verifies the filter is a
// no-op when the registry reports no streamed chunks — the common case.
// This guards against the filter accidentally over-skipping when the
// transition state is empty.
func TestHistogram_NoStreamed_BaselineUnchanged(t *testing.T) {
	t.Parallel()

	const recordsPerChunk = 10
	t0 := time.Now().Add(-1 * time.Hour)

	s := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})
	for i := range recordsPerChunk {
		s.CM.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Raw:      []byte("rec"),
		})
	}
	_ = s.CM.Seal()

	vaultID := glid.New()
	reg := &testRegistry{
		vaults: map[glid.GLID]struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}{
			vaultID: {s.CM, s.IM},
		},
	}
	eng := query.NewWithRegistry(reg, nil)

	buckets := eng.ComputeHistogram(context.Background(), query.Query{
		Start: t0.Add(-1 * time.Minute),
		End:   t0.Add(time.Duration(recordsPerChunk) * time.Second),
	}, 50)

	var total int64
	for _, b := range buckets {
		total += b.Count
	}
	if total != recordsPerChunk {
		t.Errorf("histogram total = %d; want %d", total, recordsPerChunk)
	}
}
