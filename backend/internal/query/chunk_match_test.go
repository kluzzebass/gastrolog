package query

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

func TestChunkMatchesQuery_ExcludesZeroBoundSealedFromTimeWindow(t *testing.T) {
	t.Parallel()

	lower := time.Unix(100, 0)
	upper := time.Unix(200, 0)
	q := Query{Start: lower, End: upper}

	sealing := chunk.ChunkMeta{
		State: chunk.ChunkStateSealing,
	}
	if chunkMatchesQuery(sealing, q, lower, upper, nil) {
		t.Fatal("sealing chunk with zero bounds must not match a bounded query")
	}

	active := chunk.ChunkMeta{
		State: chunk.ChunkStateActive,
	}
	if !chunkMatchesQuery(active, q, lower, upper, nil) {
		t.Fatal("active chunk with zero bounds may still match (live head)")
	}

	bounded := chunk.ChunkMeta{
		Sealed:      true,
		IngestStart: lower,
		IngestEnd:   upper,
	}
	if !chunkMatchesQuery(bounded, q, lower, upper, nil) {
		t.Fatal("sealed chunk with bounds overlapping window must match")
	}
}
