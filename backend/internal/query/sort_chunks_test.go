package query

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

func TestSortChunksReverseRanksActiveChunkFirst(t *testing.T) {
	t.Parallel()

	t0 := time.Date(2026, 6, 18, 0, 0, 0, 0, time.UTC)
	activeID := chunk.ChunkID{0x01}
	sealedID := chunk.ChunkID{0x02}

	metas := []chunk.ChunkMeta{
		{
			ID:          sealedID,
			Sealed:      true,
			IngestStart: t0.Add(5 * time.Minute),
			IngestEnd:   t0.Add(6 * time.Minute),
		},
		{
			ID:          activeID,
			Sealed:      false,
			IngestStart: t0, // stale open time; live tail is newer than sealed chunks
		},
	}

	sortChunks(metas, OrderByIngestTS, true)

	if metas[0].ID != activeID {
		t.Fatalf("active chunk should sort first for reverse, got %v then %v", metas[0].ID, metas[1].ID)
	}
}
