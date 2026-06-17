package query

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
)

type mmapTSIndexStub struct {
	entries []index.TSEntry
}

func (s *mmapTSIndexStub) IngestIndexLen(chunkID chunk.ChunkID) (uint64, error) {
	return uint64(len(s.entries)), nil
}

func (s *mmapTSIndexStub) IngestIndexEntryAt(chunkID chunk.ChunkID, rank uint64) (index.TSEntry, error) {
	if rank >= uint64(len(s.entries)) {
		return index.TSEntry{}, index.ErrIndexNotFound
	}
	return s.entries[rank], nil
}

func (s *mmapTSIndexStub) FindIngestEntryIndex(chunkID chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	tsNano := ts.UnixNano()
	for i, e := range s.entries {
		if e.TS >= tsNano {
			return uint64(i), true, nil
		}
	}
	return 0, false, nil
}

func (s *mmapTSIndexStub) SourceIndexLen(chunkID chunk.ChunkID) (uint64, error) {
	return 0, index.ErrIndexNotFound
}

func (s *mmapTSIndexStub) SourceIndexEntryAt(chunkID chunk.ChunkID, rank uint64) (index.TSEntry, error) {
	return index.TSEntry{}, index.ErrIndexNotFound
}

func (s *mmapTSIndexStub) FindSourceEntryIndex(chunkID chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	return 0, false, index.ErrIndexNotFound
}

func TestTSIndexRankBounds(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC)
	entries := []index.TSEntry{
		{TS: base.UnixNano(), Pos: 0},
		{TS: base.Add(time.Second).UnixNano(), Pos: 1},
		{TS: base.Add(2 * time.Second).UnixNano(), Pos: 2},
		{TS: base.Add(3 * time.Second).UnixNano(), Pos: 3},
	}
	stub := &mmapTSIndexStub{entries: entries}
	view := tsIndexView{
		lenFn:    stub.IngestIndexLen,
		entryAt:  stub.IngestIndexEntryAt,
		findRank: stub.FindIngestEntryIndex,
	}
	chunkID := chunk.ChunkID(glid.New())

	start, end, ok, err := tsIndexRankBounds(view, chunkID, Query{
		Start: base.Add(time.Second),
		End:   base.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatalf("tsIndexRankBounds: %v", err)
	}
	if !ok {
		t.Fatal("expected non-empty bounds")
	}
	if start != 1 || end != 3 {
		t.Fatalf("bounds = [%d,%d), want [1,3)", start, end)
	}
}

func TestTSIndexRankBoundsEmptyWhenPastEnd(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC)
	stub := &mmapTSIndexStub{entries: []index.TSEntry{{TS: base.UnixNano(), Pos: 0}}}
	view := tsIndexView{
		lenFn:    stub.IngestIndexLen,
		entryAt:  stub.IngestIndexEntryAt,
		findRank: stub.FindIngestEntryIndex,
	}

	_, _, ok, err := tsIndexRankBounds(view, chunk.ChunkID(glid.New()), Query{
		Start: base.Add(time.Hour),
		End:   base.Add(2 * time.Hour),
	})
	if err != nil {
		t.Fatalf("tsIndexRankBounds: %v", err)
	}
	if ok {
		t.Fatal("expected empty bounds")
	}
}
