package query_test

import (
	"context"
	"fmt"
	"gastrolog/internal/glid"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	"gastrolog/internal/memtest"
	"gastrolog/internal/query"
)

type cursorCountCM struct {
	chunk.ChunkManager
	opens atomic.Int64
}

func (c *cursorCountCM) ingestRankView() (chunk.IngestTSRankView, bool) {
	v, ok := c.ChunkManager.(chunk.IngestTSRankView)
	return v, ok
}

func (c *cursorCountCM) IngestTSRankLen(id chunk.ChunkID) (uint64, error) {
	if v, ok := c.ingestRankView(); ok {
		return v.IngestTSRankLen(id)
	}
	return 0, chunk.ErrIngestTSRankIndex
}

func (c *cursorCountCM) IngestTSRankAt(id chunk.ChunkID, rank uint64) (int64, uint32, error) {
	if v, ok := c.ingestRankView(); ok {
		return v.IngestTSRankAt(id, rank)
	}
	return 0, 0, chunk.ErrIngestTSRankIndex
}

func (c *cursorCountCM) FindIngestTSRank(id chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	if v, ok := c.ingestRankView(); ok {
		return v.FindIngestTSRank(id, ts)
	}
	return 0, false, chunk.ErrIngestTSRankIndex
}

func (c *cursorCountCM) OpenCursor(id chunk.ChunkID) (chunk.RecordCursor, error) {
	c.opens.Add(1)
	return c.ChunkManager.OpenCursor(id)
}

// TestLazyPrimeOpensFewChunksForLimitedSearch verifies that a reverse
// limited search over many non-overlapping sequential chunks does not
// open a cursor for every chunk up front.
func TestLazyPrimeOpensFewChunksForLimitedSearch(t *testing.T) {
	t.Parallel()

	const (
		recordsPerChunk = 10
		chunkCount      = 25
		limit           = 5
	)

	reg := &testRegistry{
		vaults: make(map[glid.GLID]struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}),
	}

	vaultID := glid.New()
	base := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(recordsPerChunk),
	})

	t0 := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	for i := range recordsPerChunk * chunkCount {
		base.CM.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Raw:      fmt.Appendf(nil, "r-%d", i),
		})
	}
	base.CM.Seal()

	counter := &cursorCountCM{ChunkManager: base.CM}
	reg.vaults[vaultID] = struct {
		cm chunk.ChunkManager
		im index.IndexManager
	}{counter, base.IM}

	eng := query.NewWithRegistry(reg, nil)

	q := query.Query{
		Limit:     limit,
		IsReverse: true,
		Start:     t0,
		End:       t0.Add(recordsPerChunk * chunkCount * time.Second),
	}

	iter, _ := eng.Search(context.Background(), q, nil)
	var raws []string
	for rec, err := range iter {
		if err != nil {
			t.Fatalf("search error: %v", err)
		}
		raws = append(raws, string(rec.Raw))
	}
	if len(raws) != limit {
		t.Fatalf("expected %d records, got %d", limit, len(raws))
	}

	// Newest records live in the last-sealed chunk only.
	lastChunkStart := recordsPerChunk * (chunkCount - 1)
	for i, raw := range raws {
		want := fmt.Sprintf("r-%d", lastChunkStart+(recordsPerChunk-1-i))
		if raw != want {
			t.Errorf("record %d: got %q want %q", i, raw, want)
		}
	}

	opens := counter.opens.Load()
	if opens > 2 {
		t.Errorf("OpenCursor calls = %d, want at most 2 for limit=%d over %d sequential chunks", opens, limit, chunkCount)
	}
}

// TestLazyPrimeUnlimitedSearchCorrectness ensures lazy priming still returns
// every record when no limit is set.
func TestLazyPrimeUnlimitedSearchCorrectness(t *testing.T) {
	t.Parallel()

	const (
		recordsPerChunk = 5
		chunkCount      = 8
	)

	reg := &testRegistry{
		vaults: make(map[glid.GLID]struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}),
	}

	vaultID := glid.New()
	base := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(recordsPerChunk),
	})

	t0 := time.Date(2026, 6, 17, 13, 0, 0, 0, time.UTC)
	total := recordsPerChunk * chunkCount
	for i := range total {
		base.CM.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Raw:      fmt.Appendf(nil, "r-%d", i),
		})
	}
	base.CM.Seal()

	reg.vaults[vaultID] = struct {
		cm chunk.ChunkManager
		im index.IndexManager
	}{base.CM, base.IM}

	eng := query.NewWithRegistry(reg, nil)

	iter, _ := eng.Search(context.Background(), query.Query{
		Start: t0,
		End:   t0.Add(time.Duration(total) * time.Second),
	}, nil)

	count := 0
	for _, err := range iter {
		if err != nil {
			t.Fatalf("search error: %v", err)
		}
		count++
	}
	if count != total {
		t.Errorf("expected %d records, got %d", total, count)
	}
}

// TestLazyPrimeBoundedOpensAtManyChunks guards the lazy-prime bound: cursor
// opens must not scale with total chunk count when the result limit is small.
func TestLazyPrimeBoundedOpensAtManyChunks(t *testing.T) {
	t.Parallel()

	const (
		recordsPerChunk = 10
		chunkCount      = 100
		limit           = 5
		maxOpens        = 4
	)

	reg := &testRegistry{
		vaults: make(map[glid.GLID]struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}),
	}

	vaultID := glid.New()
	base := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(recordsPerChunk),
	})

	t0 := time.Date(2026, 6, 17, 14, 0, 0, 0, time.UTC)
	total := recordsPerChunk * chunkCount
	for i := range total {
		base.CM.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Raw:      fmt.Appendf(nil, "r-%d", i),
		})
	}
	base.CM.Seal()

	counter := &cursorCountCM{ChunkManager: base.CM}
	reg.vaults[vaultID] = struct {
		cm chunk.ChunkManager
		im index.IndexManager
	}{counter, base.IM}

	eng := query.NewWithRegistry(reg, nil)

	iter, _ := eng.Search(context.Background(), query.Query{
		Limit:     limit,
		IsReverse: true,
		Start:     t0,
		End:       t0.Add(time.Duration(total) * time.Second),
	}, nil)

	got := 0
	for _, err := range iter {
		if err != nil {
			t.Fatalf("search error: %v", err)
		}
		got++
	}
	if got != limit {
		t.Fatalf("expected %d records, got %d", limit, got)
	}

	opens := counter.opens.Load()
	if opens > maxOpens {
		t.Errorf("OpenCursor calls = %d over %d chunks with limit=%d, want at most %d",
			opens, chunkCount, limit, maxOpens)
	}
}

func BenchmarkLazyPrimeReverseSearchCursorOpens(b *testing.B) {
	const recordsPerChunk = 10

	for _, chunkCount := range []int{25, 100, 200} {
		b.Run(fmt.Sprintf("chunks=%d", chunkCount), func(b *testing.B) {
			reg := &testRegistry{
				vaults: make(map[glid.GLID]struct {
					cm chunk.ChunkManager
					im index.IndexManager
				}),
			}

			vaultID := glid.New()
			base, err := memtest.NewVault(chunkmem.Config{
				RotationPolicy: chunk.NewRecordCountPolicy(recordsPerChunk),
			})
			if err != nil {
				b.Fatalf("memtest.NewVault: %v", err)
			}

			t0 := time.Date(2026, 6, 17, 15, 0, 0, 0, time.UTC)
			total := recordsPerChunk * chunkCount
			for i := range total {
				base.CM.Append(chunk.Record{
					IngestTS: t0.Add(time.Duration(i) * time.Second),
					Raw:      fmt.Appendf(nil, "r-%d", i),
				})
			}
			base.CM.Seal()

			counter := &cursorCountCM{ChunkManager: base.CM}
			reg.vaults[vaultID] = struct {
				cm chunk.ChunkManager
				im index.IndexManager
			}{counter, base.IM}

			eng := query.NewWithRegistry(reg, nil)
			q := query.Query{
				Limit:     5,
				IsReverse: true,
				Start:     t0,
				End:       t0.Add(time.Duration(total) * time.Second),
			}

			b.ResetTimer()
			for range b.N {
				counter.opens.Store(0)
				iter, _ := eng.Search(context.Background(), q, nil)
				for _, err := range iter {
					if err != nil {
						b.Fatalf("search error: %v", err)
					}
				}
				if opens := counter.opens.Load(); opens > 4 {
					b.Fatalf("OpenCursor calls = %d for %d chunks, want at most 4", opens, chunkCount)
				}
			}
		})
	}
}
