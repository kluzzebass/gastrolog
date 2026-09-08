package query_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/chunk/glcb/glcbtest"
	"gastrolog/internal/glid"
	indexfile "gastrolog/internal/index/file"
	"gastrolog/internal/query"
)

// sourceInterleavedEngine builds an engine over sealed chunks whose source
// timestamps interleave across chunks and tie in groups of four.
func sourceInterleavedEngine(t *testing.T, chunks, perChunk int) (*query.Engine, int) {
	t.Helper()
	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{Dir: dir, Now: time.Now})
	if err != nil {
		t.Fatalf("chunk manager: %v", err)
	}
	t.Cleanup(func() { _ = cm.Close() })
	vaultID, ingester := glid.New(), glid.New()
	root := t.TempDir()
	ingestBase := time.Now().Add(-10 * time.Minute).Truncate(time.Millisecond)
	sourceBase := ingestBase.Add(-time.Hour)
	seq := uint32(0)
	for c := range chunks {
		recs := make([]chunk.Record, 0, perChunk)
		for i := range perChunk {
			seq++
			k := i*chunks + c
			ingestTS := ingestBase.Add(time.Duration(c)*time.Second + time.Duration(i)*time.Millisecond)
			recs = append(recs, chunk.Record{
				SourceTS: sourceBase.Add(time.Duration(k/4) * 10 * time.Millisecond),
				IngestTS: ingestTS,
				WriteTS:  ingestTS,
				EventID:  chunk.EventID{IngesterID: ingester, IngestTS: ingestTS, IngestSeq: seq},
				Raw:      fmt.Appendf(nil, "c%02d-%03d", c, i),
			})
		}
		id := chunk.NewChunkID()
		path, info := glcbtest.WriteSealedBlob(t, root, id, vaultID, recs)
		if err := cm.RegisterExternalGLCB(id, path, info); err != nil {
			t.Fatalf("register chunk %d: %v", c, err)
		}
	}
	return query.New(cm, indexfile.NewManager(dir, nil, nil, cm), nil), chunks * perChunk
}

// pageThrough drives the engine the way the server does: each page is bounded
// at the previous page's cursor on the ordering axis and skips up to it.
func pageThrough(t *testing.T, eng *query.Engine, base query.Query, pageSize int) []chunk.Record {
	t.Helper()
	var out []chunk.Record
	var token *query.ResumeToken
	for page := 0; page < 1000; page++ {
		q := base
		q.Limit = pageSize
		if token != nil {
			if q.Reverse() {
				q.SourceEnd = token.HighwaterTS.Add(time.Nanosecond)
			} else {
				q.SourceStart = token.HighwaterTS
			}
			q.ResumeAfterTS = token.HighwaterTS
			q.ResumeAfterEvent = token.HighwaterEvent
		}
		it, next := eng.Search(context.Background(), q, token)
		n := 0
		var last chunk.Record
		for rec, err := range it {
			if err != nil {
				t.Fatalf("page %d: %v", page, err)
			}
			out = append(out, rec)
			last = rec
			n++
		}
		token = next()
		if token == nil {
			return out
		}
		if n == 0 {
			t.Fatalf("page %d returned nothing but a token", page)
		}
		token.HighwaterTS = base.OrderBy.RecordTS(last)
		token.HighwaterEvent = last.EventID
	}
	t.Fatal("paging never finished")
	return nil
}

// Under order=source_ts every chunk is scanned through its source index, so
// nothing about a physical position can resume it: the token carries none,
// and paging through the cursor alone loses and repeats nothing across tie
// groups, in both directions.
func TestSourceTSPagingResumesByCursorNotPosition(t *testing.T) {
	for _, chunks := range []int{1, 5} {
		t.Run(fmt.Sprintf("%d chunks", chunks), func(t *testing.T) {
			sourceTSPagingResumesByCursor(t, chunks)
		})
	}
}

func sourceTSPagingResumesByCursor(t *testing.T, chunks int) {
	eng, total := sourceInterleavedEngine(t, chunks, 24)

	q := query.Query{OrderBy: query.OrderBySourceTS, Limit: 7}
	it, next := eng.Search(context.Background(), q, nil)
	for _, err := range it {
		if err != nil {
			t.Fatal(err)
		}
	}
	token := next()
	if token == nil {
		t.Fatal("first page produced no token")
	}
	if len(token.Positions) != 0 {
		t.Errorf("rank-scanned chunks carried %d physical positions; they cannot resume by position", len(token.Positions))
	}

	for _, reverse := range []bool{false, true} {
		base := query.Query{OrderBy: query.OrderBySourceTS, IsReverse: reverse}
		recs := pageThrough(t, eng, base, 7)
		if len(recs) != total {
			t.Errorf("reverse=%v: paged %d records, want %d", reverse, len(recs), total)
		}
		seen := make(map[string]int, len(recs))
		for i, rec := range recs {
			seen[string(rec.Raw)]++
			if i == 0 {
				continue
			}
			a, b := recs[i-1].SourceTS, rec.SourceTS
			if (!reverse && b.Before(a)) || (reverse && b.After(a)) {
				t.Fatalf("reverse=%v: out of source order at %d: %v then %v", reverse, i, a, b)
			}
		}
		for raw, n := range seen {
			if n > 1 {
				t.Errorf("reverse=%v: %s repeated %d times", reverse, raw, n)
			}
		}
	}
}
