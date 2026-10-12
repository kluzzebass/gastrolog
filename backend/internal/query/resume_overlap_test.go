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

// sealedChunksEngine builds an engine over one file vault holding each record
// slice as its own sealed chunk — the production read path for sealed data.
func sealedChunksEngine(t *testing.T, chunks [][]chunk.Record) *query.Engine {
	t.Helper()
	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{Dir: dir, Now: time.Now})
	if err != nil {
		t.Fatalf("chunk manager: %v", err)
	}
	t.Cleanup(func() { _ = cm.Close() })
	vaultID, root := glid.New(), t.TempDir()
	for i, recs := range chunks {
		id := chunk.NewChunkID()
		path, info := glcbtest.WriteSealedBlob(t, root, id, vaultID, recs)
		if err := cm.RegisterExternalGLCB(id, path, info); err != nil {
			t.Fatalf("register chunk %d: %v", i, err)
		}
	}
	return query.New(cm, indexfile.NewManager(dir, nil, nil, cm), nil)
}

// overlappingChunks returns two chunks of one vault whose ranges overlap on
// both timestamp axes: the first holds a record at every one of groups
// seconds, the second a record at every odd second, tied with the first's.
// Source order is a permutation of ingest order. Every record carries a
// distinct event identity.
func overlappingChunks(groups int) ([][]chunk.Record, []string) {
	ingester := glid.New()
	ingestBase := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	sourceBase := ingestBase.Add(-time.Hour)
	chunks := make([][]chunk.Record, 2)
	var raws []string
	seq := uint32(0)
	for c := range chunks {
		for g := range groups {
			if c == 1 && g%2 == 0 {
				continue
			}
			seq++
			ts := ingestBase.Add(time.Duration(g) * time.Second)
			raw := fmt.Sprintf("c%d-g%02d", c, g)
			chunks[c] = append(chunks[c], chunk.Record{
				SourceTS: sourceBase.Add(time.Duration((g*7)%groups) * time.Second),
				IngestTS: ts,
				WriteTS:  ts,
				EventID:  chunk.EventID{IngesterID: ingester, IngestTS: ts, IngestSeq: seq},
				Raw:      []byte(raw),
			})
			raws = append(raws, raw)
		}
	}
	return chunks, raws
}

// pageWithOwnTokens pages the engine by handing each page the token the
// previous one returned, as a client paging a single node does.
func pageWithOwnTokens(t *testing.T, eng *query.Engine, base query.Query, pageSize int) []chunk.Record {
	t.Helper()
	var out []chunk.Record
	var token *query.ResumeToken
	for page := 0; page < 1000; page++ {
		q := base
		q.Limit = pageSize
		it, next := eng.Search(context.Background(), q, token)
		n := 0
		for rec, err := range it {
			if err != nil {
				t.Fatalf("page %d: %v", page, err)
			}
			out = append(out, rec)
			n++
		}
		token = next()
		if token == nil || n == 0 {
			return out
		}
	}
	t.Fatal("paging never finished")
	return nil
}

func requireEachOnceInOrder(t *testing.T, recs []chunk.Record, want []string, orderBy query.OrderBy, reverse bool) {
	t.Helper()
	seen := make(map[string]int, len(recs))
	for _, r := range recs {
		seen[string(r.Raw)]++
	}
	var lost, dup []string
	for _, raw := range want {
		if seen[raw] == 0 {
			lost = append(lost, raw)
		}
	}
	for raw, n := range seen {
		if n > 1 {
			dup = append(dup, raw)
		}
	}
	if len(lost) > 0 || len(dup) > 0 || len(recs) != len(want) {
		t.Fatalf("paged %d records, want %d: lost %v, repeated %v", len(recs), len(want), lost, dup)
	}
	for i := 1; i < len(recs); i++ {
		if c := orderBy.CompareRecords(recs[i-1], recs[i], reverse); c > 0 {
			t.Fatalf("records %d and %d are out of order: %s then %s", i-1, i, recs[i-1].Raw, recs[i].Raw)
		}
	}
}

// A resumed page bounds every chunk at the cursor. A chunk whose range ends
// before a neighbour's first surviving record can still hold records past the
// cursor, so it must still be opened: paging over chunks whose ranges overlap
// delivers every record exactly once, in canonical order, both ways and on
// both timestamp axes.
func TestPagingOverOverlappingChunksDeliversEveryRecordOnce(t *testing.T) {
	chunks, want := overlappingChunks(9)
	eng := sealedChunksEngine(t, chunks)
	for _, orderBy := range []query.OrderBy{query.OrderByIngestTS, query.OrderBySourceTS} {
		for _, reverse := range []bool{false, true} {
			for _, size := range []int{1, 4, 7} {
				t.Run(fmt.Sprintf("%s/reverse=%v/limit=%d", orderBy, reverse, size), func(t *testing.T) {
					base := query.Query{OrderBy: orderBy, IsReverse: reverse}
					requireEachOnceInOrder(t, pageWithOwnTokens(t, eng, base, size), want, orderBy, reverse)
				})
			}
		}
	}
}

// A chunk scanned through its timestamp index cannot be resumed at a physical
// position. A token that names one anyway — the last record a coordinator
// displayed from that chunk — must not bound the scan: the cursor alone
// resumes it, so the rest of the search arrives whole.
func TestResumePositionDoesNotBoundAnIndexScannedChunk(t *testing.T) {
	chunks, want := overlappingChunks(9)
	eng := sealedChunksEngine(t, chunks)
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse=%v", reverse), func(t *testing.T) {
			base := query.Query{OrderBy: query.OrderBySourceTS, IsReverse: reverse}
			first := base
			first.Limit = 4
			it, _ := eng.Search(context.Background(), first, nil)
			var page1 []chunk.Record
			for rec, err := range it {
				if err != nil {
					t.Fatal(err)
				}
				page1 = append(page1, rec)
			}
			if len(page1) != 4 {
				t.Fatalf("first page returned %d records, want 4", len(page1))
			}
			last := page1[len(page1)-1]
			token := &query.ResumeToken{
				Positions:      []query.MultiVaultPosition{{VaultID: last.VaultID, ChunkID: last.Ref.ChunkID, Position: last.Ref.Pos}},
				HighwaterTS:    last.SourceTS,
				HighwaterEvent: last.EventID,
			}
			it, _ = eng.Search(context.Background(), base, token)
			rest := append([]chunk.Record(nil), page1...)
			for rec, err := range it {
				if err != nil {
					t.Fatal(err)
				}
				rest = append(rest, rec)
			}
			requireEachOnceInOrder(t, rest, want, query.OrderBySourceTS, reverse)
		})
	}
}

// Read paths fill an event's IngestTS from the record whether or not the
// event has an ingester identity, while a resume token carries no event for a
// record without one. Both forms name the same record, so they rank equal;
// and a record without an identity ranks ahead of every identified record at
// its timestamp.
func TestCompareRecordsRanksAnEventWithoutIdentityAsZero(t *testing.T) {
	ts := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	filled := chunk.Record{IngestTS: ts, EventID: chunk.EventID{IngestTS: ts}}
	bare := chunk.Record{IngestTS: ts}
	identified := chunk.Record{IngestTS: ts, EventID: chunk.EventID{IngesterID: glid.New(), IngestTS: ts, IngestSeq: 1}}
	for _, reverse := range []bool{false, true} {
		if c := query.OrderByIngestTS.CompareRecords(filled, bare, reverse); c != 0 {
			t.Errorf("reverse=%v: identity-less events with and without IngestTS compare %d, want 0", reverse, c)
		}
	}
	if c := query.OrderByIngestTS.CompareRecords(filled, identified, false); c >= 0 {
		t.Errorf("identity-less record compares %d against an identified one at its timestamp, want < 0", c)
	}
}
