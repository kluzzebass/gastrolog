package server_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb/glcbtest"
	"gastrolog/internal/glid"
	"slices"
	"strings"
)

// searchPaged drives Search the way a paginating client does: a fixed page
// size per request and the previous response's resume token on the next.
func searchPaged(t *testing.T, client gastrologv1connect.QueryServiceClient, expr string, pageSize int64) []*gastrologv1.Record {
	t.Helper()
	var out []*gastrologv1.Record
	var token []byte
	for page := 0; page < 200; page++ {
		stream, err := client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
			Query:       &gastrologv1.Query{Expression: expr, Limit: pageSize},
			ResumeToken: token,
		}))
		if err != nil {
			t.Fatalf("Search page %d: %v", page, err)
		}
		var next []byte
		more := false
		got := 0
		for stream.Receive() {
			msg := stream.Msg()
			out = append(out, msg.Records...)
			got += len(msg.Records)
			if len(msg.ResumeToken) > 0 {
				next = msg.ResumeToken
			}
			more = more || msg.HasMore
		}
		if err := stream.Err(); err != nil && err != io.EOF {
			t.Fatalf("page %d stream error: %v", page, err)
		}
		if !more || len(next) == 0 || got == 0 {
			return out
		}
		token = next
	}
	t.Fatal("pagination did not terminate")
	return nil
}

// 400 records in 40-record groups that share an IngestTS, paged 50 at a time
// from a coordinator that does not hold the vault: every page boundary lands
// inside a tie group. No record may be lost or repeated in either direction.
func TestMultiNode_PaginationLosesNothingAcrossTieGroups(t *testing.T) {
	const groups, perGroup, pageSize = 10, 40, 50
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithoutVault("coord"))
	data := h.Node(t, "data-1")

	ingester := glid.New()
	t0 := time.Now().Add(-10 * time.Minute).Truncate(time.Millisecond)
	want := make(map[string]bool, groups*perGroup)
	seq := uint32(0)
	for g := range groups {
		ingestTS := t0.Add(time.Duration(g) * 10 * time.Millisecond)
		for i := range perGroup {
			seq++
			raw := fmt.Sprintf("g%02d-%02d", g, i)
			want[raw] = true
			if _, _, err := data.vault.CM.Append(chunk.Record{
				SourceTS: ingestTS.Add(-time.Hour),
				IngestTS: ingestTS,
				WriteTS:  ingestTS,
				EventID:  chunk.EventID{IngesterID: ingester, IngestTS: ingestTS, IngestSeq: seq},
				Attrs:    chunk.Attributes{"g": fmt.Sprint(g)},
				Raw:      []byte(raw),
			}); err != nil {
				t.Fatalf("append %s: %v", raw, err)
			}
		}
	}
	window := fmt.Sprintf("start=%s end=%s", t0.Add(-2*time.Hour).UTC().Format(time.RFC3339Nano), time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano))

	// Premise: unpaged, the coordinator sees every record.
	if got := len(searchAll(t, h.client, window)); got != groups*perGroup {
		t.Fatalf("unpaged search returned %d records, want %d", got, groups*perGroup)
	}

	for _, ord := range []string{"", "reverse=true"} {
		t.Run("ordering="+ord, func(t *testing.T) {
			recs := searchPaged(t, h.client, window+" "+ord, pageSize)
			seen := make(map[string]int, len(recs))
			for _, r := range recs {
				seen[string(r.Raw)]++
			}
			var lost, dup []string
			for raw := range want {
				if seen[raw] == 0 {
					lost = append(lost, raw)
				}
			}
			for raw, n := range seen {
				if n > 1 {
					dup = append(dup, raw)
				}
			}
			if len(lost) > 0 || len(dup) > 0 {
				t.Fatalf("paged %d records (want %d): lost %d %v…, duplicated %d %v…", len(recs), groups*perGroup, len(lost), head(lost, 5), len(dup), head(dup, 5))
			}
		})
	}
}

// sourceOrderedBlobs plants sealed chunks on a file vault whose source
// timestamps interleave across chunks and tie in groups of four, while each
// chunk's ingest timestamps stay disjoint from the others' — the shape a
// source-ordered scan meets when several chunks cover the same source window.
// Every record carries a distinct EventID. Returns the raw payloads by source
// order.
func sourceOrderedBlobs(t *testing.T, node multinodeTestNode, chunks, perChunk int) []string {
	t.Helper()
	registrar, ok := node.vault.CM.(chunk.ExternalGLCBRegistrar)
	if !ok {
		t.Fatal("file vault chunk manager must register external GLCBs")
	}
	root := t.TempDir()
	ingester := glid.New()
	ingestBase := time.Now().Add(-10 * time.Minute).Truncate(time.Millisecond)
	sourceBase := ingestBase.Add(-time.Hour)
	seq := uint32(0)
	type keyed struct {
		k   int
		raw string
	}
	var all []keyed
	for c := range chunks {
		recs := make([]chunk.Record, 0, perChunk)
		for i := range perChunk {
			seq++
			k := i*chunks + c // global source order
			raw := fmt.Sprintf("c%02d-%03d", c, i)
			ingestTS := ingestBase.Add(time.Duration(c)*time.Second + time.Duration(i)*time.Millisecond)
			recs = append(recs, chunk.Record{
				SourceTS: sourceBase.Add(time.Duration(k/4) * 10 * time.Millisecond),
				IngestTS: ingestTS,
				WriteTS:  ingestTS,
				EventID:  chunk.EventID{IngesterID: ingester, IngestTS: ingestTS, IngestSeq: seq},
				Raw:      []byte(raw),
			})
			all = append(all, keyed{k, raw})
		}
		id := chunk.NewChunkID()
		path, info := glcbtest.WriteSealedBlob(t, root, id, node.vaultID, recs)
		if err := registrar.RegisterExternalGLCB(id, path, info); err != nil {
			t.Fatalf("register chunk %d: %v", c, err)
		}
	}
	slices.SortFunc(all, func(a, b keyed) int { return a.k - b.k })
	out := make([]string, len(all))
	for i, e := range all {
		out[i] = e.raw
	}
	return out
}

// Paging under order=source_ts loses and repeats nothing, and every page
// arrives in source order, on a vault whose chunks are scanned through their
// source-timestamp index. Such a chunk has no resumable physical position; the
// pages rely on the canonical cursor alone.
func TestMultiNode_SourceTSPaginationLosesNothingAcrossTieGroups(t *testing.T) {
	// The vault on a remote node exercises the forwarded cursor; on the
	// coordinator itself, the engine's own per-chunk positions.
	t.Run("vault on data node", func(t *testing.T) {
		h := setupMultiNode(t, []string{"coord", "data-1"}, WithoutVault("coord"), WithFileVault("data-1"))
		sourceTSPagingLosesNothing(t, h, h.Node(t, "data-1"))
	})
	t.Run("vault on coordinator", func(t *testing.T) {
		h := setupMultiNode(t, []string{"coord", "data-1"}, WithFileVault("coord"), WithoutVault("data-1"))
		sourceTSPagingLosesNothing(t, h, h.Node(t, "coord"))
	})
}

func sourceTSPagingLosesNothing(t *testing.T, h *multiNodeHarness, node multinodeTestNode) {
	t.Helper()
	const chunks, perChunk, pageSize = 6, 30, 7
	want := sourceOrderedBlobs(t, node, chunks, perChunk)
	total := chunks * perChunk

	// Premise: unpaged, the coordinator sees every record in source order.
	got := rawsOf(searchAll(t, h.client, "order=source_ts"))
	if len(got) != total {
		t.Fatalf("unpaged source-ordered search returned %d records, want %d", len(got), total)
	}
	if !sourceOrdered(got, want) {
		t.Fatalf("unpaged source-ordered search is not in source order: %v…", head(got, 12))
	}

	for _, ord := range []string{"order=source_ts", "order=source_ts reverse=true"} {
		t.Run(ord, func(t *testing.T) {
			recs := searchPaged(t, h.client, ord, pageSize)
			raws := rawsOf(recs)
			if len(raws) != total {
				t.Errorf("paged %d records, want %d", len(raws), total)
			}
			seen := make(map[string]int, len(raws))
			for _, r := range raws {
				seen[r]++
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
			if len(lost) > 0 || len(dup) > 0 {
				t.Fatalf("lost %d %v…, duplicated %d %v…", len(lost), head(lost, 5), len(dup), head(dup, 5))
			}
			for i := 1; i < len(recs); i++ {
				a, b := recs[i-1].GetSourceTs().AsTime(), recs[i].GetSourceTs().AsTime()
				if (!strings.Contains(ord, "reverse") && b.Before(a)) || (strings.Contains(ord, "reverse") && b.After(a)) {
					t.Fatalf("records %d and %d are out of source order across pages: %v then %v", i-1, i, a, b)
				}
			}
		})
	}
}

// sourceOrdered reports whether got lists want's records in an order
// consistent with want's source order: records sharing a source timestamp
// (groups of four in want) may appear in any order within their group.
func sourceOrdered(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	rank := make(map[string]int, len(want))
	for i, raw := range want {
		rank[raw] = i / 4
	}
	for i := 1; i < len(got); i++ {
		if rank[got[i]] < rank[got[i-1]] {
			return false
		}
	}
	return true
}

func head(s []string, n int) []string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
