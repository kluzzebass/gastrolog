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
	"gastrolog/internal/glid"
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

// Resuming under a non-default ordering is refused outright rather than
// paged with a cursor the engine cannot yet honour on that axis; the client
// gets Unimplemented, never a silently partial page.
func TestMultiNode_ResumeUnderSourceTSOrderIsRefusedLoudly(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithoutVault("coord"))
	addMNRecordsAt(t, h.Node(t, "data-1"), "r", 30, time.Now().Add(-time.Minute))

	stream, err := h.client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{Expression: "", Limit: 10},
	}))
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	var token []byte
	for stream.Receive() {
		if rt := stream.Msg().ResumeToken; len(rt) > 0 {
			token = rt
		}
	}
	if len(token) == 0 {
		t.Fatal("page 1 produced no resume token")
	}

	stream, err = h.client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query:       &gastrologv1.Query{Expression: "order=source_ts", Limit: 10},
		ResumeToken: token,
	}))
	if err == nil {
		for stream.Receive() {
		}
		err = stream.Err()
	}
	if connect.CodeOf(err) != connect.CodeUnimplemented {
		t.Fatalf("resume under order=source_ts returned %v, want Unimplemented", err)
	}
}

func head(s []string, n int) []string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
