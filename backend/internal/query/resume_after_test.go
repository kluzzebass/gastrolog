package query_test

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/query"
)

// tieGroups returns g groups of n records; every record in a group shares
// one timestamp and each carries a distinct EventID.
func tieGroups(g, n int) []chunk.Record {
	ing := glid.New()
	base := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	var recs []chunk.Record
	seq := uint32(0)
	for i := range g {
		ts := base.Add(time.Duration(i) * time.Second)
		for j := range n {
			seq++
			recs = append(recs, chunk.Record{
				IngestTS: ts,
				EventID:  chunk.EventID{IngesterID: ing, IngestTS: ts, IngestSeq: seq},
				Attrs:    chunk.Attributes{"g": "x"},
				Raw:      []byte{byte('a' + i), byte('0' + j)},
			})
		}
	}
	return recs
}

func rawSet(recs []chunk.Record) map[string]int {
	m := map[string]int{}
	for _, r := range recs {
		m[string(r.Raw)]++
	}
	return m
}

// Paging with the cursor set to the last emitted record must hand back
// exactly the remaining records, even when the boundary falls inside a
// group sharing a timestamp — no repeats, no gaps, either direction.
func TestSearchResumesAfterCanonicalCursorInsideATieGroup(t *testing.T) {
	recs := tieGroups(3, 5) // 15 records, groups of 5
	eng := setup(t, recs)

	for _, reverse := range []bool{false, true} {
		t.Run(map[bool]string{false: "forward", true: "reverse"}[reverse], func(t *testing.T) {
			page1, err := collect(first(eng.Search(context.Background(), query.Query{Limit: 7, IsReverse: reverse}, nil)))
			if err != nil || len(page1) != 7 {
				t.Fatalf("page 1: %d records, err=%v", len(page1), err)
			}
			last := page1[len(page1)-1]
			q := query.Query{IsReverse: reverse, ResumeAfterTS: last.IngestTS, ResumeAfterEvent: last.EventID}
			if reverse {
				q.End = last.IngestTS.Add(time.Nanosecond)
			} else {
				q.Start = last.IngestTS
			}
			page2, err := collect(first(eng.Search(context.Background(), q, nil)))
			if err != nil {
				t.Fatalf("page 2: %v", err)
			}
			seen := rawSet(append(append([]chunk.Record{}, page1...), page2...))
			if len(page1)+len(page2) != len(recs) || len(seen) != len(recs) {
				t.Fatalf("pages delivered %d records covering %d distinct of %d", len(page1)+len(page2), len(seen), len(recs))
			}
			for raw, n := range seen {
				if n != 1 {
					t.Fatalf("record %q delivered %d times", raw, n)
				}
			}
		})
	}
}

// Without an ingester identity on the cursor the engine cannot order ties;
// it keeps the boundary group rather than dropping it.
func TestResumeAfterWithoutIdentityKeepsTheBoundaryGroup(t *testing.T) {
	recs := tieGroups(2, 4)
	eng := setup(t, recs)
	boundary := recs[0].IngestTS
	q := query.Query{Start: boundary, ResumeAfterTS: boundary} // no event
	got, err := collect(first(eng.Search(context.Background(), q, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(recs) {
		t.Fatalf("identity-less cursor delivered %d of %d records", len(got), len(recs))
	}
}

func first[A, B any](a A, _ B) A { return a }
