package server

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/query"
)

// queryFieldsOffTheWire names every query.Query field the forwarded
// expression does not carry, with where the peer gets it instead. A field in
// neither this set nor the round-trip assertions fails the test, so a new
// field has to be placed on the wire or explained here.
var queryFieldsOffTheWire = map[string]string{
	"Tokens":            "folded into the filter by Normalize",
	"KV":                "folded into the filter by Normalize",
	"RawExpression":     "the parsed form travels instead",
	"ContextBefore":     "context windows travel in ForwardGetContextRequest",
	"ContextAfter":      "context windows travel in ForwardGetContextRequest",
	"ResumeTS":          "set by the follow loop on the node that runs it",
	"ResumeAfterTS":     "travels in ForwardSearchRequest.ResumeToken",
	"ResumeAfterEvent":  "travels in ForwardSearchRequest.ResumeToken",
	"PartialAggregates": "travels in ForwardSearchRequest.PartialAggregates",
	"SkipCloud":         "local histogram option, never forwarded",
}

// The forwarded expression is the whole query as the peer sees it: every
// directive the coordinator set comes back from the peer's parser unchanged,
// nanoseconds included.
func TestForwardedQueryExpressionRoundTripsEveryDirective(t *testing.T) {
	t.Parallel()

	filtered, _, err := ParseExpression(`level=error host="web 1" NOT debug`)
	if err != nil {
		t.Fatalf("parse filter: %v", err)
	}
	pos := uint64(4242)
	base := time.Date(2026, 9, 4, 21, 26, 10, 123456789, time.UTC)
	q := query.Query{
		BoolExpr:    filtered.BoolExpr,
		Start:       base,
		End:         base.Add(time.Hour + time.Nanosecond),
		SourceStart: base.Add(-time.Hour + 7),
		SourceEnd:   base.Add(-time.Minute + 11),
		IsReverse:   true,
		OrderBy:     query.OrderBySourceTS,
		Limit:       137,
		Pos:         &pos,
	}

	wire := forwardedQueryExpression(q)
	got, pipeline, err := ParseExpression(wire)
	if err != nil {
		t.Fatalf("peer cannot parse %q: %v", wire, err)
	}
	if pipeline != nil {
		t.Fatalf("%q parsed with pipeline %v, want none", wire, pipeline)
	}

	checked := map[string]bool{}
	check := func(field string, ok bool) {
		t.Helper()
		checked[field] = true
		if !ok {
			want := reflect.ValueOf(q).FieldByName(field).Interface()
			have := reflect.ValueOf(got).FieldByName(field).Interface()
			t.Errorf("%s: sent %v, peer parsed %v (wire %q)", field, want, have, wire)
		}
	}
	check("BoolExpr", got.BoolExpr != nil && got.BoolExpr.String() == q.BoolExpr.String())
	check("Start", got.Start.Equal(q.Start))
	check("End", got.End.Equal(q.End))
	check("SourceStart", got.SourceStart.Equal(q.SourceStart))
	check("SourceEnd", got.SourceEnd.Equal(q.SourceEnd))
	check("IsReverse", got.IsReverse == q.IsReverse)
	check("OrderBy", got.OrderBy == q.OrderBy)
	check("Limit", got.Limit == q.Limit)
	check("Pos", got.Pos != nil && *got.Pos == *q.Pos)

	qt := reflect.TypeFor[query.Query]()
	for i := range qt.NumField() {
		f := qt.Field(i)
		_, off := queryFieldsOffTheWire[f.Name]
		switch {
		case checked[f.Name] && off:
			t.Errorf("query.Query.%s is both round-tripped and listed as off the wire", f.Name)
		case !checked[f.Name] && !off:
			t.Errorf("query.Query.%s is neither carried by the forwarded expression nor listed in queryFieldsOffTheWire", f.Name)
		}
	}
}

// An unset directive stays unset on the peer: a zero bound is no bound, an
// absent limit is no limit and an absent position addresses every record.
func TestForwardedQueryExpressionLeavesUnsetDirectivesUnset(t *testing.T) {
	t.Parallel()

	for _, q := range []query.Query{
		{},
		{Pos: new(uint64)},
		{Limit: 1},
		{OrderBy: query.OrderBySourceTS},
	} {
		wire := forwardedQueryExpression(q)
		got, _, err := ParseExpression(wire)
		if err != nil {
			t.Fatalf("peer cannot parse %q: %v", wire, err)
		}
		if !reflect.DeepEqual(got, q) {
			t.Errorf("wire %q parsed to %#v, want %#v", wire, got, q)
		}
	}
}

// A legacy token/KV query reaches the peer as the filter it means, not as an
// unfiltered scan.
func TestForwardedQueryExpressionCarriesLegacyFilters(t *testing.T) {
	t.Parallel()

	q := query.Query{
		Tokens: []string{"timeout"},
		KV:     []query.KeyValueFilter{{Key: "host", Value: "web-1"}, {Key: "level", AnyValue: true}},
	}
	wire := forwardedQueryExpression(q)
	got, _, err := ParseExpression(wire)
	if err != nil {
		t.Fatalf("peer cannot parse %q: %v", wire, err)
	}
	want := q.Normalize().BoolExpr.String()
	if got.BoolExpr == nil || got.BoolExpr.String() != want {
		t.Fatalf("wire %q carries filter %v, want %s", wire, got.BoolExpr, want)
	}
}

// streamRecordingSearcher captures every expression the coordinator opens a
// record stream with.
type streamRecordingSearcher struct {
	stubQuerySearcher
	mu   sync.Mutex
	sent []string
}

func (r *streamRecordingSearcher) SearchStream(ctx context.Context, nodeID string, req *apiv1.ForwardSearchRequest) (
	<-chan []*apiv1.ExportRecord, *apiv1.TableResult, <-chan error, func() []byte, func() []*apiv1.HistogramBucket,
) {
	r.mu.Lock()
	r.sent = append(r.sent, req.GetQuery())
	r.mu.Unlock()
	return r.stubQuerySearcher.SearchStream(ctx, nodeID, req)
}

// A pipeline the coordinator runs over gathered records asks each peer for
// the scan the pipeline consumes. The client's page size caps the pipeline's
// output, so it never reaches a peer whose records the pipeline aggregates,
// sorts or windows; a head reached through record-preserving operators asks
// for exactly its N.
func TestCollectRemoteForPipelineForwardsTheScanLimit(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		expr      string
		wantLimit int
	}{
		{"| stats dcount(host)", 0},
		{"| stats median(latency)", 0},
		{"| tail 5", 0},
		{"| sort -seq | head 3", 0},
		{"| sort -seq", 0},
		{"| dedup | head 400", 0},
		{`| where level="error" | head 20 | stats count`, 0},
		{"| head 350 | stats count", 0},
		{"| head 50 | timechart 20", 0},
		{"| eval x=1 | fields x | head 400", 400},
		{"| head 7 | sort -seq", 7},
	} {
		t.Run(tc.expr, func(t *testing.T) {
			t.Parallel()
			searcher := &streamRecordingSearcher{}
			qs, _ := newQueryServerWithRemoteVaults(t, searcher, []glid.GLID{glid.New()})
			q, pipeline, err := ParseExpression(tc.expr)
			if err != nil || pipeline == nil {
				t.Fatalf("parse %q: pipeline=%v err=%v", tc.expr, pipeline, err)
			}
			q.Limit = 100
			it, _, _ := qs.collectRemoteForPipeline(context.Background(), q, pipeline)
			for range it {
			}

			searcher.mu.Lock()
			sent := searcher.sent
			searcher.mu.Unlock()
			if len(sent) != 1 {
				t.Fatalf("opened %d record streams, want 1", len(sent))
			}
			peer, _, err := ParseExpression(sent[0])
			if err != nil {
				t.Fatalf("peer cannot parse %q: %v", sent[0], err)
			}
			if peer.Limit != tc.wantLimit {
				t.Errorf("peer scans %q under limit %d, want %d", sent[0], peer.Limit, tc.wantLimit)
			}
		})
	}
}

// A plain paged search is the one fan-out whose peers scan under the page
// size: the coordinator returns at most one page, so no peer needs more.
func TestCollectRemoteForwardsThePageSizeOfAPlainSearch(t *testing.T) {
	t.Parallel()

	searcher := &streamRecordingSearcher{}
	qs, _ := newQueryServerWithRemoteVaults(t, searcher, []glid.GLID{glid.New()})
	q, _, err := ParseExpression("level=error source_start=2026-09-04T20:00:00Z order=source_ts")
	if err != nil {
		t.Fatal(err)
	}
	q.Limit = 100
	it, _, _ := qs.collectRemote(context.Background(), q)
	for range it {
	}
	searcher.mu.Lock()
	sent := searcher.sent
	searcher.mu.Unlock()
	if len(sent) != 1 {
		t.Fatalf("opened %d record streams, want 1", len(sent))
	}
	peer, _, err := ParseExpression(sent[0])
	if err != nil {
		t.Fatalf("peer cannot parse %q: %v", sent[0], err)
	}
	if peer.Limit != 100 || peer.SourceStart.IsZero() || peer.OrderBy != query.OrderBySourceTS {
		t.Errorf("peer parsed %q as limit=%d source_start=%v order=%v; want the page size, the source bound and the ordering", sent[0], peer.Limit, peer.SourceStart, peer.OrderBy)
	}
}
