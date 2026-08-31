package query

import (
	"context"
	"fmt"
	"iter"
	"slices"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	indexmem "gastrolog/internal/index/memory"
	memattr "gastrolog/internal/index/memory/attr"
	memjson "gastrolog/internal/index/memory/json"
	memkv "gastrolog/internal/index/memory/kv"
	memtoken "gastrolog/internal/index/memory/token"
	"gastrolog/internal/querylang"
)

// clusterHalves builds a vault holding the even-indexed records of a run of
// 2n records and returns an engine over it plus a stream carrying the
// odd-indexed ones — the shape a coordinator sees when half the match set
// lives on other nodes. Timestamps alternate strictly between the two, so the
// merged stream interleaves them one for one.
//
// Each record carries a unique "id", a "host" drawn from a small set, and a
// "latency" that makes numeric aggregates meaningful.
func clusterHalves(t *testing.T, n int) (*Engine, iter.Seq2[chunk.Record, error]) {
	t.Helper()
	cm, err := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(100_000),
	})
	if err != nil {
		t.Fatalf("chunk manager: %v", err)
	}

	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	ingesterID := glid.New()
	record := func(i int) chunk.Record {
		ts := t0.Add(time.Duration(i) * time.Second)
		return chunk.Record{
			WriteTS:  ts,
			IngestTS: ts,
			EventID:  chunk.EventID{IngesterID: ingesterID, IngestTS: ts, IngestSeq: uint32(i)}, //nolint:gosec // G115: loop index is small and non-negative
			Attrs: chunk.Attributes{
				"id":      fmt.Sprintf("request-%09d", i),
				"host":    fmt.Sprintf("host-%d", i%4),
				"latency": fmt.Sprintf("%d", i),
			},
			Raw: fmt.Appendf(nil, "rec-%09d handled", i),
		}
	}

	var remote []chunk.Record
	for i := range 2 * n {
		if i%2 == 0 {
			cm.Append(record(i))
			continue
		}
		remote = append(remote, record(i))
	}
	cm.Seal()

	tokIdx := memtoken.NewIndexer(cm)
	attrIdx := memattr.NewIndexer(cm)
	kvIdx := memkv.NewIndexer(cm)
	jsonIdx := memjson.NewIndexer(cm)
	im := indexmem.NewManagerWithJSON(
		[]index.Indexer{tokIdx, attrIdx, kvIdx, jsonIdx}, tokIdx, attrIdx, kvIdx, jsonIdx, nil)
	metas, err := cm.List()
	if err != nil {
		t.Fatalf("list chunks: %v", err)
	}
	for _, m := range metas {
		if !m.Sealed {
			continue
		}
		if err := im.BuildIndexes(context.Background(), m.ID); err != nil {
			t.Fatalf("build indexes: %v", err)
		}
	}

	return New(cm, im, nil), func(yield func(chunk.Record, error) bool) {
		for _, rec := range remote {
			if !yield(rec, nil) {
				return
			}
		}
	}
}

// runClusterQuery runs expr over the merged local + remote stream on a budget
// the caller can inspect afterwards.
func runClusterQuery(t *testing.T, eng *Engine, remote iter.Seq2[chunk.Record, error], expr string, budget *Budget) *PipelineResult {
	t.Helper()
	pipeline, err := querylang.ParsePipeline(expr)
	if err != nil {
		t.Fatalf("parse %q: %v", expr, err)
	}
	result, err := eng.RunPipelineWithRemote(context.Background(), Query{BoolExpr: pipeline.Filter}, pipeline, remote, budget)
	if err != nil {
		t.Fatalf("run %q: %v", expr, err)
	}
	return result
}

// What a fan-out query retains on the coordinator must track what the pipeline
// answers with, not how many records it had to look at. Each of these shapes
// runs over 40 000 cluster records — several megabytes of record payload — on
// a budget far too small to hold them, and each retains only its own working
// set: accumulator state, a cap window, or a top-N sort buffer.
func TestClusterPipelineRetainsWorkingSetNotMatchSet(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a 40k-record cluster match set")
	}
	const perSide = 20_000

	tests := []struct {
		name string
		expr string
		// ceiling on the bytes still charged when the query finishes, chosen
		// to be a small multiple of the working set the operator names and
		// orders of magnitude below the match set.
		retained int64
	}{
		{"count by host", "| stats count by host", 4 << 10},
		{"distinct hosts", "| stats dcount(host)", 4 << 10},
		{"median latency", "| stats median(latency)", 512 << 10},
		{"values of host", "| stats values(host)", 4 << 10},
		{"tail window", "| tail 100 | stats count", 128 << 10},
		{"slice window", "| slice 1 100 | stats count", 128 << 10},
		{"sorted page", "| sort latency | head 100", 256 << 10},
	}
	const limit = 8 << 20

	// Premise: 8 MiB cannot hold this match set. Without it every case below
	// would pass against a pipeline that collected every record, and the test
	// would prove nothing.
	t.Run("premise: the match set does not fit", func(t *testing.T) {
		eng, remote := clusterHalves(t, perSide)
		pipeline, err := querylang.ParsePipeline("| sort latency")
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		budget := &Budget{limit: limit}
		if _, err := eng.RunPipelineWithRemote(context.Background(), Query{}, pipeline, remote, budget); err == nil {
			t.Fatalf("%d MiB held the whole match set, so the budgets below bound nothing", limit>>20)
		}
	})

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			eng, remote := clusterHalves(t, perSide)
			budget := &Budget{limit: limit}
			runClusterQuery(t, eng, remote, tc.expr, budget)
			if budget.used > tc.retained {
				t.Errorf("%s still holds %d bytes after answering; a working set of this shape should stay under %d",
					tc.expr, budget.used, tc.retained)
			}
		})
	}
}

// An uncapped sort is the one pipeline shape that genuinely has to hold every
// record: nothing can be emitted until the last one has been compared. It
// keeps materializing, and the budget is what bounds it — so the same query
// that streams fine with a cap must be refused when the match set does not
// fit.
func TestClusterUncappedSortStaysBoundedByTheBudget(t *testing.T) {
	eng, remote := clusterHalves(t, 5_000)
	budget := &Budget{limit: 64 << 10}

	pipeline, err := querylang.ParsePipeline("| sort latency")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = eng.RunPipelineWithRemote(context.Background(), Query{}, pipeline, remote, budget)
	assertMemoryLimit(t, err, consumerRecordBuffer, 64<<10)
}

// A cap must select from the whole cluster stream, not from either side of it.
// With local and remote records alternating, a head that ran per-source would
// return a run of one prefix instead of the interleaving.
func TestClusterCapSelectsAcrossSources(t *testing.T) {
	eng, remote := clusterHalves(t, 10)

	raws := func(result *PipelineResult) []string {
		var out []string
		for _, rec := range result.Records {
			out = append(out, string(rec.Raw))
		}
		return out
	}
	want := func(indices ...int) []string {
		out := make([]string, len(indices))
		for i, n := range indices {
			out[i] = fmt.Sprintf("rec-%09d handled", n)
		}
		return out
	}

	tests := []struct {
		expr string
		want []string
	}{
		{"| head 5", want(0, 1, 2, 3, 4)},
		{"| tail 5", want(15, 16, 17, 18, 19)},
		{"| slice 3 7", want(2, 3, 4, 5, 6)},
	}
	for _, tc := range tests {
		t.Run(tc.expr, func(t *testing.T) {
			got := raws(runClusterQuery(t, eng, remote, tc.expr, NewBudget()))
			if !slices.Equal(got, tc.want) {
				t.Errorf("%s = %v, want %v", tc.expr, got, tc.want)
			}
		})
	}
}

// The merge is what puts the two sources in one order; every ordering-sensitive
// operator downstream depends on it. Reverse queries walk both sources
// newest-first and must interleave the same way.
func TestMergeOrderedInterleavesBothDirections(t *testing.T) {
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	seq := func(offsets ...int) iter.Seq2[chunk.Record, error] {
		return func(yield func(chunk.Record, error) bool) {
			for _, off := range offsets {
				ts := t0.Add(time.Duration(off) * time.Second)
				rec := chunk.Record{IngestTS: ts, WriteTS: ts, Raw: fmt.Appendf(nil, "%d", off)}
				if !yield(rec, nil) {
					return
				}
			}
		}
	}
	collect := func(it iter.Seq2[chunk.Record, error]) string {
		var parts []string
		for rec, err := range it {
			if err != nil {
				parts = append(parts, "err")
				continue
			}
			parts = append(parts, string(rec.Raw))
		}
		return strings.Join(parts, ",")
	}

	forward := collect(mergeOrdered(seq(0, 2, 4), seq(1, 3, 5), OrderByIngestTS, false))
	if forward != "0,1,2,3,4,5" {
		t.Errorf("forward merge = %q, want %q", forward, "0,1,2,3,4,5")
	}

	reverse := collect(mergeOrdered(seq(5, 3, 1), seq(4, 2, 0), OrderByIngestTS, true))
	if reverse != "5,4,3,2,1,0" {
		t.Errorf("reverse merge = %q, want %q", reverse, "5,4,3,2,1,0")
	}

	// An exhausted side must not stall the other.
	drained := collect(mergeOrdered(seq(0, 1, 2), seq(), OrderByIngestTS, false))
	if drained != "0,1,2" {
		t.Errorf("merge with an empty side = %q, want %q", drained, "0,1,2")
	}
}

// A remote stream that fails mid-flight must fail the query rather than
// silently answering from the records that did arrive: a partial aggregate is
// a wrong number presented as an authoritative one.
func TestClusterStreamErrorFailsTheQuery(t *testing.T) {
	eng, remote := clusterHalves(t, 10)

	// The stream delivers real records and only then fails, which is the case
	// that matters: an error on the first pull leaves nothing to answer from,
	// but a mid-flight failure leaves a plausible partial set behind.
	truncated := func(yield func(chunk.Record, error) bool) {
		delivered := 0
		for rec, err := range remote {
			if err != nil || delivered == 5 {
				break
			}
			if !yield(rec, nil) {
				return
			}
			delivered++
		}
		if delivered != 5 {
			t.Errorf("premise: stream failed after %d records, want 5", delivered)
		}
		yield(chunk.Record{}, fmt.Errorf("remote vault unreachable"))
	}

	pipeline, err := querylang.ParsePipeline("| tail 5 | stats count")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = eng.RunPipelineWithRemote(context.Background(), Query{}, pipeline, truncated, NewBudget())
	if err == nil {
		t.Fatal("a remote stream that failed mid-flight must fail the pipeline, not answer from the records that arrived")
	}
	if !strings.Contains(err.Error(), "unreachable") {
		t.Errorf("error %q does not carry the remote failure", err)
	}
}

// tieGroup builds n records that all share one ordering timestamp under
// orderBy but carry distinct event identities, so only EventID separates them.
// EventID's leading field is IngestTS, so ascending seq gives ascending
// canonical rank in both orderings.
func tieGroup(orderBy OrderBy, n int) []chunk.Record {
	base := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	ingesterID := glid.New()
	out := make([]chunk.Record, n)
	for i := range n {
		// Distinct IngestTS gives each record a distinct canonical rank; under
		// order=source_ts every record still shares the one SourceTS, which is
		// what the merge compares first.
		ingest := base.Add(time.Duration(i) * time.Millisecond)
		rec := chunk.Record{
			IngestTS: ingest,
			WriteTS:  ingest,
			SourceTS: base,
			EventID:  chunk.EventID{IngesterID: ingesterID, IngestTS: ingest, IngestSeq: uint32(i)}, //nolint:gosec // G115: loop index is small and non-negative
			Raw:      fmt.Appendf(nil, "evt-%02d", i),
		}
		if orderBy == OrderByIngestTS {
			// A tie under ingest_ts too: one shared IngestTS, ranks then
			// separated by IngestSeq alone.
			rec.IngestTS = base
			rec.WriteTS = base
			rec.EventID.IngestTS = base
		}
		out[i] = rec
	}
	return out
}

func recordSeqOf(records []chunk.Record) iter.Seq2[chunk.Record, error] {
	return func(yield func(chunk.Record, error) bool) {
		for _, rec := range records {
			if !yield(rec, nil) {
				return
			}
		}
	}
}

func rawsFromIter(it iter.Seq2[chunk.Record, error]) []string {
	var out []string
	for rec, err := range it {
		if err != nil {
			out = append(out, "err")
			continue
		}
		out = append(out, string(rec.Raw))
	}
	return out
}

// A timestamp tie must not be resolved by which stream a record arrived on.
// "Local" is whichever vaults live on the node that received the query, so a
// side-dependent tie-break makes the same query return a different window
// from a different node — and under order=source_ts, where a whole second of
// syslog shares one timestamp, a head/tail cutoff lands inside a tie group as
// a matter of course rather than as an edge case.
func TestCanonicalOrderIgnoresWhichStreamCarriesTheRecord(t *testing.T) {
	const n = 8

	// Every way of splitting the same tie group across two streams. Each
	// partition still feeds both streams in canonical order, which is all
	// mergeOrdered requires of its inputs.
	partitions := map[string]func(i int) bool{
		"all on a":         func(int) bool { return true },
		"all on b":         func(int) bool { return false },
		"alternating":      func(i int) bool { return i%2 == 0 },
		"alternating swap": func(i int) bool { return i%2 == 1 },
		"front half on a":  func(i int) bool { return i < n/2 },
		"back half on a":   func(i int) bool { return i >= n/2 },
	}

	for _, orderBy := range []OrderBy{OrderByIngestTS, OrderBySourceTS} {
		t.Run(orderBy.String(), func(t *testing.T) {
			group := tieGroup(orderBy, n)

			// Canonical order is ascending seq by construction.
			var canonical []string
			for _, rec := range group {
				canonical = append(canonical, string(rec.Raw))
			}
			reversed := slices.Clone(canonical)
			slices.Reverse(reversed)

			for name, onA := range partitions {
				t.Run(name, func(t *testing.T) {
					var a, b []chunk.Record
					for i, rec := range group {
						if onA(i) {
							a = append(a, rec)
						} else {
							b = append(b, rec)
						}
					}

					got := rawsFromIter(mergeOrdered(recordSeqOf(a), recordSeqOf(b), orderBy, false))
					if !slices.Equal(got, canonical) {
						t.Errorf("forward merge = %v, want %v (the split across streams changed the order)", got, canonical)
					}

					// Reverse negates the whole comparison, so reverse
					// iteration must yield the exact reverse sequence.
					ra, rb := slices.Clone(a), slices.Clone(b)
					slices.Reverse(ra)
					slices.Reverse(rb)
					gotRev := rawsFromIter(mergeOrdered(recordSeqOf(ra), recordSeqOf(rb), orderBy, true))
					if !slices.Equal(gotRev, reversed) {
						t.Errorf("reverse merge = %v, want %v", gotRev, reversed)
					}
				})
			}
		})
	}
}

// A cap whose cutoff falls inside a tie group must take the same records
// wherever those records happen to live. Under order=source_ts the whole
// group shares one timestamp, so every record in it is a candidate and only
// the canonical order decides.
func TestCapInsideATieGroupIsStreamIndependent(t *testing.T) {
	const n = 8
	group := tieGroup(OrderBySourceTS, n)

	head := func(a, b []chunk.Record, keep int) []string {
		var out []string
		for rec, err := range mergeOrdered(recordSeqOf(a), recordSeqOf(b), OrderBySourceTS, false) {
			if err != nil {
				t.Fatalf("merge: %v", err)
			}
			out = append(out, string(rec.Raw))
			if len(out) == keep {
				break
			}
		}
		return out
	}

	// Cutoff at 3 sits inside the group in every split below.
	want := []string{"evt-00", "evt-01", "evt-02"}
	splits := [][2][]chunk.Record{
		{group[:4], group[4:]},
		{group[4:], group[:4]},
		{group, nil},
		{nil, group},
		{[]chunk.Record{group[0], group[2], group[4], group[6]}, []chunk.Record{group[1], group[3], group[5], group[7]}},
		{[]chunk.Record{group[1], group[3], group[5], group[7]}, []chunk.Record{group[0], group[2], group[4], group[6]}},
	}
	for i, split := range splits {
		if got := head(split[0], split[1], 3); !slices.Equal(got, want) {
			t.Errorf("split %d: head 3 = %v, want %v", i, got, want)
		}
	}
}

// Breaking out of the merged range early must release both sources. An
// unbounded generator that runs its deferred cleanup can only have been
// stopped — nothing else ends it — so the flag is proof of teardown rather
// than of natural exhaustion.
func TestMergeOrderedStopsBothSourcesOnEarlyBreak(t *testing.T) {
	base := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	endless := func(offset int, released *bool, delivered *int) iter.Seq2[chunk.Record, error] {
		return func(yield func(chunk.Record, error) bool) {
			defer func() { *released = true }()
			for i := 0; ; i++ {
				ts := base.Add(time.Duration(2*i+offset) * time.Second)
				if !yield(chunk.Record{IngestTS: ts, WriteTS: ts}, nil) {
					return
				}
				*delivered++
			}
		}
	}

	var aReleased, bReleased bool
	var aDelivered, bDelivered int
	merged := mergeOrdered(
		endless(0, &aReleased, &aDelivered),
		endless(1, &bReleased, &bDelivered),
		OrderByIngestTS, false)

	seen := 0
	for range merged {
		seen++
		if seen == 5 {
			break
		}
	}

	if seen != 5 {
		t.Fatalf("consumed %d records, want 5", seen)
	}
	if !aReleased || !bReleased {
		t.Errorf("early break left a source running (a released=%v, b released=%v)", aReleased, bReleased)
	}
	// Both generators are infinite, so a bounded delivery count is what says
	// the break actually cut them short.
	if aDelivered > seen || bDelivered > seen {
		t.Errorf("sources kept producing past the break (a=%d, b=%d, consumed=%d)", aDelivered, bDelivered, seen)
	}
}
