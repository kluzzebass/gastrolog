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
