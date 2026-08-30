package query

import (
	"context"
	"errors"
	"iter"
	"maps"
	"slices"
	"strings"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/querylang"
)

// PipelineResult holds the result of a pipeline execution.
// Exactly one of Table or Records will be set.
type PipelineResult struct {
	Table   *TableResult   // aggregating pipelines (has stats) or raw mode
	Records []chunk.Record // non-aggregating pipelines (without raw)
}

// CompileFilter creates a record filter function from a boolean expression.
// The DNF conversion is done once; the returned function can be called per-record.
func CompileFilter(expr querylang.Expr) func(chunk.Record) bool {
	if expr == nil {
		return func(chunk.Record) bool { return true }
	}
	dnf := querylang.ToDNF(expr)
	return dnfFilter(&dnf)
}

// pipelinePhases holds the result of classifying a pipeline's operators.
type pipelinePhases struct {
	preOps      []querylang.PipeOp
	postOps     []querylang.PipeOp
	statsOp     *querylang.StatsOp
	timechartOp *querylang.TimechartOp
	hasRaw      bool
	vizOp       querylang.PipeOp // explicit visualization operator (barchart, donut, map)
}

// classifyPipes splits pipeline operators into pre-stats and post-stats phases,
// and extracts the stats/timechart/raw flags.
func classifyPipes(pipeline *querylang.Pipeline) (*pipelinePhases, error) {
	p := &pipelinePhases{}
	for _, pipe := range pipeline.Pipes {
		switch op := pipe.(type) {
		case *querylang.StatsOp:
			if p.statsOp != nil {
				return nil, errors.New("pipeline can contain at most one stats operator")
			}
			if p.timechartOp != nil {
				return nil, errors.New("pipeline cannot contain both timechart and stats")
			}
			p.statsOp = op
		case *querylang.TimechartOp:
			if p.timechartOp != nil {
				return nil, errors.New("pipeline can contain at most one timechart operator")
			}
			if p.statsOp != nil {
				return nil, errors.New("pipeline cannot contain both timechart and stats")
			}
			p.timechartOp = op
		case *querylang.RawOp:
			p.hasRaw = true
		case *querylang.LinechartOp, *querylang.BarchartOp, *querylang.DonutOp, *querylang.HeatmapOp, *querylang.ScatterOp, *querylang.MapOp:
			if p.vizOp != nil {
				return nil, errors.New("pipeline can contain at most one visualization operator")
			}
			p.vizOp = pipe
		default:
			if p.statsOp == nil && p.timechartOp == nil {
				p.preOps = append(p.preOps, pipe)
			} else {
				p.postOps = append(p.postOps, pipe)
			}
		}
	}
	return p, nil
}

// runTimechartPipeline handles the timechart fast path.
func (e *Engine) runTimechartPipeline(ctx context.Context, q Query, ph *pipelinePhases, budget *Budget) (*PipelineResult, error) {
	table, err := e.runTimechart(ctx, q, ph.timechartOp, ph.preOps, budget)
	if err != nil {
		return nil, err
	}
	table, err = applyTableOps(ctx, table, ph.postOps, e.lookupResolver)
	if err != nil {
		return nil, err
	}
	return &PipelineResult{Table: table}, nil
}

// runAggregation feeds records into a stats aggregator and returns a table.
func (e *Engine) runAggregation(ctx context.Context, records []chunk.Record, ph *pipelinePhases, q Query, budget *Budget) (*PipelineResult, error) {
	agg, err := NewAggregator(ph.statsOp, budget)
	if err != nil {
		return nil, err
	}
	for _, rec := range records {
		if err := agg.Add(rec); err != nil {
			return nil, err
		}
	}
	table := agg.Result(q.Start, q.End)
	table, err = applyTableOps(ctx, table, ph.postOps, e.lookupResolver)
	if err != nil {
		return nil, err
	}
	return &PipelineResult{Table: table}, nil
}

// runStreamingAggregation applies streamable pre-stats operators per-record
// and feeds survivors directly into the aggregator, so the search result is
// never materialized. Only aggregator group state is held in memory.
func (e *Engine) runStreamingAggregation(ctx context.Context, it iter.Seq2[chunk.Record, error], ph *pipelinePhases, q Query, budget *Budget) (*PipelineResult, error) {
	agg, err := NewAggregator(ph.statsOp, budget)
	if err != nil {
		return nil, err
	}
	sf := newStreamFilter(ctx, ph.preOps, e.lookupResolver, budget)
	for rec, recErr := range it {
		if recErr != nil {
			return nil, recErr
		}
		rec = rec.Copy()
		materializeRecord(&rec)

		keep, applyErr := sf.apply(&rec)
		if applyErr != nil {
			return nil, applyErr
		}
		if keep {
			if err := agg.Add(rec); err != nil {
				return nil, err
			}
		}
		if sf.exhausted {
			break
		}
	}
	table := agg.Result(q.Start, q.End)
	table, err = applyTableOps(ctx, table, ph.postOps, e.lookupResolver)
	if err != nil {
		return nil, err
	}
	return &PipelineResult{Table: table}, nil
}

// RunPipeline executes a pipeline query against the search engine.
// Operators are split into pre-stats and post-stats phases.
// Without stats, returns records; with stats, returns a table.
// The raw operator forces all results into a flat table.
func (e *Engine) RunPipeline(ctx context.Context, q Query, pipeline *querylang.Pipeline) (*PipelineResult, error) {
	// Every materializing path charges what it retains against this budget, so
	// a pipeline whose working set outgrows the node fails with a named limit
	// instead of allocating until the process dies.
	return e.runPipeline(ctx, q, pipeline, nil, e.newBudget())
}

// RunPipelineWithRemote executes a pipeline over the union of this node's
// search results and a stream of records from the rest of the cluster. The
// remote stream is merged in query order and flows through the same operators
// as the local one, so a cap or an aggregate sees every cluster record exactly
// once and in order while the coordinator retains only what the pipeline
// itself holds — a window, a top-N working set, or accumulator state.
//
// remote must already yield records in q's order; the merge assumes it and
// does not re-sort. A nil remote runs the pipeline against local data alone.
//
// The caller supplies the budget so its own share of the query and the
// pipeline's stay on one ledger instead of two that each allow the full
// ceiling.
func (e *Engine) RunPipelineWithRemote(ctx context.Context, q Query, pipeline *querylang.Pipeline, remote iter.Seq2[chunk.Record, error], budget *Budget) (*PipelineResult, error) {
	return e.runPipeline(ctx, q, pipeline, remote, budget)
}

func (e *Engine) runPipeline(ctx context.Context, q Query, pipeline *querylang.Pipeline, remote iter.Seq2[chunk.Record, error], budget *Budget) (*PipelineResult, error) {
	ph, err := classifyPipes(pipeline)
	if err != nil {
		return nil, err
	}

	if ph.timechartOp != nil {
		// KNOWN GAP: a timechart bins from this node's chunk metadata and
		// index positions, not from a record stream, so remote is dropped
		// here. A capped timechart ("| head 6 | timechart 3") routes to this
		// line on a coordinator and answers from local data alone, with no
		// Truncated flag to say so. Merging remote buckets needs a bucket
		// path that a record stream can feed.
		return e.runTimechartPipeline(ctx, q, ph, budget)
	}

	// Pipeline operators control their own result limits (head, tail, slice).
	// A limit applied to the scan would cut the aggregator's or the sort's
	// input, not its output: "| stats count" over a limited scan counts the
	// first N records and reports a wrong total, and "| sort" would order an
	// arbitrary prefix. Save the incoming limit so we can reapply it to the
	// final records if the pipeline has no cap of its own; then clear it so
	// Search returns all matching records. What bounds the work is the memory
	// budget, which bounds retained bytes without changing the answer.
	origLimit := q.Limit
	q.Limit = 0

	// Head optimization: when the pipeline is just filters + head (no sort,
	// no stats), set q.Limit to avoid a full scan.
	if ph.statsOp == nil {
		if n := headOnlyLimit(ph.preOps); n > 0 {
			q.Limit = n
		}
	}

	it, _ := e.Search(ctx, q, nil)
	if remote != nil {
		it = mergeOrdered(it, remote, q.OrderBy, q.Reverse())
	}

	// Aggregating pipeline with streamable pre-ops: feed records straight
	// into the aggregator instead of materializing the search result.
	if ph.statsOp != nil && opsStreamable(ph.preOps) {
		return e.runStreamingAggregation(ctx, it, ph, q, budget)
	}

	// When the pipeline has no explicit cap, RunPipeline truncates the final
	// record output to origLimit (below). Passing that limit down lets
	// applyRecordOps bound collection instead of materializing everything.
	implicitLimit := 0
	if ph.statsOp == nil && !ph.hasRaw && len(ph.preOps) > 0 && origLimit > 0 && !hasExplicitCap(ph.preOps) {
		implicitLimit = origLimit
	}

	records, err := applyRecordOpsLimit(ctx, it, ph.preOps, e.lookupResolver, implicitLimit, budget)
	if err != nil {
		return nil, err
	}

	if ph.statsOp == nil {
		// Explicit "raw" forces table output.
		if ph.hasRaw {
			return &PipelineResult{Table: recordsToTable(records)}, nil
		}
		// Pipeline with operators but no visualizer: return records for
		// the log viewer.  Reapply the original limit if the pipeline
		// didn't already cap results via head/tail/slice.
		if len(ph.preOps) > 0 && origLimit > 0 && !hasExplicitCap(ph.preOps) {
			if len(records) > origLimit {
				records = records[:origLimit]
			}
		}
		return &PipelineResult{Records: records}, nil
	}

	return e.runAggregation(ctx, records, ph, q, budget)
}

// hasExplicitCap returns true if the pipeline contains a head, tail, or slice
// operator that already limits the number of output records.
func hasExplicitCap(ops []querylang.PipeOp) bool {
	for _, op := range ops {
		switch op.(type) {
		case *querylang.HeadOp, *querylang.TailOp, *querylang.SliceOp:
			return true
		}
	}
	return false
}

// headOnlyLimit returns the head N limit if the pipeline consists only of
// where/eval/rename/fields operators followed by a head (no sort).
// Returns 0 if head optimization cannot be applied.
func headOnlyLimit(ops []querylang.PipeOp) int {
	var headN int
	for _, op := range ops {
		switch o := op.(type) {
		case *querylang.HeadOp:
			headN = o.N
		case *querylang.SortOp, *querylang.TailOp, *querylang.SliceOp:
			return 0 // sort, tail, and slice require all records
		case *querylang.WhereOp, *querylang.EvalOp, *querylang.RenameOp, *querylang.FieldsOp, *querylang.LookupOp, *querylang.DedupOp:
			// these are fine
		default:
			return 0
		}
	}
	return headN
}

// PipelineNeedsGlobalRecords reports whether a pipeline query must see every
// cluster node's raw records on the coordinator, rather than merging the
// per-node results of running the pipeline on each of them.
// This is true when:
//   - The pipeline contains a non-distributive ordering operator (tail, sort,
//     slice) that requires all records to produce a correct result, OR
//   - A cap operator (head, tail, slice) appears before an aggregation, OR
//   - The pipeline contains a non-distributive aggregation function (avg,
//     dcount, median, first, last, values) that cannot be correctly merged
//     from per-node results.
func PipelineNeedsGlobalRecords(pipeline *querylang.Pipeline) bool {
	ph, err := classifyPipes(pipeline)
	if err != nil {
		return false
	}
	// Bare tail/sort/slice (no aggregation) still needs all records from all
	// nodes — running them on a single node's data is incorrect.
	if needsAllRecords(ph.preOps) {
		return true
	}
	if ph.statsOp == nil && ph.timechartOp == nil {
		return false
	}
	if hasExplicitCap(ph.preOps) {
		return true
	}
	return hasNonDistributiveAgg(ph.statsOp)
}

// needsAllRecords returns true if the pipeline contains operators that require
// the full record set to produce correct results (tail, sort, slice).
// Head is excluded because it can short-circuit after N records.
func needsAllRecords(ops []querylang.PipeOp) bool {
	for _, op := range ops {
		switch op.(type) {
		case *querylang.TailOp, *querylang.SortOp, *querylang.SliceOp:
			return true
		}
	}
	return false
}

// hasNonDistributiveAgg returns true if the StatsOp contains aggregate functions
// that cannot be correctly merged from independent per-node results.
// Distributive: count, sum, min, max (can be merged by summing/min/max).
// Non-distributive: avg, dcount, median, first, last, values.
func hasNonDistributiveAgg(op *querylang.StatsOp) bool {
	if op == nil {
		return false
	}
	for _, agg := range op.Aggs {
		switch strings.ToLower(agg.Func) {
		case "avg", "dcount", "median", "first", "last", "values":
			return true
		}
	}
	return false
}

// recordsToTable converts a slice of records into a flat TableResult.
// Columns are: write_ts, ingest_ts, source_ts, then all field keys
// (extracted KV/JSON + attributes, sorted), then raw.
func recordsToTable(records []chunk.Record) *TableResult {
	// Materialize extracted fields so they appear as columns.
	materializeFields(records)

	// Collect all unique attribute keys.
	keySet := make(map[string]struct{})
	for _, rec := range records {
		for k := range rec.Attrs {
			keySet[k] = struct{}{}
		}
	}
	attrKeys := slices.Sorted(maps.Keys(keySet))

	// Build column list: timestamps, attrs, raw.
	columns := make([]string, 0, 3+len(attrKeys)+1)
	columns = append(columns, "write_ts", "ingest_ts", "source_ts")
	columns = append(columns, attrKeys...)
	columns = append(columns, "raw")

	rows := make([][]string, len(records))
	for i, rec := range records {
		row := make([]string, len(columns))
		row[0] = rec.WriteTS.Format(time.RFC3339Nano)
		row[1] = rec.IngestTS.Format(time.RFC3339Nano)
		if !rec.SourceTS.IsZero() {
			row[2] = rec.SourceTS.Format(time.RFC3339Nano)
		}
		for j, k := range attrKeys {
			row[3+j] = rec.Attrs[k]
		}
		row[len(columns)-1] = string(rec.Raw)
		rows[i] = row
	}

	return &TableResult{Columns: columns, Rows: rows}
}
