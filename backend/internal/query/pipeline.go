package query

import (
	"context"
	"errors"
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
func (e *Engine) runTimechartPipeline(ctx context.Context, q Query, ph *pipelinePhases) (*PipelineResult, error) {
	table, err := e.runTimechart(ctx, q, ph.timechartOp, ph.preOps)
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
func (e *Engine) runAggregation(ctx context.Context, records []chunk.Record, ph *pipelinePhases, q Query) (*PipelineResult, error) {
	agg, err := NewAggregator(ph.statsOp)
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

// RunPipeline executes a pipeline query against the search engine.
// Operators are split into pre-stats and post-stats phases.
// Without stats, returns records; with stats, returns a table.
// The raw operator forces all results into a flat table.
func (e *Engine) RunPipeline(ctx context.Context, q Query, pipeline *querylang.Pipeline) (*PipelineResult, error) {
	ph, err := classifyPipes(pipeline)
	if err != nil {
		return nil, err
	}

	if ph.timechartOp != nil {
		return e.runTimechartPipeline(ctx, q, ph)
	}

	// Pipeline operators control their own result limits (head, tail, slice).
	// Save the incoming limit so we can reapply it if the pipeline doesn't
	// have its own cap; then clear it so Search returns all matching records.
	origLimit := q.Limit
	q.Limit = 0

	// Head optimization: when the pipeline is just filters + head (no sort,
	// no stats), set q.Limit to avoid a full scan.
	if ph.statsOp == nil {
		if n := headOnlyLimit(ph.preOps); n > 0 {
			q.Limit = n
		}
	}

	iter, _ := e.Search(ctx, q, nil)
	records, err := applyRecordOps(ctx, iter, ph.preOps, e.lookupResolver)
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

	return e.runAggregation(ctx, records, ph, q)
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

// PipelineNeedsGlobalRecords reports whether a pipeline query must gather raw
// records from all cluster nodes before running the pipeline on the coordinator.
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

// RunPipelineOnRecords executes a pipeline query where extra records (typically
// from remote cluster nodes) are merged with the local search results before
// pipeline operators run. This enables correct head/tail/slice + stats on a
// coordinator: gather raw records globally, then apply the pipeline once.
func (e *Engine) RunPipelineOnRecords(ctx context.Context, q Query, pipeline *querylang.Pipeline, extraRecords []chunk.Record) (*PipelineResult, error) {
	ph, err := classifyPipes(pipeline)
	if err != nil {
		return nil, err
	}

	// Timechart with extra records is not supported yet (would need bucket
	// merging). Fall back to local-only for now.
	if ph.timechartOp != nil {
		return e.runTimechartPipeline(ctx, q, ph)
	}

	// Clear incoming limit — pipeline operators control their own caps.
	origLimit := q.Limit
	q.Limit = 0
	if ph.statsOp == nil {
		if n := headOnlyLimit(ph.preOps); n > 0 {
			q.Limit = n
		}
	}

	iter, _ := e.Search(ctx, q, nil)
	var records []chunk.Record
	for rec, err := range iter {
		if err != nil {
			return nil, err
		}
		records = append(records, rec.Copy())
	}

	// Merge in extra (remote) records.
	records = append(records, extraRecords...)

	// Sort merged records by IngestTS to match the expected stream order.
	reverse := q.Reverse()
	slices.SortStableFunc(records, func(a, b chunk.Record) int {
		if reverse {
			return b.IngestTS.Compare(a.IngestTS)
		}
		return a.IngestTS.Compare(b.IngestTS)
	})

	// Materialize fields and apply pre-stats ops.
	materializeFields(records)
	eval := querylang.NewEvaluator()
	for _, op := range ph.preOps {
		switch o := op.(type) {
		case *querylang.WhereOp:
			records = applyRecordWhere(records, o)
		case *querylang.DedupOp:
			records = applyRecordDedup(records, parseDedupWindow(o.Window))
		case *querylang.EvalOp:
			records, err = applyRecordEval(records, o, eval)
		case *querylang.SortOp:
			applyRecordSort(records, o)
		case *querylang.HeadOp:
			records = applyRecordHead(records, o)
		case *querylang.TailOp:
			records = applyRecordTail(records, o)
		case *querylang.SliceOp:
			records = applyRecordSlice(records, o)
		case *querylang.RenameOp:
			applyRecordRename(records, o)
		case *querylang.FieldsOp:
			applyRecordFields(records, o)
		case *querylang.LookupOp:
			applyRecordLookup(ctx, records, o, e.lookupResolver)
		}
		if err != nil {
			return nil, err
		}
	}

	if ph.statsOp == nil {
		if ph.hasRaw {
			return &PipelineResult{Table: recordsToTable(records)}, nil
		}
		if len(ph.preOps) > 0 && origLimit > 0 && !hasExplicitCap(ph.preOps) {
			if len(records) > origLimit {
				records = records[:origLimit]
			}
		}
		return &PipelineResult{Records: records}, nil
	}

	return e.runAggregation(ctx, records, ph, q)
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
