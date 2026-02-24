package query

import (
	"context"
	"errors"
	"maps"
	"slices"
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
	table, err = applyTableOps(table, ph.postOps)
	if err != nil {
		return nil, err
	}
	return &PipelineResult{Table: table}, nil
}

// runAggregation feeds records into a stats aggregator and returns a table.
func (e *Engine) runAggregation(records []chunk.Record, ph *pipelinePhases, q Query) (*PipelineResult, error) {
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
	table, err = applyTableOps(table, ph.postOps)
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
	// Clear any incoming limit (e.g. proto-level pagination limit) so Search
	// returns all matching records for the pipeline to process.
	q.Limit = 0

	// Head optimization: when the pipeline is just filters + head (no sort,
	// no stats), set q.Limit to avoid a full scan.
	if ph.statsOp == nil {
		if n := headOnlyLimit(ph.preOps); n > 0 {
			q.Limit = n
		}
	}

	iter, _ := e.Search(ctx, q, nil)
	records, err := applyRecordOps(iter, ph.preOps)
	if err != nil {
		return nil, err
	}

	if ph.statsOp == nil {
		if ph.hasRaw {
			return &PipelineResult{Table: recordsToTable(records)}, nil
		}
		return &PipelineResult{Records: records}, nil
	}

	return e.runAggregation(records, ph, q)
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
		case *querylang.WhereOp, *querylang.EvalOp, *querylang.RenameOp, *querylang.FieldsOp:
			// these are fine
		default:
			return 0
		}
	}
	return headN
}

// recordsToTable converts a slice of records into a flat TableResult.
// Columns are: _write_ts, _ingest_ts, _source_ts, then all field keys
// (extracted KV/JSON + attributes, sorted), then _raw.
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
	columns = append(columns, "_write_ts", "_ingest_ts", "_source_ts")
	columns = append(columns, attrKeys...)
	columns = append(columns, "_raw")

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
