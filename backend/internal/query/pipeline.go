package query

import (
	"context"
	"fmt"
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

// RunPipeline executes a pipeline query against the search engine.
// Operators are split into pre-stats and post-stats phases.
// Without stats, returns records; with stats, returns a table.
// The raw operator forces all results into a flat table.
func (e *Engine) RunPipeline(ctx context.Context, q Query, pipeline *querylang.Pipeline) (*PipelineResult, error) {
	// Split operators into pre-stats and post-stats, detect raw.
	var preOps, postOps []querylang.PipeOp
	var statsOp *querylang.StatsOp
	hasRaw := false
	for _, pipe := range pipeline.Pipes {
		if s, ok := pipe.(*querylang.StatsOp); ok {
			if statsOp != nil {
				return nil, fmt.Errorf("pipeline can contain at most one stats operator")
			}
			statsOp = s
			continue
		}
		if _, ok := pipe.(*querylang.RawOp); ok {
			hasRaw = true
			continue // raw is consumed as a flag, not an operator to execute
		}
		if statsOp == nil {
			preOps = append(preOps, pipe)
		} else {
			postOps = append(postOps, pipe)
		}
	}

	// Determine if we can apply a head optimization: when the pipeline is
	// just filters + head (no sort, no stats), we can set q.Limit to avoid
	// a full scan.
	if statsOp == nil {
		if n := headOnlyLimit(preOps); n > 0 {
			q.Limit = n
		} else {
			// Non-aggregating pipelines need all records.
			q.Limit = 0
		}
	} else {
		// Aggregation needs all matching records.
		q.Limit = 0
	}

	// Get matching records from the search engine.
	iter, _ := e.Search(ctx, q, nil)

	// Collect and apply pre-stats operators.
	records, err := applyRecordOps(iter, preOps)
	if err != nil {
		return nil, err
	}

	// No stats.
	if statsOp == nil {
		if hasRaw {
			// Raw mode: convert records to a flat table.
			return &PipelineResult{Table: recordsToTable(records)}, nil
		}
		return &PipelineResult{Records: records}, nil
	}

	// Create aggregator and feed records.
	agg, err := NewAggregator(statsOp)
	if err != nil {
		return nil, err
	}
	for _, rec := range records {
		if err := agg.Add(rec); err != nil {
			return nil, err
		}
	}
	table := agg.Result(q.Start, q.End)

	// Apply post-stats operators to the table.
	table, err = applyTableOps(table, postOps)
	if err != nil {
		return nil, err
	}

	return &PipelineResult{Table: table}, nil
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
		case *querylang.SortOp:
			return 0 // sort requires all records
		case *querylang.WhereOp, *querylang.EvalOp, *querylang.RenameOp, *querylang.FieldsOp:
			// these are fine
		default:
			return 0
		}
	}
	return headN
}

// recordsToTable converts a slice of records into a flat TableResult.
// Columns are: _write_ts, _ingest_ts, _source_ts, then all attribute keys
// (sorted), then _raw.
func recordsToTable(records []chunk.Record) *TableResult {
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
