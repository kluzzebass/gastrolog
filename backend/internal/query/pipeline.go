package query

import (
	"context"
	"fmt"

	"gastrolog/internal/chunk"
	"gastrolog/internal/querylang"
)

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
// The query's BoolExpr and time bounds should already be set by the caller.
// The pipeline's Pipes are processed: where operators filter records,
// and the stats operator aggregates them into a TableResult.
func (e *Engine) RunPipeline(ctx context.Context, q Query, pipeline *querylang.Pipeline) (*TableResult, error) {
	// Find stats and where operators.
	var statsOp *querylang.StatsOp
	var whereFilters []func(chunk.Record) bool

	for _, pipe := range pipeline.Pipes {
		switch op := pipe.(type) {
		case *querylang.StatsOp:
			if statsOp != nil {
				return nil, fmt.Errorf("pipeline can contain at most one stats operator")
			}
			statsOp = op
		case *querylang.WhereOp:
			whereFilters = append(whereFilters, CompileFilter(op.Expr))
		default:
			return nil, fmt.Errorf("unsupported pipe operator: %T", pipe)
		}
	}

	if statsOp == nil {
		return nil, fmt.Errorf("pipeline must contain a stats operator")
	}

	// Aggregation needs all matching records — remove any limit.
	q.Limit = 0

	// Get matching records from the search engine.
	iter, _ := e.Search(ctx, q, nil)

	// Create aggregator.
	agg, err := NewAggregator(statsOp)
	if err != nil {
		return nil, err
	}

	// Process records: apply where filters, then accumulate.
	for rec, err := range iter {
		if err != nil {
			return nil, err
		}

		skip := false
		for _, f := range whereFilters {
			if !f(rec) {
				skip = true
				break
			}
		}
		if skip {
			continue
		}

		if err := agg.Add(rec); err != nil {
			return nil, err
		}
	}

	return agg.Result(q.Start, q.End), nil
}
