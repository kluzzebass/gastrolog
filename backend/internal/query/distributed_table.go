package query

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"gastrolog/internal/querylang"
)

// DistributedTable is a table-producing pipeline split for cluster execution.
// Every node runs PerNode — the filter, the operators ahead of the aggregating
// one, and that operator itself — and returns a table whose shape the
// aggregating operator fixes: group-key columns first, then one column per
// aggregate in declaration order. The coordinator combines those tables with
// Merge and runs PostOps once over the result, so an operator after stats or
// timechart sees the cluster's numbers rather than each node's share.
type DistributedTable struct {
	PerNode *querylang.Pipeline
	PostOps []querylang.PipeOp

	groupCols int
	binCol    int // group column holding a time bucket, -1 if none
	aggs      []columnMerge
}

// columnMerge is how one aggregate column combines across nodes.
type columnMerge int

const (
	mergeSum columnMerge = iota
	mergeMin
	mergeMax
	mergeAny // boolean OR
)

// PlanDistributedTable splits pipeline for cluster execution. It fails when
// the pipeline produces no table, or when an aggregate cannot be combined from
// per-node tables — such a pipeline must run once over every node's records
// instead (see PipelineNeedsGlobalRecords).
func PlanDistributedTable(pipeline *querylang.Pipeline) (*DistributedTable, error) {
	ph, err := classifyPipes(pipeline)
	if err != nil {
		return nil, err
	}
	d := &DistributedTable{PostOps: ph.postOps, binCol: -1}
	switch {
	case ph.statsOp != nil:
		d.PerNode = perNodePipeline(pipeline.Filter, ph.preOps, ph.statsOp)
		d.groupCols = len(ph.statsOp.Groups)
		for i, g := range ph.statsOp.Groups {
			if g.Bin != nil {
				d.binCol = i
			}
		}
		for _, agg := range ph.statsOp.Aggs {
			m, ok := distributiveMerge(agg.Func)
			if !ok {
				return nil, fmt.Errorf("%s cannot be combined from per-node results", agg.Func)
			}
			d.aggs = append(d.aggs, m)
		}
	case ph.timechartOp != nil:
		d.PerNode = perNodePipeline(pipeline.Filter, ph.preOps, ph.timechartOp)
		d.groupCols = 1
		if ph.timechartOp.By != "" {
			d.groupCols = 2
		}
		d.binCol = 0
		// count, then the cloud-provenance sentinels: a bucket touched by
		// cloud-derived data on any node stays flagged, and the per-node
		// cloud contributions add up like the count.
		d.aggs = []columnMerge{mergeSum, mergeAny, mergeSum}
	default:
		return nil, errors.New("pipeline does not produce a table")
	}
	return d, nil
}

func perNodePipeline(filter querylang.Expr, preOps []querylang.PipeOp, agg querylang.PipeOp) *querylang.Pipeline {
	pipes := make([]querylang.PipeOp, 0, len(preOps)+1)
	pipes = append(pipes, preOps...)
	pipes = append(pipes, agg)
	return &querylang.Pipeline{Filter: filter, Pipes: pipes}
}

// distributiveMerge returns how an aggregate function's per-node results
// combine, or false for one that needs the records themselves.
func distributiveMerge(fn string) (columnMerge, bool) {
	switch strings.ToLower(fn) {
	case "count", "sum":
		return mergeSum, true
	case "min":
		return mergeMin, true
	case "max":
		return mergeMax, true
	}
	return 0, false
}

// Merge combines tables produced by PerNode on different nodes into one.
// Rows with equal group keys become one row whose aggregate columns are
// combined per aggregate; rows are ordered by group key, chronologically on a
// time-bucket column. A nil table contributes nothing. The result is marked
// truncated if any input was.
func (d *DistributedTable) Merge(tables []*TableResult) *TableResult {
	width := d.groupCols + len(d.aggs)
	merged := &TableResult{}
	byKey := make(map[string][]string)
	var rows [][]string

	for _, t := range tables {
		if t == nil {
			continue
		}
		if merged.Columns == nil {
			merged.Columns = t.Columns
		}
		merged.Truncated = merged.Truncated || t.Truncated
		for _, row := range t.Rows {
			if len(row) != width {
				continue
			}
			key := strings.Join(row[:d.groupCols], "\x00")
			have, ok := byKey[key]
			if !ok {
				have = slices.Clone(row)
				byKey[key] = have
				rows = append(rows, have)
				continue
			}
			for i, m := range d.aggs {
				col := d.groupCols + i
				have[col] = mergeCell(have[col], row[col], m)
			}
		}
	}

	slices.SortStableFunc(rows, d.compareGroups)
	merged.Rows = rows
	return merged
}

// compareGroups orders rows by their group columns, chronologically on the
// time-bucket column and lexically elsewhere.
func (d *DistributedTable) compareGroups(a, b []string) int {
	for k := range d.groupCols {
		if a[k] == b[k] {
			continue
		}
		if k == d.binCol {
			ta, errA := time.Parse(time.RFC3339Nano, a[k])
			tb, errB := time.Parse(time.RFC3339Nano, b[k])
			if errA == nil && errB == nil {
				return ta.Compare(tb)
			}
		}
		return strings.Compare(a[k], b[k])
	}
	return 0
}

// mergeCell combines two string-encoded cells of one aggregate column.
func mergeCell(a, b string, m columnMerge) string {
	if m == mergeAny {
		return strconv.FormatBool(a == "true" || b == "true")
	}
	va, errA := strconv.ParseFloat(a, 64)
	vb, errB := strconv.ParseFloat(b, 64)
	switch {
	case errA != nil && errB != nil:
		return a
	case errA != nil:
		return b
	case errB != nil:
		return a
	}
	var v float64
	switch m {
	case mergeSum:
		v = va + vb
	case mergeMin:
		v = math.Min(va, vb)
	case mergeMax:
		v = math.Max(va, vb)
	case mergeAny:
		// handled above
	}
	if v == math.Trunc(v) && !math.IsInf(v, 0) {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// ApplyTableOps runs post-aggregation operators over a table.
func (e *Engine) ApplyTableOps(ctx context.Context, table *TableResult, ops []querylang.PipeOp) (*TableResult, error) {
	return applyTableOps(ctx, table, ops, e.lookupResolver)
}
