package server

import (
	"math"
	"strconv"
	"strings"

	"gastrolog/internal/query"
)

// mergeTableResults combines a local table result with one or more remote
// results. The merge strategy depends on the column names:
//
//   - Timechart: columns contain "_time" — group by all columns except the
//     last (count) and sum counts per bucket.
//   - Stats: aggregate columns detected by name pattern (count, sum(*), etc.)
//   - Fallback: concatenate rows.
func mergeTableResults(local *query.TableResult, remotes []*query.TableResult) *query.TableResult {
	if len(remotes) == 0 {
		return local
	}

	// Collect all results into one slice for uniform processing.
	all := make([]*query.TableResult, 0, 1+len(remotes))
	all = append(all, local)
	all = append(all, remotes...)

	cols := local.Columns
	aggCols := detectAggColumns(cols)

	if len(aggCols) == 0 {
		// No aggregation columns detected — concatenate rows.
		return concatResults(all, cols)
	}

	return mergeAggResults(all, cols, aggCols)
}

// aggType describes how to merge a column's values.
type aggType int

const (
	aggSum aggType = iota
	aggMin
	aggMax
)

// aggColumn describes a column that participates in aggregation.
type aggColumn struct {
	index int
	typ   aggType
}

// detectAggColumns identifies columns that are aggregation results and returns
// them with their merge strategy. Non-aggregate columns are group-by keys.
func detectAggColumns(cols []string) []aggColumn {
	var aggs []aggColumn
	for i, col := range cols {
		if col == "_time" {
			continue // group key, not an aggregate
		}
		lower := strings.ToLower(col)
		switch {
		case lower == "count" || strings.HasPrefix(lower, "count("):
			aggs = append(aggs, aggColumn{index: i, typ: aggSum})
		case strings.HasPrefix(lower, "sum("):
			aggs = append(aggs, aggColumn{index: i, typ: aggSum})
		case strings.HasPrefix(lower, "min("):
			aggs = append(aggs, aggColumn{index: i, typ: aggMin})
		case strings.HasPrefix(lower, "max("):
			aggs = append(aggs, aggColumn{index: i, typ: aggMax})
		case isNonDistributiveAgg(lower):
			// Non-distributive: these are routed through searchPipelineGlobal
			// (raw record gathering) so this merge path should not be reached.
			// Do NOT treat as sum — that would produce silently wrong results.
			// Skip: the column becomes a group key, producing obviously broken
			// output if the routing invariant is ever violated.
			continue
		}
	}
	return aggs
}

// isNonDistributiveAgg returns true if the column name is a non-distributive
// aggregate function that cannot be correctly merged by summing per-node results.
func isNonDistributiveAgg(lower string) bool {
	for _, prefix := range []string{"avg(", "dcount(", "median(", "first(", "last(", "values("} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

// isGroupColumn returns true if column at index i is a group-by key (not an aggregate).
func isGroupColumn(i int, aggs []aggColumn) bool {
	for _, a := range aggs {
		if a.index == i {
			return false
		}
	}
	return true
}

// mergeAggResults merges table results by grouping on non-aggregate columns
// and combining aggregate columns per their type.
func mergeAggResults(results []*query.TableResult, cols []string, aggs []aggColumn) *query.TableResult {
	type groupEntry struct {
		values []string
		order  int // insertion order for stable output
	}

	groups := make(map[string]*groupEntry)
	var insertOrder int

	for _, result := range results {
		for _, row := range result.Rows {
			if len(row) != len(cols) {
				continue
			}

			// Build group key from non-aggregate columns.
			var keyParts []string
			for i := range cols {
				if isGroupColumn(i, aggs) {
					keyParts = append(keyParts, row[i])
				}
			}
			key := strings.Join(keyParts, "\x00")

			entry, exists := groups[key]
			if !exists {
				// First occurrence: copy the row.
				values := make([]string, len(row))
				copy(values, row)
				entry = &groupEntry{values: values, order: insertOrder}
				insertOrder++
				groups[key] = entry
				continue
			}

			// Merge aggregate columns.
			for _, agg := range aggs {
				entry.values[agg.index] = mergeValue(entry.values[agg.index], row[agg.index], agg.typ)
			}
		}
	}

	// Collect rows in insertion order.
	sorted := make([][]string, len(groups))
	for _, entry := range groups {
		sorted[entry.order] = entry.values
	}

	truncated := false
	for _, r := range results {
		if r.Truncated {
			truncated = true
			break
		}
	}

	return &query.TableResult{
		Columns:   cols,
		Rows:      sorted,
		Truncated: truncated,
	}
}

// mergeValue combines two string-encoded numeric values per the aggregate type.
func mergeValue(a, b string, typ aggType) string {
	va, errA := strconv.ParseFloat(a, 64)
	vb, errB := strconv.ParseFloat(b, 64)
	if errA != nil || errB != nil {
		// Non-numeric: keep the existing value.
		return a
	}

	var result float64
	switch typ {
	case aggSum:
		result = va + vb
	case aggMin:
		result = math.Min(va, vb)
	case aggMax:
		result = math.Max(va, vb)
	}

	// Format as integer if no fractional part.
	if result == math.Trunc(result) && !math.IsInf(result, 0) {
		return strconv.FormatInt(int64(result), 10)
	}
	return strconv.FormatFloat(result, 'f', -1, 64)
}

// concatResults simply concatenates rows from all results (fallback strategy).
func concatResults(results []*query.TableResult, cols []string) *query.TableResult {
	var rows [][]string
	truncated := false
	for _, r := range results {
		rows = append(rows, r.Rows...)
		if r.Truncated {
			truncated = true
		}
	}
	return &query.TableResult{
		Columns:   cols,
		Rows:      rows,
		Truncated: truncated,
	}
}
