package query

import (
	"slices"
	"testing"

	"gastrolog/internal/querylang"
)

func planFor(t *testing.T, expr string) *DistributedTable {
	t.Helper()
	pipeline, err := querylang.ParsePipeline(expr)
	if err != nil {
		t.Fatalf("parse %q: %v", expr, err)
	}
	d, err := PlanDistributedTable(pipeline)
	if err != nil {
		t.Fatalf("plan %q: %v", expr, err)
	}
	return d
}

// The per-node pipeline stops at the aggregating operator; everything after it
// is the coordinator's.
func TestPlanDistributedTableSplitsAtTheAggregate(t *testing.T) {
	d := planFor(t, `level=error | where latency > 5 | stats count, sum(latency) as total by host | where count > 5 | sort -count | head 3 | barchart`)
	if got := d.PerNode.String(); got != `level=error | where latency>5 | stats count, sum(latency) as total by host` {
		t.Errorf("per-node pipeline = %q", got)
	}
	if len(d.PostOps) != 3 {
		t.Errorf("post-ops = %v, want where, sort, head", d.PostOps)
	}
}

// Aggregates that need the records are refused here so the caller routes them
// through the record gather instead of merging something incorrect.
func TestPlanDistributedTableRefusesHolisticAggregates(t *testing.T) {
	for _, expr := range []string{"| stats dcount(host)", "| stats median(latency) by host", "| stats values(host)", "| stats first(raw)"} {
		pipeline, err := querylang.ParsePipeline(expr)
		if err != nil {
			t.Fatalf("parse %q: %v", expr, err)
		}
		if _, err := PlanDistributedTable(pipeline); err == nil {
			t.Errorf("%s: expected a refusal", expr)
		}
	}
	pipeline, err := querylang.ParsePipeline("| where x = 1 | head 5")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PlanDistributedTable(pipeline); err == nil {
		t.Error("a pipeline without an aggregate produces no table to merge")
	}
}

// Column names play no part in the merge: an aliased sum combines like any
// other, and a group column that happens to be called "count" stays a key.
func TestDistributedTableMergeIsPositional(t *testing.T) {
	d := planFor(t, "| stats sum(latency) as total, min(latency) as lo, max(latency) as hi by host")
	cols := []string{"host", "total", "lo", "hi"}
	merged := d.Merge([]*TableResult{
		{Columns: cols, Rows: [][]string{{"b", "200", "100", "100"}, {"a", "10", "10", "10"}}},
		nil,
		{Columns: cols, Rows: [][]string{{"a", "30", "5", "12"}}},
		{Columns: cols, Rows: [][]string{{"a", "30", "7", "9"}}, Truncated: true},
	})
	want := [][]string{{"a", "70", "5", "12"}, {"b", "200", "100", "100"}}
	if !slices.EqualFunc(merged.Rows, want, slices.Equal) {
		t.Errorf("rows = %v, want %v", merged.Rows, want)
	}
	if !merged.Truncated {
		t.Error("a truncated partial must leave the merged table truncated")
	}

	d = planFor(t, "| stats sum(x) by count")
	merged = d.Merge([]*TableResult{
		{Columns: []string{"count", "sum_x"}, Rows: [][]string{{"3", "1"}}},
		{Columns: []string{"count", "sum_x"}, Rows: [][]string{{"3", "2"}}},
	})
	if want := [][]string{{"3", "3"}}; !slices.EqualFunc(merged.Rows, want, slices.Equal) {
		t.Errorf("group column named count: rows = %v, want %v", merged.Rows, want)
	}
}

// Time-bucket groups order chronologically after the merge, whichever node's
// rows arrived first.
func TestDistributedTableMergeOrdersBucketsInTime(t *testing.T) {
	d := planFor(t, "| stats count by bin(_time, 1h), host")
	cols := []string{"_time", "host", "count"}
	merged := d.Merge([]*TableResult{
		{Columns: cols, Rows: [][]string{{"2026-03-01T13:00:00Z", "a", "1"}}},
		{Columns: cols, Rows: [][]string{{"2026-03-01T09:00:00Z", "b", "2"}, {"2026-03-01T13:00:00Z", "a", "4"}}},
	})
	want := [][]string{{"2026-03-01T09:00:00Z", "b", "2"}, {"2026-03-01T13:00:00Z", "a", "5"}}
	if !slices.EqualFunc(merged.Rows, want, slices.Equal) {
		t.Errorf("rows = %v, want %v", merged.Rows, want)
	}
}

// A timechart's cloud-provenance sentinel columns are aggregates too: a bucket
// touched by cloud-derived data on any node stays flagged after the merge,
// and the per-node cloud contributions add up like the count. Treating them
// as keys would splinter one bucket into a row per disagreeing node.
func TestDistributedTableMergeTimechartCloudColumns(t *testing.T) {
	d := planFor(t, "| timechart 2")
	cols := []string{"_time", "count", TimechartCloudFlagColumn, TimechartCloudCountColumn}
	merged := d.Merge([]*TableResult{
		{Columns: cols, Rows: [][]string{{"2026-03-01T12:00:00Z", "2", "false", "0"}, {"2026-03-01T12:00:05Z", "2", "true", "1"}}},
		{Columns: cols, Rows: [][]string{{"2026-03-01T12:00:00Z", "3", "false", "0"}, {"2026-03-01T12:00:05Z", "5", "true", "2"}}},
		{Columns: cols, Rows: [][]string{{"2026-03-01T12:00:00Z", "1", "true", "1"}}},
	})
	want := [][]string{
		{"2026-03-01T12:00:00Z", "6", "true", "1"},
		{"2026-03-01T12:00:05Z", "7", "true", "3"},
	}
	if !slices.EqualFunc(merged.Rows, want, slices.Equal) {
		t.Errorf("rows = %v, want %v", merged.Rows, want)
	}

	d = planFor(t, "| timechart 2 by level")
	cols = []string{"_time", "level", "count", TimechartCloudFlagColumn, TimechartCloudCountColumn}
	merged = d.Merge([]*TableResult{
		{Columns: cols, Rows: [][]string{{"2026-03-01T12:00:00Z", "error", "2", "false", "0"}}},
		{Columns: cols, Rows: [][]string{{"2026-03-01T12:00:00Z", "error", "3", "false", "0"}, {"2026-03-01T12:00:00Z", "warn", "1", "false", "0"}}},
	})
	want = [][]string{
		{"2026-03-01T12:00:00Z", "error", "5", "false", "0"},
		{"2026-03-01T12:00:00Z", "warn", "1", "false", "0"},
	}
	if !slices.EqualFunc(merged.Rows, want, slices.Equal) {
		t.Errorf("grouped rows = %v, want %v", merged.Rows, want)
	}
}

// Malformed rows and empty tables cannot poison the merge.
func TestDistributedTableMergeSkipsMalformedRows(t *testing.T) {
	d := planFor(t, "| stats count by host")
	merged := d.Merge([]*TableResult{
		{Columns: []string{"host", "count"}},
		{Columns: []string{"host", "count"}, Rows: [][]string{{"a"}, {"a", "2", "extra"}, {"a", "3"}, {"b", "x"}, {"b", "4"}}},
	})
	want := [][]string{{"a", "3"}, {"b", "4"}}
	if !slices.EqualFunc(merged.Rows, want, slices.Equal) {
		t.Errorf("rows = %v, want %v", merged.Rows, want)
	}
	if len(d.Merge(nil).Rows) != 0 {
		t.Error("merging nothing must yield an empty table")
	}
}

// An avg travels as two cells per node, its sum and its count, and leaves the
// merge as one quotient under the declared name. A node whose values were all
// non-numeric contributes a count of zero and an empty sum.
func TestDistributedTableMergeFinalizesAvgFromSumAndCount(t *testing.T) {
	d := planFor(t, "| stats avg(latency) as mean, count by host")
	partial := []string{"host", "mean", "mean count", "count"}
	merged := d.Merge([]*TableResult{
		{Columns: partial, Rows: [][]string{{"a", "60", "3", "3"}, {"b", "", "0", "1"}}},
		{Columns: partial, Rows: [][]string{{"a", "100", "1", "1"}, {"b", "7", "1", "1"}}},
	})
	if want := []string{"host", "mean", "count"}; !slices.Equal(merged.Columns, want) {
		t.Errorf("columns = %v, want %v", merged.Columns, want)
	}
	want := [][]string{{"a", "40", "4"}, {"b", "7", "2"}}
	if !slices.EqualFunc(merged.Rows, want, slices.Equal) {
		t.Errorf("rows = %v, want %v", merged.Rows, want)
	}

	merged = d.Merge([]*TableResult{{Columns: partial, Rows: [][]string{{"c", "", "0", "2"}}}})
	if want := [][]string{{"c", "", "2"}}; !slices.EqualFunc(merged.Rows, want, slices.Equal) {
		t.Errorf("no numeric values: rows = %v, want %v", merged.Rows, want)
	}
}
