package query

import (
	"context"
	"strconv"
	"testing"
	"time"

	"gastrolog/internal/querylang"
)

// timechartCounts reduces a timechart table to per-bucket counts in row order,
// keyed by the group value when the table has a group column.
func timechartCounts(t *testing.T, table *TableResult) map[string]int64 {
	t.Helper()
	if table == nil {
		t.Fatal("expected a timechart table")
	}
	countIdx := -1
	for i, c := range table.Columns {
		if c == "count" {
			countIdx = i
		}
	}
	if countIdx < 0 {
		t.Fatalf("no count column in %v", table.Columns)
	}
	out := make(map[string]int64)
	for _, row := range table.Rows {
		n, err := strconv.ParseInt(row[countIdx], 10, 64)
		if err != nil {
			t.Fatalf("count %q: %v", row[countIdx], err)
		}
		key := row[0]
		if countIdx > 1 {
			key = row[0] + "/" + row[1]
		}
		out[key] += n
	}
	return out
}

func sumCounts(m map[string]int64) int64 {
	var s int64
	for _, v := range m {
		s += v
	}
	return s
}

// A timechart fed by a cap must bin the cap's survivors from the whole cluster
// stream. The local half alone holds every even-indexed record, so a tail
// applied to it lands in both buckets; the cluster's tail is the newest run
// and lands only in the last one.
func TestClusterCappedTimechartBinsTheMergedStream(t *testing.T) {
	eng, remote := clusterHalves(t, 10)
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	q := Query{Start: t0, End: t0.Add(20 * time.Second)}

	pipeline, err := querylang.ParsePipeline("| tail 10 | timechart 2")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	result, err := eng.RunPipelineWithRemote(context.Background(), q, pipeline, remote, NewBudget())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	counts := timechartCounts(t, result.Table)
	first := t0.Format(time.RFC3339)
	second := t0.Add(10 * time.Second).Format(time.RFC3339)
	if counts[first] != 0 || counts[second] != 10 {
		t.Errorf("tail 10 over the cluster should fill only the newest bucket, got %v", counts)
	}
	if result.Table.Truncated {
		t.Error("a complete answer must not be flagged truncated")
	}
}

// With a group field the same rule holds per group: the newest eight records
// carry two of each of the four hosts.
func TestClusterCappedTimechartGroupsTheMergedStream(t *testing.T) {
	eng, remote := clusterHalves(t, 10)
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	q := Query{Start: t0, End: t0.Add(20 * time.Second)}

	pipeline, err := querylang.ParsePipeline("| tail 8 | timechart 2 by host")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	result, err := eng.RunPipelineWithRemote(context.Background(), q, pipeline, remote, NewBudget())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	counts := timechartCounts(t, result.Table)
	second := t0.Add(10 * time.Second).Format(time.RFC3339)
	for h := range 4 {
		key := second + "/host-" + strconv.Itoa(h)
		if counts[key] != 2 {
			t.Errorf("%s = %d, want 2 (got %v)", key, counts[key], counts)
		}
	}
	if got := sumCounts(counts); got != 8 {
		t.Errorf("grouped counts sum to %d, want 8", got)
	}
}

// Without an explicit range, a capped cluster timechart spans the records the
// cap kept — the same buckets whichever node coordinates, since no node's
// chunk metadata is consulted. Every survivor is counted, the last included.
func TestClusterCappedTimechartDerivesRangeFromSurvivors(t *testing.T) {
	eng, remote := clusterHalves(t, 10)

	pipeline, err := querylang.ParsePipeline("| head 6 | timechart 3")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	result, err := eng.RunPipelineWithRemote(context.Background(), Query{}, pipeline, remote, NewBudget())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	counts := timechartCounts(t, result.Table)
	if got := sumCounts(counts); got != 6 {
		t.Errorf("head 6 should bin 6 records, got %d in %v", got, counts)
	}
	if len(result.Table.Rows) != 3 {
		t.Errorf("want 3 buckets, got %d", len(result.Table.Rows))
	}
	for k, v := range counts {
		if v != 2 {
			t.Errorf("bucket %s = %d, want 2 (six survivors one second apart over three buckets)", k, v)
		}
	}
}
