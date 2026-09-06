package server_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
)

// setupSkewedStatsCluster seeds three nodes so that every node's partial
// aggregate disagrees with the cluster's: the coordinator holds one host-a
// record and two host-b records, each data node holds three host-a records.
// Cluster-wide: host-a count 7 (latency sum 70), host-b count 2 (latency sum
// 200). Per node, host-b leads on the coordinator and is absent elsewhere.
//
// All nine records fall within [t0, t0+9s).
func setupSkewedStatsCluster(t *testing.T) *multiNodeHarness {
	t.Helper()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"})
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	seq := 0
	add := func(node string, host, latency string) {
		ts := t0.Add(time.Duration(seq) * time.Second)
		seq++
		h.Node(t, node).vault.CM.Append(chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "%s-%d", host, seq),
			Attrs:    map[string]string{"host": host, "latency": latency},
		})
	}
	add("coord", "host-a", "10")
	add("coord", "host-b", "100")
	add("coord", "host-b", "100")
	for _, n := range []string{"data-1", "data-2"} {
		for range 3 {
			add(n, "host-a", "10")
		}
	}
	return h
}

func rowsOf(table *gastrologv1.TableResult) [][]string {
	if table == nil {
		return nil
	}
	out := make([][]string, len(table.Rows))
	for i, r := range table.Rows {
		out[i] = r.Values
	}
	return out
}

// A stats table is merged by the structure of the stats operator, not by the
// look of its column names, and the operators after stats run once over the
// merged table. Each case here has a per-node answer that differs from the
// cluster's, so a merge that guesses columns by name or a pipeline that runs
// its tail on every node gives a visibly different table.
func TestMultiNode_StatsMergeIsStructuralAndPostOpsRunOnce(t *testing.T) {
	t.Parallel()
	h := setupSkewedStatsCluster(t)

	// Premise: the coordinator does fan out, and the group counts are the
	// cluster's.
	if got := rowsOf(searchTable(t, h.client, "| stats count by host")); !slices.EqualFunc(got, [][]string{{"host-a", "7"}, {"host-b", "2"}}, slices.Equal) {
		t.Fatalf("cluster counts by host = %v; the fan-out under test is not happening", got)
	}

	tests := []struct {
		expr string
		want [][]string
	}{
		// An alias must not hide the aggregate from the merge.
		{"| stats sum(latency) as total by host", [][]string{{"host-a", "70"}, {"host-b", "200"}}},
		// A filter after stats applies to the cluster count (7), not to each
		// node's partial (1 or 3).
		{"| stats count by host | where count > 5", [][]string{{"host-a", "7"}}},
		// The top group cluster-wide is host-a; on the coordinator alone it is
		// host-b.
		{"| stats count by host | sort -count | head 1", [][]string{{"host-a", "7"}}},
		// A derived column is computed from the merged count, so it cannot
		// splinter the groups.
		{"| stats count by host | eval doubled = count * 2", [][]string{{"host-a", "7", "14"}, {"host-b", "2", "4"}}},
		// No group key at all: a single row for the cluster.
		{"| stats count, sum(latency) as total | where count > 5", [][]string{{"9", "270"}}},
	}
	for _, tc := range tests {
		t.Run(tc.expr, func(t *testing.T) {
			table := searchTable(t, h.client, tc.expr)
			got := rowsOf(table)
			if !slices.EqualFunc(got, tc.want, slices.Equal) {
				t.Errorf("%s\n got %v\nwant %v (columns %v)", tc.expr, got, tc.want, table.GetColumns())
			}
		})
	}
}

// The same holds for a timechart: an operator after it sees the cluster's
// bucket, not each node's share of it.
func TestMultiNode_TimechartPostOpsRunOnce(t *testing.T) {
	t.Parallel()
	h := setupSkewedStatsCluster(t)

	// One 10-second bucket holds all nine records; every node's share is at
	// most three, so a per-node filter at > 4 keeps nothing.
	expr := "start=2025-06-15T10:00:00Z end=2025-06-15T10:00:10Z | timechart 1 | where count > 4"
	table := searchTable(t, h.client, expr)
	got := rowsOf(table)
	if len(got) != 1 || got[0][1] != "9" {
		t.Errorf("%s\n got %v\nwant one bucket with count 9", expr, got)
	}
}
