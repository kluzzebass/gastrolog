package server_test

import (
	"context"
	"io"
	"strconv"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
)

// bucketCounts reduces a timechart table to its per-bucket counts in row order.
func bucketCounts(t *testing.T, table *gastrologv1.TableResult) []int64 {
	t.Helper()
	if table == nil {
		t.Fatal("expected a timechart table, got none")
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
	out := make([]int64, 0, len(table.Rows))
	for _, row := range table.Rows {
		n, err := strconv.ParseInt(row.Values[countIdx], 10, 64)
		if err != nil {
			t.Fatalf("count %q: %v", row.Values[countIdx], err)
		}
		out = append(out, n)
	}
	return out
}

// A cap ahead of a timechart is a cluster-wide operator: the buckets must hold
// the survivors of the cap applied to the merged cluster stream, not to the
// coordinator's own records. The interleaved cluster puts one record every
// three seconds on each node, so a tail or slice selected on the coordinator
// alone lands in visibly different buckets than the cluster's.
func TestMultiNode_CappedTimechartBinsClusterSurvivors(t *testing.T) {
	t.Parallel()
	const perNode = 10
	h, _ := setupInterleavedCluster(t, perNode)

	// Premise: the fan-out under test is real.
	if got := len(searchAll(t, h.client, "")); got != perNode*3 {
		t.Fatalf("cluster holds %d records, not %d", got, perNode*3)
	}

	// Records sit at t0+0s … t0+29s, one per second across the three nodes;
	// three 10-second buckets over [t0, t0+30s).
	const window = "start=2025-06-15T10:00:00Z end=2025-06-15T10:00:30Z"
	tests := []struct {
		expr string
		want []int64
	}{
		// The newest nine cluster records are t0+21s…t0+29s: all in the last
		// bucket. The coordinator's own newest nine spread three per bucket.
		{window + " | tail 9 | timechart 3", []int64{0, 0, 9}},
		// Cluster records 10..18 are t0+9s…t0+17s: one in the first bucket,
		// eight in the second. The coordinator alone has only its record 10,
		// which sits in the last bucket.
		{window + " | slice 10 18 | timechart 3", []int64{1, 8, 0}},
	}
	for _, tc := range tests {
		t.Run(tc.expr, func(t *testing.T) {
			table := searchTable(t, h.client, tc.expr)
			got := bucketCounts(t, table)
			if len(got) != len(tc.want) {
				t.Fatalf("%s: buckets %v, want %v", tc.expr, got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("%s: buckets %v, want %v", tc.expr, got, tc.want)
					break
				}
			}
			if table.Truncated {
				t.Errorf("%s: a complete answer must not be flagged truncated", tc.expr)
			}
		})
	}
}

// A coordinator with no vault of its own must still answer a capped timechart
// from the cluster's records; an answer of all-zero buckets presented without
// a truncation flag is a wrong answer, not an empty one.
func TestMultiNode_CappedTimechartOnVaultlessCoordinator(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithoutVault("coord"))
	addMNRecords(t, h.Node(t, "data-1"), "one", 5, nil)
	addMNRecords(t, h.Node(t, "data-2"), "two", 5, nil)

	stream, err := h.client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{Expression: "start=2025-06-15T10:00:00Z end=2025-06-15T10:00:06Z | head 6 | timechart 3"},
	}))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	var table *gastrologv1.TableResult
	for stream.Receive() {
		if stream.Msg().TableResult != nil {
			table = stream.Msg().TableResult
		}
	}
	if err := stream.Err(); err != nil && err != io.EOF {
		t.Fatalf("stream error: %v", err)
	}
	var sum int64
	for _, n := range bucketCounts(t, table) {
		sum += n
	}
	if sum != 6 {
		t.Errorf("head 6 | timechart on a vaultless coordinator binned %d records, want 6", sum)
	}
}
