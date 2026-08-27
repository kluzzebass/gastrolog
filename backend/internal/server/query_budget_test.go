package server_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
)

// searchExpectingError runs a Search RPC and returns the error it fails with,
// whether the failure surfaces on the call or on the stream.
func searchExpectingError(t *testing.T, client gastrologv1connect.QueryServiceClient, expr string) error {
	t.Helper()
	stream, err := client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{Expression: expr},
	}))
	if err != nil {
		return err
	}
	for stream.Receive() {
	}
	err = stream.Err()
	if err == nil || err == io.EOF {
		t.Fatalf("expected %q to fail, got success", expr)
	}
	return err
}

// A query can ask for a collector far larger than the node it runs on. The
// budget refuses it at the RPC boundary with a message naming the limit,
// instead of the node allocating tens of gigabytes of record slots.
func TestSearchRejectsQueryOverMemoryBudget(t *testing.T) {
	client := newQueryTestSetup(t, 10)

	for _, expr := range []string{
		"| tail 200000000",
		"| slice 1 200000000",
	} {
		t.Run(expr, func(t *testing.T) {
			err := searchExpectingError(t, client, expr)
			if got := connect.CodeOf(err); got != connect.CodeResourceExhausted {
				t.Errorf("code: got %v, want %v (err: %v)", got, connect.CodeResourceExhausted, err)
			}
			msg := err.Error()
			if !strings.Contains(msg, "query memory budget") {
				t.Errorf("error %q does not name the budget that was exceeded", msg)
			}
			if !strings.Contains(msg, "MiB") {
				t.Errorf("error %q does not state the limit", msg)
			}
		})
	}
}

// The budget must not get in the way of the queries this product exists to
// run. These go through the real RPC on the same paths the bound guards.
func TestSearchWithinMemoryBudgetStillSucceeds(t *testing.T) {
	client := newQueryTestSetup(t, 2000)

	tests := []struct {
		name string
		expr string
		want string
	}{
		{"aggregate every record", "| stats count", "2000"},
		{"distinct count over every record", "| stats dcount(raw)", "1"},
		{"tail a reasonable window", "| tail 50 | stats count", "50"},
		{"sorted page", "| sort ingest_ts | head 100 | stats count", "100"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stream, err := client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
				Query: &gastrologv1.Query{Expression: tc.expr},
			}))
			if err != nil {
				t.Fatalf("Search: %v", err)
			}
			var got string
			for stream.Receive() {
				if tbl := stream.Msg().TableResult; tbl != nil && len(tbl.Rows) > 0 {
					got = tbl.Rows[len(tbl.Rows)-1].Values[0]
				}
			}
			if err := stream.Err(); err != nil && err != io.EOF {
				t.Fatalf("stream error: %v", err)
			}
			if got != tc.want {
				t.Errorf("%s: got %q, want %q", tc.expr, got, tc.want)
			}
		})
	}
}

// Fan-out queries gather every node's records on the coordinator before the
// pipeline runs. The budget covers that gather and the pipeline as one
// account, and normal cluster aggregation still works.
func TestSearchMemoryBudgetAcrossNodes(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"})
	addMNRecords(t, h.Node(t, "coord"), "coord", 50, map[string]string{"host": "a"})
	addMNRecords(t, h.Node(t, "data-1"), "one", 50, map[string]string{"host": "b"})
	addMNRecords(t, h.Node(t, "data-2"), "two", 50, map[string]string{"host": "c"})

	// A cap larger than any node can hold is refused on the coordinator, which
	// is where the gathered cluster records land.
	err := searchExpectingError(t, h.client, "| tail 200000000")
	if got := connect.CodeOf(err); got != connect.CodeResourceExhausted {
		t.Errorf("code: got %v, want %v (err: %v)", got, connect.CodeResourceExhausted, err)
	}

	// The same shape at a sane size still aggregates across all three nodes.
	table := searchTable(t, h.client, "| tail 150 | stats count by host")
	counts := tableToMap(t, table, "host", "count")
	for _, host := range []string{"a", "b", "c"} {
		if counts[host] != "50" {
			t.Errorf("host %s: got %q, want \"50\" (counts: %v)", host, counts[host], counts)
		}
	}
}
