package server_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	"gastrolog/internal/chunk"
)

// addMNDistinctRecords appends n records to node, each with its own "id".
func addMNDistinctRecords(t *testing.T, node multinodeTestNode, prefix string, n int) {
	t.Helper()
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range n {
		ts := t0.Add(time.Duration(i) * time.Second)
		if _, _, err := node.vault.CM.Append(chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "%s-%d", prefix, i),
			Attrs:    chunk.Attributes{"id": fmt.Sprintf("%s-%d", prefix, i)},
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
}

// A peer whose share of a fan-out query outgrows its own memory budget fails
// that query, and the client hears ResourceExhausted — the answer it gets
// when the coordinator's share overflows — not an Internal fault, after the
// error has crossed the production ForwardSearch handler and client.
func TestMultiNode_PeerMemoryOverflowReachesClientAsResourceExhausted(t *testing.T) {
	t.Parallel()
	nodes := []string{"coord", "data-1", "data-2", "data-3"}
	const perNode = 400
	const expr = "| stats count by id"
	populate := func(h *multiNodeHarness) {
		for _, id := range nodes[1:] {
			addMNDistinctRecords(t, h.Node(t, id), id, perNode)
		}
	}

	// Premise: at the standard ceiling every peer answers its share.
	roomy := setupMultiNode(t, nodes, WithoutVault("coord"), WithClusterSearch())
	populate(roomy)
	if rows := len(searchTable(t, roomy.client, expr).GetRows()); rows != 3*perNode {
		t.Fatalf("premise: %s returned %d groups, want %d", expr, rows, 3*perNode)
	}

	tight := setupMultiNode(t, nodes, WithoutVault("coord"), WithClusterSearch(), WithPeerQueryMemoryLimit(8<<10))
	populate(tight)
	err := searchExpectingError(t, tight.client, expr)
	if got := connect.CodeOf(err); got != connect.CodeResourceExhausted {
		t.Fatalf("code = %v, want %v (err: %v)", got, connect.CodeResourceExhausted, err)
	}
	if !strings.Contains(err.Error(), "stats group state") {
		t.Errorf("error %q does not name the structure that overflowed on the peer", err)
	}
}
