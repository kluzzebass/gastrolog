package server_test

import (
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// A dedup window is a cluster-wide operator: it must see every node's records
// merged in query order, so a duplicate whose copies live on two different
// nodes is removed once and nothing else is lost. The coordinator holds no
// vault, so an answer from its own records alone is empty; a dedup run per
// node would keep both copies of the cross-node duplicate.
func TestMultiNode_DedupRunsOverTheClusterStream(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithoutVault("coord"))

	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	ingester := glid.New()
	record := func(i int) chunk.Record {
		ts := t0.Add(time.Duration(i) * time.Second)
		return chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			EventID:  chunk.EventID{IngesterID: ingester, IngestTS: ts, IngestSeq: uint32(i)}, //nolint:gosec // G115: small loop index
			Raw:      fmt.Appendf(nil, "event-%d", i),
		}
	}
	for i := range 5 {
		h.Node(t, "data-1").vault.CM.Append(record(i))
	}
	for i := 5; i < 10; i++ {
		h.Node(t, "data-2").vault.CM.Append(record(i))
	}
	// A second copy of event 0, on the other node.
	h.Node(t, "data-2").vault.CM.Append(record(0))

	// Premise: the cluster really holds eleven records, two of them the same
	// event. Counted per node, since the streaming search collapses copies of
	// one event across vaults on its own.
	if got := rowsOf(searchTable(t, h.client, "| stats count")); len(got) != 1 || got[0][0] != "11" {
		t.Fatalf("cluster holds %v records, want 11", got)
	}

	got := searchAll(t, h.client, "| dedup")
	if len(got) != 10 {
		t.Fatalf("| dedup returned %d records, want 10 (eleven records, one cross-node duplicate)", len(got))
	}
	seen := make(map[string]int)
	for _, r := range got {
		seen[string(r.Raw)]++
	}
	for i := range 10 {
		if seen[fmt.Sprintf("event-%d", i)] != 1 {
			t.Errorf("event-%d appears %d times after dedup, want once", i, seen[fmt.Sprintf("event-%d", i)])
		}
	}
}
