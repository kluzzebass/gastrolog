package server_test

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// An empty-string comparison means the same thing on every node: the filter
// form and the where operator agree, and the coordinator (which holds no
// vault) gets the data nodes' answer.
func TestMultiNode_EmptyValueComparisonAgreesEverywhere(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithoutVault("coord"))
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for ni, n := range []string{"data-1", "data-2"} {
		for i, attrs := range []map[string]string{{"note": "a b"}, {"note": ""}, {"other": "v"}} {
			ts := t0.Add(time.Duration(ni*10+i) * time.Second)
			h.Node(t, n).vault.CM.Append(chunk.Record{IngestTS: ts, WriteTS: ts, Raw: []byte("line"), Attrs: attrs})
		}
	}
	tests := []struct {
		expr string
		want int
	}{
		{`note=""`, 2},
		{`| where note = ""`, 2},
		{`note!=""`, 2},
		{`| where note != ""`, 2},
		{`note=*`, 4},
	}
	for _, tc := range tests {
		if got := len(searchAll(t, h.client, tc.expr)); got != tc.want {
			t.Errorf("%s returned %d records, want %d", tc.expr, got, tc.want)
		}
	}
}
