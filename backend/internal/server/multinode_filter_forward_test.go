package server_test

import (
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// Every filter form must reach remote nodes meaning what it meant on the
// coordinator. The coordinator holds no vault, so whatever it returns came
// back from the data nodes through the forwarded query text; a form the
// remote parser misreads shows up as zero matches, or as the wrong records
// for a negation.
func TestMultiNode_FilterFormsSurviveForwarding(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithoutVault("coord"))
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for ni, n := range []string{"data-1", "data-2"} {
		for i := range 5 {
			ts := t0.Add(time.Duration(ni*10+i) * time.Second)
			h.Node(t, n).vault.CM.Append(chunk.Record{
				IngestTS: ts, WriteTS: ts,
				Raw:   fmt.Appendf(nil, "disk error on node %d item %d path=/var/log", ni, i),
				Attrs: map[string]string{"host": "h", "note": "a b"},
			})
		}
		ts := t0.Add(time.Duration(ni*10+7) * time.Second)
		h.Node(t, n).vault.CM.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts,
			Raw:   fmt.Appendf(nil, "all quiet on node %d", ni),
			Attrs: map[string]string{"host": "h", "note": ""},
		})
	}

	// Premise: the key=value form fans out, so a shortfall below is the
	// filter form, not the fan-out.
	if got := len(searchAll(t, h.client, "host=h")); got != 12 {
		t.Fatalf("host=h found %d records, want 12", got)
	}

	tests := []struct {
		expr string
		want int
	}{
		{`error`, 10},
		{`"disk error"`, 10},
		{`/disk.*full/`, 0},
		{`/disk error/`, 10},
		{`/\/var\/log/`, 10},
		{`dis*`, 10},
		{`NOT error`, 2},
		{`error host=h`, 10},
		{`*="a b"`, 10},
		{`| where note = ""`, 2},
		{`| where note != ""`, 10},
		{`len(raw) > 30`, 10},
		{`error | where note = "a b"`, 10},
		{`| where note != "" | where host = "h"`, 10},
	}
	for _, tc := range tests {
		t.Run(tc.expr, func(t *testing.T) {
			if got := len(searchAll(t, h.client, tc.expr)); got != tc.want {
				t.Errorf("%s returned %d records from the data nodes, want %d", tc.expr, got, tc.want)
			}
		})
	}
}
