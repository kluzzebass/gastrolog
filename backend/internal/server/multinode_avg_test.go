package server_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// An avg is combined from per-node sums and counts, never from per-node
// averages. The values are dealt so that every node's own average differs
// from the cluster's, and so does the average of the averages.
func TestMultiNode_AvgCombinesFromPartials(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithoutVault("coord"))
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	seq := 0
	add := func(node, host string, latency int) {
		ts := t0.Add(time.Duration(seq) * time.Second)
		seq++
		h.Node(t, node).vault.CM.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts,
			Raw:   fmt.Appendf(nil, "%s %d", host, latency),
			Attrs: map[string]string{"host": host, "latency": fmt.Sprint(latency)},
		})
	}
	// host-a: data-1 holds 10, 20, 30 (avg 20); data-2 holds 100 (avg 100).
	// Cluster avg 40; average of averages 60; sum of averages 120.
	for _, v := range []int{10, 20, 30} {
		add("data-1", "host-a", v)
	}
	add("data-2", "host-a", 100)
	// host-b: one non-numeric latency on each node, one number on data-2.
	add("data-1", "host-b", 0)
	h.Node(t, "data-1").vault.CM.Append(chunk.Record{IngestTS: t0.Add(time.Hour), WriteTS: t0.Add(time.Hour), Raw: []byte("host-b n/a"), Attrs: map[string]string{"host": "host-b", "latency": "n/a"}})
	h.Node(t, "data-2").vault.CM.Append(chunk.Record{IngestTS: t0.Add(2 * time.Hour), WriteTS: t0.Add(2 * time.Hour), Raw: []byte("host-b n/a"), Attrs: map[string]string{"host": "host-b", "latency": "n/a"}})
	// host-c: no numeric latency anywhere.
	h.Node(t, "data-2").vault.CM.Append(chunk.Record{IngestTS: t0.Add(3 * time.Hour), WriteTS: t0.Add(3 * time.Hour), Raw: []byte("host-c"), Attrs: map[string]string{"host": "host-c"}})

	tests := []struct {
		expr string
		want [][]string
	}{
		{"| stats avg(latency) by host", [][]string{{"host-a", "40"}, {"host-b", "0"}, {"host-c", ""}}},
		{"| stats avg(latency) as mean, count, sum(latency) as total by host", [][]string{{"host-a", "40", "4", "160"}, {"host-b", "0", "3", "0"}, {"host-c", "", "1", ""}}},
		{"| stats avg(latency)", [][]string{{"32"}}},
		{"| stats avg(latency) by host | where avg_latency > 30", [][]string{{"host-a", "40"}}},
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

	// None of that gathered records: every answer was merged from per-node
	// tables. An aggregate that truly needs the records still opens streams,
	// which is what makes the zero above meaningful.
	if n := h.remote.recordStreams.Load(); n != 0 {
		t.Errorf("avg queries opened %d record streams; they should merge per-node tables", n)
	}
	searchTable(t, h.client, "| stats dcount(host)")
	if h.remote.recordStreams.Load() == 0 {
		t.Fatal("dcount did not open a record stream, so the counter observes nothing")
	}
}
