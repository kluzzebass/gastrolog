package query_test

import (
	"context"
	"strconv"
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
)

// A timechart or histogram that derives its range from chunk metadata must
// count every record, the newest included: metadata records the newest
// timestamp inclusively while the query's end is exclusive.
func TestDerivedRangeCountsTheNewestRecord(t *testing.T) {
	records := []chunk.Record{
		{IngestTS: t1, Attrs: attrsA, Raw: []byte("one")},
		{IngestTS: t2, Attrs: attrsA, Raw: []byte("two")},
		{IngestTS: t3, Attrs: attrsA, Raw: []byte("three")},
		{IngestTS: t4, Attrs: attrsA, Raw: []byte("four")},
	}
	eng := setup(t, records)

	// Unfiltered takes the metadata fast path; filtered takes the record scan.
	for _, expr := range []string{"| timechart 2", "source=srcA | timechart 2", "| where source = \"srcA\" | timechart 2"} {
		pipeline, err := querylang.ParsePipeline(expr)
		if err != nil {
			t.Fatal(err)
		}
		result, err := eng.RunPipeline(context.Background(), query.Query{BoolExpr: pipeline.Filter}, pipeline)
		if err != nil {
			t.Fatalf("%s: %v", expr, err)
		}
		var total int64
		for _, row := range result.Table.Rows {
			n, err := strconv.ParseInt(row[1], 10, 64)
			if err != nil {
				t.Fatalf("count %q: %v", row[1], err)
			}
			total += n
		}
		if total != int64(len(records)) {
			t.Errorf("%s with a derived range counted %d records, want %d", expr, total, len(records))
		}
	}

	filter, err := querylang.Parse("source=srcA")
	if err != nil {
		t.Fatal(err)
	}
	for name, q := range map[string]query.Query{"unfiltered": {}, "filtered": {BoolExpr: filter}} {
		var total int64
		for _, b := range eng.ComputeHistogram(context.Background(), q, 2) {
			total += b.Count
		}
		if total != int64(len(records)) {
			t.Errorf("%s histogram with a derived range counted %d records, want %d", name, total, len(records))
		}
	}
}
