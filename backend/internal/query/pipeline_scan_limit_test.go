package query_test

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
)

// The scan feeding a pipeline is limited only when a head is reached through
// operators that keep every record; anything that drops, reorders, windows or
// aggregates records needs every match.
func TestPipelineScanLimit(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		expr string
		want int
	}{
		{"| head 5", 5},
		{"| eval x=1 | rename x as y | fields y | head 5", 5},
		{"| head 5 | sort -value", 5},
		{"| head 5 | where level=\"error\"", 5},
		{"| head 5 | tail 2", 5},
		{"| where level=\"error\" | head 5", 0},
		{"| dedup | head 5", 0},
		{"| sort -value | head 5", 0},
		{"| tail 5", 0},
		{"| slice 2 5", 0},
		{"| sort -value", 0},
		{"| eval x=1", 0},
		{"| head 5 | stats count", 0},
		{"| stats dcount(level)", 0},
		{"| head 5 | timechart 10", 0},
	} {
		t.Run(tc.expr, func(t *testing.T) {
			t.Parallel()
			pipeline, err := querylang.ParsePipeline(tc.expr)
			if err != nil {
				t.Fatalf("parse %q: %v", tc.expr, err)
			}
			if got := query.PipelineScanLimit(pipeline); got != tc.want {
				t.Errorf("PipelineScanLimit(%q) = %d, want %d", tc.expr, got, tc.want)
			}
		})
	}
}

// A head behind a filtering operator counts the records that survive the
// filter, so the scan feeding it reads past the head's N.
func TestRunPipelineHeadBehindAFilterReadsPastN(t *testing.T) {
	t.Parallel()
	eng := newStatsTestEngine(t, 200)

	for _, tc := range []struct {
		expr string
		want []string
	}{
		// Odd records are level=error.
		{`| where level="error" | head 5`, []string{"rec-0001", "rec-0003", "rec-0005", "rec-0007", "rec-0009"}},
		{`| where level="error" | head 3 | sort -value`, []string{"rec-0005", "rec-0003", "rec-0001"}},
	} {
		t.Run(tc.expr, func(t *testing.T) {
			t.Parallel()
			pipeline, err := querylang.ParsePipeline(tc.expr)
			if err != nil {
				t.Fatalf("parse %q: %v", tc.expr, err)
			}
			// The incoming limit is the client's page size: it caps the
			// output, never the scan.
			result, err := eng.RunPipeline(context.Background(), query.Query{Limit: 2}, pipeline)
			if err != nil {
				t.Fatalf("RunPipeline: %v", err)
			}
			got := make([]string, len(result.Records))
			for i, rec := range result.Records {
				got[i] = string(rec.Raw)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("%s = %v, want %v", tc.expr, got, tc.want)
			}
		})
	}
}

// A page size below the match set caps a pipeline's output but not what its
// operators read: a tail over a 300-record vault with a page of 10 still
// returns the vault's last records.
func TestRunPipelinePageSizeCapsOutputNotInput(t *testing.T) {
	t.Parallel()
	eng := newStatsTestEngine(t, 300)

	pipeline, err := querylang.ParsePipeline("| tail 3")
	if err != nil {
		t.Fatal(err)
	}
	result, err := eng.RunPipeline(context.Background(), query.Query{Limit: 10}, pipeline)
	if err != nil {
		t.Fatalf("RunPipeline: %v", err)
	}
	var got []string
	for _, rec := range result.Records {
		got = append(got, string(rec.Raw))
	}
	want := []string{fmt.Sprintf("rec-%04d", 297), fmt.Sprintf("rec-%04d", 298), fmt.Sprintf("rec-%04d", 299)}
	if !slices.Equal(got, want) {
		t.Errorf("| tail 3 = %v, want %v", got, want)
	}
}
