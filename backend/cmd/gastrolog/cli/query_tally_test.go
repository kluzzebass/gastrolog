package cli

import (
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
)

// An aggregating pipeline answers with a table and no records. The tally must
// still report a result, because the command's exit status is derived from it:
// counting only records makes every successful aggregation look like a miss.
func TestTallyCountsTableRowsAsResults(t *testing.T) {
	var tally queryTally
	handler := queryStreamHandler(&tally, "json", nil, true)

	resp := &gastrologv1.SearchResponse{
		TableResult: &gastrologv1.TableResult{
			Columns: []string{"host", "count"},
			Rows: []*gastrologv1.TableRow{
				{Values: []string{"a", "3"}},
				{Values: []string{"b", "7"}},
			},
		},
	}
	if err := handler(resp); err != nil {
		t.Fatalf("handler: %v", err)
	}

	found, unit := tally.results()
	if found != 2 {
		t.Errorf("table with 2 rows tallied %d results, want 2", found)
	}
	if unit != "rows" {
		t.Errorf("unit = %q, want %q", unit, "rows")
	}
	if found == 0 {
		t.Error("a successful aggregation reports no results, so the command would exit nonzero")
	}
}

func TestTallyCountsRecords(t *testing.T) {
	var tally queryTally
	handler := queryStreamHandler(&tally, "json", nil, true)

	resp := &gastrologv1.SearchResponse{
		Records: []*gastrologv1.Record{{}, {}, {}},
	}
	if err := handler(resp); err != nil {
		t.Fatalf("handler: %v", err)
	}

	found, unit := tally.results()
	if found != 3 || unit != "records" {
		t.Errorf("results() = (%d, %q), want (3, \"records\")", found, unit)
	}
}

// A query that matched nothing must stay distinguishable from one that
// returned something, in both shapes — that is what the exit status means.
func TestTallyReportsNothingForEmptyResults(t *testing.T) {
	for _, tc := range []struct {
		name string
		resp *gastrologv1.SearchResponse
	}{
		{"no records", &gastrologv1.SearchResponse{}},
		{"empty table", &gastrologv1.SearchResponse{
			TableResult: &gastrologv1.TableResult{Columns: []string{"host"}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var tally queryTally
			if err := queryStreamHandler(&tally, "json", nil, true)(tc.resp); err != nil {
				t.Fatalf("handler: %v", err)
			}
			if found, _ := tally.results(); found != 0 {
				t.Errorf("empty result tallied %d, want 0", found)
			}
		})
	}
}

// Rows arrive in their own response messages; every one counts.
func TestTallyAccumulatesAcrossResponses(t *testing.T) {
	var tally queryTally
	handler := queryStreamHandler(&tally, "json", nil, true)

	for range 3 {
		resp := &gastrologv1.SearchResponse{
			TableResult: &gastrologv1.TableResult{
				Columns: []string{"n"},
				Rows:    []*gastrologv1.TableRow{{Values: []string{"1"}}, {Values: []string{"2"}}},
			},
		}
		if err := handler(resp); err != nil {
			t.Fatalf("handler: %v", err)
		}
	}

	if found, _ := tally.results(); found != 6 {
		t.Errorf("three 2-row responses tallied %d, want 6", found)
	}
}
