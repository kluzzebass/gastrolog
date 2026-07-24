package server

import (
	"testing"

	"gastrolog/internal/query"
)

// TestMergeTableResultsCloudSelectivityColumns verifies the multi-node
// merge of a `| timechart` TableResult treats the cloud-provenance sentinel
// columns (query.TimechartCloudFlagColumn/CloudCountColumn — see
// gastrolog-4of7c) as aggregates, not group keys. Every cluster node
// independently computes its own local applyCloudSelectivity estimate for a
// bucket; if these columns were left as ordinary group-by keys (the default
// for any unrecognized column — see detectAggColumns), two nodes disagreeing
// on has_cloud_data or cloud_count for the same (_time, group) bucket would
// splinter into separate rows instead of merging into one, corrupting the
// cluster-wide histogram. Cloud flags must OR across nodes; cloud counts
// must sum, same as the "count" column.
func TestMergeTableResultsCloudSelectivityColumns(t *testing.T) {
	cols := []string{"_time", "count", query.TimechartCloudFlagColumn, query.TimechartCloudCountColumn}

	// Node A: bucket touched by cloud data, scaled estimate contributes 1.
	local := &query.TableResult{
		Columns: cols,
		Rows: [][]string{
			{"2026-03-01T12:00:00Z", "2", "false", "0"},
			{"2026-03-01T12:00:05Z", "2", "true", "1"},
		},
	}
	// Node B: same two buckets. Bucket 0 stays exact/local on this node too.
	// Bucket 1 also saw cloud data (different local ratio on this node —
	// scaled estimate contributes 2 here).
	remote := &query.TableResult{
		Columns: cols,
		Rows: [][]string{
			{"2026-03-01T12:00:00Z", "3", "false", "0"},
			{"2026-03-01T12:00:05Z", "5", "true", "2"},
		},
	}

	merged := mergeTableResults(local, []*query.TableResult{remote})

	if len(merged.Rows) != 2 {
		t.Fatalf("expected 2 merged buckets (not splintered per-node), got %d: %v", len(merged.Rows), merged.Rows)
	}

	countIdx, flagIdx, cloudCountIdx := -1, -1, -1
	for i, c := range merged.Columns {
		switch c {
		case "count":
			countIdx = i
		case query.TimechartCloudFlagColumn:
			flagIdx = i
		case query.TimechartCloudCountColumn:
			cloudCountIdx = i
		}
	}
	if countIdx == -1 || flagIdx == -1 || cloudCountIdx == -1 {
		t.Fatalf("expected count/%s/%s columns in merged result, got %v", query.TimechartCloudFlagColumn, query.TimechartCloudCountColumn, merged.Columns)
	}

	var bucket0, bucket1 []string
	for _, row := range merged.Rows {
		switch row[0] {
		case "2026-03-01T12:00:00Z":
			bucket0 = row
		case "2026-03-01T12:00:05Z":
			bucket1 = row
		}
	}
	if bucket0 == nil || bucket1 == nil {
		t.Fatalf("missing expected buckets in merged rows: %v", merged.Rows)
	}

	// Bucket 0: neither node saw cloud data — stays unflagged, counts sum.
	if bucket0[flagIdx] != "false" {
		t.Errorf("bucket 0 %s = %q, want false (no node saw cloud data)", query.TimechartCloudFlagColumn, bucket0[flagIdx])
	}
	if bucket0[countIdx] != "5" {
		t.Errorf("bucket 0 count = %q, want 5 (2+3 summed across nodes)", bucket0[countIdx])
	}

	// Bucket 1: both nodes saw cloud data — OR'd flag, summed count and cloud count.
	if bucket1[flagIdx] != "true" {
		t.Errorf("bucket 1 %s = %q, want true (flag ORs across nodes)", query.TimechartCloudFlagColumn, bucket1[flagIdx])
	}
	if bucket1[countIdx] != "7" {
		t.Errorf("bucket 1 count = %q, want 7 (2+5 summed across nodes)", bucket1[countIdx])
	}
	if bucket1[cloudCountIdx] != "3" {
		t.Errorf("bucket 1 %s = %q, want 3 (1+2 summed across nodes)", query.TimechartCloudCountColumn, bucket1[cloudCountIdx])
	}
}

// TestMergeTableResultsCloudFlagOrsAcrossDisagreeingNodes covers the
// asymmetric case where only ONE node's bucket touched cloud-backed data
// (e.g. that node owns the cloud-backed chunk locally, the other doesn't
// see it at all — SkipCloud/topology differences). The merged bucket must
// still end up flagged; it must not silently drop the estimate provenance
// because the majority of nodes reported "false".
func TestMergeTableResultsCloudFlagOrsAcrossDisagreeingNodes(t *testing.T) {
	cols := []string{"_time", "count", query.TimechartCloudFlagColumn, query.TimechartCloudCountColumn}

	local := &query.TableResult{
		Columns: cols,
		Rows:    [][]string{{"2026-03-01T12:00:00Z", "4", "false", "0"}},
	}
	remoteA := &query.TableResult{
		Columns: cols,
		Rows:    [][]string{{"2026-03-01T12:00:00Z", "1", "false", "0"}},
	}
	remoteB := &query.TableResult{
		Columns: cols,
		Rows:    [][]string{{"2026-03-01T12:00:00Z", "2", "true", "2"}},
	}

	merged := mergeTableResults(local, []*query.TableResult{remoteA, remoteB})
	if len(merged.Rows) != 1 {
		t.Fatalf("expected 1 merged bucket, got %d: %v", len(merged.Rows), merged.Rows)
	}

	flagIdx, countIdx, cloudCountIdx := -1, -1, -1
	for i, c := range merged.Columns {
		switch c {
		case "count":
			countIdx = i
		case query.TimechartCloudFlagColumn:
			flagIdx = i
		case query.TimechartCloudCountColumn:
			cloudCountIdx = i
		}
	}

	row := merged.Rows[0]
	if row[flagIdx] != "true" {
		t.Errorf("%s = %q, want true (one node's cloud-derived data must not be lost)", query.TimechartCloudFlagColumn, row[flagIdx])
	}
	if row[countIdx] != "7" {
		t.Errorf("count = %q, want 7 (4+1+2 summed across all 3 nodes)", row[countIdx])
	}
	if row[cloudCountIdx] != "2" {
		t.Errorf("%s = %q, want 2 (0+0+2 summed across all 3 nodes)", query.TimechartCloudCountColumn, row[cloudCountIdx])
	}
}
