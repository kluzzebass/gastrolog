package cli

import (
	"testing"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

// TestVaultHasStageActivity verifies the stage-counter table's row-inclusion
// rule: a vault appears only when at least one discrete stage milestone has a
// non-zero total on that node (gastrolog-4r784a).
func TestVaultHasStageActivity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		vs   *v1.VaultStats
		want bool
	}{
		{"nil", nil, false},
		{"all zero", &v1.VaultStats{}, false},
		{"segments completed", &v1.VaultStats{SegmentsCompletedTotal: 1}, true},
		{"chunks built", &v1.VaultStats{ChunksBuiltTotal: 3}, true},
		{"head purges", &v1.VaultStats{HeadPurgesTotal: 2}, true},
		{"glcb failed only", &v1.VaultStats{GlcbPullsFailedTotal: 1}, true},
		{"retention deletes", &v1.VaultStats{RetentionDeletesTotal: 5}, true},
		{
			"throughput without stages", // append rate set, no stage counts
			&v1.VaultStats{AppendRecords: &v1.ThroughputRate{InstantPerSec: 10}},
			false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := vaultHasStageActivity(tc.vs); got != tc.want {
				t.Fatalf("vaultHasStageActivity(%s) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}
