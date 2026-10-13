package cli

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/server"
)

// TestVaultHasStageActivity verifies the stage-counter table's row-inclusion
// rule: a vault appears only when at least one discrete stage milestone has a
// non-zero total on that node.
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

// A removed node's status says what happened and the way back, not a
// topology it no longer has.
func TestPrintRemoval(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	removedAt := time.Date(2026, 10, 12, 9, 30, 0, 0, time.UTC)
	printRemoval(&printer{format: "table", w: &out}, &v1.GetClusterStatusResponse{
		ClusterEnabled: true,
		LocalNodeId:    []byte("06giv0bgl1pmffimofp5q1pgko"),
		Removal:        &v1.NodeRemoval{RemovedAt: timestamppb.New(removedAt), Reason: "removed from cluster by leader"},
	})
	got := out.String()
	for _, want := range []string{
		"06giv0bgl1pmffimofp5q1pgko",
		removedAt.Format(time.RFC3339),
		"removed from cluster by leader",
		server.RemovedNodeRemedy,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output lacks %q:\n%s", want, got)
		}
	}
}
