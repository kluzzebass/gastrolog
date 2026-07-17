package server

import (
	"testing"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/system"
)

// TestMergeClusterAlarmFloodThreshold pins the merge semantics of the
// alarm-flood threshold: absent leaves the stored value alone, a value
// overwrites it, and an explicit 0 resets to "default" (every node's rate
// monitor resolves 0 to alert.DefaultFloodThreshold when it converges).
func TestMergeClusterAlarmFloodThreshold(t *testing.T) {
	t.Parallel()

	cfg := system.ClusterConfig{AlarmFloodThreshold: 25}

	// Absent field: merge-skip.
	if err := mergeCluster(&apiv1.PutClusterSettings{}, &cfg); err != nil {
		t.Fatalf("mergeCluster: %v", err)
	}
	if cfg.AlarmFloodThreshold != 25 {
		t.Fatalf("threshold = %d after empty merge, want 25", cfg.AlarmFloodThreshold)
	}

	// Set.
	v := uint32(40)
	if err := mergeCluster(&apiv1.PutClusterSettings{AlarmFloodThreshold: &v}, &cfg); err != nil {
		t.Fatalf("mergeCluster: %v", err)
	}
	if cfg.AlarmFloodThreshold != 40 {
		t.Fatalf("threshold = %d, want 40", cfg.AlarmFloodThreshold)
	}

	// Explicit 0 = reset to default.
	zero := uint32(0)
	if err := mergeCluster(&apiv1.PutClusterSettings{AlarmFloodThreshold: &zero}, &cfg); err != nil {
		t.Fatalf("mergeCluster: %v", err)
	}
	if cfg.AlarmFloodThreshold != 0 {
		t.Fatalf("threshold = %d after reset, want 0 (stored default marker)", cfg.AlarmFloodThreshold)
	}
}
