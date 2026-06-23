package chunking_test

import (
	"testing"

	"gastrolog/internal/pipeline/chunking"
)

func TestCatchUpBudgetScalesWithBacklog(t *testing.T) {
	t.Parallel()
	policy := chunking.ManifestRotationPolicy{MaxRecords: 100_000}

	small := chunking.CatchUpBudget(10, policy)
	large := chunking.CatchUpBudget(10_000, policy)
	if small >= large {
		t.Fatalf("budget small=%d large=%d, want large > small", small, large)
	}
	if small < 32 {
		t.Fatalf("budget floor = %d, want >= 32", small)
	}
	if large > 4096 {
		t.Fatalf("budget ceiling = %d, want <= 4096", large)
	}
}
