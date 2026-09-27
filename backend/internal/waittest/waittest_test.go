package waittest_test

import (
	"testing"

	"gastrolog/internal/waittest"
)

func TestForReturnsOnceTheConditionHolds(t *testing.T) {
	t.Parallel()
	calls := 0
	waittest.For(t, "third poll", func() bool {
		calls++
		return calls >= 3
	})
	if calls != 3 {
		t.Fatalf("cond polled %d times, want 3", calls)
	}
}

func TestForSeesAnImmediatelyTrueCondition(t *testing.T) {
	t.Parallel()
	waittest.For(t, "immediate", func() bool { return true })
}
