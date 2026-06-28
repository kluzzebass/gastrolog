package errs

import (
	"errors"
	"testing"
)

func TestSummaryJoinNil(t *testing.T) {
	t.Parallel()
	if SummaryJoin(nil) != nil {
		t.Fatal("expected nil")
	}
}

func TestSummaryJoinSingle(t *testing.T) {
	t.Parallel()
	err := errors.New("one")
	if SummaryJoin(err) != err {
		t.Fatal("expected same error instance")
	}
}

func TestSummaryJoinDedupesAndSeparates(t *testing.T) {
	t.Parallel()
	a := errors.New("leadership transfer in progress")
	b := errors.New("no raft leader")
	got := SummaryJoin(a, a, a, b)
	want := "leadership transfer in progress (×3); no raft leader"
	if got.Error() != want {
		t.Fatalf("Error() = %q, want %q", got.Error(), want)
	}
	if !errors.Is(got, a) || !errors.Is(got, b) {
		t.Fatalf("Unwrap missing: %v", errors.Unwrap(got))
	}
}
