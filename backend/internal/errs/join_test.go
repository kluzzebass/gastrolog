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

func TestUnpackReturnsSummaryJoinSubErrors(t *testing.T) {
	t.Parallel()
	a := errors.New("a")
	b := errors.New("b")
	subs := Unpack(SummaryJoin(a, b))
	if len(subs) != 2 || subs[0] != a || subs[1] != b {
		t.Fatalf("Unpack = %v, want [a b]", subs)
	}
}

func TestUnpackNonAggregate(t *testing.T) {
	t.Parallel()
	a := errors.New("a")
	b := errors.New("b")
	// A single error (including SummaryJoin's single-error passthrough) and a
	// stdlib join are not SummaryJoin aggregates: annotation wraps and
	// sentinel attachments must stay whole for errors.Is.
	for _, err := range []error{nil, a, SummaryJoin(a), errors.Join(a, b)} {
		if subs := Unpack(err); subs != nil {
			t.Fatalf("Unpack(%v) = %v, want nil", err, subs)
		}
	}
}
