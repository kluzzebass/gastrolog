package server

import (
	"testing"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
)

// TestQueryConvertLastAll verifies the "last=all" sentinel is parsed as
// an explicit unbounded range — both Start and End come back zero,
// distinct from "no last= directive at all" which keeps whatever the
// caller already had on the proto. See gastrolog-2zdsc.
func TestQueryConvertLastAll(t *testing.T) {
	t.Parallel()
	q, _, err := protoToQuery(&apiv1.Query{Expression: "last=all reverse=true"})
	if err != nil {
		t.Fatalf("protoToQuery: %v", err)
	}
	if !q.Start.IsZero() {
		t.Errorf("Start should be zero for last=all, got %v", q.Start)
	}
	if !q.End.IsZero() {
		t.Errorf("End should be zero for last=all, got %v", q.End)
	}
	if !q.Reverse() {
		t.Errorf("Reverse should still be honored alongside last=all")
	}
}

// TestQueryConvertLastDuration sanity-checks that the duration path is
// still wired up after the last=all branch landed.
func TestQueryConvertLastDuration(t *testing.T) {
	t.Parallel()
	q, _, err := protoToQuery(&apiv1.Query{Expression: "last=5m"})
	if err != nil {
		t.Fatalf("protoToQuery: %v", err)
	}
	if q.Start.IsZero() || q.End.IsZero() {
		t.Errorf("last=5m should produce a bounded range, got start=%v end=%v", q.Start, q.End)
	}
}
