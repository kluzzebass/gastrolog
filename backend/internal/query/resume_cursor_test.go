package query

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// A token whose last record had no ingester identity carries no event, and
// ApplyResumeCursor then keeps the identity-less bounds.
func TestApplyResumeCursorWithoutIdentityKeepsExclusiveReverseBound(t *testing.T) {
	hw := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	q := Query{IsReverse: true}
	ApplyResumeCursor(&q, &ResumeToken{HighwaterTS: hw})
	if !q.End.Equal(hw) {
		t.Fatalf("identity-less reverse bound = %v, want exclusive at %v", q.End, hw)
	}
	q = Query{IsReverse: true}
	ApplyResumeCursor(&q, &ResumeToken{HighwaterTS: hw, HighwaterEvent: chunk.EventID{IngesterID: glid.New(), IngestTS: hw, IngestSeq: 1}})
	if !q.End.Equal(hw.Add(time.Nanosecond)) {
		t.Fatalf("identified reverse bound = %v, want one tick past %v", q.End, hw)
	}
	if q.ResumeAfterTS.IsZero() || q.ResumeAfterEvent.IngesterID.IsZero() {
		t.Fatal("cursor was not installed on the query")
	}
}

// Under order=source_ts the cursor narrows the source window, and a query
// that already carries a cursor is left untouched.
func TestApplyResumeCursorNarrowsTheOrderingAxisOnce(t *testing.T) {
	hw := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	q := Query{OrderBy: OrderBySourceTS, Start: hw.Add(time.Hour)}
	ApplyResumeCursor(&q, &ResumeToken{HighwaterTS: hw})
	if !q.SourceStart.Equal(hw) || !q.Start.Equal(hw.Add(time.Hour)) {
		t.Fatalf("source order must narrow the source window only: source=%v ingest=%v", q.SourceStart, q.Start)
	}
	ApplyResumeCursor(&q, &ResumeToken{HighwaterTS: hw.Add(time.Minute)})
	if !q.ResumeAfterTS.Equal(hw) {
		t.Fatalf("a second cursor replaced the first: %v", q.ResumeAfterTS)
	}
}
