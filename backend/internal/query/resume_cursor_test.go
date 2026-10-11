package query

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// The highwater bound keeps the boundary timestamp in reach in both
// directions, whether or not the cursor carries an identity: the cursor, not
// the bound, decides which records at that timestamp were already emitted.
func TestApplyResumeCursorKeepsTheBoundaryTimestampInReach(t *testing.T) {
	hw := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	identified := chunk.EventID{IngesterID: glid.New(), IngestTS: hw, IngestSeq: 1}
	for _, event := range []chunk.EventID{{}, identified} {
		q := Query{IsReverse: true}
		ApplyResumeCursor(&q, &ResumeToken{HighwaterTS: hw, HighwaterEvent: event})
		if !q.End.Equal(hw.Add(time.Nanosecond)) {
			t.Errorf("event %v: reverse bound = %v, want one tick past %v", event, q.End, hw)
		}
		q = Query{}
		ApplyResumeCursor(&q, &ResumeToken{HighwaterTS: hw, HighwaterEvent: event})
		if !q.Start.Equal(hw) {
			t.Errorf("event %v: forward bound = %v, want inclusive at %v", event, q.Start, hw)
		}
		if !q.ResumeAfterTS.Equal(hw) || q.ResumeAfterEvent != event {
			t.Errorf("event %v: cursor was not installed on the query", event)
		}
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
