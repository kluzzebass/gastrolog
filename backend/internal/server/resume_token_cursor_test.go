package server

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/query"
)

// The cursor's identity must survive the wire, or the next page falls back to
// timestamp-only resumption and ties are repeated.
func TestResumeTokenRoundTripsTheCursorEvent(t *testing.T) {
	ts := time.Date(2026, 9, 5, 12, 0, 0, 123456789, time.UTC)
	ev := chunk.EventID{IngesterID: glid.New(), NodeID: glid.New(), IngestTS: ts, IngestSeq: 42}
	in := &query.ResumeToken{HighwaterTS: ts, HighwaterEvent: ev}

	out, err := ProtoToResumeToken(ResumeTokenToProto(in))
	if err != nil || out == nil {
		t.Fatalf("round trip: token=%v err=%v", out, err)
	}
	if !out.HighwaterTS.Equal(ts) {
		t.Fatalf("HighwaterTS = %v, want %v", out.HighwaterTS, ts)
	}
	if out.HighwaterEvent.Compare(ev) != 0 || out.HighwaterEvent.NodeID != ev.NodeID {
		t.Fatalf("HighwaterEvent = %+v, want %+v", out.HighwaterEvent, ev)
	}
}

// A token whose last record had no ingester identity carries no event, and
// ApplyResumeCursor then keeps the identity-less bounds.
func TestApplyResumeCursorWithoutIdentityKeepsExclusiveReverseBound(t *testing.T) {
	hw := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	q := query.Query{IsReverse: true}
	ApplyResumeCursor(&q, &query.ResumeToken{HighwaterTS: hw})
	if !q.End.Equal(hw) {
		t.Fatalf("identity-less reverse bound = %v, want exclusive at %v", q.End, hw)
	}
	q = query.Query{IsReverse: true}
	ApplyResumeCursor(&q, &query.ResumeToken{HighwaterTS: hw, HighwaterEvent: chunk.EventID{IngesterID: glid.New(), IngestTS: hw, IngestSeq: 1}})
	if !q.End.Equal(hw.Add(time.Nanosecond)) {
		t.Fatalf("identified reverse bound = %v, want one tick past %v", q.End, hw)
	}
	if q.ResumeAfterTS.IsZero() || q.ResumeAfterEvent.IngesterID.IsZero() {
		t.Fatal("cursor was not installed on the query")
	}
}
