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
