package server

import (
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/query"
)

// emitMark records the canonical position of the last record a page emitted:
// its timestamp on the query's ordering axis and its EventID. The resume
// token carries both so the next page resumes strictly after it.
type emitMark struct {
	orderBy query.OrderBy
	ts      time.Time
	event   chunk.EventID
}

func (m *emitMark) note(rec chunk.Record) {
	if m == nil {
		return
	}
	m.ts = m.orderBy.RecordTS(rec)
	m.event = rec.EventID
}

// remoteCursorToken is the resume token a remote vault receives: the
// coordinator's cursor and nothing else. Positions are never forwarded — the
// remote decodes this field as a full ResumeToken, and positions minted by
// whichever node coordinated the previous page are not one.
func remoteCursorToken(q query.Query) []byte {
	if q.ResumeAfterTS.IsZero() {
		return nil
	}
	return ResumeTokenToProto(&query.ResumeToken{HighwaterTS: q.ResumeAfterTS, HighwaterEvent: q.ResumeAfterEvent})
}
