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

// remoteTokenOrCursor is the resume token a remote vault receives. Remote
// positions are never carried across pages, so absent a per-vault token the
// remote gets the coordinator's cursor and nothing else.
func remoteTokenOrCursor(q query.Query, token []byte) []byte {
	if token != nil || q.ResumeAfterTS.IsZero() {
		return token
	}
	return ResumeTokenToProto(&query.ResumeToken{HighwaterTS: q.ResumeAfterTS, HighwaterEvent: q.ResumeAfterEvent})
}
