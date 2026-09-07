package query

import "time"

// ApplyResumeCursor makes q resume strictly after the record a resume token
// names: it bounds the ordering axis at the token's highwater and installs the
// canonical cursor every scanner skips up to. The engine applies it to its own
// tokens; the server applies it ahead of time as well, since the same bounds
// shape its histogram and the query it forwards. Applying it twice is
// harmless, and a query that already carries a cursor is left alone.
func ApplyResumeCursor(q *Query, resume *ResumeToken) {
	if resume == nil || resume.HighwaterTS.IsZero() || !q.ResumeAfterTS.IsZero() {
		return
	}
	hasIdentity := !resume.HighwaterEvent.IngesterID.IsZero()
	narrowQueryByHighwater(q, resume.HighwaterTS, hasIdentity)
	q.ResumeAfterTS = resume.HighwaterTS
	q.ResumeAfterEvent = resume.HighwaterEvent
}

// narrowQueryByHighwater bounds q at a resume-token highwater on the query's
// ordering axis: forward it becomes the inclusive lower bound; with reverse
// the exclusive upper bound, placed one tick past the highwater when the
// cursor carries an identity so the boundary group stays reachable. No-op
// when highwater is zero or already inside the existing bounds.
func narrowQueryByHighwater(q *Query, highwater time.Time, includeBoundary bool) {
	if highwater.IsZero() {
		return
	}
	var lower, upper *time.Time
	if q.OrderBy == OrderBySourceTS {
		lower, upper = &q.SourceStart, &q.SourceEnd
	} else {
		lower, upper = &q.Start, &q.End
	}
	if q.Reverse() {
		// The upper bound is exclusive: to keep records sharing the
		// boundary timestamp in reach of the cursor it must sit one tick
		// past the highwater. Without an identity to order ties there is
		// nothing to resume within the group, so the bound excludes it.
		bound := highwater
		if includeBoundary {
			bound = highwater.Add(time.Nanosecond)
		}
		if upper.IsZero() || bound.Before(*upper) {
			*upper = bound
		}
		return
	}
	if lower.IsZero() || highwater.After(*lower) {
		*lower = highwater
	}
}
