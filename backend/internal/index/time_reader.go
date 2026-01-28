package index

import (
	"sort"
	"time"

	"gastrolog/internal/chunk"
)

// TimeIndexReader provides binary search over a loaded time index.
type TimeIndexReader struct {
	chunkID chunk.ChunkID
	entries []TimeIndexEntry
}

// NewTimeIndexReader wraps a decoded set of time index entries for binary search.
func NewTimeIndexReader(chunkID chunk.ChunkID, entries []TimeIndexEntry) *TimeIndexReader {
	return &TimeIndexReader{chunkID: chunkID, entries: entries}
}

// FindStart binary searches for the latest entry at or before tStart.
// Returns (ref, true) if found — the caller should Seek to ref.
// Returns (zero, false) if tStart is before all entries — the caller should
// scan from the beginning of the chunk.
func (r *TimeIndexReader) FindStart(tStart time.Time) (chunk.RecordRef, bool) {
	n := len(r.entries)
	if n == 0 {
		return chunk.RecordRef{}, false
	}

	// Find the first entry with Timestamp > tStart.
	i := sort.Search(n, func(i int) bool {
		return r.entries[i].Timestamp.After(tStart)
	})

	// i is the count of entries with Timestamp <= tStart.
	if i == 0 {
		return chunk.RecordRef{}, false
	}

	e := r.entries[i-1]
	return chunk.RecordRef{ChunkID: r.chunkID, Pos: e.RecordPos}, true
}
