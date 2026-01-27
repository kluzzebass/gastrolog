package time

import (
	"sort"
	gotime "time"

	"github.com/kluzzebass/gastrolog/internal/chunk"
	"github.com/kluzzebass/gastrolog/internal/index"
)

// TimeIndexReader provides binary search over a loaded time index.
type TimeIndexReader struct {
	chunkID chunk.ChunkID
	entries []index.TimeIndexEntry
}

// Open loads and validates a time index file for the given chunk.
func Open(dir string, chunkID chunk.ChunkID) (*TimeIndexReader, error) {
	entries, err := LoadIndex(dir, chunkID)
	if err != nil {
		return nil, err
	}
	return &TimeIndexReader{chunkID: chunkID, entries: entries}, nil
}

// FindStart binary searches for the latest entry at or before tStart.
// Returns (ref, true) if found — the caller should Seek to ref.
// Returns (zero, false) if tStart is before all entries — the caller should
// scan from the beginning of the chunk.
func (r *TimeIndexReader) FindStart(tStart gotime.Time) (chunk.RecordRef, bool) {
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
