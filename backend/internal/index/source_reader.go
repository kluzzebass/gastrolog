package index

import (
	"bytes"
	"sort"

	"gastrolog/internal/chunk"
)

// SourceIndexReader provides binary search lookup over a loaded source index.
type SourceIndexReader struct {
	chunkID chunk.ChunkID
	entries []SourceIndexEntry // sorted by SourceID raw bytes
}

// NewSourceIndexReader wraps a decoded set of source index entries for lookup.
// The entries must be sorted by SourceID raw bytes (as produced by both file
// and memory indexers).
func NewSourceIndexReader(chunkID chunk.ChunkID, entries []SourceIndexEntry) *SourceIndexReader {
	return &SourceIndexReader{chunkID: chunkID, entries: entries}
}

// Lookup binary searches for sourceID in the key table.
// Returns (positions, true) if found, or (nil, false) if not present.
func (r *SourceIndexReader) Lookup(sourceID chunk.SourceID) ([]uint64, bool) {
	n := len(r.entries)
	if n == 0 {
		return nil, false
	}

	target := [16]byte(sourceID)

	i := sort.Search(n, func(i int) bool {
		entry := [16]byte(r.entries[i].SourceID)
		return bytes.Compare(entry[:], target[:]) >= 0
	})

	if i < n && [16]byte(r.entries[i].SourceID) == target {
		return r.entries[i].Positions, true
	}

	return nil, false
}
