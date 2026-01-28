package index

import (
	"sort"

	"github.com/kluzzebass/gastrolog/internal/chunk"
)

// TokenIndexReader provides binary search lookup over a loaded token index.
type TokenIndexReader struct {
	chunkID chunk.ChunkID
	entries []TokenIndexEntry // sorted by Token
}

// NewTokenIndexReader wraps a decoded set of token index entries for lookup.
// The entries must be sorted by Token (as produced by both file and memory indexers).
func NewTokenIndexReader(chunkID chunk.ChunkID, entries []TokenIndexEntry) *TokenIndexReader {
	return &TokenIndexReader{chunkID: chunkID, entries: entries}
}

// Lookup binary searches for token in the index.
// Returns (positions, true) if found, or (nil, false) if not present.
func (r *TokenIndexReader) Lookup(token string) ([]uint64, bool) {
	n := len(r.entries)
	if n == 0 {
		return nil, false
	}

	i := sort.Search(n, func(i int) bool {
		return r.entries[i].Token >= token
	})

	if i < n && r.entries[i].Token == token {
		return r.entries[i].Positions, true
	}

	return nil, false
}
