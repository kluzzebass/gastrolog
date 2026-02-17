package index

import (
	"sort"

	"gastrolog/internal/chunk"
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

// LookupPrefix returns the union of all positions for tokens that start with the given prefix.
// Uses binary search to find the start, then scans forward while the prefix matches.
// Returns (positions, true) if any tokens matched, (nil, false) if none matched.
func (r *TokenIndexReader) LookupPrefix(prefix string) ([]uint64, bool) {
	n := len(r.entries)
	if n == 0 || prefix == "" {
		return nil, false
	}

	// Binary search for first entry >= prefix.
	i := sort.Search(n, func(i int) bool {
		return r.entries[i].Token >= prefix
	})

	// Scan forward while tokens start with prefix, unioning positions.
	var result []uint64
	for i < n && len(r.entries[i].Token) >= len(prefix) && r.entries[i].Token[:len(prefix)] == prefix {
		result = unionPositions(result, r.entries[i].Positions)
		i++
	}

	if len(result) == 0 {
		return nil, false
	}
	return result, true
}

// unionPositions returns all unique positions from both sorted slices, in sorted order.
func unionPositions(a, b []uint64) []uint64 {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	result := make([]uint64, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] == b[j] {
			result = append(result, a[i])
			i++
			j++
		} else if a[i] < b[j] {
			result = append(result, a[i])
			i++
		} else {
			result = append(result, b[j])
			j++
		}
	}
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}
