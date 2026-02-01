package index

import (
	"sort"

	"gastrolog/internal/chunk"
)

// AttrKeyIndexReader provides binary search lookup over a loaded attr key index.
type AttrKeyIndexReader struct {
	chunkID chunk.ChunkID
	entries []AttrKeyIndexEntry // sorted by Key
}

// NewAttrKeyIndexReader wraps a decoded set of attr key index entries for lookup.
func NewAttrKeyIndexReader(chunkID chunk.ChunkID, entries []AttrKeyIndexEntry) *AttrKeyIndexReader {
	return &AttrKeyIndexReader{chunkID: chunkID, entries: entries}
}

// Lookup binary searches for key in the index.
// Returns (positions, true) if found, or (nil, false) if not present.
func (r *AttrKeyIndexReader) Lookup(key string) ([]uint64, bool) {
	n := len(r.entries)
	if n == 0 {
		return nil, false
	}

	i := sort.Search(n, func(i int) bool {
		return r.entries[i].Key >= key
	})

	if i < n && r.entries[i].Key == key {
		return r.entries[i].Positions, true
	}

	return nil, false
}

// AttrValueIndexReader provides binary search lookup over a loaded attr value index.
type AttrValueIndexReader struct {
	chunkID chunk.ChunkID
	entries []AttrValueIndexEntry // sorted by Value
}

// NewAttrValueIndexReader wraps a decoded set of attr value index entries for lookup.
func NewAttrValueIndexReader(chunkID chunk.ChunkID, entries []AttrValueIndexEntry) *AttrValueIndexReader {
	return &AttrValueIndexReader{chunkID: chunkID, entries: entries}
}

// Lookup binary searches for value in the index.
// Returns (positions, true) if found, or (nil, false) if not present.
func (r *AttrValueIndexReader) Lookup(value string) ([]uint64, bool) {
	n := len(r.entries)
	if n == 0 {
		return nil, false
	}

	i := sort.Search(n, func(i int) bool {
		return r.entries[i].Value >= value
	})

	if i < n && r.entries[i].Value == value {
		return r.entries[i].Positions, true
	}

	return nil, false
}

// AttrKVIndexReader provides binary search lookup over a loaded attr kv index.
type AttrKVIndexReader struct {
	chunkID chunk.ChunkID
	entries []AttrKVIndexEntry // sorted by (Key, Value)
}

// NewAttrKVIndexReader wraps a decoded set of attr kv index entries for lookup.
func NewAttrKVIndexReader(chunkID chunk.ChunkID, entries []AttrKVIndexEntry) *AttrKVIndexReader {
	return &AttrKVIndexReader{chunkID: chunkID, entries: entries}
}

// Lookup binary searches for (key, value) pair in the index.
// Returns (positions, true) if found, or (nil, false) if not present.
func (r *AttrKVIndexReader) Lookup(key, value string) ([]uint64, bool) {
	n := len(r.entries)
	if n == 0 {
		return nil, false
	}

	i := sort.Search(n, func(i int) bool {
		if r.entries[i].Key != key {
			return r.entries[i].Key >= key
		}
		return r.entries[i].Value >= value
	})

	if i < n && r.entries[i].Key == key && r.entries[i].Value == value {
		return r.entries[i].Positions, true
	}

	return nil, false
}
