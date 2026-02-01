package index

import (
	"sort"
	"strings"

	"gastrolog/internal/chunk"
)

// KVKeyIndexReader provides binary search lookup over a loaded kv key index.
type KVKeyIndexReader struct {
	chunkID chunk.ChunkID
	entries []KVKeyIndexEntry // sorted by Key
}

// NewKVKeyIndexReader wraps a decoded set of kv key index entries for lookup.
func NewKVKeyIndexReader(chunkID chunk.ChunkID, entries []KVKeyIndexEntry) *KVKeyIndexReader {
	return &KVKeyIndexReader{chunkID: chunkID, entries: entries}
}

// Lookup binary searches for key in the index.
// Key is matched case-insensitively (compared lowercase).
// Returns (positions, true) if found, or (nil, false) if not present.
func (r *KVKeyIndexReader) Lookup(key string) ([]uint64, bool) {
	keyLower := strings.ToLower(key)
	n := len(r.entries)
	if n == 0 {
		return nil, false
	}

	i := sort.Search(n, func(i int) bool {
		return r.entries[i].Key >= keyLower
	})

	if i < n && r.entries[i].Key == keyLower {
		return r.entries[i].Positions, true
	}

	return nil, false
}

// KVValueIndexReader provides binary search lookup over a loaded kv value index.
type KVValueIndexReader struct {
	chunkID chunk.ChunkID
	entries []KVValueIndexEntry // sorted by Value
}

// NewKVValueIndexReader wraps a decoded set of kv value index entries for lookup.
func NewKVValueIndexReader(chunkID chunk.ChunkID, entries []KVValueIndexEntry) *KVValueIndexReader {
	return &KVValueIndexReader{chunkID: chunkID, entries: entries}
}

// Lookup binary searches for value in the index.
// Value is matched case-insensitively (compared lowercase).
// Returns (positions, true) if found, or (nil, false) if not present.
func (r *KVValueIndexReader) Lookup(value string) ([]uint64, bool) {
	valueLower := strings.ToLower(value)
	n := len(r.entries)
	if n == 0 {
		return nil, false
	}

	i := sort.Search(n, func(i int) bool {
		return r.entries[i].Value >= valueLower
	})

	if i < n && r.entries[i].Value == valueLower {
		return r.entries[i].Positions, true
	}

	return nil, false
}

// KVIndexReader provides lookup operations on a kv index.
type KVIndexReader struct {
	chunkID chunk.ChunkID
	entries []KVIndexEntry
}

// NewKVIndexReader creates a reader for the given kv index entries.
// Entries must be sorted by (key, value) for binary search to work.
func NewKVIndexReader(chunkID chunk.ChunkID, entries []KVIndexEntry) *KVIndexReader {
	return &KVIndexReader{
		chunkID: chunkID,
		entries: entries,
	}
}

// Entries returns the underlying index entries.
func (r *KVIndexReader) Entries() []KVIndexEntry {
	return r.entries
}

// Lookup finds positions for records containing the given key=value pair.
// Both key and value are matched case-insensitively (compared lowercase).
// Returns the positions and true if found, or nil and false if not found.
func (r *KVIndexReader) Lookup(key, value string) ([]uint64, bool) {
	keyLower := strings.ToLower(key)
	valueLower := strings.ToLower(value)

	// Binary search for the key=value pair
	i := sort.Search(len(r.entries), func(i int) bool {
		if r.entries[i].Key != keyLower {
			return r.entries[i].Key >= keyLower
		}
		return r.entries[i].Value >= valueLower
	})

	if i < len(r.entries) && r.entries[i].Key == keyLower && r.entries[i].Value == valueLower {
		return r.entries[i].Positions, true
	}
	return nil, false
}
