package index

import (
	"sort"
	"strings"

	"gastrolog/internal/chunk"
)

// JSONIndexReader provides lookup operations over loaded JSON index entries.
type JSONIndexReader struct {
	chunkID    chunk.ChunkID
	pathIdx    []JSONPathIndexEntry // sorted by Path
	pvIdx      []JSONPVIndexEntry   // sorted by (Path, Value)
	pathStatus JSONIndexStatus
	pvStatus   JSONIndexStatus
}

// NewJSONIndexReader creates a reader for the given JSON index entries.
// pathEntries must be sorted by Path, pvEntries must be sorted by (Path, Value).
func NewJSONIndexReader(
	chunkID chunk.ChunkID,
	pathEntries []JSONPathIndexEntry,
	pathStatus JSONIndexStatus,
	pvEntries []JSONPVIndexEntry,
	pvStatus JSONIndexStatus,
) *JSONIndexReader {
	return &JSONIndexReader{
		chunkID:    chunkID,
		pathIdx:    pathEntries,
		pvIdx:      pvEntries,
		pathStatus: pathStatus,
		pvStatus:   pvStatus,
	}
}

// LookupPath finds positions for records containing the given JSON path.
// Path is matched case-sensitively (paths are stored as-is from the JSON keys).
// Returns (positions, true) if found, or (nil, false) if not present.
func (r *JSONIndexReader) LookupPath(path string) ([]uint64, bool) {
	path = strings.ToLower(path)
	n := len(r.pathIdx)
	if n == 0 {
		return nil, false
	}

	i := sort.Search(n, func(i int) bool {
		return r.pathIdx[i].Path >= path
	})

	if i < n && r.pathIdx[i].Path == path {
		return r.pathIdx[i].Positions, true
	}
	return nil, false
}

// LookupPathValue finds positions for records containing the given JSON path=value pair.
// Both path and value are matched case-insensitively.
// Returns (positions, true) if found, or (nil, false) if not present.
func (r *JSONIndexReader) LookupPathValue(path, value string) ([]uint64, bool) {
	path = strings.ToLower(path)
	value = strings.ToLower(value)
	n := len(r.pvIdx)
	if n == 0 {
		return nil, false
	}

	i := sort.Search(n, func(i int) bool {
		if r.pvIdx[i].Path != path {
			return r.pvIdx[i].Path >= path
		}
		return r.pvIdx[i].Value >= value
	})

	if i < n && r.pvIdx[i].Path == path && r.pvIdx[i].Value == value {
		return r.pvIdx[i].Positions, true
	}
	return nil, false
}

// LookupPathPrefix finds positions for records containing any JSON path with the given prefix.
// Returns the union of positions from all matching paths.
// The prefix is separated by null bytes, so "service" matches "service", "service\x00name", etc.
func (r *JSONIndexReader) LookupPathPrefix(prefix string) ([]uint64, bool) {
	prefix = strings.ToLower(prefix)
	n := len(r.pathIdx)
	if n == 0 {
		return nil, false
	}

	// Find first entry >= prefix
	i := sort.Search(n, func(i int) bool {
		return r.pathIdx[i].Path >= prefix
	})

	var positions []uint64
	for i < n {
		entry := r.pathIdx[i]
		// Check if this path starts with prefix and is either exact match
		// or followed by null byte (path separator).
		if !strings.HasPrefix(entry.Path, prefix) {
			break
		}
		if len(entry.Path) == len(prefix) || entry.Path[len(prefix)] == 0 {
			positions = append(positions, entry.Positions...)
		}
		i++
	}

	if len(positions) == 0 {
		return nil, false
	}

	// Sort and deduplicate positions.
	sort.Slice(positions, func(i, j int) bool { return positions[i] < positions[j] })
	positions = dedup(positions)

	return positions, true
}

// PathStatus returns the status of the path index.
func (r *JSONIndexReader) PathStatus() JSONIndexStatus {
	return r.pathStatus
}

// PVStatus returns the status of the path-value index.
func (r *JSONIndexReader) PVStatus() JSONIndexStatus {
	return r.pvStatus
}

// dedup removes duplicates from a sorted uint64 slice in place.
func dedup(s []uint64) []uint64 {
	if len(s) <= 1 {
		return s
	}
	j := 0
	for i := 1; i < len(s); i++ {
		if s[i] != s[j] {
			j++
			s[j] = s[i]
		}
	}
	return s[:j+1]
}
