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

// PVStatus returns the status of the path-value index.
func (r *JSONIndexReader) PVStatus() JSONIndexStatus {
	return r.pvStatus
}

