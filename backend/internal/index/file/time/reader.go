package time

import (
	"github.com/kluzzebass/gastrolog/internal/chunk"
	"github.com/kluzzebass/gastrolog/internal/index"
)

// Open loads and validates a time index file, returning a reader for
// binary search positioning.
func Open(dir string, chunkID chunk.ChunkID) (*index.TimeIndexReader, error) {
	entries, err := LoadIndex(dir, chunkID)
	if err != nil {
		return nil, err
	}
	return index.NewTimeIndexReader(chunkID, entries), nil
}
