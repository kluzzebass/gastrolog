package source

import (
	"github.com/kluzzebass/gastrolog/internal/chunk"
	"github.com/kluzzebass/gastrolog/internal/index"
)

// Open loads and validates a source index file, returning a reader for
// binary search lookup by SourceID.
func Open(dir string, chunkID chunk.ChunkID) (*index.SourceIndexReader, error) {
	entries, err := LoadIndex(dir, chunkID)
	if err != nil {
		return nil, err
	}
	return index.NewSourceIndexReader(chunkID, entries), nil
}
