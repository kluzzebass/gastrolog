package time

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
)

// Open retrieves the in-memory time index entries for the given chunk,
// returning a reader for binary search positioning.
func Open(indexer *Indexer, chunkID chunk.ChunkID) (*index.TimeIndexReader, error) {
	entries, ok := indexer.Get(chunkID)
	if !ok {
		return nil, index.ErrIndexNotFound
	}
	return index.NewTimeIndexReader(chunkID, entries), nil
}
