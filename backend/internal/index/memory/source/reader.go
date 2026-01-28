package source

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
)

// Open retrieves the in-memory source index entries for the given chunk,
// returning a reader for binary search lookup by SourceID.
func Open(indexer *Indexer, chunkID chunk.ChunkID) (*index.SourceIndexReader, error) {
	entries, ok := indexer.Get(chunkID)
	if !ok {
		return nil, index.ErrIndexNotFound
	}
	return index.NewSourceIndexReader(chunkID, entries), nil
}
