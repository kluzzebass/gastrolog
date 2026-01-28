package token

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
)

// Open loads and validates a token index file, returning a reader for
// binary search lookup by token.
func Open(dir string, chunkID chunk.ChunkID) (*index.TokenIndexReader, error) {
	entries, err := LoadIndex(dir, chunkID)
	if err != nil {
		return nil, err
	}
	return index.NewTokenIndexReader(chunkID, entries), nil
}
