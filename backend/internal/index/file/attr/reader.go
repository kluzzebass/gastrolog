package attr

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
)

// OpenKeyIndex loads and returns an attr key index reader.
func OpenKeyIndex(dir string, chunkID chunk.ChunkID) (*index.AttrKeyIndexReader, error) {
	entries, err := LoadKeyIndex(dir, chunkID)
	if err != nil {
		return nil, err
	}
	return index.NewAttrKeyIndexReader(chunkID, entries), nil
}

// OpenValueIndex loads and returns an attr value index reader.
func OpenValueIndex(dir string, chunkID chunk.ChunkID) (*index.AttrValueIndexReader, error) {
	entries, err := LoadValueIndex(dir, chunkID)
	if err != nil {
		return nil, err
	}
	return index.NewAttrValueIndexReader(chunkID, entries), nil
}

// OpenKVIndex loads and returns an attr kv index reader.
func OpenKVIndex(dir string, chunkID chunk.ChunkID) (*index.AttrKVIndexReader, error) {
	entries, err := LoadKVIndex(dir, chunkID)
	if err != nil {
		return nil, err
	}
	return index.NewAttrKVIndexReader(chunkID, entries), nil
}
