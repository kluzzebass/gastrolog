package kv

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
)

// OpenKeyIndex loads and returns a kv key index reader with its status.
func OpenKeyIndex(dir string, chunkID chunk.ChunkID) (*index.KVKeyIndexReader, index.KVIndexStatus, error) {
	entries, status, err := LoadKeyIndex(dir, chunkID)
	if err != nil {
		return nil, status, err
	}
	return index.NewKVKeyIndexReader(chunkID, entries), status, nil
}

// OpenValueIndex loads and returns a kv value index reader with its status.
func OpenValueIndex(dir string, chunkID chunk.ChunkID) (*index.KVValueIndexReader, index.KVIndexStatus, error) {
	entries, status, err := LoadValueIndex(dir, chunkID)
	if err != nil {
		return nil, status, err
	}
	return index.NewKVValueIndexReader(chunkID, entries), status, nil
}

// OpenKVIndex loads and returns a kv index reader with its status.
func OpenKVIndex(dir string, chunkID chunk.ChunkID) (*index.KVIndexReader, index.KVIndexStatus, error) {
	entries, status, err := LoadKVIndex(dir, chunkID)
	if err != nil {
		return nil, status, err
	}
	return index.NewKVIndexReader(chunkID, entries), status, nil
}
