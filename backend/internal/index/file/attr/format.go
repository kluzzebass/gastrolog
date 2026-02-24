package attr

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
	"gastrolog/internal/index"
	"gastrolog/internal/index/inverted"
)

const (
	currentVersion = 0x01

	entryCountSize = 4
	headerSize     = format.HeaderSize + entryCountSize

	keyIndexFileName   = "_attr_key.idx"
	valueIndexFileName = "_attr_val.idx"
	kvIndexFileName    = "_attr_kv.idx"
)

// Key index format

func encodeKeyIndex(entries []index.AttrKeyIndexEntry) []byte {
	header := make([]byte, headerSize)
	h := format.Header{Type: format.TypeAttrKeyIndex, Version: currentVersion, Flags: format.FlagComplete}
	h.EncodeInto(header)
	return inverted.EncodeKeyIndex(entries, header, format.HeaderSize)
}

func decodeKeyIndex(data []byte) ([]index.AttrKeyIndexEntry, error) {
	if len(data) < headerSize {
		return nil, inverted.ErrIndexTooSmall
	}

	h, err := format.DecodeAndValidate(data, format.TypeAttrKeyIndex, currentVersion)
	if err != nil {
		return nil, fmt.Errorf("attr key index: %w", err)
	}
	if h.Flags&format.FlagComplete == 0 {
		return nil, errors.New("attr key index: incomplete (missing complete flag)")
	}

	return inverted.DecodeKeyIndex(data, headerSize, func(key string, positions []uint64) index.AttrKeyIndexEntry {
		return index.AttrKeyIndexEntry{Key: key, Positions: positions}
	})
}

// Value index format

func encodeValueIndex(entries []index.AttrValueIndexEntry) []byte {
	header := make([]byte, headerSize)
	h := format.Header{Type: format.TypeAttrValueIndex, Version: currentVersion, Flags: format.FlagComplete}
	h.EncodeInto(header)
	return inverted.EncodeValueIndex(entries, header, format.HeaderSize)
}

func decodeValueIndex(data []byte) ([]index.AttrValueIndexEntry, error) {
	if len(data) < headerSize {
		return nil, inverted.ErrIndexTooSmall
	}

	h, err := format.DecodeAndValidate(data, format.TypeAttrValueIndex, currentVersion)
	if err != nil {
		return nil, fmt.Errorf("attr value index: %w", err)
	}
	if h.Flags&format.FlagComplete == 0 {
		return nil, errors.New("attr value index: incomplete (missing complete flag)")
	}

	return inverted.DecodeValueIndex(data, headerSize, func(value string, positions []uint64) index.AttrValueIndexEntry {
		return index.AttrValueIndexEntry{Value: value, Positions: positions}
	})
}

// KV index format

func encodeKVIndex(entries []index.AttrKVIndexEntry) []byte {
	header := make([]byte, headerSize)
	h := format.Header{Type: format.TypeAttrKVIndex, Version: currentVersion, Flags: format.FlagComplete}
	h.EncodeInto(header)
	return inverted.EncodeKVIndex(entries, header, format.HeaderSize)
}

func decodeKVIndex(data []byte) ([]index.AttrKVIndexEntry, error) {
	if len(data) < headerSize {
		return nil, inverted.ErrIndexTooSmall
	}

	h, err := format.DecodeAndValidate(data, format.TypeAttrKVIndex, currentVersion)
	if err != nil {
		return nil, fmt.Errorf("attr kv index: %w", err)
	}
	if h.Flags&format.FlagComplete == 0 {
		return nil, errors.New("attr kv index: incomplete (missing complete flag)")
	}

	return inverted.DecodeKVIndex(data, headerSize, func(key, value string, positions []uint64) index.AttrKVIndexEntry {
		return index.AttrKVIndexEntry{Key: key, Value: value, Positions: positions}
	})
}

// Load functions

func LoadKeyIndex(dir string, chunkID chunk.ChunkID) ([]index.AttrKeyIndexEntry, error) {
	path := KeyIndexPath(dir, chunkID)
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("read attr key index: %w", err)
	}
	return decodeKeyIndex(data)
}

func LoadValueIndex(dir string, chunkID chunk.ChunkID) ([]index.AttrValueIndexEntry, error) {
	path := ValueIndexPath(dir, chunkID)
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("read attr value index: %w", err)
	}
	return decodeValueIndex(data)
}

func LoadKVIndex(dir string, chunkID chunk.ChunkID) ([]index.AttrKVIndexEntry, error) {
	path := KVIndexPath(dir, chunkID)
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("read attr kv index: %w", err)
	}
	return decodeKVIndex(data)
}

// Path helpers

func KeyIndexPath(dir string, chunkID chunk.ChunkID) string {
	return filepath.Join(dir, chunkID.String(), keyIndexFileName)
}

func ValueIndexPath(dir string, chunkID chunk.ChunkID) string {
	return filepath.Join(dir, chunkID.String(), valueIndexFileName)
}

func KVIndexPath(dir string, chunkID chunk.ChunkID) string {
	return filepath.Join(dir, chunkID.String(), kvIndexFileName)
}

// Lowercase helper for keys and values
func lowercase(s string) string {
	return strings.ToLower(s)
}

// TempFilePattern helpers for cleanup

func KeyTempFilePattern(dir string, chunkID chunk.ChunkID) string {
	return filepath.Join(dir, chunkID.String(), keyIndexFileName+".tmp.*")
}

func ValueTempFilePattern(dir string, chunkID chunk.ChunkID) string {
	return filepath.Join(dir, chunkID.String(), valueIndexFileName+".tmp.*")
}

func KVTempFilePattern(dir string, chunkID chunk.ChunkID) string {
	return filepath.Join(dir, chunkID.String(), kvIndexFileName+".tmp.*")
}
