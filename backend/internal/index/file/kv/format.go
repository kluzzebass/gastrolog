package kv

import (
	"errors"
	"fmt"
	"path/filepath"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
	"gastrolog/internal/index"
	"gastrolog/internal/index/idxmmap"
	"gastrolog/internal/index/inverted"
)

const (
	currentVersion = 0x01

	statusSize     = 1
	entryCountSize = 4
	headerSize     = format.HeaderSize + statusSize + entryCountSize

	keyIndexFileName   = "kv_key.idx"
	valueIndexFileName = "kv_val.idx"
	kvIndexFileName    = "kv_kv.idx"

	// Status byte values
	statusComplete = 0x00
	statusCapped   = 0x01
)

var (
	ErrInvalidStatus = inverted.ErrInvalidStatus
)

// Key index format

func encodeKeyIndex(entries []index.KVKeyIndexEntry, status index.KVIndexStatus) []byte {
	header := make([]byte, headerSize)
	h := format.Header{Type: format.TypeKVKeyIndex, Version: currentVersion, Flags: format.FlagComplete}
	h.EncodeInto(header)

	// Status byte
	if status == index.KVCapped {
		header[format.HeaderSize] = statusCapped
	} else {
		header[format.HeaderSize] = statusComplete
	}

	return inverted.EncodeKeyIndex(entries, header, format.HeaderSize+statusSize)
}

func decodeKeyIndex(data []byte) ([]index.KVKeyIndexEntry, index.KVIndexStatus, error) {
	if len(data) < headerSize {
		return nil, index.KVComplete, inverted.ErrIndexTooSmall
	}

	h, err := format.DecodeAndValidate(data, format.TypeKVKeyIndex, currentVersion)
	if err != nil {
		return nil, index.KVComplete, fmt.Errorf("kv key index: %w", err)
	}
	if h.Flags&format.FlagComplete == 0 {
		return nil, index.KVComplete, errors.New("kv key index: incomplete (missing complete flag)")
	}

	// Read status
	status, err := decodeStatus(data[format.HeaderSize])
	if err != nil {
		return nil, index.KVComplete, err
	}

	entries, err := inverted.DecodeKeyIndex(data, headerSize, func(key string, positions []uint64) index.KVKeyIndexEntry {
		return index.KVKeyIndexEntry{Key: key, Positions: positions}
	})
	if err != nil {
		return nil, status, err
	}

	return entries, status, nil
}

// Value index format

func encodeValueIndex(entries []index.KVValueIndexEntry, status index.KVIndexStatus) []byte {
	header := make([]byte, headerSize)
	h := format.Header{Type: format.TypeKVValueIndex, Version: currentVersion, Flags: format.FlagComplete}
	h.EncodeInto(header)

	// Status byte
	if status == index.KVCapped {
		header[format.HeaderSize] = statusCapped
	} else {
		header[format.HeaderSize] = statusComplete
	}

	return inverted.EncodeValueIndex(entries, header, format.HeaderSize+statusSize)
}

func decodeValueIndex(data []byte) ([]index.KVValueIndexEntry, index.KVIndexStatus, error) {
	if len(data) < headerSize {
		return nil, index.KVComplete, inverted.ErrIndexTooSmall
	}

	h, err := format.DecodeAndValidate(data, format.TypeKVValueIndex, currentVersion)
	if err != nil {
		return nil, index.KVComplete, fmt.Errorf("kv value index: %w", err)
	}
	if h.Flags&format.FlagComplete == 0 {
		return nil, index.KVComplete, errors.New("kv value index: incomplete (missing complete flag)")
	}

	// Read status
	status, err := decodeStatus(data[format.HeaderSize])
	if err != nil {
		return nil, index.KVComplete, err
	}

	entries, err := inverted.DecodeValueIndex(data, headerSize, func(value string, positions []uint64) index.KVValueIndexEntry {
		return index.KVValueIndexEntry{Value: value, Positions: positions}
	})
	if err != nil {
		return nil, status, err
	}

	return entries, status, nil
}

// KV index format

func encodeKVIndex(entries []index.KVIndexEntry, status index.KVIndexStatus) []byte {
	header := make([]byte, headerSize)
	h := format.Header{Type: format.TypeKVIndex, Version: currentVersion, Flags: format.FlagComplete}
	h.EncodeInto(header)

	// Status byte
	if status == index.KVCapped {
		header[format.HeaderSize] = statusCapped
	} else {
		header[format.HeaderSize] = statusComplete
	}

	return inverted.EncodeKVIndex(entries, header, format.HeaderSize+statusSize)
}

func decodeKVIndex(data []byte) ([]index.KVIndexEntry, index.KVIndexStatus, error) {
	if len(data) < headerSize {
		return nil, index.KVComplete, inverted.ErrIndexTooSmall
	}

	h, err := format.DecodeAndValidate(data, format.TypeKVIndex, currentVersion)
	if err != nil {
		return nil, index.KVComplete, fmt.Errorf("kv index: %w", err)
	}
	if h.Flags&format.FlagComplete == 0 {
		return nil, index.KVComplete, errors.New("kv index: incomplete (missing complete flag)")
	}

	// Read status
	status, err := decodeStatus(data[format.HeaderSize])
	if err != nil {
		return nil, index.KVComplete, err
	}

	entries, err := inverted.DecodeKVIndex(data, headerSize, func(key, value string, positions []uint64) index.KVIndexEntry {
		return index.KVIndexEntry{Key: key, Value: value, Positions: positions}
	})
	if err != nil {
		return nil, status, err
	}

	return entries, status, nil
}

func decodeStatus(b byte) (index.KVIndexStatus, error) {
	switch b {
	case statusComplete:
		return index.KVComplete, nil
	case statusCapped:
		return index.KVCapped, nil
	default:
		return index.KVComplete, ErrInvalidStatus
	}
}

// Load functions — all use idxmmap.Load to avoid slurping the index file
// into a heap-allocated []byte. The decoders return a 3-tuple (entries,
// status, error); the status is captured via closure since the generic
// helper only handles single-value returns. See gastrolog-3rvws.

func LoadKeyIndex(dir string, chunkID chunk.ChunkID) ([]index.KVKeyIndexEntry, index.KVIndexStatus, error) {
	status := index.KVComplete
	entries, err := idxmmap.Load(KeyIndexPath(dir, chunkID), func(data []byte) ([]index.KVKeyIndexEntry, error) {
		e, s, err := decodeKeyIndex(data)
		status = s
		return e, err
	})
	return entries, status, err
}

func LoadValueIndex(dir string, chunkID chunk.ChunkID) ([]index.KVValueIndexEntry, index.KVIndexStatus, error) {
	status := index.KVComplete
	entries, err := idxmmap.Load(ValueIndexPath(dir, chunkID), func(data []byte) ([]index.KVValueIndexEntry, error) {
		e, s, err := decodeValueIndex(data)
		status = s
		return e, err
	})
	return entries, status, err
}

func LoadKVIndex(dir string, chunkID chunk.ChunkID) ([]index.KVIndexEntry, index.KVIndexStatus, error) {
	status := index.KVComplete
	entries, err := idxmmap.Load(KVIndexPath(dir, chunkID), func(data []byte) ([]index.KVIndexEntry, error) {
		e, s, err := decodeKVIndex(data)
		status = s
		return e, err
	})
	return entries, status, err
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
