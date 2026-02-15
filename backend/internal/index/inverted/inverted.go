// Package inverted provides generic encode/decode for inverted index formats.
//
// This package supports three index types:
//   - Key index: maps a single string (key) to positions
//   - Value index: maps a single string (value) to positions
//   - KV index: maps a key-value pair to positions
//
// The binary format is:
//
//	[header][status?][entry_count:u32][string_table][posting_blob]
//
// String table entry (key or value index):
//
//	[str_len:u16][str_bytes][posting_offset:u32][posting_count:u32]
//
// String table entry (kv index):
//
//	[key_len:u16][key_bytes][val_len:u16][val_bytes][posting_offset:u32][posting_count:u32]
//
// Posting blob: concatenated position arrays, each position is u32.
package inverted

import (
	"encoding/binary"
	"errors"
)

// Size constants for binary format.
const (
	StringLenSize     = 2
	PostingOffsetSize = 4
	PostingCountSize  = 4
	PositionSize      = 4
	EntryCountSize    = 4
	StatusSize        = 1
)

var (
	ErrIndexTooSmall       = errors.New("index too small")
	ErrStringSizeMismatch  = errors.New("string table size mismatch")
	ErrPostingSizeMismatch = errors.New("posting list size mismatch")
	ErrInvalidStatus       = errors.New("invalid status byte")
)

// KeyEntry is an entry with a single string key and positions.
type KeyEntry interface {
	GetKey() string
	GetPositions() []uint64
}

// ValueEntry is an entry with a single string value and positions.
type ValueEntry interface {
	GetValue() string
	GetPositions() []uint64
}

// KVEntry is an entry with key, value, and positions.
type KVEntry interface {
	GetKey() string
	GetValue() string
	GetPositions() []uint64
}

// EncodeKeyIndex encodes a slice of key entries into binary format.
// headerBytes is prepended to the output (includes format header, optional status, entry count placeholder).
// The entry count is written at entryCountOffset within headerBytes.
func EncodeKeyIndex[T KeyEntry](entries []T, headerBytes []byte, entryCountOffset int) []byte {
	totalPositions := 0
	totalKeyBytes := 0
	for _, e := range entries {
		totalPositions += len(e.GetPositions())
		totalKeyBytes += len(e.GetKey())
	}

	stringTableSize := len(entries)*(StringLenSize+PostingOffsetSize+PostingCountSize) + totalKeyBytes
	postingBlobSize := totalPositions * PositionSize
	buf := make([]byte, len(headerBytes)+stringTableSize+postingBlobSize)

	// Copy header
	copy(buf, headerBytes)

	// Write entry count
	binary.LittleEndian.PutUint32(buf[entryCountOffset:], uint32(len(entries)))

	stringCursor := len(headerBytes)
	postingCursor := len(headerBytes) + stringTableSize
	postingOffset := 0

	for _, e := range entries {
		keyBytes := []byte(e.GetKey())
		binary.LittleEndian.PutUint16(buf[stringCursor:], uint16(len(keyBytes)))
		stringCursor += StringLenSize

		copy(buf[stringCursor:], keyBytes)
		stringCursor += len(keyBytes)

		binary.LittleEndian.PutUint32(buf[stringCursor:], uint32(postingOffset))
		stringCursor += PostingOffsetSize

		binary.LittleEndian.PutUint32(buf[stringCursor:], uint32(len(e.GetPositions())))
		stringCursor += PostingCountSize

		for _, pos := range e.GetPositions() {
			binary.LittleEndian.PutUint32(buf[postingCursor:], uint32(pos))
			postingCursor += PositionSize
		}

		postingOffset += len(e.GetPositions()) * PositionSize
	}

	return buf
}

// EncodeValueIndex encodes a slice of value entries into binary format.
func EncodeValueIndex[T ValueEntry](entries []T, headerBytes []byte, entryCountOffset int) []byte {
	totalPositions := 0
	totalValueBytes := 0
	for _, e := range entries {
		totalPositions += len(e.GetPositions())
		totalValueBytes += len(e.GetValue())
	}

	stringTableSize := len(entries)*(StringLenSize+PostingOffsetSize+PostingCountSize) + totalValueBytes
	postingBlobSize := totalPositions * PositionSize
	buf := make([]byte, len(headerBytes)+stringTableSize+postingBlobSize)

	copy(buf, headerBytes)
	binary.LittleEndian.PutUint32(buf[entryCountOffset:], uint32(len(entries)))

	stringCursor := len(headerBytes)
	postingCursor := len(headerBytes) + stringTableSize
	postingOffset := 0

	for _, e := range entries {
		valBytes := []byte(e.GetValue())
		binary.LittleEndian.PutUint16(buf[stringCursor:], uint16(len(valBytes)))
		stringCursor += StringLenSize

		copy(buf[stringCursor:], valBytes)
		stringCursor += len(valBytes)

		binary.LittleEndian.PutUint32(buf[stringCursor:], uint32(postingOffset))
		stringCursor += PostingOffsetSize

		binary.LittleEndian.PutUint32(buf[stringCursor:], uint32(len(e.GetPositions())))
		stringCursor += PostingCountSize

		for _, pos := range e.GetPositions() {
			binary.LittleEndian.PutUint32(buf[postingCursor:], uint32(pos))
			postingCursor += PositionSize
		}

		postingOffset += len(e.GetPositions()) * PositionSize
	}

	return buf
}

// EncodeKVIndex encodes a slice of kv entries into binary format.
func EncodeKVIndex[T KVEntry](entries []T, headerBytes []byte, entryCountOffset int) []byte {
	totalPositions := 0
	totalStringBytes := 0
	for _, e := range entries {
		totalPositions += len(e.GetPositions())
		totalStringBytes += len(e.GetKey()) + len(e.GetValue())
	}

	// Each entry: keyLen(2) + key + valLen(2) + val + offset(4) + count(4)
	stringTableSize := len(entries)*(StringLenSize+StringLenSize+PostingOffsetSize+PostingCountSize) + totalStringBytes
	postingBlobSize := totalPositions * PositionSize
	buf := make([]byte, len(headerBytes)+stringTableSize+postingBlobSize)

	copy(buf, headerBytes)
	binary.LittleEndian.PutUint32(buf[entryCountOffset:], uint32(len(entries)))

	stringCursor := len(headerBytes)
	postingCursor := len(headerBytes) + stringTableSize
	postingOffset := 0

	for _, e := range entries {
		keyBytes := []byte(e.GetKey())
		binary.LittleEndian.PutUint16(buf[stringCursor:], uint16(len(keyBytes)))
		stringCursor += StringLenSize
		copy(buf[stringCursor:], keyBytes)
		stringCursor += len(keyBytes)

		valBytes := []byte(e.GetValue())
		binary.LittleEndian.PutUint16(buf[stringCursor:], uint16(len(valBytes)))
		stringCursor += StringLenSize
		copy(buf[stringCursor:], valBytes)
		stringCursor += len(valBytes)

		binary.LittleEndian.PutUint32(buf[stringCursor:], uint32(postingOffset))
		stringCursor += PostingOffsetSize

		binary.LittleEndian.PutUint32(buf[stringCursor:], uint32(len(e.GetPositions())))
		stringCursor += PostingCountSize

		for _, pos := range e.GetPositions() {
			binary.LittleEndian.PutUint32(buf[postingCursor:], uint32(pos))
			postingCursor += PositionSize
		}

		postingOffset += len(e.GetPositions()) * PositionSize
	}

	return buf
}

// DecodeKeyIndex decodes a key index from binary data.
// dataStart is the offset where the string table begins (after header).
// The entry count is expected at dataStart-EntryCountSize.
// newEntry creates a new entry and sets its key and positions.
func DecodeKeyIndex[T any](data []byte, dataStart int, newEntry func(key string, positions []uint64) T) ([]T, error) {
	if len(data) < dataStart {
		return nil, ErrIndexTooSmall
	}

	entryCount := binary.LittleEndian.Uint32(data[dataStart-EntryCountSize : dataStart])

	// Scan to find posting blob start
	scanCursor := dataStart
	for i := uint32(0); i < entryCount; i++ {
		if scanCursor+StringLenSize > len(data) {
			return nil, ErrStringSizeMismatch
		}
		keyLen := int(binary.LittleEndian.Uint16(data[scanCursor : scanCursor+StringLenSize]))
		scanCursor += StringLenSize + keyLen + PostingOffsetSize + PostingCountSize
		if scanCursor > len(data) {
			return nil, ErrStringSizeMismatch
		}
	}

	postingBlobStart := scanCursor
	postingBlobSize := len(data) - postingBlobStart

	entries := make([]T, entryCount)
	cursor := dataStart
	for i := range entries {
		keyLen := int(binary.LittleEndian.Uint16(data[cursor : cursor+StringLenSize]))
		cursor += StringLenSize

		key := string(data[cursor : cursor+keyLen])
		cursor += keyLen

		pOffset := int(binary.LittleEndian.Uint32(data[cursor : cursor+PostingOffsetSize]))
		cursor += PostingOffsetSize

		pCount := int(binary.LittleEndian.Uint32(data[cursor : cursor+PostingCountSize]))
		cursor += PostingCountSize

		pEnd := pOffset + pCount*PositionSize
		if pEnd > postingBlobSize {
			return nil, ErrPostingSizeMismatch
		}

		positions := make([]uint64, pCount)
		pCursor := postingBlobStart + pOffset
		for j := range pCount {
			positions[j] = uint64(binary.LittleEndian.Uint32(data[pCursor : pCursor+PositionSize]))
			pCursor += PositionSize
		}

		entries[i] = newEntry(key, positions)
	}

	return entries, nil
}

// DecodeValueIndex decodes a value index from binary data.
// dataStart is the offset where the string table begins (after header).
// The entry count is expected at dataStart-EntryCountSize.
func DecodeValueIndex[T any](data []byte, dataStart int, newEntry func(value string, positions []uint64) T) ([]T, error) {
	if len(data) < dataStart {
		return nil, ErrIndexTooSmall
	}

	entryCount := binary.LittleEndian.Uint32(data[dataStart-EntryCountSize : dataStart])

	scanCursor := dataStart
	for i := uint32(0); i < entryCount; i++ {
		if scanCursor+StringLenSize > len(data) {
			return nil, ErrStringSizeMismatch
		}
		valLen := int(binary.LittleEndian.Uint16(data[scanCursor : scanCursor+StringLenSize]))
		scanCursor += StringLenSize + valLen + PostingOffsetSize + PostingCountSize
		if scanCursor > len(data) {
			return nil, ErrStringSizeMismatch
		}
	}

	postingBlobStart := scanCursor
	postingBlobSize := len(data) - postingBlobStart

	entries := make([]T, entryCount)
	cursor := dataStart
	for i := range entries {
		valLen := int(binary.LittleEndian.Uint16(data[cursor : cursor+StringLenSize]))
		cursor += StringLenSize

		value := string(data[cursor : cursor+valLen])
		cursor += valLen

		pOffset := int(binary.LittleEndian.Uint32(data[cursor : cursor+PostingOffsetSize]))
		cursor += PostingOffsetSize

		pCount := int(binary.LittleEndian.Uint32(data[cursor : cursor+PostingCountSize]))
		cursor += PostingCountSize

		pEnd := pOffset + pCount*PositionSize
		if pEnd > postingBlobSize {
			return nil, ErrPostingSizeMismatch
		}

		positions := make([]uint64, pCount)
		pCursor := postingBlobStart + pOffset
		for j := range pCount {
			positions[j] = uint64(binary.LittleEndian.Uint32(data[pCursor : pCursor+PositionSize]))
			pCursor += PositionSize
		}

		entries[i] = newEntry(value, positions)
	}

	return entries, nil
}

// DecodeKVIndex decodes a kv index from binary data.
// dataStart is the offset where the string table begins (after header).
// The entry count is expected at dataStart-EntryCountSize.
func DecodeKVIndex[T any](data []byte, dataStart int, newEntry func(key, value string, positions []uint64) T) ([]T, error) {
	if len(data) < dataStart {
		return nil, ErrIndexTooSmall
	}

	entryCount := binary.LittleEndian.Uint32(data[dataStart-EntryCountSize : dataStart])

	scanCursor := dataStart
	for i := uint32(0); i < entryCount; i++ {
		if scanCursor+StringLenSize > len(data) {
			return nil, ErrStringSizeMismatch
		}
		keyLen := int(binary.LittleEndian.Uint16(data[scanCursor : scanCursor+StringLenSize]))
		scanCursor += StringLenSize + keyLen

		if scanCursor+StringLenSize > len(data) {
			return nil, ErrStringSizeMismatch
		}
		valLen := int(binary.LittleEndian.Uint16(data[scanCursor : scanCursor+StringLenSize]))
		scanCursor += StringLenSize + valLen + PostingOffsetSize + PostingCountSize
		if scanCursor > len(data) {
			return nil, ErrStringSizeMismatch
		}
	}

	postingBlobStart := scanCursor
	postingBlobSize := len(data) - postingBlobStart

	entries := make([]T, entryCount)
	cursor := dataStart
	for i := range entries {
		keyLen := int(binary.LittleEndian.Uint16(data[cursor : cursor+StringLenSize]))
		cursor += StringLenSize
		key := string(data[cursor : cursor+keyLen])
		cursor += keyLen

		valLen := int(binary.LittleEndian.Uint16(data[cursor : cursor+StringLenSize]))
		cursor += StringLenSize
		value := string(data[cursor : cursor+valLen])
		cursor += valLen

		pOffset := int(binary.LittleEndian.Uint32(data[cursor : cursor+PostingOffsetSize]))
		cursor += PostingOffsetSize

		pCount := int(binary.LittleEndian.Uint32(data[cursor : cursor+PostingCountSize]))
		cursor += PostingCountSize

		pEnd := pOffset + pCount*PositionSize
		if pEnd > postingBlobSize {
			return nil, ErrPostingSizeMismatch
		}

		positions := make([]uint64, pCount)
		pCursor := postingBlobStart + pOffset
		for j := range pCount {
			positions[j] = uint64(binary.LittleEndian.Uint32(data[pCursor : pCursor+PositionSize]))
			pCursor += PositionSize
		}

		entries[i] = newEntry(key, value, positions)
	}

	return entries, nil
}
