package attr

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
	"gastrolog/internal/index"
)

const (
	currentVersion = 0x01

	entryCountSize    = 4
	headerSize        = format.HeaderSize + entryCountSize
	stringLenSize     = 2
	postingOffsetSize = 4
	postingCountSize  = 4
	positionSize      = 4

	keyIndexFileName   = "_attr_key.idx"
	valueIndexFileName = "_attr_val.idx"
	kvIndexFileName    = "_attr_kv.idx"
)

var (
	ErrIndexTooSmall       = errors.New("attr index too small")
	ErrKeySizeMismatch     = errors.New("attr index key table size mismatch")
	ErrPostingSizeMismatch = errors.New("attr index posting list size mismatch")
)

// Key index format

func encodeKeyIndex(entries []index.AttrKeyIndexEntry) []byte {
	totalPositions := 0
	totalKeyBytes := 0
	for _, e := range entries {
		totalPositions += len(e.Positions)
		totalKeyBytes += len(e.Key)
	}

	keyTableSize := len(entries)*(stringLenSize+postingOffsetSize+postingCountSize) + totalKeyBytes
	postingBlobSize := totalPositions * positionSize
	buf := make([]byte, headerSize+keyTableSize+postingBlobSize)

	cursor := 0
	h := format.Header{Type: format.TypeAttrKeyIndex, Version: currentVersion, Flags: 0}
	cursor += h.EncodeInto(buf[cursor:])

	binary.LittleEndian.PutUint32(buf[cursor:cursor+entryCountSize], uint32(len(entries)))
	cursor += entryCountSize

	keyCursor := cursor
	postingCursor := headerSize + keyTableSize
	postingOffset := 0

	for _, e := range entries {
		keyBytes := []byte(e.Key)
		binary.LittleEndian.PutUint16(buf[keyCursor:keyCursor+stringLenSize], uint16(len(keyBytes)))
		keyCursor += stringLenSize

		copy(buf[keyCursor:keyCursor+len(keyBytes)], keyBytes)
		keyCursor += len(keyBytes)

		binary.LittleEndian.PutUint32(buf[keyCursor:keyCursor+postingOffsetSize], uint32(postingOffset))
		keyCursor += postingOffsetSize

		binary.LittleEndian.PutUint32(buf[keyCursor:keyCursor+postingCountSize], uint32(len(e.Positions)))
		keyCursor += postingCountSize

		for _, pos := range e.Positions {
			binary.LittleEndian.PutUint32(buf[postingCursor:postingCursor+positionSize], uint32(pos))
			postingCursor += positionSize
		}

		postingOffset += len(e.Positions) * positionSize
	}

	return buf
}

func decodeKeyIndex(data []byte) ([]index.AttrKeyIndexEntry, error) {
	if len(data) < headerSize {
		return nil, ErrIndexTooSmall
	}

	_, err := format.DecodeAndValidate(data, format.TypeAttrKeyIndex, currentVersion)
	if err != nil {
		return nil, fmt.Errorf("attr key index: %w", err)
	}
	cursor := format.HeaderSize

	entryCount := binary.LittleEndian.Uint32(data[cursor : cursor+entryCountSize])
	cursor += entryCountSize

	scanCursor := cursor
	for i := uint32(0); i < entryCount; i++ {
		if scanCursor+stringLenSize > len(data) {
			return nil, ErrKeySizeMismatch
		}
		keyLen := int(binary.LittleEndian.Uint16(data[scanCursor : scanCursor+stringLenSize]))
		scanCursor += stringLenSize + keyLen + postingOffsetSize + postingCountSize
		if scanCursor > len(data) {
			return nil, ErrKeySizeMismatch
		}
	}

	postingBlobStart := scanCursor
	postingBlobSize := len(data) - postingBlobStart

	entries := make([]index.AttrKeyIndexEntry, entryCount)
	for i := range entries {
		keyLen := int(binary.LittleEndian.Uint16(data[cursor : cursor+stringLenSize]))
		cursor += stringLenSize

		entries[i].Key = string(data[cursor : cursor+keyLen])
		cursor += keyLen

		pOffset := int(binary.LittleEndian.Uint32(data[cursor : cursor+postingOffsetSize]))
		cursor += postingOffsetSize

		pCount := int(binary.LittleEndian.Uint32(data[cursor : cursor+postingCountSize]))
		cursor += postingCountSize

		pEnd := pOffset + pCount*positionSize
		if pEnd > postingBlobSize {
			return nil, ErrPostingSizeMismatch
		}

		entries[i].Positions = make([]uint64, pCount)
		pCursor := postingBlobStart + pOffset
		for j := 0; j < pCount; j++ {
			entries[i].Positions[j] = uint64(binary.LittleEndian.Uint32(data[pCursor : pCursor+positionSize]))
			pCursor += positionSize
		}
	}

	return entries, nil
}

// Value index format

func encodeValueIndex(entries []index.AttrValueIndexEntry) []byte {
	totalPositions := 0
	totalValueBytes := 0
	for _, e := range entries {
		totalPositions += len(e.Positions)
		totalValueBytes += len(e.Value)
	}

	keyTableSize := len(entries)*(stringLenSize+postingOffsetSize+postingCountSize) + totalValueBytes
	postingBlobSize := totalPositions * positionSize
	buf := make([]byte, headerSize+keyTableSize+postingBlobSize)

	cursor := 0
	h := format.Header{Type: format.TypeAttrValueIndex, Version: currentVersion, Flags: 0}
	cursor += h.EncodeInto(buf[cursor:])

	binary.LittleEndian.PutUint32(buf[cursor:cursor+entryCountSize], uint32(len(entries)))
	cursor += entryCountSize

	keyCursor := cursor
	postingCursor := headerSize + keyTableSize
	postingOffset := 0

	for _, e := range entries {
		valBytes := []byte(e.Value)
		binary.LittleEndian.PutUint16(buf[keyCursor:keyCursor+stringLenSize], uint16(len(valBytes)))
		keyCursor += stringLenSize

		copy(buf[keyCursor:keyCursor+len(valBytes)], valBytes)
		keyCursor += len(valBytes)

		binary.LittleEndian.PutUint32(buf[keyCursor:keyCursor+postingOffsetSize], uint32(postingOffset))
		keyCursor += postingOffsetSize

		binary.LittleEndian.PutUint32(buf[keyCursor:keyCursor+postingCountSize], uint32(len(e.Positions)))
		keyCursor += postingCountSize

		for _, pos := range e.Positions {
			binary.LittleEndian.PutUint32(buf[postingCursor:postingCursor+positionSize], uint32(pos))
			postingCursor += positionSize
		}

		postingOffset += len(e.Positions) * positionSize
	}

	return buf
}

func decodeValueIndex(data []byte) ([]index.AttrValueIndexEntry, error) {
	if len(data) < headerSize {
		return nil, ErrIndexTooSmall
	}

	_, err := format.DecodeAndValidate(data, format.TypeAttrValueIndex, currentVersion)
	if err != nil {
		return nil, fmt.Errorf("attr value index: %w", err)
	}
	cursor := format.HeaderSize

	entryCount := binary.LittleEndian.Uint32(data[cursor : cursor+entryCountSize])
	cursor += entryCountSize

	scanCursor := cursor
	for i := uint32(0); i < entryCount; i++ {
		if scanCursor+stringLenSize > len(data) {
			return nil, ErrKeySizeMismatch
		}
		valLen := int(binary.LittleEndian.Uint16(data[scanCursor : scanCursor+stringLenSize]))
		scanCursor += stringLenSize + valLen + postingOffsetSize + postingCountSize
		if scanCursor > len(data) {
			return nil, ErrKeySizeMismatch
		}
	}

	postingBlobStart := scanCursor
	postingBlobSize := len(data) - postingBlobStart

	entries := make([]index.AttrValueIndexEntry, entryCount)
	for i := range entries {
		valLen := int(binary.LittleEndian.Uint16(data[cursor : cursor+stringLenSize]))
		cursor += stringLenSize

		entries[i].Value = string(data[cursor : cursor+valLen])
		cursor += valLen

		pOffset := int(binary.LittleEndian.Uint32(data[cursor : cursor+postingOffsetSize]))
		cursor += postingOffsetSize

		pCount := int(binary.LittleEndian.Uint32(data[cursor : cursor+postingCountSize]))
		cursor += postingCountSize

		pEnd := pOffset + pCount*positionSize
		if pEnd > postingBlobSize {
			return nil, ErrPostingSizeMismatch
		}

		entries[i].Positions = make([]uint64, pCount)
		pCursor := postingBlobStart + pOffset
		for j := 0; j < pCount; j++ {
			entries[i].Positions[j] = uint64(binary.LittleEndian.Uint32(data[pCursor : pCursor+positionSize]))
			pCursor += positionSize
		}
	}

	return entries, nil
}

// KV index format

func encodeKVIndex(entries []index.AttrKVIndexEntry) []byte {
	totalPositions := 0
	totalStringBytes := 0
	for _, e := range entries {
		totalPositions += len(e.Positions)
		totalStringBytes += len(e.Key) + len(e.Value)
	}

	// Each entry: keyLen(2) + key + valLen(2) + val + offset(4) + count(4)
	keyTableSize := len(entries)*(stringLenSize+stringLenSize+postingOffsetSize+postingCountSize) + totalStringBytes
	postingBlobSize := totalPositions * positionSize
	buf := make([]byte, headerSize+keyTableSize+postingBlobSize)

	cursor := 0
	h := format.Header{Type: format.TypeAttrKVIndex, Version: currentVersion, Flags: 0}
	cursor += h.EncodeInto(buf[cursor:])

	binary.LittleEndian.PutUint32(buf[cursor:cursor+entryCountSize], uint32(len(entries)))
	cursor += entryCountSize

	keyCursor := cursor
	postingCursor := headerSize + keyTableSize
	postingOffset := 0

	for _, e := range entries {
		keyBytes := []byte(e.Key)
		binary.LittleEndian.PutUint16(buf[keyCursor:keyCursor+stringLenSize], uint16(len(keyBytes)))
		keyCursor += stringLenSize
		copy(buf[keyCursor:keyCursor+len(keyBytes)], keyBytes)
		keyCursor += len(keyBytes)

		valBytes := []byte(e.Value)
		binary.LittleEndian.PutUint16(buf[keyCursor:keyCursor+stringLenSize], uint16(len(valBytes)))
		keyCursor += stringLenSize
		copy(buf[keyCursor:keyCursor+len(valBytes)], valBytes)
		keyCursor += len(valBytes)

		binary.LittleEndian.PutUint32(buf[keyCursor:keyCursor+postingOffsetSize], uint32(postingOffset))
		keyCursor += postingOffsetSize

		binary.LittleEndian.PutUint32(buf[keyCursor:keyCursor+postingCountSize], uint32(len(e.Positions)))
		keyCursor += postingCountSize

		for _, pos := range e.Positions {
			binary.LittleEndian.PutUint32(buf[postingCursor:postingCursor+positionSize], uint32(pos))
			postingCursor += positionSize
		}

		postingOffset += len(e.Positions) * positionSize
	}

	return buf
}

func decodeKVIndex(data []byte) ([]index.AttrKVIndexEntry, error) {
	if len(data) < headerSize {
		return nil, ErrIndexTooSmall
	}

	_, err := format.DecodeAndValidate(data, format.TypeAttrKVIndex, currentVersion)
	if err != nil {
		return nil, fmt.Errorf("attr kv index: %w", err)
	}
	cursor := format.HeaderSize

	entryCount := binary.LittleEndian.Uint32(data[cursor : cursor+entryCountSize])
	cursor += entryCountSize

	scanCursor := cursor
	for i := uint32(0); i < entryCount; i++ {
		if scanCursor+stringLenSize > len(data) {
			return nil, ErrKeySizeMismatch
		}
		keyLen := int(binary.LittleEndian.Uint16(data[scanCursor : scanCursor+stringLenSize]))
		scanCursor += stringLenSize + keyLen

		if scanCursor+stringLenSize > len(data) {
			return nil, ErrKeySizeMismatch
		}
		valLen := int(binary.LittleEndian.Uint16(data[scanCursor : scanCursor+stringLenSize]))
		scanCursor += stringLenSize + valLen + postingOffsetSize + postingCountSize
		if scanCursor > len(data) {
			return nil, ErrKeySizeMismatch
		}
	}

	postingBlobStart := scanCursor
	postingBlobSize := len(data) - postingBlobStart

	entries := make([]index.AttrKVIndexEntry, entryCount)
	for i := range entries {
		keyLen := int(binary.LittleEndian.Uint16(data[cursor : cursor+stringLenSize]))
		cursor += stringLenSize
		entries[i].Key = string(data[cursor : cursor+keyLen])
		cursor += keyLen

		valLen := int(binary.LittleEndian.Uint16(data[cursor : cursor+stringLenSize]))
		cursor += stringLenSize
		entries[i].Value = string(data[cursor : cursor+valLen])
		cursor += valLen

		pOffset := int(binary.LittleEndian.Uint32(data[cursor : cursor+postingOffsetSize]))
		cursor += postingOffsetSize

		pCount := int(binary.LittleEndian.Uint32(data[cursor : cursor+postingCountSize]))
		cursor += postingCountSize

		pEnd := pOffset + pCount*positionSize
		if pEnd > postingBlobSize {
			return nil, ErrPostingSizeMismatch
		}

		entries[i].Positions = make([]uint64, pCount)
		pCursor := postingBlobStart + pOffset
		for j := 0; j < pCount; j++ {
			entries[i].Positions[j] = uint64(binary.LittleEndian.Uint32(data[pCursor : pCursor+positionSize]))
			pCursor += positionSize
		}
	}

	return entries, nil
}

// Load functions

func LoadKeyIndex(dir string, chunkID chunk.ChunkID) ([]index.AttrKeyIndexEntry, error) {
	path := KeyIndexPath(dir, chunkID)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read attr key index: %w", err)
	}
	return decodeKeyIndex(data)
}

func LoadValueIndex(dir string, chunkID chunk.ChunkID) ([]index.AttrValueIndexEntry, error) {
	path := ValueIndexPath(dir, chunkID)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read attr value index: %w", err)
	}
	return decodeValueIndex(data)
}

func LoadKVIndex(dir string, chunkID chunk.ChunkID) ([]index.AttrKVIndexEntry, error) {
	path := KVIndexPath(dir, chunkID)
	data, err := os.ReadFile(path)
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
