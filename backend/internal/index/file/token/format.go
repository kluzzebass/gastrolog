package token

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
	"gastrolog/internal/index"
)

const (
	currentVersion = 0x01

	keyCountSize = 4
	headerSize   = format.HeaderSize + keyCountSize

	tokenLenSize      = 2
	postingOffsetSize = 4 // uint32 byte offset into posting blob
	postingCountSize  = 4

	positionSize = 4 // uint32 (4GB max chunk size)

	indexFileName = "_token.idx"
)

var (
	ErrIndexTooSmall       = errors.New("token index too small")
	ErrKeySizeMismatch     = errors.New("token index key table size mismatch")
	ErrPostingSizeMismatch = errors.New("token index posting list size mismatch")
	ErrIndexIncomplete     = errors.New("token index incomplete (missing complete flag)")
)

// encodeIndex encodes token index entries into binary format.
// NOTE: This allocates the entire index in memory. For large indexes,
// use encodeIndexToFile instead.
func encodeIndex(entries []index.TokenIndexEntry) []byte {
	// Sort entries by Token for deterministic output and binary search.
	sorted := make([]index.TokenIndexEntry, len(entries))
	copy(sorted, entries)
	slices.SortFunc(sorted, func(a, b index.TokenIndexEntry) int {
		return cmp.Compare(a.Token, b.Token)
	})

	// Count total positions and token bytes for sizing.
	totalPositions := 0
	totalTokenBytes := 0
	for _, e := range sorted {
		totalPositions += len(e.Positions)
		totalTokenBytes += len(e.Token)
	}

	// Key entry: tokenLen (2) + token (variable) + postingOffset (4) + postingCount (4)
	keyTableSize := len(sorted)*(tokenLenSize+postingOffsetSize+postingCountSize) + totalTokenBytes
	postingBlobSize := totalPositions * positionSize
	buf := make([]byte, headerSize+keyTableSize+postingBlobSize)

	// Write header.
	cursor := 0
	h := format.Header{Type: format.TypeTokenIndex, Version: currentVersion, Flags: format.FlagComplete}
	cursor += h.EncodeInto(buf[cursor:])

	binary.LittleEndian.PutUint32(buf[cursor:cursor+keyCountSize], uint32(len(sorted)))
	cursor += keyCountSize

	// Write key table and posting blob.
	keyCursor := cursor
	postingCursor := headerSize + keyTableSize
	postingOffset := 0

	for _, e := range sorted {
		tokenBytes := []byte(e.Token)
		binary.LittleEndian.PutUint16(buf[keyCursor:keyCursor+tokenLenSize], uint16(len(tokenBytes)))
		keyCursor += tokenLenSize

		copy(buf[keyCursor:keyCursor+len(tokenBytes)], tokenBytes)
		keyCursor += len(tokenBytes)

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

// decodeIndex decodes binary token index data back into entries.
func decodeIndex(data []byte) ([]index.TokenIndexEntry, error) {
	if len(data) < headerSize {
		return nil, ErrIndexTooSmall
	}

	h, err := format.DecodeAndValidate(data, format.TypeTokenIndex, currentVersion)
	if err != nil {
		return nil, fmt.Errorf("token index: %w", err)
	}
	if h.Flags&format.FlagComplete == 0 {
		return nil, ErrIndexIncomplete
	}
	cursor := format.HeaderSize

	keyCount := binary.LittleEndian.Uint32(data[cursor : cursor+keyCountSize])
	cursor += keyCountSize

	// We need to scan through the key table to find where postings start.
	// First pass: count total key table size.
	scanCursor := cursor
	for range keyCount {
		if scanCursor+tokenLenSize > len(data) {
			return nil, ErrKeySizeMismatch
		}
		tokenLen := int(binary.LittleEndian.Uint16(data[scanCursor : scanCursor+tokenLenSize]))
		scanCursor += tokenLenSize + tokenLen + postingOffsetSize + postingCountSize
		if scanCursor > len(data) {
			return nil, ErrKeySizeMismatch
		}
	}

	postingBlobStart := scanCursor
	postingBlobSize := len(data) - postingBlobStart

	entries := make([]index.TokenIndexEntry, keyCount)
	for i := range entries {
		tokenLen := int(binary.LittleEndian.Uint16(data[cursor : cursor+tokenLenSize]))
		cursor += tokenLenSize

		entries[i].Token = string(data[cursor : cursor+tokenLen])
		cursor += tokenLen

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
		for j := range pCount {
			entries[i].Positions[j] = uint64(binary.LittleEndian.Uint32(data[pCursor : pCursor+positionSize]))
			pCursor += positionSize
		}
	}

	return entries, nil
}

func LoadIndex(dir string, chunkID chunk.ChunkID) ([]index.TokenIndexEntry, error) {
	path := IndexPath(dir, chunkID)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read token index: %w", err)
	}
	return decodeIndex(data)
}

// IndexPath returns the path to the token index file for a chunk.
func IndexPath(dir string, chunkID chunk.ChunkID) string {
	return filepath.Join(dir, chunkID.String(), indexFileName)
}

// TempFilePattern returns the glob pattern for temporary index files.
func TempFilePattern(dir string, chunkID chunk.ChunkID) string {
	return filepath.Join(dir, chunkID.String(), indexFileName+".tmp.*")
}
