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

	"github.com/google/uuid"
)

const (
	currentVersion = 0x01

	chunkIDSize  = 16
	keyCountSize = 4
	headerSize   = format.HeaderSize + chunkIDSize + keyCountSize

	tokenLenSize      = 2
	postingOffsetSize = 8
	postingCountSize  = 4

	positionSize = 8

	indexFileName = "_token.idx"
)

var (
	ErrIndexTooSmall       = errors.New("token index too small")
	ErrChunkIDMismatch     = errors.New("token index chunk ID mismatch")
	ErrKeySizeMismatch     = errors.New("token index key table size mismatch")
	ErrPostingSizeMismatch = errors.New("token index posting list size mismatch")
)

// encodeIndexToFile writes token index entries directly to a file without
// buffering the entire index in memory.
//
// Layout:
//
//	Header:  signature (1) | type (1) | version (1) | flags (1) | chunkID (16) | keyCount (4)
//	Keys:    tokenLen (2) | token (variable) | postingOffset (8) | postingCount (4)  (repeated keyCount times)
//	Postings: position (8)  (flat, referenced by offset/count in keys)
func encodeIndexToFile(w *os.File, chunkID chunk.ChunkID, entries []index.TokenIndexEntry) error {
	// Entries must already be sorted by caller.

	// Write header.
	headerBuf := make([]byte, headerSize)
	cursor := 0
	h := format.Header{Type: format.TypeTokenIndex, Version: currentVersion, Flags: 0}
	cursor += h.EncodeInto(headerBuf[cursor:])

	uid := uuid.UUID(chunkID)
	copy(headerBuf[cursor:cursor+chunkIDSize], uid[:])
	cursor += chunkIDSize
	binary.LittleEndian.PutUint32(headerBuf[cursor:cursor+keyCountSize], uint32(len(entries)))

	if _, err := w.Write(headerBuf); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	// Write key table.
	// Reuse a small buffer for each key entry.
	keyBuf := make([]byte, tokenLenSize+postingOffsetSize+postingCountSize)
	postingOffset := 0

	for _, e := range entries {
		// Write token length.
		binary.LittleEndian.PutUint16(keyBuf[:tokenLenSize], uint16(len(e.Token)))
		if _, err := w.Write(keyBuf[:tokenLenSize]); err != nil {
			return fmt.Errorf("write token len: %w", err)
		}

		// Write token bytes.
		if _, err := w.WriteString(e.Token); err != nil {
			return fmt.Errorf("write token: %w", err)
		}

		// Write posting offset and count.
		binary.LittleEndian.PutUint64(keyBuf[:postingOffsetSize], uint64(postingOffset))
		binary.LittleEndian.PutUint32(keyBuf[postingOffsetSize:postingOffsetSize+postingCountSize], uint32(len(e.Positions)))
		if _, err := w.Write(keyBuf[:postingOffsetSize+postingCountSize]); err != nil {
			return fmt.Errorf("write posting ref: %w", err)
		}

		postingOffset += len(e.Positions) * positionSize
	}

	// Write posting blob.
	// Use a buffer to batch writes for efficiency.
	const batchSize = 512 // positions per batch
	posBuf := make([]byte, batchSize*positionSize)

	for _, e := range entries {
		positions := e.Positions
		for len(positions) > 0 {
			n := len(positions)
			if n > batchSize {
				n = batchSize
			}
			for i := 0; i < n; i++ {
				binary.LittleEndian.PutUint64(posBuf[i*positionSize:(i+1)*positionSize], positions[i])
			}
			if _, err := w.Write(posBuf[:n*positionSize]); err != nil {
				return fmt.Errorf("write positions: %w", err)
			}
			positions = positions[n:]
		}
	}

	return nil
}

// encodeIndex encodes token index entries into binary format.
// NOTE: This allocates the entire index in memory. For large indexes,
// use encodeIndexToFile instead.
func encodeIndex(chunkID chunk.ChunkID, entries []index.TokenIndexEntry) []byte {
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

	// Key entry: tokenLen (2) + token (variable) + postingOffset (8) + postingCount (4)
	keyTableSize := len(sorted)*(tokenLenSize+postingOffsetSize+postingCountSize) + totalTokenBytes
	postingBlobSize := totalPositions * positionSize
	buf := make([]byte, headerSize+keyTableSize+postingBlobSize)

	// Write header.
	cursor := 0
	h := format.Header{Type: format.TypeTokenIndex, Version: currentVersion, Flags: 0}
	cursor += h.EncodeInto(buf[cursor:])

	uid := uuid.UUID(chunkID)
	copy(buf[cursor:cursor+chunkIDSize], uid[:])
	cursor += chunkIDSize
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

		binary.LittleEndian.PutUint64(buf[keyCursor:keyCursor+postingOffsetSize], uint64(postingOffset))
		keyCursor += postingOffsetSize

		binary.LittleEndian.PutUint32(buf[keyCursor:keyCursor+postingCountSize], uint32(len(e.Positions)))
		keyCursor += postingCountSize

		for _, pos := range e.Positions {
			binary.LittleEndian.PutUint64(buf[postingCursor:postingCursor+positionSize], pos)
			postingCursor += positionSize
		}

		postingOffset += len(e.Positions) * positionSize
	}

	return buf
}

// decodeIndex decodes binary token index data back into entries.
func decodeIndex(chunkID chunk.ChunkID, data []byte) ([]index.TokenIndexEntry, error) {
	if len(data) < headerSize {
		return nil, ErrIndexTooSmall
	}

	_, err := format.DecodeAndValidate(data, format.TypeTokenIndex, currentVersion)
	if err != nil {
		return nil, fmt.Errorf("token index: %w", err)
	}
	cursor := format.HeaderSize

	var storedID uuid.UUID
	copy(storedID[:], data[cursor:cursor+chunkIDSize])
	expectedID := uuid.UUID(chunkID)
	if storedID != expectedID {
		return nil, ErrChunkIDMismatch
	}
	cursor += chunkIDSize

	keyCount := binary.LittleEndian.Uint32(data[cursor : cursor+keyCountSize])
	cursor += keyCountSize

	// We need to scan through the key table to find where postings start.
	// First pass: count total key table size.
	scanCursor := cursor
	for i := uint32(0); i < keyCount; i++ {
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

		pOffset := int(binary.LittleEndian.Uint64(data[cursor : cursor+postingOffsetSize]))
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
			entries[i].Positions[j] = binary.LittleEndian.Uint64(data[pCursor : pCursor+positionSize])
			pCursor += positionSize
		}
	}

	return entries, nil
}

func LoadIndex(dir string, chunkID chunk.ChunkID) ([]index.TokenIndexEntry, error) {
	path := filepath.Join(dir, chunkID.String(), indexFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read token index: %w", err)
	}
	return decodeIndex(chunkID, data)
}
