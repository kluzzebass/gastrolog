package token

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/google/uuid"
	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
)

const (
	signatureByte = 'i'
	typeByte      = 'k'
	versionByte   = 0x01
	flagsByte     = 0x00

	signatureSize = 1
	typeSize      = 1
	versionSize   = 1
	flagsSize     = 1
	chunkIDSize   = 16
	keyCountSize  = 4
	headerSize    = signatureSize + typeSize + versionSize + flagsSize + chunkIDSize + keyCountSize

	tokenLenSize      = 2
	postingOffsetSize = 8
	postingCountSize  = 4

	positionSize = 8

	indexFileName = "_token.idx"
)

var (
	ErrIndexTooSmall       = errors.New("token index too small")
	ErrSignatureMismatch   = errors.New("token index signature mismatch")
	ErrVersionMismatch     = errors.New("token index version mismatch")
	ErrChunkIDMismatch     = errors.New("token index chunk ID mismatch")
	ErrKeySizeMismatch     = errors.New("token index key table size mismatch")
	ErrPostingSizeMismatch = errors.New("token index posting list size mismatch")
)

// encodeIndex encodes token index entries into binary format.
// Entries are sorted by Token for deterministic output and binary search.
//
// Layout:
//
//	Header:  signature (1) | type (1) | version (1) | flags (1) | chunkID (16) | keyCount (4)
//	Keys:    tokenLen (2) | token (variable) | postingOffset (8) | postingCount (4)  (repeated keyCount times)
//	Postings: position (8)  (flat, referenced by offset/count in keys)
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
	buf[cursor] = signatureByte
	cursor += signatureSize
	buf[cursor] = typeByte
	cursor += typeSize
	buf[cursor] = versionByte
	cursor += versionSize
	buf[cursor] = flagsByte
	cursor += flagsSize
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

	cursor := 0
	if data[cursor] != signatureByte || data[cursor+signatureSize] != typeByte {
		return nil, ErrSignatureMismatch
	}
	cursor += signatureSize + typeSize
	if data[cursor] != versionByte {
		return nil, ErrVersionMismatch
	}
	cursor += versionSize + flagsSize

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
