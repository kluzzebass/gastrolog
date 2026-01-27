package source

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/google/uuid"
	"github.com/kluzzebass/gastrolog/internal/chunk"
	"github.com/kluzzebass/gastrolog/internal/index"
)

const (
	signatureByte = 'i'
	typeByte      = 's'
	versionByte   = 0x01
	flagsByte     = 0x00

	signatureSize = 1
	typeSize      = 1
	versionSize   = 1
	flagsSize     = 1
	chunkIDSize   = 16
	keyCountSize  = 4
	headerSize    = signatureSize + typeSize + versionSize + flagsSize + chunkIDSize + keyCountSize

	sourceIDSize      = 16
	postingOffsetSize = 8
	postingCountSize  = 4
	keyEntrySize      = sourceIDSize + postingOffsetSize + postingCountSize

	positionSize = 8

	indexFileName = "_source.idx"
)

var (
	ErrIndexTooSmall       = errors.New("source index too small")
	ErrSignatureMismatch   = errors.New("source index signature mismatch")
	ErrVersionMismatch     = errors.New("source index version mismatch")
	ErrChunkIDMismatch     = errors.New("source index chunk ID mismatch")
	ErrKeySizeMismatch     = errors.New("source index key table size mismatch")
	ErrPostingSizeMismatch = errors.New("source index posting list size mismatch")
)

// encodeIndex encodes source index entries into binary format.
// Entries are sorted by SourceID bytes for deterministic output.
//
// Layout:
//
//	Header:  signature (1) | type (1) | version (1) | flags (1) | chunkID (16) | keyCount (4)
//	Keys:    sourceID (16) | postingOffset (8) | postingCount (4)  (repeated keyCount times)
//	Postings: position (8)  (flat, referenced by offset/count in keys)
func encodeIndex(chunkID chunk.ChunkID, entries []index.SourceIndexEntry) []byte {
	// Sort entries by SourceID bytes for deterministic output.
	sorted := make([]index.SourceIndexEntry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool {
		a := [16]byte(sorted[i].SourceID)
		b := [16]byte(sorted[j].SourceID)
		return bytes.Compare(a[:], b[:]) < 0
	})

	// Count total positions for sizing.
	totalPositions := 0
	for _, e := range sorted {
		totalPositions += len(e.Positions)
	}

	keyTableSize := len(sorted) * keyEntrySize
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
		uid := uuid.UUID(e.SourceID)
		copy(buf[keyCursor:keyCursor+sourceIDSize], uid[:])
		keyCursor += sourceIDSize

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

// decodeIndex decodes binary source index data back into entries.
func decodeIndex(chunkID chunk.ChunkID, data []byte) ([]index.SourceIndexEntry, error) {
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

	keyTableSize := int(keyCount) * keyEntrySize
	if len(data) < headerSize+keyTableSize {
		return nil, ErrKeySizeMismatch
	}

	postingBlobStart := headerSize + keyTableSize
	postingBlobSize := len(data) - postingBlobStart

	entries := make([]index.SourceIndexEntry, keyCount)
	for i := range entries {
		var uid uuid.UUID
		copy(uid[:], data[cursor:cursor+sourceIDSize])
		entries[i].SourceID = chunk.SourceID(uid)
		cursor += sourceIDSize

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

func LoadIndex(dir string, chunkID chunk.ChunkID) ([]index.SourceIndexEntry, error) {
	path := filepath.Join(dir, chunkID.String(), indexFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read source index: %w", err)
	}
	return decodeIndex(chunkID, data)
}
