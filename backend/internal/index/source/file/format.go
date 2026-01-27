package file

import (
	"encoding/binary"
	"errors"
	"sort"

	"github.com/google/uuid"
	"github.com/kluzzebass/gastrolog/internal/chunk"
	indexsource "github.com/kluzzebass/gastrolog/internal/index/source"
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
	keyCountSize  = 4
	headerSize    = signatureSize + typeSize + versionSize + flagsSize + keyCountSize

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
	ErrKeySizeMismatch     = errors.New("source index key table size mismatch")
	ErrPostingSizeMismatch = errors.New("source index posting list size mismatch")
)

// encodeIndex encodes source index entries into binary format.
// Entries are sorted by SourceID bytes for deterministic output.
//
// Layout:
//
//	Header:  signature (1) | type (1) | version (1) | flags (1) | keyCount (4)
//	Keys:    sourceID (16) | postingOffset (8) | postingCount (4)  (repeated keyCount times)
//	Postings: position (8)  (flat, referenced by offset/count in keys)
func encodeIndex(entries []indexsource.IndexEntry) []byte {
	// Sort entries by SourceID bytes for deterministic output.
	sorted := make([]indexsource.IndexEntry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool {
		a := uuid.UUID(sorted[i].SourceID)
		b := uuid.UUID(sorted[j].SourceID)
		return a.String() < b.String()
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
	binary.LittleEndian.PutUint32(buf[cursor:cursor+keyCountSize], uint32(len(sorted)))
	cursor += keyCountSize

	// Write key table and posting blob.
	// postingOffset is relative to the start of the posting blob.
	keyCursor := cursor
	postingCursor := headerSize + keyTableSize
	postingOffset := 0

	for _, e := range sorted {
		// sourceID (16 bytes)
		uid := uuid.UUID(e.SourceID)
		copy(buf[keyCursor:keyCursor+sourceIDSize], uid[:])
		keyCursor += sourceIDSize

		// postingOffset (8 bytes) — byte offset into posting blob
		binary.LittleEndian.PutUint64(buf[keyCursor:keyCursor+postingOffsetSize], uint64(postingOffset))
		keyCursor += postingOffsetSize

		// postingCount (4 bytes)
		binary.LittleEndian.PutUint32(buf[keyCursor:keyCursor+postingCountSize], uint32(len(e.Positions)))
		keyCursor += postingCountSize

		// Write positions into posting blob.
		for _, pos := range e.Positions {
			binary.LittleEndian.PutUint64(buf[postingCursor:postingCursor+positionSize], pos)
			postingCursor += positionSize
		}

		postingOffset += len(e.Positions) * positionSize
	}

	return buf
}

// decodeIndex decodes binary source index data back into entries.
func decodeIndex(data []byte) ([]indexsource.IndexEntry, error) {
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

	keyCount := binary.LittleEndian.Uint32(data[cursor : cursor+keyCountSize])
	cursor += keyCountSize

	keyTableSize := int(keyCount) * keyEntrySize
	if len(data) < headerSize+keyTableSize {
		return nil, ErrKeySizeMismatch
	}

	postingBlobStart := headerSize + keyTableSize
	postingBlobSize := len(data) - postingBlobStart

	entries := make([]indexsource.IndexEntry, keyCount)
	for i := range entries {
		// sourceID
		var uid uuid.UUID
		copy(uid[:], data[cursor:cursor+sourceIDSize])
		entries[i].SourceID = chunk.SourceID(uid)
		cursor += sourceIDSize

		// postingOffset
		pOffset := int(binary.LittleEndian.Uint64(data[cursor : cursor+postingOffsetSize]))
		cursor += postingOffsetSize

		// postingCount
		pCount := int(binary.LittleEndian.Uint32(data[cursor : cursor+postingCountSize]))
		cursor += postingCountSize

		// Validate posting range.
		pEnd := pOffset + pCount*positionSize
		if pEnd > postingBlobSize {
			return nil, ErrPostingSizeMismatch
		}

		// Read positions from posting blob.
		entries[i].Positions = make([]uint64, pCount)
		pCursor := postingBlobStart + pOffset
		for j := 0; j < pCount; j++ {
			entries[i].Positions[j] = binary.LittleEndian.Uint64(data[pCursor : pCursor+positionSize])
			pCursor += positionSize
		}
	}

	return entries, nil
}
