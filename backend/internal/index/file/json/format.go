package json

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
	"gastrolog/internal/index"
)

const (
	currentVersion = 0x01

	// File header layout:
	//   [4B] format.Header (signature, type='J', version, flags)
	//   [1B] status (0x00=complete, 0x01=capped)
	//   [28B] offsets:
	//     dict_offset:u32  dict_count:u32
	//     path_offset:u32  path_count:u32
	//     pv_offset:u32    pv_count:u32
	//     blob_offset:u32
	statusSize  = 1
	offsetsSize = 7 * 4 // 7 uint32 values

	fileHeaderSize = format.HeaderSize + statusSize + offsetsSize // 33 bytes

	// String dictionary entry: [len:u16][bytes]
	stringLenSize = 2

	// Path posting table entry: [dictID:u32][blob_offset:u32][count:u32]
	pathEntrySize = 3 * 4

	// Path-value posting table entry: [pathID:u32][valueID:u32][blob_offset:u32][count:u32]
	pvEntrySize = 4 * 4

	// Posting position: u32
	positionSize = 4

	statusComplete = 0x00
	statusCapped   = 0x01

	indexFileName = "_json.idx"
)

var (
	ErrIndexTooSmall = errors.New("json index too small")
	ErrInvalidStatus = errors.New("json index invalid status byte")
	ErrCorruptIndex  = errors.New("json index corrupt")
)

// fileOffsets holds the decoded offset table from the file header.
type fileOffsets struct {
	dictOffset  uint32
	dictCount   uint32
	pathOffset  uint32
	pathCount   uint32
	pvOffset    uint32
	pvCount     uint32
	blobOffset  uint32
}

// encodeIndex encodes the JSON index into binary format.
// dict must be sorted. pathEntries sorted by dictID. pvEntries sorted by (pathID, valueID).
func encodeIndex(
	dict []string,
	pathEntries []pathTableEntry,
	pvEntries []pvTableEntry,
	postingBlob []byte,
	status index.JSONIndexStatus,
) []byte {
	// Calculate sizes.
	dictSize := 0
	for _, s := range dict {
		dictSize += stringLenSize + len(s)
	}
	pathTableSize := len(pathEntries) * pathEntrySize
	pvTableSize := len(pvEntries) * pvEntrySize

	totalSize := fileHeaderSize + dictSize + pathTableSize + pvTableSize + len(postingBlob)
	buf := make([]byte, totalSize)

	// Write file header.
	cursor := 0
	h := format.Header{Type: format.TypeJSONIndex, Version: currentVersion, Flags: format.FlagComplete}
	cursor += h.EncodeInto(buf[cursor:])

	// Status byte.
	if status == index.JSONCapped {
		buf[cursor] = statusCapped
	} else {
		buf[cursor] = statusComplete
	}
	cursor++

	// Offsets.
	dictOff := uint32(fileHeaderSize)
	pathOff := dictOff + uint32(dictSize)
	pvOff := pathOff + uint32(pathTableSize)
	blobOff := pvOff + uint32(pvTableSize)

	binary.LittleEndian.PutUint32(buf[cursor:], dictOff)
	cursor += 4
	binary.LittleEndian.PutUint32(buf[cursor:], uint32(len(dict)))
	cursor += 4
	binary.LittleEndian.PutUint32(buf[cursor:], pathOff)
	cursor += 4
	binary.LittleEndian.PutUint32(buf[cursor:], uint32(len(pathEntries)))
	cursor += 4
	binary.LittleEndian.PutUint32(buf[cursor:], pvOff)
	cursor += 4
	binary.LittleEndian.PutUint32(buf[cursor:], uint32(len(pvEntries)))
	cursor += 4
	binary.LittleEndian.PutUint32(buf[cursor:], blobOff)
	cursor += 4

	// String dictionary.
	for _, s := range dict {
		binary.LittleEndian.PutUint16(buf[cursor:], uint16(len(s)))
		cursor += stringLenSize
		copy(buf[cursor:], s)
		cursor += len(s)
	}

	// Path posting table.
	for _, e := range pathEntries {
		binary.LittleEndian.PutUint32(buf[cursor:], e.dictID)
		cursor += 4
		binary.LittleEndian.PutUint32(buf[cursor:], e.blobOffset)
		cursor += 4
		binary.LittleEndian.PutUint32(buf[cursor:], e.count)
		cursor += 4
	}

	// Path-value posting table.
	for _, e := range pvEntries {
		binary.LittleEndian.PutUint32(buf[cursor:], e.pathID)
		cursor += 4
		binary.LittleEndian.PutUint32(buf[cursor:], e.valueID)
		cursor += 4
		binary.LittleEndian.PutUint32(buf[cursor:], e.blobOffset)
		cursor += 4
		binary.LittleEndian.PutUint32(buf[cursor:], e.count)
		cursor += 4
	}

	// Posting blob.
	copy(buf[cursor:], postingBlob)

	return buf
}

// pathTableEntry represents a row in the path posting table.
type pathTableEntry struct {
	dictID     uint32
	blobOffset uint32
	count      uint32
}

// pvTableEntry represents a row in the path-value posting table.
type pvTableEntry struct {
	pathID     uint32
	valueID    uint32
	blobOffset uint32
	count      uint32
}

func decodeStatus(b byte) (index.JSONIndexStatus, error) {
	switch b {
	case statusComplete:
		return index.JSONComplete, nil
	case statusCapped:
		return index.JSONCapped, nil
	default:
		return index.JSONComplete, ErrInvalidStatus
	}
}

// decodeIndex decodes the binary JSON index into path and path-value entries.
func decodeIndex(data []byte) ([]index.JSONPathIndexEntry, []index.JSONPVIndexEntry, index.JSONIndexStatus, error) {
	if len(data) < fileHeaderSize {
		return nil, nil, index.JSONComplete, ErrIndexTooSmall
	}

	h, err := format.DecodeAndValidate(data, format.TypeJSONIndex, currentVersion)
	if err != nil {
		return nil, nil, index.JSONComplete, fmt.Errorf("json index: %w", err)
	}
	if h.Flags&format.FlagComplete == 0 {
		return nil, nil, index.JSONComplete, fmt.Errorf("json index: incomplete (missing complete flag)")
	}

	status, err := decodeStatus(data[format.HeaderSize])
	if err != nil {
		return nil, nil, index.JSONComplete, err
	}

	// Read offsets.
	off := format.HeaderSize + statusSize
	offsets := fileOffsets{
		dictOffset: binary.LittleEndian.Uint32(data[off:]),
		dictCount:  binary.LittleEndian.Uint32(data[off+4:]),
		pathOffset: binary.LittleEndian.Uint32(data[off+8:]),
		pathCount:  binary.LittleEndian.Uint32(data[off+12:]),
		pvOffset:   binary.LittleEndian.Uint32(data[off+16:]),
		pvCount:    binary.LittleEndian.Uint32(data[off+20:]),
		blobOffset: binary.LittleEndian.Uint32(data[off+24:]),
	}

	// Read string dictionary.
	dict := make([]string, offsets.dictCount)
	cursor := int(offsets.dictOffset)
	for i := range dict {
		if cursor+stringLenSize > len(data) {
			return nil, nil, status, ErrCorruptIndex
		}
		sLen := int(binary.LittleEndian.Uint16(data[cursor:]))
		cursor += stringLenSize
		if cursor+sLen > len(data) {
			return nil, nil, status, ErrCorruptIndex
		}
		dict[i] = string(data[cursor : cursor+sLen])
		cursor += sLen
	}

	// Read path posting table.
	pathEntries := make([]index.JSONPathIndexEntry, offsets.pathCount)
	cursor = int(offsets.pathOffset)
	for i := range pathEntries {
		if cursor+pathEntrySize > len(data) {
			return nil, nil, status, ErrCorruptIndex
		}
		dictID := binary.LittleEndian.Uint32(data[cursor:])
		blobOff := binary.LittleEndian.Uint32(data[cursor+4:])
		count := binary.LittleEndian.Uint32(data[cursor+8:])
		cursor += pathEntrySize

		if int(dictID) >= len(dict) {
			return nil, nil, status, ErrCorruptIndex
		}

		positions := readPositions(data, offsets.blobOffset, blobOff, count)
		if positions == nil && count > 0 {
			return nil, nil, status, ErrCorruptIndex
		}

		pathEntries[i] = index.JSONPathIndexEntry{
			Path:      dict[dictID],
			Positions: positions,
		}
	}

	// Read path-value posting table.
	pvEntries := make([]index.JSONPVIndexEntry, offsets.pvCount)
	cursor = int(offsets.pvOffset)
	for i := range pvEntries {
		if cursor+pvEntrySize > len(data) {
			return nil, nil, status, ErrCorruptIndex
		}
		pathID := binary.LittleEndian.Uint32(data[cursor:])
		valueID := binary.LittleEndian.Uint32(data[cursor+4:])
		blobOff := binary.LittleEndian.Uint32(data[cursor+8:])
		count := binary.LittleEndian.Uint32(data[cursor+12:])
		cursor += pvEntrySize

		if int(pathID) >= len(dict) || int(valueID) >= len(dict) {
			return nil, nil, status, ErrCorruptIndex
		}

		positions := readPositions(data, offsets.blobOffset, blobOff, count)
		if positions == nil && count > 0 {
			return nil, nil, status, ErrCorruptIndex
		}

		pvEntries[i] = index.JSONPVIndexEntry{
			Path:      dict[pathID],
			Value:     dict[valueID],
			Positions: positions,
		}
	}

	return pathEntries, pvEntries, status, nil
}

// readPositions reads count uint32 positions from the posting blob.
func readPositions(data []byte, blobBase, blobOff, count uint32) []uint64 {
	if count == 0 {
		return nil
	}
	start := int(blobBase + blobOff)
	end := start + int(count)*positionSize
	if end > len(data) {
		return nil
	}
	positions := make([]uint64, count)
	for i := range positions {
		positions[i] = uint64(binary.LittleEndian.Uint32(data[start+i*positionSize:]))
	}
	return positions
}

// Load functions

// LoadIndex loads the JSON index from disk.
func LoadIndex(dir string, chunkID chunk.ChunkID) ([]index.JSONPathIndexEntry, []index.JSONPVIndexEntry, index.JSONIndexStatus, error) {
	path := IndexPath(dir, chunkID)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, index.JSONComplete, fmt.Errorf("read json index: %w", err)
	}
	return decodeIndex(data)
}

// Path helpers

func IndexPath(dir string, chunkID chunk.ChunkID) string {
	return filepath.Join(dir, chunkID.String(), indexFileName)
}

func TempFilePattern(dir string, chunkID chunk.ChunkID) string {
	return filepath.Join(dir, chunkID.String(), indexFileName+".tmp.*")
}
