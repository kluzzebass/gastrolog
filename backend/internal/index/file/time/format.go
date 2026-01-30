package time

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	gotime "time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
	"gastrolog/internal/index"

	"github.com/google/uuid"
)

const (
	currentVersion = 0x01

	chunkIDSize    = 16
	entryCountSize = 4
	timestampSize  = 8
	recordPosSize  = 4 // uint32 (4GB max chunk size)

	headerSize = format.HeaderSize + chunkIDSize + entryCountSize
	entrySize  = timestampSize + recordPosSize

	indexFileName = "_time.idx"
)

var (
	ErrIndexTooSmall     = errors.New("time index too small")
	ErrChunkIDMismatch   = errors.New("time index chunk ID mismatch")
	ErrEntrySizeMismatch = errors.New("time index entry size mismatch")
)

// encodeIndex encodes time index entries into binary format.
//
// Layout:
//
//	Header (24 bytes):
//	  signature (1 byte, 'i')
//	  type (1 byte, 't')
//	  version (1 byte)
//	  flags (1 byte, reserved)
//	  chunkID (16 bytes, UUID)
//	  entryCount (4 bytes, little-endian uint32)
//
//	Entries (16 bytes each):
//	  timestamp (8 bytes, Unix microseconds, little-endian int64)
//	  recordPos (8 bytes, little-endian uint64)
func encodeIndex(chunkID chunk.ChunkID, entries []index.TimeIndexEntry) []byte {
	buf := make([]byte, headerSize+len(entries)*entrySize)

	cursor := 0
	h := format.Header{Type: format.TypeTimeIndex, Version: currentVersion, Flags: 0}
	cursor += h.EncodeInto(buf[cursor:])

	uid := uuid.UUID(chunkID)
	copy(buf[cursor:cursor+chunkIDSize], uid[:])
	cursor += chunkIDSize
	binary.LittleEndian.PutUint32(buf[cursor:cursor+entryCountSize], uint32(len(entries)))
	cursor += entryCountSize

	for _, e := range entries {
		binary.LittleEndian.PutUint64(buf[cursor:cursor+timestampSize], uint64(e.Timestamp.UnixMicro()))
		cursor += timestampSize
		binary.LittleEndian.PutUint32(buf[cursor:cursor+recordPosSize], uint32(e.RecordPos))
		cursor += recordPosSize
	}

	return buf
}

func decodeIndex(chunkID chunk.ChunkID, data []byte) ([]index.TimeIndexEntry, error) {
	if len(data) < headerSize {
		return nil, ErrIndexTooSmall
	}

	_, err := format.DecodeAndValidate(data, format.TypeTimeIndex, currentVersion)
	if err != nil {
		return nil, fmt.Errorf("time index: %w", err)
	}
	cursor := format.HeaderSize

	var storedID uuid.UUID
	copy(storedID[:], data[cursor:cursor+chunkIDSize])
	expectedID := uuid.UUID(chunkID)
	if storedID != expectedID {
		return nil, ErrChunkIDMismatch
	}
	cursor += chunkIDSize

	count := binary.LittleEndian.Uint32(data[cursor : cursor+entryCountSize])
	cursor += entryCountSize

	expected := headerSize + int(count)*entrySize
	if len(data) != expected {
		return nil, ErrEntrySizeMismatch
	}

	entries := make([]index.TimeIndexEntry, count)
	for i := range entries {
		micros := int64(binary.LittleEndian.Uint64(data[cursor : cursor+timestampSize]))
		entries[i].Timestamp = gotime.UnixMicro(micros)
		cursor += timestampSize
		entries[i].RecordPos = uint64(binary.LittleEndian.Uint32(data[cursor : cursor+recordPosSize]))
		cursor += recordPosSize
	}

	return entries, nil
}

func LoadIndex(dir string, chunkID chunk.ChunkID) ([]index.TimeIndexEntry, error) {
	path := filepath.Join(dir, chunkID.String(), indexFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read time index: %w", err)
	}
	return decodeIndex(chunkID, data)
}
