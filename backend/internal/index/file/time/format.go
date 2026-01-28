package time

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	gotime "time"

	"github.com/google/uuid"
	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
)

const (
	signatureByte = 'i'
	typeByte      = 't'
	versionByte   = 0x01
	flagsByte     = 0x00

	signatureSize  = 1
	typeSize       = 1
	versionSize    = 1
	flagsSize      = 1
	chunkIDSize    = 16
	entryCountSize = 4
	timestampSize  = 8
	recordPosSize  = 8

	headerSize = signatureSize + typeSize + versionSize + flagsSize + chunkIDSize + entryCountSize
	entrySize  = timestampSize + recordPosSize

	indexFileName = "_time.idx"
)

var (
	ErrIndexTooSmall     = errors.New("time index too small")
	ErrSignatureMismatch = errors.New("time index signature mismatch")
	ErrVersionMismatch   = errors.New("time index version mismatch")
	ErrChunkIDMismatch   = errors.New("time index chunk ID mismatch")
	ErrEntrySizeMismatch = errors.New("time index entry size mismatch")
)

func encodeIndex(chunkID chunk.ChunkID, entries []index.TimeIndexEntry) []byte {
	buf := make([]byte, headerSize+len(entries)*entrySize)

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
	binary.LittleEndian.PutUint32(buf[cursor:cursor+entryCountSize], uint32(len(entries)))
	cursor += entryCountSize

	for _, e := range entries {
		binary.LittleEndian.PutUint64(buf[cursor:cursor+timestampSize], uint64(e.Timestamp.UnixMicro()))
		cursor += timestampSize
		binary.LittleEndian.PutUint64(buf[cursor:cursor+recordPosSize], e.RecordPos)
		cursor += recordPosSize
	}

	return buf
}

func decodeIndex(chunkID chunk.ChunkID, data []byte) ([]index.TimeIndexEntry, error) {
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
		entries[i].RecordPos = binary.LittleEndian.Uint64(data[cursor : cursor+recordPosSize])
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
