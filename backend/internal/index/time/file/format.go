package file

import (
	"encoding/binary"
	"errors"

	indextime "github.com/kluzzebass/gastrolog/internal/index/time"
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
	entryCountSize = 4
	timestampSize  = 8
	recordPosSize  = 8

	headerSize = signatureSize + typeSize + versionSize + flagsSize + entryCountSize
	entrySize  = timestampSize + recordPosSize

	indexFileName = "_time.idx"
)

var (
	ErrIndexTooSmall     = errors.New("time index too small")
	ErrSignatureMismatch = errors.New("time index signature mismatch")
	ErrVersionMismatch   = errors.New("time index version mismatch")
	ErrEntrySizeMismatch = errors.New("time index entry size mismatch")
)

func encodeIndex(entries []indextime.IndexEntry) []byte {
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
	binary.LittleEndian.PutUint32(buf[cursor:cursor+entryCountSize], uint32(len(entries)))
	cursor += entryCountSize

	for _, e := range entries {
		binary.LittleEndian.PutUint64(buf[cursor:cursor+timestampSize], uint64(e.TimestampUS))
		cursor += timestampSize
		binary.LittleEndian.PutUint64(buf[cursor:cursor+recordPosSize], e.RecordPos)
		cursor += recordPosSize
	}

	return buf
}

func decodeIndex(data []byte) ([]indextime.IndexEntry, error) {
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

	count := binary.LittleEndian.Uint32(data[cursor : cursor+entryCountSize])
	cursor += entryCountSize

	expected := headerSize + int(count)*entrySize
	if len(data) != expected {
		return nil, ErrEntrySizeMismatch
	}

	entries := make([]indextime.IndexEntry, count)
	for i := range entries {
		entries[i].TimestampUS = int64(binary.LittleEndian.Uint64(data[cursor : cursor+timestampSize]))
		cursor += timestampSize
		entries[i].RecordPos = binary.LittleEndian.Uint64(data[cursor : cursor+recordPosSize])
		cursor += recordPosSize
	}

	return entries, nil
}
