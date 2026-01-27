package file

import (
	"encoding/binary"
	"errors"

	indextime "github.com/kluzzebass/gastrolog/internal/index/time"
)

const (
	versionByte = 0x01
	flagsByte   = 0x00

	headerSize = 1 + 1 + 4 // version + flags + entry_count
	entrySize  = 8 + 8     // timestamp_us + record_pos

	indexFileName = "time.idx"
)

var (
	ErrIndexTooSmall     = errors.New("time index too small")
	ErrVersionMismatch   = errors.New("time index version mismatch")
	ErrEntrySizeMismatch = errors.New("time index entry size mismatch")
)

func encodeIndex(entries []indextime.IndexEntry) []byte {
	buf := make([]byte, headerSize+len(entries)*entrySize)

	buf[0] = versionByte
	buf[1] = flagsByte
	binary.LittleEndian.PutUint32(buf[2:6], uint32(len(entries)))

	offset := headerSize
	for _, e := range entries {
		binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(e.TimestampUS))
		binary.LittleEndian.PutUint64(buf[offset+8:offset+16], uint64(e.RecordPos))
		offset += entrySize
	}

	return buf
}

func decodeIndex(data []byte) ([]indextime.IndexEntry, error) {
	if len(data) < headerSize {
		return nil, ErrIndexTooSmall
	}

	if data[0] != versionByte {
		return nil, ErrVersionMismatch
	}

	count := binary.LittleEndian.Uint32(data[2:6])
	expected := headerSize + int(count)*entrySize
	if len(data) != expected {
		return nil, ErrEntrySizeMismatch
	}

	entries := make([]indextime.IndexEntry, count)
	offset := headerSize
	for i := range entries {
		entries[i].TimestampUS = int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		entries[i].RecordPos = int64(binary.LittleEndian.Uint64(data[offset+8 : offset+16]))
		offset += entrySize
	}

	return entries, nil
}
