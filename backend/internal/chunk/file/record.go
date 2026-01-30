package file

import (
	"encoding/binary"
	"errors"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
)

// idx.log entry layout (28 bytes):
//
//	ingestTS      (8 bytes, int64, Unix microseconds)
//	writeTS       (8 bytes, int64, Unix microseconds)
//	sourceLocalID (4 bytes, uint32, chunk-local source ID)
//	rawOffset     (4 bytes, uint32, byte offset into raw.log data section)
//	rawSize       (4 bytes, uint32, length of raw data)
const (
	IdxEntrySize = 28

	idxIngestTSOffset      = 0
	idxWriteTSOffset       = 8
	idxSourceLocalIDOffset = 16
	idxRawOffsetOffset     = 20
	idxRawSizeOffset       = 24

	// File versions.
	RawLogVersion = 0x01
	IdxLogVersion = 0x01

	// MaxRawLogSize is the hard limit for raw.log (4GB - 1).
	// This ensures rawOffset (uint32) can address all bytes.
	MaxRawLogSize = 1<<32 - 1
)

var (
	ErrRawTooLarge      = errors.New("raw data would exceed max raw.log size")
	ErrInvalidEntry     = errors.New("invalid idx.log entry")
	ErrInvalidRecordIdx = errors.New("invalid record index")
)

// IdxEntry represents a single entry in idx.log.
type IdxEntry struct {
	IngestTS      time.Time
	WriteTS       time.Time
	SourceLocalID uint32
	RawOffset     uint32 // Byte offset into raw.log (after header)
	RawSize       uint32
}

// EncodeIdxEntry encodes an idx.log entry into a 28-byte buffer.
func EncodeIdxEntry(e IdxEntry, buf []byte) {
	binary.LittleEndian.PutUint64(buf[idxIngestTSOffset:], uint64(e.IngestTS.UnixMicro()))
	binary.LittleEndian.PutUint64(buf[idxWriteTSOffset:], uint64(e.WriteTS.UnixMicro()))
	binary.LittleEndian.PutUint32(buf[idxSourceLocalIDOffset:], e.SourceLocalID)
	binary.LittleEndian.PutUint32(buf[idxRawOffsetOffset:], e.RawOffset)
	binary.LittleEndian.PutUint32(buf[idxRawSizeOffset:], e.RawSize)
}

// DecodeIdxEntry decodes an idx.log entry from a 28-byte buffer.
func DecodeIdxEntry(buf []byte) IdxEntry {
	return IdxEntry{
		IngestTS:      time.UnixMicro(int64(binary.LittleEndian.Uint64(buf[idxIngestTSOffset:]))),
		WriteTS:       time.UnixMicro(int64(binary.LittleEndian.Uint64(buf[idxWriteTSOffset:]))),
		SourceLocalID: binary.LittleEndian.Uint32(buf[idxSourceLocalIDOffset:]),
		RawOffset:     binary.LittleEndian.Uint32(buf[idxRawOffsetOffset:]),
		RawSize:       binary.LittleEndian.Uint32(buf[idxRawSizeOffset:]),
	}
}

// IdxFileOffset returns the byte offset in idx.log for a given record index.
func IdxFileOffset(recordIndex uint64) int64 {
	return int64(format.HeaderSize) + int64(recordIndex)*int64(IdxEntrySize)
}

// RecordCount returns the number of records in an idx.log file given its size.
func RecordCount(idxFileSize int64) uint64 {
	if idxFileSize <= int64(format.HeaderSize) {
		return 0
	}
	return uint64(idxFileSize-int64(format.HeaderSize)) / uint64(IdxEntrySize)
}

// RawDataOffset returns the byte offset in raw.log where data begins (after header).
func RawDataOffset() int64 {
	return int64(format.HeaderSize)
}

// BuildRecord constructs a chunk.Record from an IdxEntry and raw data.
// The raw slice is used directly (no copy).
func BuildRecord(entry IdxEntry, raw []byte, sourceID chunk.SourceID) chunk.Record {
	return chunk.Record{
		IngestTS: entry.IngestTS,
		WriteTS:  entry.WriteTS,
		SourceID: sourceID,
		Raw:      raw,
	}
}

// BuildRecordCopy constructs a chunk.Record from an IdxEntry and raw data.
// The raw data is copied.
func BuildRecordCopy(entry IdxEntry, raw []byte, sourceID chunk.SourceID) chunk.Record {
	rawCopy := make([]byte, len(raw))
	copy(rawCopy, raw)
	return chunk.Record{
		IngestTS: entry.IngestTS,
		WriteTS:  entry.WriteTS,
		SourceID: sourceID,
		Raw:      rawCopy,
	}
}
