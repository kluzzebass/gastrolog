package file

import (
	"encoding/binary"
	"errors"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
)

// idx.log entry layout (38 bytes):
//
//	sourceTS   (8 bytes, int64, Unix microseconds, 0 if not available)
//	ingestTS   (8 bytes, int64, Unix microseconds)
//	writeTS    (8 bytes, int64, Unix microseconds)
//	rawOffset  (4 bytes, uint32, byte offset into raw.log data section)
//	rawSize    (4 bytes, uint32, length of raw data)
//	attrOffset (4 bytes, uint32, byte offset into attr.log data section)
//	attrSize   (2 bytes, uint16, length of encoded attributes)
const (
	IdxEntrySize = 38

	idxSourceTSOffset   = 0
	idxIngestTSOffset   = 8
	idxWriteTSOffset    = 16
	idxRawOffsetOffset  = 24
	idxRawSizeOffset    = 28
	idxAttrOffsetOffset = 32
	idxAttrSizeOffset   = 36

	// File versions.
	RawLogVersion  = 0x01
	IdxLogVersion  = 0x01
	AttrLogVersion = 0x01

	// IdxHeaderSize is the total header size for idx.log.
	// 4 bytes common header + 8 bytes createdAt (Unix microseconds).
	IdxHeaderSize = 12

	// MaxRawLogSize is the hard limit for raw.log (4GB - 1).
	// This ensures rawOffset (uint32) can address all bytes.
	MaxRawLogSize = 1<<32 - 1

	// MaxAttrLogSize is the hard limit for attr.log (4GB - 1).
	// This ensures attrOffset (uint32) can address all bytes.
	MaxAttrLogSize = 1<<32 - 1
)

var (
	ErrRawTooLarge      = errors.New("raw data would exceed max raw.log size")
	ErrAttrTooLarge     = errors.New("attributes would exceed max attr.log size")
	ErrInvalidEntry     = errors.New("invalid idx.log entry")
	ErrInvalidRecordIdx = errors.New("invalid record index")
)

// IdxEntry represents a single entry in idx.log.
type IdxEntry struct {
	SourceTS   time.Time // When the log was generated (zero if not available)
	IngestTS   time.Time
	WriteTS    time.Time
	RawOffset  uint32 // Byte offset into raw.log (after header)
	RawSize    uint32
	AttrOffset uint32 // Byte offset into attr.log (after header)
	AttrSize   uint16 // Length of encoded attributes
}

// EncodeIdxEntry encodes an idx.log entry into a 38-byte buffer.
func EncodeIdxEntry(e IdxEntry, buf []byte) {
	binary.LittleEndian.PutUint64(buf[idxSourceTSOffset:], uint64(e.SourceTS.UnixMicro()))
	binary.LittleEndian.PutUint64(buf[idxIngestTSOffset:], uint64(e.IngestTS.UnixMicro()))
	binary.LittleEndian.PutUint64(buf[idxWriteTSOffset:], uint64(e.WriteTS.UnixMicro()))
	binary.LittleEndian.PutUint32(buf[idxRawOffsetOffset:], e.RawOffset)
	binary.LittleEndian.PutUint32(buf[idxRawSizeOffset:], e.RawSize)
	binary.LittleEndian.PutUint32(buf[idxAttrOffsetOffset:], e.AttrOffset)
	binary.LittleEndian.PutUint16(buf[idxAttrSizeOffset:], e.AttrSize)
}

// DecodeIdxEntry decodes an idx.log entry from a 38-byte buffer.
func DecodeIdxEntry(buf []byte) IdxEntry {
	return IdxEntry{
		SourceTS:   time.UnixMicro(int64(binary.LittleEndian.Uint64(buf[idxSourceTSOffset:]))),
		IngestTS:   time.UnixMicro(int64(binary.LittleEndian.Uint64(buf[idxIngestTSOffset:]))),
		WriteTS:    time.UnixMicro(int64(binary.LittleEndian.Uint64(buf[idxWriteTSOffset:]))),
		RawOffset:  binary.LittleEndian.Uint32(buf[idxRawOffsetOffset:]),
		RawSize:    binary.LittleEndian.Uint32(buf[idxRawSizeOffset:]),
		AttrOffset: binary.LittleEndian.Uint32(buf[idxAttrOffsetOffset:]),
		AttrSize:   binary.LittleEndian.Uint16(buf[idxAttrSizeOffset:]),
	}
}

// IdxFileOffset returns the byte offset in idx.log for a given record index.
func IdxFileOffset(recordIndex uint64) int64 {
	return int64(IdxHeaderSize) + int64(recordIndex)*int64(IdxEntrySize)
}

// RecordCount returns the number of records in an idx.log file given its size.
func RecordCount(idxFileSize int64) uint64 {
	if idxFileSize <= int64(IdxHeaderSize) {
		return 0
	}
	return uint64(idxFileSize-int64(IdxHeaderSize)) / uint64(IdxEntrySize)
}

// RawDataOffset returns the byte offset in raw.log where data begins (after header).
func RawDataOffset() int64 {
	return int64(format.HeaderSize)
}

// BuildRecord constructs a chunk.Record from an IdxEntry, raw data, and attributes.
// The raw slice and attrs are used directly (no copy).
func BuildRecord(entry IdxEntry, raw []byte, attrs chunk.Attributes) chunk.Record {
	return chunk.Record{
		SourceTS: entry.SourceTS,
		IngestTS: entry.IngestTS,
		WriteTS:  entry.WriteTS,
		Attrs:    attrs,
		Raw:      raw,
	}
}

// BuildRecordCopy constructs a chunk.Record from an IdxEntry, raw data, and attributes.
// The raw data and attrs are copied.
func BuildRecordCopy(entry IdxEntry, raw []byte, attrs chunk.Attributes) chunk.Record {
	rawCopy := make([]byte, len(raw))
	copy(rawCopy, raw)
	return chunk.Record{
		SourceTS: entry.SourceTS,
		IngestTS: entry.IngestTS,
		WriteTS:  entry.WriteTS,
		Attrs:    attrs.Copy(),
		Raw:      rawCopy,
	}
}
