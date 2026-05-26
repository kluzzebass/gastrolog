package spool

import (
	"encoding/binary"
	"errors"
	"time"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/glid"
)

// spool idx.log entry layout (82 bytes): chunk idx entry fields plus vault_seq.
const (
	SpoolIdxEntrySize = chunkfile.IdxEntrySize + 8

	spoolVaultSeqOffset = 0
)

var ErrInvalidSpoolEntry = errors.New("invalid spool idx entry")

// IdxEntry is one committed spool record index entry.
type IdxEntry struct {
	VaultSeq   uint64
	SourceTS   time.Time
	IngestTS   time.Time
	WriteTS    time.Time
	RawOffset  uint32
	RawSize    uint32
	AttrOffset uint32
	AttrSize   uint16
	IngestSeq  uint32
	IngesterID glid.GLID
	NodeID     glid.GLID
}

// EncodeIdxEntry writes entry into an 82-byte buffer.
func EncodeIdxEntry(e IdxEntry, buf []byte) {
	if len(buf) < SpoolIdxEntrySize {
		panic("spool: idx entry buffer too small")
	}
	binary.LittleEndian.PutUint64(buf[spoolVaultSeqOffset:], e.VaultSeq)
	var chunkBuf [chunkfile.IdxEntrySize]byte
	chunkfile.EncodeIdxEntry(chunkfile.IdxEntry{
		SourceTS:   e.SourceTS,
		IngestTS:   e.IngestTS,
		WriteTS:    e.WriteTS,
		RawOffset:  e.RawOffset,
		RawSize:    e.RawSize,
		AttrOffset: e.AttrOffset,
		AttrSize:   e.AttrSize,
		IngestSeq:  e.IngestSeq,
		IngesterID: e.IngesterID,
		NodeID:     e.NodeID,
	}, chunkBuf[:])
	copy(buf[8:], chunkBuf[:])
}

// DecodeIdxEntry reads entry from an 82-byte buffer.
func DecodeIdxEntry(buf []byte) IdxEntry {
	if len(buf) < SpoolIdxEntrySize {
		panic("spool: idx entry buffer too small")
	}
	ce := chunkfile.DecodeIdxEntry(buf[8:])
	return IdxEntry{
		VaultSeq:   binary.LittleEndian.Uint64(buf[spoolVaultSeqOffset:]),
		SourceTS:   ce.SourceTS,
		IngestTS:   ce.IngestTS,
		WriteTS:    ce.WriteTS,
		RawOffset:  ce.RawOffset,
		RawSize:    ce.RawSize,
		AttrOffset: ce.AttrOffset,
		AttrSize:   ce.AttrSize,
		IngestSeq:  ce.IngestSeq,
		IngesterID: ce.IngesterID,
		NodeID:     ce.NodeID,
	}
}

// IdxHeaderSize is idx.log header size (format header + createdAt).
const IdxHeaderSize = chunkfile.IdxHeaderSize

// SlotIdxFileOffset returns the byte offset of the idx slot for vaultSeq within a window.
func SlotIdxFileOffset(windowStart, vaultSeq uint64) int64 {
	return int64(IdxHeaderSize) + int64(vaultSeq-windowStart)*int64(SpoolIdxEntrySize) //nolint:gosec // G115: bounded by window size
}

// WindowSlotCount returns the number of idx slots in a window [start..end] inclusive.
func WindowSlotCount(start, end uint64) uint64 {
	return end - start + 1
}

// WindowIdxFileSize returns the idx.log size for a fully allocated window index.
func WindowIdxFileSize(start, end uint64) int64 {
	return int64(IdxHeaderSize) + int64(WindowSlotCount(start, end))*int64(SpoolIdxEntrySize) //nolint:gosec // G115: bounded
}

// IdxFileOffset returns the byte offset of recordIndex in idx.log.
func IdxFileOffset(recordIndex uint64) int64 {
	return int64(IdxHeaderSize) + int64(recordIndex)*int64(SpoolIdxEntrySize) //nolint:gosec // G115: record index bounded by segment size
}

// RecordCount returns committed record count from idx.log file size.
func RecordCount(idxFileSize int64) uint64 {
	if idxFileSize <= int64(IdxHeaderSize) {
		return 0
	}
	return uint64(idxFileSize-int64(IdxHeaderSize)) / uint64(SpoolIdxEntrySize)
}

// EntryFromRecord builds a spool idx entry template from a record with VaultSeq set.
func EntryFromRecord(rec chunk.Record, rawOffset, attrOffset uint32, rawSize uint32, attrSize uint16) IdxEntry {
	return IdxEntry{
		VaultSeq:   rec.VaultSeq,
		SourceTS:   rec.SourceTS,
		IngestTS:   rec.IngestTS,
		WriteTS:    rec.WriteTS,
		RawOffset:  rawOffset,
		RawSize:    rawSize,
		AttrOffset: attrOffset,
		AttrSize:   attrSize,
		IngestSeq:  rec.EventID.IngestSeq,
		IngesterID: rec.EventID.IngesterID,
		NodeID:     rec.EventID.NodeID,
	}
}

// BuildRecord reconstructs a chunk.Record from a spool idx entry and payload slices.
func BuildRecord(entry IdxEntry, raw []byte, attrs chunk.Attributes) chunk.Record {
	rec := chunkfile.BuildRecord(chunkfile.IdxEntry{
		SourceTS:   entry.SourceTS,
		IngestTS:   entry.IngestTS,
		WriteTS:    entry.WriteTS,
		RawOffset:  entry.RawOffset,
		RawSize:    entry.RawSize,
		AttrOffset: entry.AttrOffset,
		AttrSize:   entry.AttrSize,
		IngestSeq:  entry.IngestSeq,
		IngesterID: entry.IngesterID,
		NodeID:     entry.NodeID,
	}, raw, attrs)
	rec.VaultSeq = entry.VaultSeq
	return rec
}
