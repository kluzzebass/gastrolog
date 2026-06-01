package segment

import (
	"encoding/binary"
	"errors"
	"time"

	"gastrolog/internal/format"
	"gastrolog/internal/glid"
)

const (
	formatVersion = 0x01

	// Wire layout sizes derived from field types — keep HeaderSize in sync.
	headerIDSize         = glid.Size
	headerVaultSize      = glid.Size
	headerRecordCountOff = format.HeaderSize + headerIDSize + headerVaultSize
	headerDataEndOff     = headerRecordCountOff + format.SizeU32
	headerFirstIngestOff = headerDataEndOff + format.SizeU32
	headerLastIngestOff  = headerFirstIngestOff + format.SizeU64
	headerChecksumOff    = headerLastIngestOff + format.SizeU64

	// HeaderSize is the fixed on-disk segment header length (version 1).
	HeaderSize = headerChecksumOff + format.SizeU32

	// FlagComplete marks a segment that has been renamed to completed/.
	FlagComplete = format.FlagComplete
)

var (
	ErrHeaderTooSmall = errors.New("segment header too small")
	ErrBadHeader      = errors.New("invalid segment header")
	ErrDataEnd        = errors.New("segment data end out of range")
)

// Meta identifies a segment file on disk.
type Meta struct {
	ID      glid.GLID
	VaultID glid.GLID
}

// Header is the decoded fixed segment header (inspectable without scanning records).
// Merge bounds use IngestTS only; full EventID order lives in each frame and in the
// per-segment btree index (design-notes §36).
type Header struct {
	format.Header
	ID              glid.GLID
	VaultID         glid.GLID
	RecordCount     uint32
	DataEnd         uint32 // byte offset where the last written record starts
	FirstIngestTS   time.Time
	LastIngestTS    time.Time
	SegmentChecksum uint32 // CRC32(IEEE) of bytes [HeaderSize:validEnd), validEnd = end of frame at DataEnd
}

func encodeHeader(h Header, buf []byte) {
	format.Header{Type: format.TypeSegment, Version: formatVersion, Flags: h.Flags}.EncodeInto(buf)

	copy(buf[format.HeaderSize:format.HeaderSize+headerIDSize], h.ID.Bytes())
	copy(buf[format.HeaderSize+headerIDSize:format.HeaderSize+headerIDSize+headerVaultSize], h.VaultID.Bytes())

	binary.LittleEndian.PutUint32(buf[headerRecordCountOff:], h.RecordCount)
	binary.LittleEndian.PutUint32(buf[headerDataEndOff:], h.DataEnd)
	binary.LittleEndian.PutUint64(buf[headerFirstIngestOff:], tsNanos(h.FirstIngestTS))
	binary.LittleEndian.PutUint64(buf[headerLastIngestOff:], tsNanos(h.LastIngestTS))
	binary.LittleEndian.PutUint32(buf[headerChecksumOff:], h.SegmentChecksum)
}

func decodeHeader(buf []byte) (Header, error) {
	if len(buf) < HeaderSize {
		return Header{}, ErrHeaderTooSmall
	}
	h, err := format.DecodeAndValidate(buf[:format.HeaderSize], format.TypeSegment, formatVersion)
	if err != nil {
		return Header{}, err
	}

	hdr := Header{Header: h}
	hdr.ID = glid.FromBytes(buf[format.HeaderSize : format.HeaderSize+headerIDSize])
	hdr.VaultID = glid.FromBytes(buf[format.HeaderSize+headerIDSize : format.HeaderSize+headerIDSize+headerVaultSize])
	hdr.RecordCount = binary.LittleEndian.Uint32(buf[headerRecordCountOff:])
	hdr.DataEnd = binary.LittleEndian.Uint32(buf[headerDataEndOff:])
	hdr.FirstIngestTS = tsFromNanos(binary.LittleEndian.Uint64(buf[headerFirstIngestOff:]))
	hdr.LastIngestTS = tsFromNanos(binary.LittleEndian.Uint64(buf[headerLastIngestOff:]))
	hdr.SegmentChecksum = binary.LittleEndian.Uint32(buf[headerChecksumOff:])
	return hdr, nil
}

func tsNanos(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	return uint64(t.UnixNano())
}

func tsFromNanos(n uint64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, int64(n)).UTC() //nolint:gosec // G115: segment timestamps are positive in practice
}
