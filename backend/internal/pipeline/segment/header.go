package segment

import (
	"encoding/binary"
	"errors"
	"time"

	"gastrolog/internal/format"
	"gastrolog/internal/glid"
)

const (
	// Wire layout sizes derived from field types — keep HeaderSize in sync.
	headerIDSize              = glid.Size
	headerVaultSize           = glid.Size
	headerRecordCountOff      = format.HeaderSize + headerIDSize + headerVaultSize
	headerDataEndOff          = headerRecordCountOff + format.SizeU32
	headerFirstIngestOff      = headerDataEndOff + format.SizeU32
	headerLastIngestOff       = headerFirstIngestOff + format.SizeU64
	headerIndexOffOff         = headerLastIngestOff + format.SizeU64
	headerChecksumOff         = headerIndexOffOff + format.SizeU32
	headerIndexCRCOff         = headerChecksumOff + format.SizeU64
	headerFirstSourceOff      = headerIndexCRCOff + format.SizeU32
	headerLastSourceOff       = headerFirstSourceOff + format.SizeU64
	headerSourceIndexOffOff   = headerLastSourceOff + format.SizeU64
	headerSourceIndexCountOff = headerSourceIndexOffOff + format.SizeU32
	headerSourceIndexCRCOff   = headerSourceIndexCountOff + format.SizeU32

	// HeaderSizeV1 is the on-disk segment header length for format version 1.
	HeaderSizeV1 = headerIndexCRCOff + format.SizeU32
	// HeaderSize is the fixed on-disk segment header length (current format version).
	HeaderSize = headerSourceIndexCRCOff + format.SizeU32

	// FlagComplete marks a segment that has been renamed to completed/.
	FlagComplete = format.FlagComplete
)

// FormatVersion returns the on-disk segment format version for newly created files.
func FormatVersion() byte { return formatVersion }

// ErrHeaderTooSmall is returned when a segment file is shorter than the
// fixed header.
var ErrHeaderTooSmall = errors.New("segment header too small")

// Meta identifies a segment file on disk.
type Meta struct {
	ID      glid.GLID
	VaultID glid.GLID
}

// Header is the decoded fixed segment header (inspectable without scanning records).
// Merge bounds use IngestTS only; full EventID order lives in the index tail (see index.go).
type Header struct {
	format.Header
	ID            glid.GLID
	VaultID       glid.GLID
	RecordCount   uint32
	DataEnd       uint32 // byte offset where the last written record starts
	FirstIngestTS time.Time
	LastIngestTS  time.Time
	IndexOffset   uint32 // byte offset where the EventID index starts; 0 while working
	// SegmentChecksum is XXH64 of record bytes [HeaderSize:IndexOffset).
	// A non-linear digest, NOT a CRC: each frame ends with its own CRC32 and
	// rolling a CRC over lenPrefix ++ body ++ bodyCRC cancels the content
	// contribution (CRC(M ++ CRC(M)) is constant), leaving the checksum blind
	// to same-length substitution (gastrolog-1vepg0). Zero while empty.
	SegmentChecksum uint64
	IndexChecksum   uint32 // CRC32(IEEE) of index bytes [IndexOffset:IndexOffset+RecordCount*IndexEntrySize)
	// Source index tail (format v2); zero/empty while working or on v1 segments.
	FirstSourceTS       time.Time
	LastSourceTS        time.Time
	SourceIndexOffset   uint32 // byte offset where the SourceTS index starts
	SourceIndexCount    uint32 // sparse entries (non-zero SourceTS only)
	SourceIndexChecksum uint32 // CRC32(IEEE) of source index bytes
}

// IsUnpopulated reports the zero-header sentinel: the struct carries no
// decoded segment stats (RecordCount and SegmentChecksum both zero), either
// because the caller never read the header from disk or because the segment
// is genuinely empty — re-reading the header from disk is the correct move in
// both cases. Publish paths use this to decide whether metadata must come
// from a header-only disk read (gastrolog-faj2yv).
func (h Header) IsUnpopulated() bool {
	return h.RecordCount == 0 && h.SegmentChecksum == 0
}

func encodeHeader(h Header, buf []byte) {
	format.Header{Type: format.TypeSegment, Version: formatVersion, Flags: h.Flags}.EncodeInto(buf)

	copy(buf[format.HeaderSize:format.HeaderSize+headerIDSize], h.ID.Bytes())
	copy(buf[format.HeaderSize+headerIDSize:format.HeaderSize+headerIDSize+headerVaultSize], h.VaultID.Bytes())

	binary.LittleEndian.PutUint32(buf[headerRecordCountOff:], h.RecordCount)
	binary.LittleEndian.PutUint32(buf[headerDataEndOff:], h.DataEnd)
	binary.LittleEndian.PutUint64(buf[headerFirstIngestOff:], tsNanos(h.FirstIngestTS))
	binary.LittleEndian.PutUint64(buf[headerLastIngestOff:], tsNanos(h.LastIngestTS))
	binary.LittleEndian.PutUint32(buf[headerIndexOffOff:], h.IndexOffset)
	binary.LittleEndian.PutUint64(buf[headerChecksumOff:], h.SegmentChecksum)
	binary.LittleEndian.PutUint32(buf[headerIndexCRCOff:], h.IndexChecksum)
	binary.LittleEndian.PutUint64(buf[headerFirstSourceOff:], tsNanos(h.FirstSourceTS))
	binary.LittleEndian.PutUint64(buf[headerLastSourceOff:], tsNanos(h.LastSourceTS))
	binary.LittleEndian.PutUint32(buf[headerSourceIndexOffOff:], h.SourceIndexOffset)
	binary.LittleEndian.PutUint32(buf[headerSourceIndexCountOff:], h.SourceIndexCount)
	binary.LittleEndian.PutUint32(buf[headerSourceIndexCRCOff:], h.SourceIndexChecksum)
}

func decodeHeader(buf []byte) (Header, error) {
	if len(buf) < HeaderSizeV1 {
		return Header{}, ErrHeaderTooSmall
	}
	h, err := format.Decode(buf[:format.HeaderSize])
	if err != nil {
		return Header{}, err
	}
	if h.Type != format.TypeSegment {
		return Header{}, format.ErrTypeMismatch
	}
	if h.Version != formatVersionV1 && h.Version != formatVersionV2 {
		return Header{}, format.ErrVersionMismatch
	}
	if h.Version == formatVersionV2 && len(buf) < HeaderSize {
		return Header{}, ErrHeaderTooSmall
	}

	hdr := Header{Header: h}
	hdr.ID = glid.FromBytes(buf[format.HeaderSize : format.HeaderSize+headerIDSize])
	hdr.VaultID = glid.FromBytes(buf[format.HeaderSize+headerIDSize : format.HeaderSize+headerIDSize+headerVaultSize])
	hdr.RecordCount = binary.LittleEndian.Uint32(buf[headerRecordCountOff:])
	hdr.DataEnd = binary.LittleEndian.Uint32(buf[headerDataEndOff:])
	hdr.FirstIngestTS = tsFromNanos(binary.LittleEndian.Uint64(buf[headerFirstIngestOff:]))
	hdr.LastIngestTS = tsFromNanos(binary.LittleEndian.Uint64(buf[headerLastIngestOff:]))
	hdr.IndexOffset = binary.LittleEndian.Uint32(buf[headerIndexOffOff:])
	hdr.SegmentChecksum = binary.LittleEndian.Uint64(buf[headerChecksumOff:])
	hdr.IndexChecksum = binary.LittleEndian.Uint32(buf[headerIndexCRCOff:])
	if h.Version == formatVersionV2 {
		hdr.FirstSourceTS = tsFromNanos(binary.LittleEndian.Uint64(buf[headerFirstSourceOff:]))
		hdr.LastSourceTS = tsFromNanos(binary.LittleEndian.Uint64(buf[headerLastSourceOff:]))
		hdr.SourceIndexOffset = binary.LittleEndian.Uint32(buf[headerSourceIndexOffOff:])
		hdr.SourceIndexCount = binary.LittleEndian.Uint32(buf[headerSourceIndexCountOff:])
		hdr.SourceIndexChecksum = binary.LittleEndian.Uint32(buf[headerSourceIndexCRCOff:])
	} else if hdr.IndexOffset > 0 {
		hdr.SourceIndexOffset = hdr.IndexOffset + hdr.RecordCount*IndexEntrySize
	}
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
