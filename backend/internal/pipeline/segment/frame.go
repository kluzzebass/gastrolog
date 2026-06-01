package segment

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"time"

	"gastrolog/internal/format"
	"gastrolog/internal/glid"
	"gastrolog/internal/record"
)

const (
	frameLenPrefixSize = format.SizeU32
	eventIDWireSize    = glid.Size + glid.Size + format.SizeU64 + format.SizeU32
	frameCRCSize       = format.SizeU32
)

var (
	ErrFrameTooSmall  = errors.New("segment frame too small")
	ErrFrameCRC       = errors.New("segment frame CRC mismatch")
	ErrFrameLength    = errors.New("segment frame length invalid")
	ErrTruncatedFrame = errors.New("segment frame truncated")
)

// encodeFrame builds the frame body (excluding the u32 frameLen prefix).
// Layout: EventID + sourceTS + writeTS + inline attrs + raw + frame CRC32.
func encodeFrame(rec *record.Record, writeTS time.Time) ([]byte, error) {
	if rec == nil {
		return nil, errors.New("nil record")
	}
	attrBlob, err := rec.Attrs.Encode()
	if err != nil {
		return nil, err
	}

	bodyLen := eventIDWireSize + format.SizeU64 + format.SizeU64 + len(attrBlob) + format.SizeU32 + len(rec.Raw) + frameCRCSize
	body := make([]byte, bodyLen)
	off := 0

	encodeEventID(body[off:], rec.EventID)
	off += eventIDWireSize

	binary.LittleEndian.PutUint64(body[off:], tsNanos(rec.SourceTS))
	off += format.SizeU64
	binary.LittleEndian.PutUint64(body[off:], tsNanos(writeTS))
	off += format.SizeU64

	copy(body[off:], attrBlob)
	off += len(attrBlob)

	binary.LittleEndian.PutUint32(body[off:], uint32(len(rec.Raw))) //nolint:gosec // G115: raw bounded by ingest limits
	off += format.SizeU32
	copy(body[off:], rec.Raw)
	off += len(rec.Raw)

	crc := crc32.ChecksumIEEE(body[:off])
	binary.LittleEndian.PutUint32(body[off:], crc)
	return body, nil
}

func decodeFrameBody(body []byte) (record.Record, error) {
	minBody := eventIDWireSize + format.SizeU64 + format.SizeU64 + format.SizeU16 + format.SizeU32 + frameCRCSize
	if len(body) < minBody {
		return record.Record{}, ErrFrameTooSmall
	}
	off := 0

	eventID, err := decodeEventID(body[off : off+eventIDWireSize])
	if err != nil {
		return record.Record{}, err
	}
	off += eventIDWireSize

	rec := record.Record{
		EventID:  eventID,
		SourceTS: tsFromNanos(binary.LittleEndian.Uint64(body[off:])),
		IngestTS: eventID.IngestTS,
	}
	off += format.SizeU64
	rec.WriteTS = tsFromNanos(binary.LittleEndian.Uint64(body[off:]))
	off += format.SizeU64

	attrs, n, err := record.DecodeAttributes(body[off:])
	if err != nil {
		return record.Record{}, err
	}
	rec.Attrs = attrs
	off += n

	if len(body)-off < format.SizeU32 {
		return record.Record{}, ErrTruncatedFrame
	}
	rawLen := binary.LittleEndian.Uint32(body[off:])
	off += format.SizeU32
	if int(rawLen) > len(body)-off-frameCRCSize {
		return record.Record{}, ErrTruncatedFrame
	}
	rec.Raw = make([]byte, rawLen)
	copy(rec.Raw, body[off:off+int(rawLen)])
	off += int(rawLen)

	if len(body)-off < frameCRCSize {
		return record.Record{}, ErrTruncatedFrame
	}
	wantCRC := binary.LittleEndian.Uint32(body[off:])
	gotCRC := crc32.ChecksumIEEE(body[:off])
	if wantCRC != gotCRC {
		return record.Record{}, ErrFrameCRC
	}
	return rec, nil
}

func encodeEventID(buf []byte, id record.EventID) {
	copy(buf[0:glid.Size], id.IngesterID.Bytes())
	copy(buf[glid.Size:glid.Size*2], id.NodeID.Bytes())
	binary.LittleEndian.PutUint64(buf[glid.Size*2:], tsNanos(id.IngestTS))
	binary.LittleEndian.PutUint32(buf[glid.Size*2+format.SizeU64:], id.IngestSeq)
}

func decodeEventID(buf []byte) (record.EventID, error) {
	if len(buf) < eventIDWireSize {
		return record.EventID{}, ErrFrameTooSmall
	}
	return record.EventID{
		IngesterID: glid.FromBytes(buf[0:glid.Size]),
		NodeID:     glid.FromBytes(buf[glid.Size : glid.Size*2]),
		IngestTS:   tsFromNanos(binary.LittleEndian.Uint64(buf[glid.Size*2:])),
		IngestSeq:  binary.LittleEndian.Uint32(buf[glid.Size*2+format.SizeU64:]),
	}, nil
}
