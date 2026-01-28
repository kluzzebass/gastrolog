package file

import (
	"encoding/binary"
	"errors"
	"math"
	"time"

	"gastrolog/internal/chunk"
)

const (
	MagicByte   = 0x69
	VersionByte = 0x01

	SizeFieldBytes     = 4
	MagicFieldBytes    = 1
	VersionBytes       = 1
	IngestTSBytes      = 8
	WriteTSBytes       = 8
	SourceLocalIDBytes = 4
	RawLenBytes        = 4

	HeaderBytes   = SizeFieldBytes + MagicFieldBytes + VersionBytes + IngestTSBytes + WriteTSBytes + SourceLocalIDBytes + RawLenBytes
	MinRecordSize = HeaderBytes + SizeFieldBytes
)

var (
	ErrRecordTooSmall    = errors.New("record size too small")
	ErrRecordTooLarge    = errors.New("record size too large")
	ErrMagicMismatch     = errors.New("record magic mismatch")
	ErrVersionMismatch   = errors.New("record version mismatch")
	ErrSizeMismatch      = errors.New("record size mismatch")
	ErrRawLengthInvalid  = errors.New("record raw length invalid")
	ErrRawLengthMismatch = errors.New("record raw length mismatch")
	ErrNoPreviousRecord  = errors.New("no previous record")
)

func RecordSize(rawLen int) (uint32, error) {
	if rawLen < 0 {
		return 0, ErrRawLengthInvalid
	}
	size := uint64(MinRecordSize) + uint64(rawLen)
	if size > math.MaxUint32 {
		return 0, ErrRecordTooLarge
	}
	return uint32(size), nil
}

func EncodeRecord(record chunk.Record, localID uint32) ([]byte, error) {
	rawLen := len(record.Raw)
	size, err := RecordSize(rawLen)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf[:SizeFieldBytes], size)
	cursor := SizeFieldBytes
	buf[cursor] = MagicByte
	cursor += MagicFieldBytes
	buf[cursor] = VersionByte
	cursor += VersionBytes
	binary.LittleEndian.PutUint64(buf[cursor:cursor+IngestTSBytes], uint64(record.IngestTS.UnixMicro()))
	cursor += IngestTSBytes
	binary.LittleEndian.PutUint64(buf[cursor:cursor+WriteTSBytes], uint64(record.WriteTS.UnixMicro()))
	cursor += WriteTSBytes
	binary.LittleEndian.PutUint32(buf[cursor:cursor+SourceLocalIDBytes], localID)
	cursor += SourceLocalIDBytes
	binary.LittleEndian.PutUint32(buf[cursor:cursor+RawLenBytes], uint32(rawLen))
	cursor += RawLenBytes
	copy(buf[cursor:cursor+rawLen], record.Raw)
	cursor += rawLen
	binary.LittleEndian.PutUint32(buf[cursor:cursor+SizeFieldBytes], size)

	return buf, nil
}

func DecodeRecord(buf []byte) (chunk.Record, uint32, error) {
	if len(buf) < int(MinRecordSize) {
		return chunk.Record{}, 0, ErrRecordTooSmall
	}
	size := binary.LittleEndian.Uint32(buf[:SizeFieldBytes])
	if size != uint32(len(buf)) {
		return chunk.Record{}, 0, ErrSizeMismatch
	}

	cursor := SizeFieldBytes
	if buf[cursor] != MagicByte {
		return chunk.Record{}, 0, ErrMagicMismatch
	}
	cursor += MagicFieldBytes
	if buf[cursor] != VersionByte {
		return chunk.Record{}, 0, ErrVersionMismatch
	}
	cursor += VersionBytes

	ingestTS := binary.LittleEndian.Uint64(buf[cursor : cursor+IngestTSBytes])
	cursor += IngestTSBytes
	writeTS := binary.LittleEndian.Uint64(buf[cursor : cursor+WriteTSBytes])
	cursor += WriteTSBytes
	localID := binary.LittleEndian.Uint32(buf[cursor : cursor+SourceLocalIDBytes])
	cursor += SourceLocalIDBytes
	rawLen := binary.LittleEndian.Uint32(buf[cursor : cursor+RawLenBytes])
	cursor += RawLenBytes
	rawEnd := cursor + int(rawLen)
	if rawEnd+SizeFieldBytes != len(buf) {
		return chunk.Record{}, 0, ErrRawLengthMismatch
	}

	raw := make([]byte, rawLen)
	copy(raw, buf[cursor:rawEnd])
	cursor = rawEnd
	trailing := binary.LittleEndian.Uint32(buf[cursor : cursor+SizeFieldBytes])
	if trailing != size {
		return chunk.Record{}, 0, ErrSizeMismatch
	}

	return chunk.Record{
		IngestTS: time.UnixMicro(int64(ingestTS)),
		WriteTS:  time.UnixMicro(int64(writeTS)),
		Raw:      raw,
	}, localID, nil
}
