package file

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/kluzzebass/gastrolog/internal/chunk"
)

func TestEncodeDecodeRecordRoundTrip(t *testing.T) {
	record := chunk.Record{
		IngestTS: time.UnixMicro(123456),
		SourceID: chunk.NewSourceID(),
		Raw:      []byte("payload"),
	}
	buf, err := EncodeRecord(record, 7)
	if err != nil {
		t.Fatalf("encode record: %v", err)
	}
	got, localID, err := DecodeRecord(buf)
	if err != nil {
		t.Fatalf("decode record: %v", err)
	}
	if localID != 7 {
		t.Fatalf("local id: want %d got %d", 7, localID)
	}
	if !record.IngestTS.Equal(got.IngestTS) {
		t.Fatalf("ingest ts: want %v got %v", record.IngestTS, got.IngestTS)
	}
	if !bytes.Equal(record.Raw, got.Raw) {
		t.Fatalf("raw: want %q got %q", record.Raw, got.Raw)
	}
}

func TestDecodeRecordSizeMismatch(t *testing.T) {
	record := chunk.Record{IngestTS: time.UnixMicro(1), SourceID: chunk.NewSourceID(), Raw: []byte("abc")}
	buf, err := EncodeRecord(record, 1)
	if err != nil {
		t.Fatalf("encode record: %v", err)
	}
	binary.LittleEndian.PutUint32(buf[:SizeFieldBytes], uint32(len(buf)+1))
	if _, _, err := DecodeRecord(buf); err != ErrSizeMismatch {
		t.Fatalf("expected size mismatch, got %v", err)
	}
}

func TestDecodeRecordMagicMismatch(t *testing.T) {
	record := chunk.Record{IngestTS: time.UnixMicro(1), SourceID: chunk.NewSourceID(), Raw: []byte("abc")}
	buf, err := EncodeRecord(record, 2)
	if err != nil {
		t.Fatalf("encode record: %v", err)
	}
	buf[SizeFieldBytes] = MagicByte + 1
	if _, _, err := DecodeRecord(buf); err != ErrMagicMismatch {
		t.Fatalf("expected magic mismatch, got %v", err)
	}
}

func TestDecodeRecordVersionMismatch(t *testing.T) {
	record := chunk.Record{IngestTS: time.UnixMicro(1), SourceID: chunk.NewSourceID(), Raw: []byte("abc")}
	buf, err := EncodeRecord(record, 3)
	if err != nil {
		t.Fatalf("encode record: %v", err)
	}
	buf[SizeFieldBytes+MagicFieldBytes] = VersionByte + 1
	if _, _, err := DecodeRecord(buf); err != ErrVersionMismatch {
		t.Fatalf("expected version mismatch, got %v", err)
	}
}

func TestDecodeRecordRawLengthMismatch(t *testing.T) {
	record := chunk.Record{IngestTS: time.UnixMicro(1), SourceID: chunk.NewSourceID(), Raw: []byte("abc")}
	buf, err := EncodeRecord(record, 4)
	if err != nil {
		t.Fatalf("encode record: %v", err)
	}
	rawLenOffset := SizeFieldBytes + MagicFieldBytes + VersionBytes + IngestTSBytes + SourceLocalIDBytes
	binary.LittleEndian.PutUint32(buf[rawLenOffset:rawLenOffset+RawLenBytes], 99)
	if _, _, err := DecodeRecord(buf); err != ErrRawLengthMismatch {
		t.Fatalf("expected raw length mismatch, got %v", err)
	}
}

func TestDecodeRecordTooSmall(t *testing.T) {
	buf := make([]byte, MinRecordSize-1)
	if _, _, err := DecodeRecord(buf); err != ErrRecordTooSmall {
		t.Fatalf("expected too small, got %v", err)
	}
}

func TestRecordSizeRawLengthInvalid(t *testing.T) {
	if _, err := RecordSize(-1); err != ErrRawLengthInvalid {
		t.Fatalf("expected raw length invalid, got %v", err)
	}
}

func TestRecordSizeTooLarge(t *testing.T) {
	if _, err := RecordSize(int(^uint32(0))); err != ErrRecordTooLarge {
		t.Fatalf("expected record too large, got %v", err)
	}
}
