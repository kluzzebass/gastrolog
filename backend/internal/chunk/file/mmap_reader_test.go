package file

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kluzzebass/gastrolog/internal/chunk"
)

func TestMmapReaderRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "records.log")

	buf, err := EncodeRecord(chunk.Record{
		IngestTS: time.UnixMicro(123),
		SourceID: chunk.SourceID{},
		Raw:      []byte("payload"),
	}, 1)
	if err != nil {
		t.Fatalf("encode record: %v", err)
	}
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	reader, err := OpenMmapReader(path)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	got, localID, next, err := reader.ReadRecordAt(0)
	if err != nil {
		t.Fatalf("read record: %v", err)
	}
	if next != int64(len(buf)) {
		t.Fatalf("next offset: want %d got %d", len(buf), next)
	}
	if localID != 1 {
		t.Fatalf("local id: want %d got %d", 1, localID)
	}
	assertRecordEqual(t, chunk.Record{
		IngestTS: time.UnixMicro(123),
		Raw:      []byte("payload"),
	}, got)
}

func TestMmapReaderSizeMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "records.log")

	buf, err := EncodeRecord(chunk.Record{
		IngestTS: time.UnixMicro(1),
		SourceID: chunk.NewSourceID(),
		Raw:      []byte("payload"),
	}, 1)
	if err != nil {
		t.Fatalf("encode record: %v", err)
	}
	trailingOffset := len(buf) - SizeFieldBytes
	corrupt := uint32(len(buf) + 1)
	binary.LittleEndian.PutUint32(buf[trailingOffset:], corrupt)
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	reader, err := OpenMmapReader(path)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	if _, _, _, err := reader.ReadRecordAt(0); err != ErrSizeMismatch {
		t.Fatalf("expected size mismatch, got %v", err)
	}
}

func TestOpenMmapReaderEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "records.log")

	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	if _, err := OpenMmapReader(path); err != ErrMmapEmpty {
		t.Fatalf("expected empty error, got %v", err)
	}
}
