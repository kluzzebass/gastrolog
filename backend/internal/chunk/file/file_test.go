package file

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

func TestFileWriterReaderRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "records.log")

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	defer file.Close()

	records := []chunk.Record{
		{IngestTS: time.UnixMicro(111), SourceID: chunk.SourceID{}, Raw: []byte("alpha")},
		{IngestTS: time.UnixMicro(222), SourceID: chunk.SourceID{}, Raw: []byte("beta-gamma")},
	}
	localIDs := []uint32{1, 2}

	var offsets []int64
	for i, rec := range records {
		offset, _, err := appendRecord(file, rec, localIDs[i])
		if err != nil {
			t.Fatalf("append record: %v", err)
		}
		offsets = append(offsets, int64(offset))
	}
	reader, err := OpenReader(path)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	for i, offset := range offsets {
		got, localID, next, err := reader.ReadRecordAt(offset)
		if err != nil {
			t.Fatalf("read record: %v", err)
		}
		if next <= offset {
			t.Fatalf("expected next offset > %d, got %d", offset, next)
		}
		if localID != localIDs[i] {
			t.Fatalf("local id: want %d got %d", localIDs[i], localID)
		}
		assertRecordEqual(t, records[i], got)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	byteReader := NewReader(bytes.NewReader(data), nil)
	for i, offset := range offsets {
		got, localID, _, err := byteReader.ReadRecordAt(offset)
		if err != nil {
			t.Fatalf("read record via bytes: %v", err)
		}
		if localID != localIDs[i] {
			t.Fatalf("local id: want %d got %d", localIDs[i], localID)
		}
		assertRecordEqual(t, records[i], got)
	}

	mapped, err := OpenMmapReader(path)
	if err != nil {
		t.Fatalf("open mmap reader: %v", err)
	}
	defer mapped.Close()
	for i, offset := range offsets {
		got, localID, _, err := mapped.ReadRecordAt(offset)
		if err != nil {
			t.Fatalf("read record via mmap: %v", err)
		}
		if localID != localIDs[i] {
			t.Fatalf("local id: want %d got %d", localIDs[i], localID)
		}
		assertRecordEqual(t, records[i], got)
	}
}

func TestReadRecordBeforeMultipleRecords(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "records.log")

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}

	records := []chunk.Record{
		{IngestTS: time.UnixMicro(100), SourceID: chunk.SourceID{}, Raw: []byte("first")},
		{IngestTS: time.UnixMicro(200), SourceID: chunk.SourceID{}, Raw: []byte("second-longer")},
		{IngestTS: time.UnixMicro(300), SourceID: chunk.SourceID{}, Raw: []byte("third")},
	}
	localIDs := []uint32{1, 2, 3}

	var totalSize int64
	for i, rec := range records {
		_, size, err := appendRecord(file, rec, localIDs[i])
		if err != nil {
			t.Fatalf("append record %d: %v", i, err)
		}
		totalSize += int64(size)
	}
	file.Close()

	// Test backward seek with file Reader.
	reader, err := OpenReader(path)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	offset := totalSize
	for i := len(records) - 1; i >= 0; i-- {
		got, localID, prev, err := reader.ReadRecordBefore(offset)
		if err != nil {
			t.Fatalf("ReadRecordBefore at offset %d (record %d): %v", offset, i, err)
		}
		if localID != localIDs[i] {
			t.Fatalf("record %d: local id want %d got %d", i, localIDs[i], localID)
		}
		assertRecordEqual(t, records[i], got)
		offset = prev
	}

	// Should get ErrNoPreviousRecord at beginning of file.
	if _, _, _, err := reader.ReadRecordBefore(offset); err != ErrNoPreviousRecord {
		t.Fatalf("expected ErrNoPreviousRecord at offset %d, got %v", offset, err)
	}

	// Test backward seek with MmapReader.
	mapped, err := OpenMmapReader(path)
	if err != nil {
		t.Fatalf("open mmap reader: %v", err)
	}
	defer mapped.Close()

	offset = totalSize
	for i := len(records) - 1; i >= 0; i-- {
		got, localID, prev, err := mapped.ReadRecordBefore(offset)
		if err != nil {
			t.Fatalf("mmap ReadRecordBefore at offset %d (record %d): %v", offset, i, err)
		}
		if localID != localIDs[i] {
			t.Fatalf("mmap record %d: local id want %d got %d", i, localIDs[i], localID)
		}
		assertRecordEqual(t, records[i], got)
		offset = prev
	}

	if _, _, _, err := mapped.ReadRecordBefore(offset); err != ErrNoPreviousRecord {
		t.Fatalf("mmap: expected ErrNoPreviousRecord at offset %d, got %v", offset, err)
	}
}

func TestReadRecordBeforeBoundary(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "records.log")

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	rec := chunk.Record{IngestTS: time.UnixMicro(1), SourceID: chunk.SourceID{}, Raw: []byte("x")}
	_, _, err = appendRecord(file, rec, 1)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	file.Close()

	reader, err := OpenReader(path)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	// offset 0 is before any record
	if _, _, _, err := reader.ReadRecordBefore(0); err != ErrNoPreviousRecord {
		t.Fatalf("expected ErrNoPreviousRecord at offset 0, got %v", err)
	}
	// offset less than MinRecordSize
	if _, _, _, err := reader.ReadRecordBefore(SizeFieldBytes); err != ErrNoPreviousRecord {
		t.Fatalf("expected ErrNoPreviousRecord at offset %d, got %v", SizeFieldBytes, err)
	}
}

func TestFileReaderSizeMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "records.log")

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	record := chunk.Record{IngestTS: time.UnixMicro(123), SourceID: chunk.NewSourceID(), Raw: []byte("payload")}
	offset, size, err := appendRecord(file, record, 1)
	if err != nil {
		t.Fatalf("append record: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	defer file.Close()

	corrupt := size + 1
	var buf [SizeFieldBytes]byte
	binary.LittleEndian.PutUint32(buf[:], corrupt)
	if _, err := file.WriteAt(buf[:], int64(offset)+int64(size)-SizeFieldBytes); err != nil {
		t.Fatalf("write corrupt size: %v", err)
	}

	reader, err := OpenReader(path)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	if _, _, _, err := reader.ReadRecordAt(int64(offset)); err != ErrSizeMismatch {
		t.Fatalf("expected size mismatch, got %v", err)
	}
}

func assertRecordEqual(t *testing.T, want, got chunk.Record) {
	t.Helper()
	if !want.IngestTS.Equal(got.IngestTS) {
		t.Fatalf("ingest ts: want %v got %v", want.IngestTS, got.IngestTS)
	}
	if want.SourceID != (chunk.SourceID{}) && want.SourceID != got.SourceID {
		t.Fatalf("source id: want %s got %s", want.SourceID.String(), got.SourceID.String())
	}
	if !bytes.Equal(want.Raw, got.Raw) {
		t.Fatalf("raw: want %q got %q", want.Raw, got.Raw)
	}
}
