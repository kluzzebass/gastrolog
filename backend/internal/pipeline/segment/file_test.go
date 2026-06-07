package segment_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/format"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/record"
)

func sampleRecord(seq uint32) *record.Record {
	ingester := glid.New()
	node := glid.New()
	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	return &record.Record{
		SourceTS: ts.Add(-time.Second),
		IngestTS: ts,
		EventID: record.EventID{
			IngesterID: ingester,
			NodeID:     node,
			IngestTS:   ts,
			IngestSeq:  seq,
		},
		Attrs: record.Attributes{"env": "prod", "level": "error"},
		Raw:   []byte("log line"),
	}
}

func TestCreateAppendReadRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg")

	writeTS := time.Date(2024, 6, 1, 12, 1, 0, 0, time.UTC)
	meta := segment.Meta{ID: glid.New(), VaultID: glid.New()}

	sf, err := segment.Create(path, meta)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	rec1 := sampleRecord(0)
	rec2 := sampleRecord(1)
	if err := sf.Append(rec1, writeTS); err != nil {
		t.Fatalf("Append 1: %v", err)
	}
	firstDataEnd := sf.Header().DataEnd
	if firstDataEnd != segment.HeaderSize {
		t.Fatalf("after first append DataEnd = %d, want %d (start of record)", firstDataEnd, segment.HeaderSize)
	}
	if err := sf.Append(rec2, writeTS); err != nil {
		t.Fatalf("Append 2: %v", err)
	}
	hdr := sf.Header()
	if hdr.RecordCount != 2 || hdr.VaultID != meta.VaultID {
		t.Fatalf("header = %+v", hdr)
	}
	if hdr.DataEnd == firstDataEnd {
		t.Fatalf("DataEnd should advance to start of second record, still %d", hdr.DataEnd)
	}
	if err := sf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	sf2, err := segment.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer sf2.Close()

	got, err := sf2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d records", len(got))
	}
	if got[0].EventID != rec1.EventID || string(got[0].Raw) != string(rec1.Raw) {
		t.Errorf("record 0 mismatch: %+v", got[0])
	}
	if got[1].WriteTS != writeTS {
		t.Errorf("WriteTS = %v, want %v", got[1].WriteTS, writeTS)
	}
	if got[1].Attrs["env"] != "prod" {
		t.Errorf("attrs = %v", got[1].Attrs)
	}
}

func TestHeaderInspectableWithoutScanningRecords(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg")
	meta := segment.Meta{ID: glid.New(), VaultID: glid.New()}

	sf, err := segment.Create(path, meta)
	if err != nil {
		t.Fatal(err)
	}
	rec := sampleRecord(0)
	if err := sf.Append(rec, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := sf.Close(); err != nil {
		t.Fatal(err)
	}

	sf2, err := segment.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sf2.Close()

	hdr := sf2.Header()
	if hdr.DataEnd != segment.HeaderSize {
		t.Errorf("DataEnd = %d, want start of sole record at %d", hdr.DataEnd, segment.HeaderSize)
	}
	if !hdr.FirstIngestTS.Equal(rec.EventID.IngestTS) || !hdr.LastIngestTS.Equal(rec.EventID.IngestTS) {
		t.Errorf("ingest TS in header = first %v last %v", hdr.FirstIngestTS, hdr.LastIngestTS)
	}
}

func TestRecoverTornTail(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg")
	meta := segment.Meta{ID: glid.New(), VaultID: glid.New()}

	sf, err := segment.Create(path, meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := sf.Append(sampleRecord(0), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	validEnd := uint32(info.Size()) //nolint:gosec // G115: test file size
	if err := sf.Close(); err != nil {
		t.Fatal(err)
	}

	// Torn next append: garbage after the end of the last complete frame.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("GARBAGE"), int64(validEnd)); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	sf2, err := segment.Open(path)
	if err != nil {
		t.Fatalf("Open after torn tail: %v", err)
	}
	defer sf2.Close()

	info, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != int64(validEnd) {
		t.Fatalf("size = %d, want %d (end of frame at DataEnd)", info.Size(), validEnd)
	}
	if sf2.Header().DataEnd != segment.HeaderSize {
		t.Fatalf("DataEnd = %d, want start of last record", sf2.Header().DataEnd)
	}
	got, err := sf2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records after recovery", len(got))
	}
}

func TestRecoverHeaderLag(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg")

	sf, err := segment.Create(path, segment.Meta{ID: glid.New(), VaultID: glid.New()})
	if err != nil {
		t.Fatal(err)
	}
	if err := sf.Append(sampleRecord(0), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	afterFirst := uint32(info.Size()) //nolint:gosec // G115: test file size
	if err := sf.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate crash after writing second frame but before header rewrite:
	// copy a valid second frame onto disk while header still describes one record.
	sf2, err := segment.Create(filepath.Join(dir, "donor"), segment.Meta{ID: glid.New(), VaultID: glid.New()})
	if err != nil {
		t.Fatal(err)
	}
	if err := sf2.Append(sampleRecord(0), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := sf2.Append(sampleRecord(1), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	donorInfo, err := os.Stat(filepath.Join(dir, "donor"))
	if err != nil {
		t.Fatal(err)
	}
	_ = sf2.Close()

	frame2 := make([]byte, donorInfo.Size()-int64(afterFirst))
	df, err := os.Open(filepath.Join(dir, "donor"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := df.ReadAt(frame2, int64(afterFirst)); err != nil {
		t.Fatal(err)
	}
	_ = df.Close()

	target, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.WriteAt(frame2, int64(afterFirst)); err != nil {
		t.Fatal(err)
	}
	_ = target.Close()

	sf3, err := segment.Open(path)
	if err != nil {
		t.Fatalf("Open with lagging header: %v", err)
	}
	defer sf3.Close()

	got, err := sf3.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d records, want 2 recovered from disk", len(got))
	}
}

func TestCorruptLastFrameDroppedOnOpen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg")

	sf, err := segment.Create(path, segment.Meta{ID: glid.New(), VaultID: glid.New()})
	if err != nil {
		t.Fatal(err)
	}
	if err := sf.Append(sampleRecord(0), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := sf.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	off := int64(segment.HeaderSize + format.SizeU32 + format.SizeU64)
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, off); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0x01
	if _, err := f.WriteAt(b, off); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	sf2, err := segment.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer sf2.Close()

	got, err := sf2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("corrupt sole frame should be dropped, got %d records", len(got))
	}
}

func TestMarkComplete(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg")

	sf, err := segment.Create(path, segment.Meta{ID: glid.New(), VaultID: glid.New()})
	if err != nil {
		t.Fatal(err)
	}
	if err := sf.MarkComplete(); err != nil {
		t.Fatal(err)
	}
	if err := sf.Close(); err != nil {
		t.Fatal(err)
	}

	sf2, err := segment.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sf2.Close()
	if sf2.Header().Flags&segment.FlagComplete == 0 {
		t.Error("expected FlagComplete set")
	}
}

func TestEncodeFrameAndSize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg")
	writeTS := time.Date(2024, 6, 1, 12, 1, 0, 0, time.UTC)
	rec := sampleRecord(0)

	body, err := segment.EncodeFrame(rec, writeTS)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) == 0 {
		t.Fatal("expected non-empty frame body")
	}

	sf, err := segment.Create(path, segment.Meta{ID: glid.New(), VaultID: glid.New()})
	if err != nil {
		t.Fatal(err)
	}
	if err := sf.AppendFrame(rec, writeTS, body); err != nil {
		t.Fatal(err)
	}
	size, err := sf.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size <= segment.HeaderSize {
		t.Fatalf("Size() = %d, want > header", size)
	}
	if err := sf.Close(); err != nil {
		t.Fatal(err)
	}
}
