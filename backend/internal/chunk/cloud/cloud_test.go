package cloud_test

import (
	"bytes"
	"gastrolog/internal/glid"
	"io"
	"os"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/cloud"
)

func testRecords() (chunk.ChunkID, glid.GLID, []chunk.Record) {
	chunkID := chunk.NewChunkID()
	vaultID := glid.New()
	ingesterID := glid.New()
	now := time.Now().Truncate(time.Nanosecond)

	records := []chunk.Record{
		{
			SourceTS: now.Add(-2 * time.Second),
			IngestTS: now.Add(-1 * time.Second),
			WriteTS:  now,
			EventID:  chunk.EventID{IngesterID: ingesterID, IngestTS: now.Add(-1 * time.Second), IngestSeq: 1},
			Attrs:    chunk.Attributes{"host": "web-1", "level": "info"},
			Raw:      []byte("first log message"),
		},
		{
			SourceTS: now.Add(-1 * time.Second),
			IngestTS: now,
			WriteTS:  now.Add(1 * time.Millisecond),
			EventID:  chunk.EventID{IngesterID: ingesterID, IngestTS: now, IngestSeq: 2},
			Attrs:    chunk.Attributes{"host": "web-1", "level": "error"},
			Raw:      []byte("second log message"),
		},
		{
			// No SourceTS.
			IngestTS: now.Add(1 * time.Second),
			WriteTS:  now.Add(2 * time.Millisecond),
			EventID:  chunk.EventID{IngesterID: ingesterID, IngestTS: now.Add(1 * time.Second), IngestSeq: 3},
			Attrs:    chunk.Attributes{"host": "db-1"},
			Raw:      []byte("third message without source ts"),
		},
	}
	return chunkID, vaultID, records
}

func assertRecord(t *testing.T, i int, got, want chunk.Record) {
	t.Helper()
	if !got.WriteTS.Equal(want.WriteTS) {
		t.Errorf("[%d] WriteTS = %v, want %v", i, got.WriteTS, want.WriteTS)
	}
	if !got.IngestTS.Equal(want.IngestTS) {
		t.Errorf("[%d] IngestTS = %v, want %v", i, got.IngestTS, want.IngestTS)
	}
	if !got.SourceTS.Equal(want.SourceTS) {
		t.Errorf("[%d] SourceTS = %v, want %v", i, got.SourceTS, want.SourceTS)
	}
	if got.EventID.IngesterID != want.EventID.IngesterID {
		t.Errorf("[%d] IngesterID mismatch", i)
	}
	if got.EventID.IngestSeq != want.EventID.IngestSeq {
		t.Errorf("[%d] IngestSeq = %d, want %d", i, got.EventID.IngestSeq, want.EventID.IngestSeq)
	}
	if string(got.Raw) != string(want.Raw) {
		t.Errorf("[%d] Raw = %q, want %q", i, got.Raw, want.Raw)
	}
	for k, v := range want.Attrs {
		if got.Attrs[k] != v {
			t.Errorf("[%d] Attrs[%q] = %q, want %q", i, k, got.Attrs[k], v)
		}
	}
	if len(got.Attrs) != len(want.Attrs) {
		t.Errorf("[%d] Attrs count = %d, want %d", i, len(got.Attrs), len(want.Attrs))
	}
}

// writeBlobToTempFile writes a blob to a temp file and returns it seeked to start.
func writeBlobToTempFile(t *testing.T, chunkID chunk.ChunkID, vaultID glid.GLID, records []chunk.Record) *os.File {
	t.Helper()

	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	defer enc.Close()
	w := cloud.NewWriter(chunkID, vaultID)
	for _, rec := range records {
		if err := w.Add(rec); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	var buf bytes.Buffer
	if _, err := w.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	t.Logf("blob size: %d bytes (%d records)", buf.Len(), len(records))

	tmp, err := os.CreateTemp(t.TempDir(), "glcb-test-*")
	if err != nil {
		t.Fatalf("create temp: %v", err)
	}
	if _, err := io.Copy(tmp, &buf); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("seek: %v", err)
	}
	return tmp
}

func TestRoundTrip(t *testing.T) {
	chunkID, vaultID, records := testRecords()

	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	defer enc.Close()
	w := cloud.NewWriter(chunkID, vaultID)
	for _, rec := range records {
		if err := w.Add(rec); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	// Verify writer metadata.
	wm := w.Meta()
	if wm.RecordCount != 3 {
		t.Errorf("writer meta count = %d, want 3", wm.RecordCount)
	}
	if wm.ChunkID != chunkID {
		t.Errorf("writer meta chunkID mismatch")
	}
	if wm.VaultID != vaultID {
		t.Errorf("writer meta vaultID mismatch")
	}

	tmp := writeBlobToTempFile(t, chunkID, vaultID, records)

	rd, err := cloud.NewReader(tmp)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer rd.Close()

	// Verify reader metadata.
	rm := rd.Meta()
	if rm.RecordCount != 3 {
		t.Errorf("reader meta count = %d, want 3", rm.RecordCount)
	}
	if rm.ChunkID != chunkID {
		t.Errorf("reader meta chunkID mismatch")
	}
	if rm.VaultID != vaultID {
		t.Errorf("reader meta vaultID mismatch")
	}
	if rm.SourceStart.IsZero() {
		t.Error("reader meta sourceStart is zero")
	}

	// Sequential read.
	for i, want := range records {
		got, err := rd.ReadRecord(uint32(i))
		if err != nil {
			t.Fatalf("ReadRecord[%d]: %v", i, err)
		}
		assertRecord(t, i, got, want)
	}

	// Past-end.
	_, err = rd.ReadRecord(3)
	if err != chunk.ErrNoMoreRecords {
		t.Errorf("expected ErrNoMoreRecords, got %v", err)
	}

	// Random access: read record 2, then 0.
	got, err := rd.ReadRecord(2)
	if err != nil {
		t.Fatalf("ReadRecord[2]: %v", err)
	}
	assertRecord(t, 2, got, records[2])

	got, err = rd.ReadRecord(0)
	if err != nil {
		t.Fatalf("ReadRecord[0]: %v", err)
	}
	assertRecord(t, 0, got, records[0])
}

func TestSeekableCursor(t *testing.T) {
	chunkID, vaultID, records := testRecords()
	tmp := writeBlobToTempFile(t, chunkID, vaultID, records)

	rd, err := cloud.NewReader(tmp)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	cursor := cloud.NewSeekableCursor(rd, chunkID)
	defer cursor.Close()

	// Forward iteration.
	for i, want := range records {
		got, ref, err := cursor.Next()
		if err != nil {
			t.Fatalf("Next[%d]: %v", i, err)
		}
		if ref.Pos != uint64(i) {
			t.Errorf("[%d] Pos = %d", i, ref.Pos)
		}
		assertRecord(t, i, got, want)
	}

	_, _, err = cursor.Next()
	if err != chunk.ErrNoMoreRecords {
		t.Errorf("expected ErrNoMoreRecords, got %v", err)
	}

	// Seek to record 2, then Prev to get records 1, 0.
	if err := cursor.Seek(chunk.RecordRef{ChunkID: chunkID, Pos: 2}); err != nil {
		t.Fatalf("Seek: %v", err)
	}

	got, ref, err := cursor.Prev()
	if err != nil {
		t.Fatalf("Prev after Seek(2): %v", err)
	}
	if ref.Pos != 1 {
		t.Errorf("Prev pos = %d, want 1", ref.Pos)
	}
	assertRecord(t, 1, got, records[1])

	got, ref, err = cursor.Prev()
	if err != nil {
		t.Fatalf("Prev: %v", err)
	}
	if ref.Pos != 0 {
		t.Errorf("Prev pos = %d, want 0", ref.Pos)
	}
	assertRecord(t, 0, got, records[0])

	// Prev past start.
	_, _, err = cursor.Prev()
	if err != chunk.ErrNoMoreRecords {
		t.Errorf("expected ErrNoMoreRecords, got %v", err)
	}

	// Seek to end (recordCount), then Prev to get last record.
	if err := cursor.Seek(chunk.RecordRef{ChunkID: chunkID, Pos: 3}); err != nil {
		t.Fatalf("Seek to end: %v", err)
	}
	got, ref, err = cursor.Prev()
	if err != nil {
		t.Fatalf("Prev from end: %v", err)
	}
	if ref.Pos != 2 {
		t.Errorf("Prev pos = %d, want 2", ref.Pos)
	}
	assertRecord(t, 2, got, records[2])

	// Seek to 1, Next should give record 1.
	if err := cursor.Seek(chunk.RecordRef{ChunkID: chunkID, Pos: 1}); err != nil {
		t.Fatalf("Seek(1): %v", err)
	}
	got, ref, err = cursor.Next()
	if err != nil {
		t.Fatalf("Next after Seek(1): %v", err)
	}
	if ref.Pos != 1 {
		t.Errorf("Next pos = %d, want 1", ref.Pos)
	}
	assertRecord(t, 1, got, records[1])
}

func TestEmptyBlob(t *testing.T) {
	chunkID := chunk.NewChunkID()
	vaultID := glid.New()

	tmp := writeBlobToTempFile(t, chunkID, vaultID, nil)

	rd, err := cloud.NewReader(tmp)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer rd.Close()

	if rd.Meta().RecordCount != 0 {
		t.Errorf("expected 0 records, got %d", rd.Meta().RecordCount)
	}

	_, err = rd.ReadRecord(0)
	if err != chunk.ErrNoMoreRecords {
		t.Errorf("expected ErrNoMoreRecords, got %v", err)
	}
}

// TestLargeRoundTrip verifies GLCB round-trip with a realistic record count.
// Mimics scatterbox: 17K records with sequential IngestTS.
func TestLargeRoundTrip(t *testing.T) {
	chunkID := chunk.NewChunkID()
	vaultID := glid.New()
	ingesterID := glid.New()
	base := time.Now().Truncate(time.Nanosecond)

	const n = 17_000
	records := make([]chunk.Record, n)
	for i := range n {
		records[i] = chunk.Record{
			IngestTS: base.Add(time.Duration(i) * time.Millisecond),
			WriteTS:  base.Add(time.Duration(i) * time.Millisecond),
			EventID:  chunk.EventID{IngesterID: ingesterID, IngestSeq: uint32(i)},
			Attrs:    chunk.Attributes{"ingester_type": "scatterbox", "seq": string(rune(i))},
			Raw:      []byte(`{"seq":` + string(rune(i)) + `}`),
		}
	}

	tmp := writeBlobToTempFile(t, chunkID, vaultID, records)

	rd, err := cloud.NewReader(tmp)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	if rd.Meta().RecordCount != n {
		t.Fatalf("meta.RecordCount = %d, want %d", rd.Meta().RecordCount, n)
	}

	// Forward cursor: read all records, verify count.
	cursor := cloud.NewSeekableCursor(rd, chunkID)
	defer cursor.Close()

	var fwdCount int
	for {
		_, _, err := cursor.Next()
		if err == chunk.ErrNoMoreRecords {
			break
		}
		if err != nil {
			t.Fatalf("Next[%d]: %v", fwdCount, err)
		}
		fwdCount++
	}
	if fwdCount != n {
		t.Fatalf("forward cursor read %d records, want %d", fwdCount, n)
	}

	// Reverse cursor: read all records from end, verify count.
	if err := cursor.Seek(chunk.RecordRef{ChunkID: chunkID, Pos: uint64(n)}); err != nil {
		t.Fatalf("Seek to end: %v", err)
	}
	var revCount int
	for {
		_, _, err := cursor.Prev()
		if err == chunk.ErrNoMoreRecords {
			break
		}
		if err != nil {
			t.Fatalf("Prev[%d]: %v", revCount, err)
		}
		revCount++
	}
	if revCount != n {
		t.Fatalf("reverse cursor read %d records, want %d", revCount, n)
	}

	t.Logf("GLCB round-trip: %d records, forward=%d, reverse=%d — all match", n, fwdCount, revCount)
}
