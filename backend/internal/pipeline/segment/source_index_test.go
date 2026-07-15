package segment_test

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/format"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/record"
)

func recordWithSource(ingester, node glid.GLID, seq uint32, ingest, source time.Time, raw byte) *record.Record {
	rec := fixedEventRecord(ingester, node, seq, ingest, raw)
	if source.IsZero() {
		rec.SourceTS = time.Time{}
	} else {
		rec.SourceTS = source
	}
	return rec
}

func TestBuildSourceIndexSparseAndBounds(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "seg")
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	writeTS := base.Add(time.Minute)
	ingester, node := glid.New(), glid.New()

	srcA := base.Add(-3 * time.Second)
	srcB := base.Add(-2 * time.Second)
	srcC := base.Add(-1 * time.Second)
	recs := []*record.Record{
		recordWithSource(ingester, node, 0, base, srcB, 'b'),
		recordWithSource(ingester, node, 1, base.Add(time.Second), time.Time{}, 'z'),
		recordWithSource(ingester, node, 2, base.Add(2*time.Second), srcA, 'a'),
		recordWithSource(ingester, node, 3, base.Add(3*time.Second), srcC, 'c'),
	}
	hdr := finalizeSegment(t, path, segment.Meta{ID: glid.New(), VaultID: glid.New()}, recs, writeTS)

	if hdr.Version != segment.FormatVersion() {
		t.Fatalf("version = %d, want %d", hdr.Version, segment.FormatVersion())
	}
	if hdr.SourceIndexCount != 3 {
		t.Fatalf("SourceIndexCount = %d, want 3", hdr.SourceIndexCount)
	}
	if !hdr.FirstSourceTS.Equal(srcA) || !hdr.LastSourceTS.Equal(srcC) {
		t.Fatalf("bounds = %v..%v, want %v..%v", hdr.FirstSourceTS, hdr.LastSourceTS, srcA, srcC)
	}

	wantEnd := int64(hdr.IndexOffset) + int64(hdr.RecordCount)*segment.IndexEntrySize + int64(hdr.SourceIndexCount)*segment.SourceIndexEntrySize
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != wantEnd {
		t.Fatalf("file size = %d, want %d", info.Size(), wantEnd)
	}

	sf, err := segment.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sf.Close()

	pos, ok, err := sf.FindSourceStartPosition(srcB)
	if err != nil || !ok {
		t.Fatalf("FindSourceStartPosition(srcB) = (%d, %v, %v)", pos, ok, err)
	}
	rec, err := sf.RecordAtEventOrder(pos)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Raw[0] != 'b' {
		t.Fatalf("start record = %q, want b", rec.Raw)
	}

	_, ok, err = sf.FindSourceStartPosition(srcC.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected no position past last source")
	}
}

func TestBuildSourceIndexAllZeroSourceTS(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "seg")
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	ingester, node := glid.New(), glid.New()
	recs := []*record.Record{
		recordWithSource(ingester, node, 0, base, time.Time{}, 'a'),
		recordWithSource(ingester, node, 1, base.Add(time.Second), time.Time{}, 'b'),
	}
	hdr := finalizeSegment(t, path, segment.Meta{ID: glid.New(), VaultID: glid.New()}, recs, base)

	if hdr.SourceIndexCount != 0 {
		t.Fatalf("SourceIndexCount = %d, want 0", hdr.SourceIndexCount)
	}
	if !hdr.FirstSourceTS.IsZero() || !hdr.LastSourceTS.IsZero() {
		t.Fatal("expected zero source bounds")
	}
	eventEnd := int64(hdr.IndexOffset) + int64(hdr.RecordCount)*segment.IndexEntrySize
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != eventEnd {
		t.Fatalf("file size = %d, want %d (no source tail)", info.Size(), eventEnd)
	}
}

func TestFindSourceStartPositionTieBreakByEventOrder(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "seg")
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	ingester, node := glid.New(), glid.New()
	shared := base.Add(-time.Second)
	recs := []*record.Record{
		recordWithSource(ingester, node, 1, base.Add(time.Second), shared, 'b'),
		recordWithSource(ingester, node, 0, base, shared, 'a'),
	}
	finalizeSegment(t, path, segment.Meta{ID: glid.New(), VaultID: glid.New()}, recs, base)

	sf, err := segment.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sf.Close()

	pos, ok, err := sf.FindSourceStartPosition(shared)
	if err != nil || !ok {
		t.Fatalf("FindSourceStartPosition = (%d, %v, %v)", pos, ok, err)
	}
	rec, err := sf.RecordAtEventOrder(pos)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Raw[0] != 'a' {
		t.Fatalf("tie-break position = %q, want earlier EventID record a", rec.Raw)
	}
}

func TestOpenRejectsBadSourceIndexChecksum(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "seg")
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	ingester, node := glid.New(), glid.New()
	recs := []*record.Record{
		recordWithSource(ingester, node, 0, base, base.Add(-time.Second), 'a'),
	}
	hdr := finalizeSegment(t, path, segment.Meta{ID: glid.New(), VaultID: glid.New()}, recs, base)

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	crcOff := segment.HeaderSize - format.SizeU32
	var crcBuf [format.SizeU32]byte
	binary.LittleEndian.PutUint32(crcBuf[:], hdr.SourceIndexChecksum^0xffffffff)
	if _, err := f.WriteAt(crcBuf[:], int64(crcOff)); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	_, err = segment.Open(path)
	if err == nil {
		t.Fatal("expected open failure on bad source checksum")
	}
}

func TestOpenRejectsTruncatedSourceTail(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "seg")
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	ingester, node := glid.New(), glid.New()
	recs := []*record.Record{
		recordWithSource(ingester, node, 0, base, base.Add(-time.Second), 'a'),
	}
	finalizeSegment(t, path, segment.Meta{ID: glid.New(), VaultID: glid.New()}, recs, base)

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-1); err != nil {
		t.Fatal(err)
	}

	_, err = segment.Open(path)
	if err == nil {
		t.Fatal("expected open failure on truncated source tail")
	}
}

func TestOpenRejectsWrongFormatVersion(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "seg-badver")
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	ingester, node := glid.New(), glid.New()
	recs := []*record.Record{
		recordWithSource(ingester, node, 0, base, time.Time{}, 'a'),
	}

	// Finalize a valid segment, then stamp a version byte that is not the
	// single supported format version.
	finalizeSegment(t, path, segment.Meta{ID: glid.New(), VaultID: glid.New()}, recs, base)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var typeHdr [format.HeaderSize]byte
	if _, err := f.ReadAt(typeHdr[:], 0); err != nil {
		t.Fatal(err)
	}
	typeHdr[2] = 0x02
	if _, err := f.WriteAt(typeHdr[:format.HeaderSize], 0); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	if _, err := segment.Open(path); !errors.Is(err, format.ErrVersionMismatch) {
		t.Fatalf("Open() = %v, want format.ErrVersionMismatch", err)
	}
}

func TestBuildIndexAlreadyBuiltUnchanged(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "seg")
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	recs := []*record.Record{
		recordWithSource(glid.New(), glid.New(), 0, base, base.Add(-time.Second), 'a'),
	}
	finalizeSegment(t, path, segment.Meta{ID: glid.New(), VaultID: glid.New()}, recs, base)

	sf, err := segment.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sf.Close()
	if err := sf.BuildIndex(); !errors.Is(err, segment.ErrIndexAlreadyBuilt) {
		t.Fatalf("BuildIndex() = %v, want ErrIndexAlreadyBuilt", err)
	}
}
