package segment_test

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/record"
)

func fixedEventRecord(ingester, node glid.GLID, seq uint32, ts time.Time, raw byte) *record.Record {
	return &record.Record{
		SourceTS: ts.Add(-time.Millisecond),
		IngestTS: ts,
		EventID: record.EventID{
			IngesterID: ingester,
			NodeID:     node,
			IngestTS:   ts,
			IngestSeq:  seq,
		},
		Attrs: record.Attributes{"k": "v"},
		Raw:   []byte{raw},
	}
}

func appendRecords(t *testing.T, sf *segment.File, recs []*record.Record, writeTS time.Time) {
	t.Helper()
	for _, rec := range recs {
		if err := sf.Append(rec, writeTS); err != nil {
			t.Fatal(err)
		}
	}
}

func finalizeSegment(t *testing.T, path string, meta segment.Meta, recs []*record.Record, writeTS time.Time) segment.Header {
	t.Helper()
	sf, err := segment.Create(path, meta)
	if err != nil {
		t.Fatal(err)
	}
	appendRecords(t, sf, recs, writeTS)
	if err := sf.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := sf.Finalize(); err != nil {
		t.Fatal(err)
	}
	hdr := sf.Header()
	if err := sf.Close(); err != nil {
		t.Fatal(err)
	}
	return hdr
}

func recordsByEventOrder(t *testing.T, path string) []record.Record {
	t.Helper()
	sf, err := segment.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sf.Close()
	n := sf.Header().RecordCount
	out := make([]record.Record, n)
	for i := range n {
		rec, err := sf.RecordAtEventOrder(i)
		if err != nil {
			t.Fatalf("RecordAtEventOrder(%d): %v", i, err)
		}
		out[i] = rec
	}
	return out
}

func TestBuildIndexOrdersByEventID(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "seg")
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	writeTS := base.Add(time.Minute)
	ingester, node := glid.New(), glid.New()

	recs := []*record.Record{
		fixedEventRecord(ingester, node, 2, base.Add(2*time.Second), 'c'),
		fixedEventRecord(ingester, node, 0, base, 'a'),
		fixedEventRecord(ingester, node, 1, base.Add(time.Second), 'b'),
	}
	hdr := finalizeSegment(t, path, segment.Meta{ID: glid.New(), VaultID: glid.New()}, recs, writeTS)
	if hdr.IndexOffset == 0 {
		t.Fatal("expected index tail")
	}

	got := recordsByEventOrder(t, path)
	for i, want := range []byte{'a', 'b', 'c'} {
		if got[i].Raw[0] != want {
			t.Fatalf("EventOrder(%d) = %q, want %c", i, got[i].Raw, want)
		}
	}
}

func TestBuildIndexManyRecordsAppendOrderReversed(t *testing.T) {
	t.Parallel()
	const n = 512
	path := filepath.Join(t.TempDir(), "seg")
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	writeTS := base.Add(time.Minute)
	ingester, node := glid.New(), glid.New()

	recs := make([]*record.Record, n)
	for i := range n {
		seq := uint32(n - 1 - i)
		recs[i] = fixedEventRecord(ingester, node, seq, base.Add(time.Duration(seq)*time.Millisecond), byte(seq%256))
	}
	hdr := finalizeSegment(t, path, segment.Meta{ID: glid.New(), VaultID: glid.New()}, recs, writeTS)

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	wantIndexBytes := int64(n) * segment.IndexEntrySize
	if info.Size() != int64(hdr.IndexOffset)+wantIndexBytes {
		t.Fatalf("file size = %d, want %d (records + index)", info.Size(), hdr.IndexOffset+uint32(wantIndexBytes))
	}

	got := recordsByEventOrder(t, path)
	if len(got) != n {
		t.Fatalf("len = %d, want %d", len(got), n)
	}
	if !slices.IsSortedFunc(got, func(a, b record.Record) int {
		return a.EventID.Compare(b.EventID)
	}) {
		t.Fatal("RecordAtEventOrder stream not sorted by EventID")
	}
	for i := range n {
		if got[i].EventID.IngestSeq != uint32(i) {
			t.Fatalf("position %d IngestSeq = %d, want %d", i, got[i].EventID.IngestSeq, i)
		}
	}
}

func TestBuildIndexVariedTimestampsShuffle(t *testing.T) {
	t.Parallel()
	const n = 200
	path := filepath.Join(t.TempDir(), "seg")
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	writeTS := base.Add(time.Minute)

	recs := make([]*record.Record, n)
	for i := range n {
		// Spread timestamps so EventID order is not append order.
		ts := base.Add(time.Duration(i%17) * time.Second).Add(time.Duration(i/17) * time.Minute)
		recs[i] = fixedEventRecord(glid.New(), glid.New(), uint32(i), ts, byte(i%256))
	}
	perm := []int{97, 3, 188, 41, 155, 0, 199, 72, 11, 144}
	for i := 10; i < n; i++ {
		perm = append(perm, i)
	}
	shuffled := make([]*record.Record, n)
	for i, j := range perm {
		shuffled[i] = recs[j]
	}

	finalizeSegment(t, path, segment.Meta{ID: glid.New(), VaultID: glid.New()}, shuffled, writeTS)

	got := recordsByEventOrder(t, path)
	want := slices.Clone(shuffled)
	slices.SortFunc(want, func(a, b *record.Record) int {
		return a.EventID.Compare(b.EventID)
	})
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].EventID != want[i].EventID {
			t.Fatalf("position %d EventID = %+v, want %+v", i, got[i].EventID, want[i].EventID)
		}
	}
}

func TestIndexSurvivesReopen(t *testing.T) {
	t.Parallel()
	const n = 64
	path := filepath.Join(t.TempDir(), "seg")
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	ingester, node := glid.New(), glid.New()

	recs := make([]*record.Record, n)
	for i := range n {
		recs[i] = fixedEventRecord(ingester, node, uint32(i), base.Add(time.Duration(i)*time.Millisecond), 'x')
	}
	hdr := finalizeSegment(t, path, segment.Meta{ID: glid.New(), VaultID: glid.New()}, recs, base)

	sf, err := segment.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sf.Close()

	if sf.Header().IndexOffset != hdr.IndexOffset {
		t.Fatalf("IndexOffset = %d, want %d", sf.Header().IndexOffset, hdr.IndexOffset)
	}
	if sf.Header().IndexChecksum != hdr.IndexChecksum {
		t.Fatalf("IndexChecksum = %x, want %x", sf.Header().IndexChecksum, hdr.IndexChecksum)
	}
	for i := range n {
		if _, err := sf.RecordAtEventOrder(uint32(i)); err != nil {
			t.Fatalf("RecordAtEventOrder(%d): %v", i, err)
		}
	}
}

func TestRecordAtEventOrderBounds(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "seg")
	ts := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	recs := []*record.Record{fixedEventRecord(glid.New(), glid.New(), 0, ts, 'x')}
	finalizeSegment(t, path, segment.Meta{ID: glid.New(), VaultID: glid.New()}, recs, ts)

	sf, err := segment.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sf.Close()

	if _, err := sf.RecordAtEventOrder(1); !errors.Is(err, segment.ErrIndexBounds) {
		t.Fatalf("RecordAtEventOrder(1) = %v, want ErrIndexBounds", err)
	}
}

func TestOpenRejectsCorruptIndexChecksum(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "seg")
	ts := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	ingester, node := glid.New(), glid.New()
	recs := make([]*record.Record, 32)
	for i := range recs {
		recs[i] = fixedEventRecord(ingester, node, uint32(i), ts.Add(time.Duration(i)*time.Millisecond), 'x')
	}
	hdr := finalizeSegment(t, path, segment.Meta{ID: glid.New(), VaultID: glid.New()}, recs, ts)

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xff}, int64(hdr.IndexOffset)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := segment.Open(path); err == nil {
		t.Fatal("expected open to fail on corrupt index checksum")
	}
}

func TestFinalizeTwiceRejected(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "seg")
	ts := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)

	sf, err := segment.Create(path, segment.Meta{ID: glid.New(), VaultID: glid.New()})
	if err != nil {
		t.Fatal(err)
	}
	if err := sf.Append(fixedEventRecord(glid.New(), glid.New(), 0, ts, 'x'), ts); err != nil {
		t.Fatal(err)
	}
	if err := sf.Finalize(); err != nil {
		t.Fatal(err)
	}
	if err := sf.BuildIndex(); !errors.Is(err, segment.ErrIndexAlreadyBuilt) {
		t.Fatalf("BuildIndex() = %v, want ErrIndexAlreadyBuilt", err)
	}
}

func TestReadAllStopsBeforeIndexTail(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "seg")
	ts := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	recs := make([]*record.Record, 48)
	for i := range recs {
		recs[i] = fixedEventRecord(glid.New(), glid.New(), uint32(i), ts.Add(time.Duration(i)*time.Millisecond), 'x')
	}
	finalizeSegment(t, path, segment.Meta{ID: glid.New(), VaultID: glid.New()}, recs, ts)

	sf, err := segment.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sf.Close()

	got, err := sf.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(recs) {
		t.Fatalf("ReadAll len = %d, want %d", len(got), len(recs))
	}
}

func TestRecordAtEventOrderRequiresIndex(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "seg")
	ts := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)

	sf, err := segment.Create(path, segment.Meta{ID: glid.New(), VaultID: glid.New()})
	if err != nil {
		t.Fatal(err)
	}
	if err := sf.Append(fixedEventRecord(glid.New(), glid.New(), 0, ts, 'x'), ts); err != nil {
		t.Fatal(err)
	}
	if err := sf.Sync(); err != nil {
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

	if _, err := sf2.RecordAtEventOrder(0); !errors.Is(err, segment.ErrNoIndex) {
		t.Fatalf("RecordAtEventOrder() = %v, want ErrNoIndex", err)
	}
}

func TestFinalizeEmptySegmentNoIndex(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "seg")

	sf, err := segment.Create(path, segment.Meta{ID: glid.New(), VaultID: glid.New()})
	if err != nil {
		t.Fatal(err)
	}
	if err := sf.Finalize(); err != nil {
		t.Fatal(err)
	}
	if sf.Header().IndexOffset != 0 {
		t.Fatalf("IndexOffset = %d, want 0 for empty segment", sf.Header().IndexOffset)
	}
	if err := sf.Close(); err != nil {
		t.Fatal(err)
	}
}
