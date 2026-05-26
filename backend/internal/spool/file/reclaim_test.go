package file

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/spool"
)

func spoolRec(seq uint64, raw string) chunk.Record {
	ts := time.Unix(int64(seq), 0).UTC()
	return chunk.Record{
		IngestTS: ts,
		WriteTS:  ts,
		EventID: chunk.EventID{
			IngesterID: glid.New(),
			NodeID:     glid.New(),
			IngestTS:   ts,
			IngestSeq:  uint32(seq),
		},
		Attrs:    chunk.Attributes{"k": "v"},
		Raw:      []byte(raw),
		VaultSeq: seq,
	}
}

func TestReclaimBlockedAboveWatermark(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })

	if _, err := m.Append(spoolRec(100, "a")); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SealActive(); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Append(spoolRec(200, "b")); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SealActive(); err != nil {
		t.Fatal(err)
	}

	m.SetReclaimThroughSeq(150)
	if err := m.Reclaim(spool.SegmentID(200)); err != spool.ErrReclaimBlocked {
		t.Fatalf("reclaim high segment err = %v, want %v", err, spool.ErrReclaimBlocked)
	}
	if reclaimable := m.ListReclaimable(); len(reclaimable) != 1 || reclaimable[0].FirstSeq != 100 {
		t.Fatalf("reclaimable = %+v", reclaimable)
	}
}

func TestReclaimRemovesSegmentOnDisk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := m.Append(spoolRec(100, "a")); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SealActive(); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Append(spoolRec(200, "b")); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SealActive(); err != nil {
		t.Fatal(err)
	}

	m.SetReclaimThroughSeq(150)
	if err := m.Reclaim(spool.SegmentID(100)); err != nil {
		t.Fatal(err)
	}
	segDir := filepath.Join(dir, spool.SegmentID(100).DirName())
	if _, err := os.Stat(segDir); !os.IsNotExist(err) {
		t.Fatalf("segment dir still exists: %v", err)
	}
	if segs := m.ListSegments(); len(segs) != 1 || segs[0].FirstSeq != 200 {
		t.Fatalf("segments after reclaim = %+v", segs)
	}
	_ = m.Close()

	m2, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m2.Close() })
	if segs := m2.ListSegments(); len(segs) != 1 || segs[0].FirstSeq != 200 {
		t.Fatalf("reopened segments = %+v", segs)
	}
}

func TestReclaimBlockedOnActiveSegment(t *testing.T) {
	t.Parallel()
	m, err := NewManager(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })

	if _, err := m.Append(spoolRec(300, "live")); err != nil {
		t.Fatal(err)
	}
	m.SetReclaimThroughSeq(500)
	if err := m.Reclaim(spool.SegmentID(300)); err != spool.ErrReclaimBlocked {
		t.Fatalf("reclaim active err = %v, want %v", err, spool.ErrReclaimBlocked)
	}
}

func TestRecoverTruncatesPartialIdxTail(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Append(spoolRec(1000, "ok")); err != nil {
		t.Fatal(err)
	}
	segDir := filepath.Join(dir, spool.SegmentID(1000).DirName())
	seg, err := OpenSegmentForTest(segDir, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	if err := WritePartialIdxTail(seg, 40); err != nil {
		t.Fatal(err)
	}
	seg.closeFiles()

	seg2, err := ReopenSegment(segDir, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	defer seg2.closeFiles()
	if seg2.recordCount != 1 {
		t.Fatalf("recordCount = %d, want 1", seg2.recordCount)
	}
	idxInfo, err := os.Stat(filepath.Join(segDir, "idx.log"))
	if err != nil {
		t.Fatal(err)
	}
	wantSize := int64(spool.IdxHeaderSize) + int64(spool.SpoolIdxEntrySize)
	if idxInfo.Size() != wantSize {
		t.Fatalf("idx.log size = %d, want %d", idxInfo.Size(), wantSize)
	}
}
