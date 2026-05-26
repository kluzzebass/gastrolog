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

func TestFileSpoolSegmentIdentityAndBounds(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })

	rec := func(seq uint64) chunk.Record {
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
			Raw:      []byte("data"),
			VaultSeq: seq,
		}
	}
	meta, err := m.Append(rec(500))
	if err != nil {
		t.Fatal(err)
	}
	if meta.ID != spool.SegmentID(500) || meta.FirstSeq != 500 {
		t.Fatalf("first append meta = %+v", meta)
	}
	if _, err := m.Append(rec(501)); err != nil {
		t.Fatal(err)
	}
	meta, err = m.Append(rec(502))
	if err != nil {
		t.Fatal(err)
	}
	if meta.LastSeq != 502 || meta.RecordCount != 3 {
		t.Fatalf("third append meta = %+v", meta)
	}

	m2, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m2.Close() })
	reopened, ok := m2.Meta(spool.SegmentID(500))
	if !ok {
		t.Fatal("segment missing after reopen")
	}
	if reopened.FirstSeq != 500 || reopened.LastSeq != 502 || reopened.RecordCount != 3 {
		t.Fatalf("reopened meta = %+v", reopened)
	}
	segDir := filepath.Join(dir, spool.SegmentID(500).DirName())
	seg, err := ReopenSegment(segDir, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { seg.closeFiles() })
	recs, err := seg.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 || recs[2].VaultSeq != 502 {
		t.Fatalf("records = %+v", recs)
	}
}

func TestFileSpoolCrashTruncatesOrphanRawTail(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	rec := chunk.Record{
		IngestTS: time.Unix(1, 0).UTC(),
		WriteTS:  time.Unix(1, 0).UTC(),
		EventID: chunk.EventID{
			IngesterID: glid.New(),
			NodeID:     glid.New(),
			IngestTS:   time.Unix(1, 0).UTC(),
			IngestSeq:  1,
		},
		Raw:      []byte("ok"),
		VaultSeq: 1000,
	}
	if _, err := m.Append(rec); err != nil {
		t.Fatal(err)
	}
	segDir := filepath.Join(dir, spool.SegmentID(1000).DirName())
	seg, err := OpenSegmentForTest(segDir, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteOrphanRaw(seg, []byte("orphan-bytes")); err != nil {
		t.Fatal(err)
	}
	seg.closeFiles()

	seg2, err := ReopenSegment(segDir, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	defer seg2.closeFiles()
	if seg2.recordCount != 1 {
		t.Fatalf("recordCount = %d, want 1 after truncate", seg2.recordCount)
	}
	recs, err := seg2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || string(recs[0].Raw) != "ok" {
		t.Fatalf("records after recovery = %+v", recs)
	}
}

func TestFileSpoolCrashTruncatesOrphanIdxTail(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })

	rec := chunk.Record{
		IngestTS: time.Unix(1, 0).UTC(),
		WriteTS:  time.Unix(1, 0).UTC(),
		EventID: chunk.EventID{
			IngesterID: glid.New(),
			NodeID:     glid.New(),
			IngestTS:   time.Unix(1, 0).UTC(),
			IngestSeq:  1,
		},
		Attrs:    chunk.Attributes{"k": "v"},
		Raw:      []byte("first"),
		VaultSeq: 2000,
	}
	if _, err := m.Append(rec); err != nil {
		t.Fatal(err)
	}
	segDir := filepath.Join(dir, spool.SegmentID(2000).DirName())
	seg, err := OpenSegmentForTest(segDir, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	// Idx entry references raw/attr offsets that were never written (idx ahead of data).
	orphan := spool.EntryFromRecord(
		chunk.Record{
			IngestTS: time.Unix(2, 0).UTC(),
			WriteTS:  time.Unix(2, 0).UTC(),
			EventID: chunk.EventID{
				IngesterID: glid.New(),
				NodeID:     glid.New(),
				IngestTS:   time.Unix(2, 0).UTC(),
				IngestSeq:  2,
			},
			Attrs:    chunk.Attributes{"k": "v2"},
			Raw:      []byte("missing-on-disk"),
			VaultSeq: 2001,
		},
		9999,
		9999,
		17,
		4,
	)
	if err := WriteOrphanIdxEntry(seg, orphan); err != nil {
		t.Fatal(err)
	}
	seg.closeFiles()

	seg2, err := ReopenSegment(segDir, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	defer seg2.closeFiles()
	if seg2.recordCount != 1 {
		t.Fatalf("recordCount = %d, want 1 after idx tail rollback", seg2.recordCount)
	}
	recs, err := seg2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].VaultSeq != 2000 || string(recs[0].Raw) != "first" {
		t.Fatalf("records after recovery = %+v", recs)
	}
	idxInfo, err := os.Stat(filepath.Join(segDir, "idx.log"))
	if err != nil {
		t.Fatal(err)
	}
	wantIdxSize := int64(spool.IdxHeaderSize) + int64(spool.SpoolIdxEntrySize)
	if idxInfo.Size() != wantIdxSize {
		t.Fatalf("idx.log size = %d, want %d", idxInfo.Size(), wantIdxSize)
	}
}

func TestParseSegmentDirName(t *testing.T) {
	t.Parallel()
	id, err := spool.ParseSegmentID("00000000000000000123")
	if err != nil {
		t.Fatal(err)
	}
	if id != spool.SegmentID(123) {
		t.Fatalf("id = %d", id)
	}
	if spool.SegmentID(123).DirName() != "00000000000000000123" {
		t.Fatal("DirName mismatch")
	}
}
