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

func TestFileSpoolWindowIdentityAndBounds(t *testing.T) {
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
	id := spool.WindowID{Start: 500, End: 520}
	if err := m.EnsureWindow(id.Start, id.End); err != nil {
		t.Fatal(err)
	}
	if err := m.PutSlot(rec(500)); err != nil {
		t.Fatal(err)
	}
	if err := m.PutSlot(rec(501)); err != nil {
		t.Fatal(err)
	}
	if err := m.PutSlot(rec(502)); err != nil {
		t.Fatal(err)
	}
	meta, ok := m.Meta(id)
	if !ok {
		t.Fatal("window metadata missing")
	}
	if meta.Window != id || meta.FirstSeq != 500 || meta.EndSeq != 520 {
		t.Fatalf("window meta = %+v", meta)
	}
	if meta.LastSeq != 502 || meta.RecordCount != 3 {
		t.Fatalf("third put meta = %+v", meta)
	}

	sealed, err := m.SealWindow(id)
	if err != nil {
		t.Fatal(err)
	}
	if !sealed.Sealed {
		t.Fatalf("sealed meta = %+v", sealed)
	}

	m2, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m2.Close() })
	reopened, ok := m2.Meta(id)
	if !ok {
		t.Fatal("window missing after reopen")
	}
	if reopened.FirstSeq != 500 || reopened.LastSeq != 502 || reopened.RecordCount != 3 {
		t.Fatalf("reopened meta = %+v", reopened)
	}
	windowDir := filepath.Join(dir, id.DirName())
	win, err := ReopenWindow(windowDir, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { win.closeFiles() })
	recs, err := win.ReadAllSlots()
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
	id := spool.WindowID{Start: 1000, End: 1020}
	if err := m.EnsureWindow(id.Start, id.End); err != nil {
		t.Fatal(err)
	}
	if err := m.PutSlot(rec); err != nil {
		t.Fatal(err)
	}
	windowDir := filepath.Join(dir, id.DirName())
	win, err := OpenWindowForTest(windowDir, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteOrphanRaw(win, []byte("orphan-bytes")); err != nil {
		t.Fatal(err)
	}
	win.closeFiles()

	win2, err := ReopenWindow(windowDir, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	defer win2.closeFiles()
	if win2.recordCount != 1 {
		t.Fatalf("recordCount = %d, want 1 after truncate", win2.recordCount)
	}
	recs, err := win2.ReadAllSlots()
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
	id := spool.WindowID{Start: 2000, End: 2020}
	if err := m.EnsureWindow(id.Start, id.End); err != nil {
		t.Fatal(err)
	}
	if err := m.PutSlot(rec); err != nil {
		t.Fatal(err)
	}
	windowDir := filepath.Join(dir, id.DirName())
	win, err := OpenWindowForTest(windowDir, 0o640)
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
	if err := WriteOrphanIdxEntry(win, orphan); err != nil {
		t.Fatal(err)
	}
	win.closeFiles()

	win2, err := ReopenWindow(windowDir, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	defer win2.closeFiles()
	if win2.recordCount != 1 {
		t.Fatalf("recordCount = %d, want 1 after idx tail rollback", win2.recordCount)
	}
	recs, err := win2.ReadAllSlots()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].VaultSeq != 2000 || string(recs[0].Raw) != "first" {
		t.Fatalf("records after recovery = %+v", recs)
	}
	idxInfo, err := os.Stat(filepath.Join(windowDir, "idx.log"))
	if err != nil {
		t.Fatal(err)
	}
	wantIdxSize := spool.WindowIdxFileSize(id.Start, id.End)
	if idxInfo.Size() > wantIdxSize {
		t.Fatalf("idx.log size = %d, want <= %d", idxInfo.Size(), wantIdxSize)
	}
}

func TestPutSlotOutOfOrder(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	id := spool.WindowID{Start: 3000, End: 3010}
	if err := m.EnsureWindow(id.Start, id.End); err != nil {
		t.Fatal(err)
	}
	if err := m.PutSlot(spoolRecForWindow(3002, "late")); err != nil {
		t.Fatal(err)
	}
	if err := m.PutSlot(spoolRecForWindow(3001, "early")); err != nil {
		t.Fatal(err)
	}
	meta, ok := m.Meta(id)
	if !ok {
		t.Fatal("window metadata missing")
	}
	if meta.RecordCount != 2 || meta.LastSeq != 3002 {
		t.Fatalf("meta = %+v", meta)
	}
}

func TestPutSlotRequiresWindow(t *testing.T) {
	t.Parallel()
	m, err := NewManager(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	if err := m.PutSlot(spoolRecForWindow(9999, "missing-window")); err == nil {
		t.Fatal("expected PutSlot to fail without EnsureWindow")
	}
}

func TestParseWindowDirName(t *testing.T) {
	t.Parallel()
	id, err := spool.ParseWindowDirName("w-00000000000000000123-00000000000000000199")
	if err != nil {
		t.Fatal(err)
	}
	if id != (spool.WindowID{Start: 123, End: 199}) {
		t.Fatalf("id = %+v", id)
	}
	if (spool.WindowID{Start: 123, End: 199}).DirName() != "w-00000000000000000123-00000000000000000199" {
		t.Fatal("DirName mismatch")
	}
}

func spoolRecForWindow(seq uint64, raw string) chunk.Record {
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
