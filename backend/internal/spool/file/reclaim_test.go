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

	w1 := spool.WindowID{Start: 100, End: 150}
	w2 := spool.WindowID{Start: 200, End: 250}
	if err := m.EnsureWindow(w1.Start, w1.End); err != nil {
		t.Fatal(err)
	}
	if err := m.EnsureWindow(w2.Start, w2.End); err != nil {
		t.Fatal(err)
	}
	if err := m.PutSlot(spoolRec(100, "a")); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SealWindow(w1); err != nil {
		t.Fatal(err)
	}
	if err := m.PutSlot(spoolRec(200, "b")); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SealWindow(w2); err != nil {
		t.Fatal(err)
	}

	m.SetReclaimThroughSeq(150)
	if err := m.Reclaim(w2); err != spool.ErrReclaimBlocked {
		t.Fatalf("reclaim high segment err = %v, want %v", err, spool.ErrReclaimBlocked)
	}
	if reclaimable := m.ListReclaimable(); len(reclaimable) != 1 || reclaimable[0].FirstSeq != 100 {
		t.Fatalf("reclaimable = %+v", reclaimable)
	}
}

func TestReclaimRemovesWindowOnDisk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}

	w1 := spool.WindowID{Start: 100, End: 150}
	w2 := spool.WindowID{Start: 200, End: 250}
	if err := m.EnsureWindow(w1.Start, w1.End); err != nil {
		t.Fatal(err)
	}
	if err := m.EnsureWindow(w2.Start, w2.End); err != nil {
		t.Fatal(err)
	}
	if err := m.PutSlot(spoolRec(100, "a")); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SealWindow(w1); err != nil {
		t.Fatal(err)
	}
	if err := m.PutSlot(spoolRec(200, "b")); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SealWindow(w2); err != nil {
		t.Fatal(err)
	}

	m.SetReclaimThroughSeq(150)
	if err := m.Reclaim(w1); err != nil {
		t.Fatal(err)
	}
	windowDir := filepath.Join(dir, w1.DirName())
	if _, err := os.Stat(windowDir); !os.IsNotExist(err) {
		t.Fatalf("window dir still exists: %v", err)
	}
	if wins := m.ListWindows(); len(wins) != 1 || wins[0].FirstSeq != 200 {
		t.Fatalf("windows after reclaim = %+v", wins)
	}
	_ = m.Close()

	m2, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m2.Close() })
	if wins := m2.ListWindows(); len(wins) != 1 || wins[0].FirstSeq != 200 {
		t.Fatalf("reopened windows = %+v", wins)
	}
}

func TestReclaimBlockedOnActiveSegment(t *testing.T) {
	t.Parallel()
	m, err := NewManager(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })

	w1 := spool.WindowID{Start: 300, End: 350}
	if err := m.EnsureWindow(w1.Start, w1.End); err != nil {
		t.Fatal(err)
	}
	if err := m.PutSlot(spoolRec(300, "live")); err != nil {
		t.Fatal(err)
	}
	m.SetReclaimThroughSeq(500)
	if err := m.Reclaim(w1); err != spool.ErrSegmentNotSealed {
		t.Fatalf("reclaim active err = %v, want %v", err, spool.ErrSegmentNotSealed)
	}
}

func TestRecoverTruncatesPartialIdxTail(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	id := spool.WindowID{Start: 1000, End: 1050}
	if err := m.EnsureWindow(id.Start, id.End); err != nil {
		t.Fatal(err)
	}
	if err := m.PutSlot(spoolRec(1000, "ok")); err != nil {
		t.Fatal(err)
	}
	windowDir := filepath.Join(dir, id.DirName())
	win, err := OpenWindowForTest(windowDir, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	if err := WritePartialIdxTail(win, 40); err != nil {
		t.Fatal(err)
	}
	win.closeFiles()

	win2, err := ReopenWindow(windowDir, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	defer win2.closeFiles()
	if win2.recordCount != 1 {
		t.Fatalf("recordCount = %d, want 1", win2.recordCount)
	}
	idxInfo, err := os.Stat(filepath.Join(windowDir, "idx.log"))
	if err != nil {
		t.Fatal(err)
	}
	wantMax := spool.WindowIdxFileSize(id.Start, id.End)
	if idxInfo.Size() > wantMax {
		t.Fatalf("idx.log size = %d, want <= %d", idxInfo.Size(), wantMax)
	}
}
