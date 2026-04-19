package file

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

func TestImportRecordsPreservesNonZeroWriteTS(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	ingest0 := base.Add(-2 * time.Hour)
	ingest1 := base.Add(-1 * time.Hour)
	write0 := base.Add(-90 * time.Minute)
	write1 := base.Add(-45 * time.Minute)
	fixedNow := base

	dir := t.TempDir()
	cm, err := NewManager(Config{
		Dir: dir,
		Now: func() time.Time { return fixedNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cm.Close() }()

	cid := chunk.NewChunkID()
	recs := []chunk.Record{
		{IngestTS: ingest0, WriteTS: write0, Raw: []byte("a")},
		{IngestTS: ingest1, WriteTS: write1, Raw: []byte("b")},
	}
	i := 0
	iter := chunk.RecordIterator(func() (chunk.Record, error) {
		if i >= len(recs) {
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		r := recs[i]
		i++
		return r, nil
	})

	meta, err := cm.ImportRecords(cid, iter)
	if err != nil {
		t.Fatal(err)
	}
	if meta.ID != cid {
		t.Fatalf("chunk id: want %v got %v", cid, meta.ID)
	}
	if !meta.WriteStart.Equal(write0) || !meta.WriteEnd.Equal(write1) {
		t.Fatalf("chunk write bounds: want [%v,%v] got [%v,%v]", write0, write1, meta.WriteStart, meta.WriteEnd)
	}

	cur, err := cm.OpenCursor(cid)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cur.Close() }()

	for wantPos, wantW := range []time.Time{write0, write1} {
		rec, ref, err := cur.Next()
		if err != nil {
			t.Fatalf("pos %d: %v", wantPos, err)
		}
		if int(ref.Pos) != wantPos {
			t.Errorf("pos: want %d got %d", wantPos, ref.Pos)
		}
		if !rec.WriteTS.Equal(wantW) {
			t.Errorf("record %d WriteTS: want %v got %v", wantPos, wantW, rec.WriteTS)
		}
	}
}

func TestImportRecordsStampsWriteTSWhenZero(t *testing.T) {
	t.Parallel()

	fixedNow := time.Date(2026, 4, 19, 15, 30, 0, 0, time.UTC)
	dir := t.TempDir()
	cm, err := NewManager(Config{
		Dir: dir,
		Now: func() time.Time { return fixedNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cm.Close() }()

	cid := chunk.NewChunkID()
	ts := fixedNow.Add(-time.Minute)
	recs := []chunk.Record{
		{IngestTS: ts, Raw: []byte("a")},
		{IngestTS: ts, Raw: []byte("b")},
	}
	i := 0
	iter := chunk.RecordIterator(func() (chunk.Record, error) {
		if i >= len(recs) {
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		r := recs[i]
		i++
		return r, nil
	})

	meta, err := cm.ImportRecords(cid, iter)
	if err != nil {
		t.Fatal(err)
	}
	if !meta.WriteStart.Equal(fixedNow) || !meta.WriteEnd.Equal(fixedNow) {
		t.Fatalf("chunk write bounds: want [%v,%v] got [%v,%v]", fixedNow, fixedNow, meta.WriteStart, meta.WriteEnd)
	}

	cur, err := cm.OpenCursor(cid)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cur.Close() }()

	for range 2 {
		rec, _, err := cur.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !rec.WriteTS.Equal(fixedNow) {
			t.Errorf("WriteTS: want %v got %v", fixedNow, rec.WriteTS)
		}
	}
}
