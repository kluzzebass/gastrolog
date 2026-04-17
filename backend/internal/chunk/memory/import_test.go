package memory

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

func TestImportRecordsHonorsExplicitID(t *testing.T) {
	t.Parallel()
	mgr := newImportTestManager(t)

	targetID := chunk.NewChunkID()

	i := 0
	recs := []chunk.Record{
		{Raw: []byte("rec1"), SourceTS: time.Now()},
		{Raw: []byte("rec2"), SourceTS: time.Now()},
	}
	iter := chunk.RecordIterator(func() (chunk.Record, error) {
		if i >= len(recs) {
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		r := recs[i]
		i++
		return r, nil
	})

	meta, err := mgr.ImportRecords(targetID, iter)
	if err != nil {
		t.Fatal(err)
	}
	if meta.ID != targetID {
		t.Errorf("expected chunk ID %s, got %s", targetID, meta.ID)
	}
	if meta.RecordCount != 2 {
		t.Errorf("expected 2 records, got %d", meta.RecordCount)
	}
}

func TestImportRecordsWithZeroIDGeneratesNew(t *testing.T) {
	t.Parallel()
	mgr := newImportTestManager(t)

	i := 0
	recs := []chunk.Record{{Raw: []byte("rec1"), SourceTS: time.Now()}}
	iter := chunk.RecordIterator(func() (chunk.Record, error) {
		if i >= len(recs) {
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		r := recs[i]
		i++
		return r, nil
	})

	meta, err := mgr.ImportRecords(chunk.ChunkID{}, iter)
	if err != nil {
		t.Fatal(err)
	}
	if meta.ID == (chunk.ChunkID{}) {
		t.Error("expected non-zero chunk ID")
	}
}

func newImportTestManager(t *testing.T) *Manager {
	t.Helper()
	mgr, err := NewManager(Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
		Now:            time.Now,
		MetaStore:      NewMetaStore(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	return mgr
}
