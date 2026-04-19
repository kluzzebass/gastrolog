package file

import (
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

func TestReadIdxLogEntries_roundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	wantIngest := []time.Time{
		mustParseTS(t, "2026-04-19T10:00:00Z"),
		mustParseTS(t, "2026-04-19T10:00:02Z"),
	}
	wantWrite := []time.Time{
		mustParseTS(t, "2026-04-19T11:00:00Z"),
		mustParseTS(t, "2026-04-19T11:00:01Z"),
	}

	manager, err := NewManager(Config{Dir: dir, Now: time.Now})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = manager.Close() }()

	recs := []chunk.Record{
		{IngestTS: wantIngest[0], WriteTS: wantWrite[0], Raw: []byte("a")},
		{IngestTS: wantIngest[1], WriteTS: wantWrite[1], Raw: []byte("bb")},
	}
	chunkID := chunk.NewChunkID()
	j := 0
	iter := chunk.RecordIterator(func() (chunk.Record, error) {
		if j >= len(recs) {
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		r := recs[j]
		j++
		return r, nil
	})
	if _, err := manager.ImportRecords(chunkID, iter); err != nil {
		t.Fatalf("ImportRecords: %v", err)
	}

	idxPath := filepath.Join(dir, chunkID.String(), idxLogFileName)
	got, err := ReadIdxLogEntries(idxPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(recs) {
		t.Fatalf("len: want %d got %d", len(recs), len(got))
	}
	for i := range recs {
		if !got[i].IngestTS.Equal(wantIngest[i]) {
			t.Errorf("record %d IngestTS: want %v got %v", i, wantIngest[i], got[i].IngestTS)
		}
		if !got[i].WriteTS.Equal(wantWrite[i]) {
			t.Errorf("record %d WriteTS: want %v got %v", i, wantWrite[i], got[i].WriteTS)
		}
	}
}

func mustParseTS(t *testing.T, s string) time.Time {
	t.Helper()
	tt, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return tt.UTC()
}
