package file

import (
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// Regression: ImportRecords must not hold m.mu across the write loop.
// Replication imports large sealed chunks; List() blocked the UI for minutes.
//
// The record iterator parks the import mid-loop until released, so List must
// complete while the import is provably inside its write loop. If
// ImportRecords held m.mu across the loop, List could not return until the
// iterator is released — deterministic under any machine load, unlike the
// previous wall-clock race that both false-positived under full-suite -race
// load and false-negatived when a lock-holding import finished quickly.
func TestImportRecordsDoesNotBlockList(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cm, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cm.Close() }()

	const n = 8
	cid := chunk.NewChunkID()
	entered := make(chan struct{})
	release := make(chan struct{})
	var idx int
	iter := chunk.RecordIterator(func() (chunk.Record, error) {
		switch idx {
		case 0:
			close(entered)
		case 1:
			// The import already consumed and wrote record 0; park it inside
			// the write loop until List has proven it can run concurrently.
			<-release
		}
		if idx >= n {
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		idx++
		return chunk.Record{
			IngestTS: time.Now().UTC(),
			Raw:      []byte("record"),
		}, nil
	})

	// unpark releases the parked import exactly once; the failure paths call
	// it before t.Fatal so cleanup (cm.Close needs m.mu) never wedges behind
	// a lock-holding import.
	var releaseOnce sync.Once
	unpark := func() { releaseOnce.Do(func() { close(release) }) }
	defer unpark()

	importDone := make(chan error, 1)
	go func() {
		_, err := cm.ImportRecords(cid, iter)
		importDone <- err
	}()

	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		unpark()
		t.Fatal("ImportRecords never called the record iterator")
	}

	listDone := make(chan error, 1)
	go func() {
		_, err := cm.List()
		listDone <- err
	}()
	select {
	case err := <-listDone:
		if err != nil {
			t.Fatalf("List during import: %v", err)
		}
	case <-time.After(30 * time.Second):
		unpark()
		t.Fatal("List blocked while ImportRecords was inside its write loop")
	}

	unpark()
	select {
	case err := <-importDone:
		if err != nil {
			t.Fatalf("ImportRecords: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("ImportRecords did not finish after release")
	}
}

func TestImportRecordsRejectsDuplicateID(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cm, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cm.Close() }()

	cid := chunk.NewChunkID()
	var emptyIdx int
	empty := chunk.RecordIterator(func() (chunk.Record, error) {
		if emptyIdx > 0 {
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		emptyIdx++
		return chunk.Record{IngestTS: time.Now().UTC(), Raw: []byte("seed")}, nil
	})
	if _, err := cm.ImportRecords(cid, empty); err != nil {
		t.Fatalf("first import: %v", err)
	}
	var oneIdx int
	one := chunk.RecordIterator(func() (chunk.Record, error) {
		if oneIdx > 0 {
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		oneIdx++
		return chunk.Record{IngestTS: time.Now().UTC(), Raw: []byte("x")}, nil
	})
	if _, err := cm.ImportRecords(cid, one); err == nil {
		t.Fatal("expected error importing duplicate chunk id")
	}
}

func TestImportRecordsConcurrentListStress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test")
	}
	t.Parallel()

	dir := t.TempDir()
	cm, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cm.Close() }()

	cid := chunk.NewChunkID()
	var idx int
	iter := chunk.RecordIterator(func() (chunk.Record, error) {
		if idx >= 20000 {
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		idx++
		return chunk.Record{IngestTS: time.Now().UTC(), Raw: []byte("x")}, nil
	})

	var wg sync.WaitGroup
	wg.Go(func() {
		if _, err := cm.ImportRecords(cid, iter); err != nil {
			t.Errorf("import: %v", err)
		}
	})
	for range 20 {
		wg.Go(func() {
			if _, err := cm.List(); err != nil {
				t.Errorf("list: %v", err)
			}
		})
	}
	wg.Wait()
}
