package file

import (
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// Regression: ImportRecords must not hold m.mu across the write loop.
// Replication imports large sealed chunks; List() blocked the UI for minutes.
func TestImportRecordsDoesNotBlockList(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cm, err := NewManager(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cm.Close() }()

	const n = 5000
	cid := chunk.NewChunkID()
	var idx int
	iter := chunk.RecordIterator(func() (chunk.Record, error) {
		if idx >= n {
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		idx++
		return chunk.Record{
			IngestTS: time.Now().UTC(),
			Raw:      []byte("record"),
		}, nil
	})

	importDone := make(chan error, 1)
	go func() {
		_, err := cm.ImportRecords(cid, iter)
		importDone <- err
	}()

	deadline := time.After(2 * time.Second)
	for {
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
			select {
			case err := <-importDone:
				if err != nil {
					t.Fatalf("ImportRecords: %v", err)
				}
				return
			case <-time.After(50 * time.Millisecond):
				continue
			}
		case <-deadline:
			t.Fatal("List blocked while ImportRecords was writing")
		case err := <-importDone:
			if err != nil {
				t.Fatalf("ImportRecords finished early: %v", err)
			}
			return
		}
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
