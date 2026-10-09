package file

// A search reads a sealed chunk's index sections while it holds an open
// cursor on that same chunk, in the same goroutine: the cursor pins the
// chunk for the scan, and the time-index scanner reads sections as it goes.
// If a section read re-acquired the per-chunk read lock, a writer queued in
// between — retention deleting the chunk, a cloud upload, an eviction —
// would deadlock it: Go's RWMutex makes the second read wait behind the
// writer, and the writer waits for the first read, held by the goroutine now
// blocked. The search then never finishes, never closes its cursor, and
// every later read of the chunk queues behind the stuck writer forever.

import (
	"context"
	"fmt"
	"iter"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	filetoken "gastrolog/internal/index/file/token"
	"gastrolog/internal/query"
	"gastrolog/internal/waittest"
)

func TestSectionReadUnderOpenCursorSurvivesAQueuedWriter(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cm, err := NewManager(Config{Dir: dir, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cm.Close() }()
	tokenIndexer := filetoken.NewIndexer(dir, cm, nil)
	im := indexfile.NewManager(dir, []index.Indexer{tokenIndexer}, nil, cm)
	cm.SetIndexBuilders([]chunk.ChunkIndexBuilder{im.BuildAdapter()})

	base := time.Now()
	for i := range 50 {
		ts := base.Add(time.Duration(i) * time.Millisecond)
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: []byte(fmt.Sprintf("record-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	id := cm.Active().ID
	if err := cm.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := cm.PostSealProcess(context.Background(), id); err != nil {
		t.Fatalf("PostSealProcess: %v", err)
	}

	// The search's cursor: holds the chunk's read lock until Close.
	cursor, err := cm.OpenCursor(id)
	if err != nil {
		t.Fatalf("OpenCursor: %v", err)
	}

	// A writer arrives while the cursor is open — retention deleting it.
	deleted := make(chan error, 1)
	go func() { deleted <- cm.Delete(id) }()
	lk := cm.chunkLockFor(id)
	waittest.For(t, "delete queued for the chunk's write lock", func() bool {
		if lk.TryRLock() {
			lk.RUnlock()
			return false
		}
		return true
	})

	// The scanner's next step, still in the cursor-holding role: read the
	// ingest-time index section. It must complete.
	read := make(chan error, 1)
	go func() {
		read <- cm.WithGLCBSection(id, format.TypeIngestIndex, func(uint8, []byte) error { return nil })
	}()
	waittest.For(t, "section read under an open cursor completes despite the queued writer", func() bool {
		select {
		case err := <-read:
			if err != nil {
				t.Errorf("section read: %v", err)
			}
			return true
		default:
			return false
		}
	})

	// The scan finishes, the cursor closes, and the writer proceeds.
	_ = cursor.Close()
	waittest.For(t, "delete completes once the cursor closes", func() bool {
		select {
		case err := <-deleted:
			if err != nil {
				t.Errorf("Delete: %v", err)
			}
			return true
		default:
			return false
		}
	})
}

// End to end through the query engine: a time-bounded search over a sealed
// chunk whose ingest timestamps are out of order runs the rank scanner,
// which reads an index section per record while the search's cursor holds
// the chunk. A retention delete queued mid-search must not wedge it: the
// search drains completely, its cursor closes, and the delete then runs.
func TestSearchDrainsWhileARetentionDeleteIsQueued(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cm, err := NewManager(Config{Dir: dir, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	// Close only after a clean drain: on the failure path the search's
	// scanner is wedged holding the chunk, and teardown must not wait on it.
	drainedCleanly := false
	defer func() {
		if drainedCleanly {
			_ = cm.Close()
		}
	}()
	tokenIndexer := filetoken.NewIndexer(dir, cm, nil)
	im := indexfile.NewManager(dir, []index.Indexer{tokenIndexer}, nil, cm)
	cm.SetIndexBuilders([]chunk.ChunkIndexBuilder{im.BuildAdapter()})

	// Out-of-order ingest timestamps: the rank scanner, not a sequential
	// scan, serves this chunk.
	const n = 40
	base := time.Now().Add(-time.Minute).Truncate(time.Microsecond)
	for i := range n {
		off := time.Duration((i*17)%n) * time.Millisecond
		ts := base.Add(off)
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ts, WriteTS: base.Add(time.Duration(i) * time.Millisecond),
			Raw: []byte(fmt.Sprintf("record-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	id := cm.Active().ID
	if err := cm.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := cm.PostSealProcess(context.Background(), id); err != nil {
		t.Fatalf("PostSealProcess: %v", err)
	}
	if meta, err := cm.Meta(id); err != nil || meta.IngestTSMonotonic {
		t.Fatalf("premise: chunk must have out-of-order ingest timestamps (monotonic=%v err=%v)", meta.IngestTSMonotonic, err)
	}

	eng := query.New(cm, im, nil)
	seq, _ := eng.Search(context.Background(), query.Query{
		Start: base.Add(-time.Second),
		End:   base.Add(time.Minute),
	}, nil)
	// stop is called only by the goroutine that drains the search: stopping a
	// wedged scanner would block this test instead of failing it.
	next, stop := iter.Pull2(seq)

	// First record: the search's cursor is now open on the chunk.
	if _, err, ok := next(); !ok || err != nil {
		stop()
		t.Fatalf("first record: ok=%v err=%v", ok, err)
	}

	deleted := make(chan error, 1)
	go func() { deleted <- cm.Delete(id) }()
	lk := cm.chunkLockFor(id)
	waittest.For(t, "delete queued for the chunk's write lock", func() bool {
		if lk.TryRLock() {
			lk.RUnlock()
			return false
		}
		return true
	})

	drained := make(chan int, 1)
	go func() {
		got := 1
		for {
			_, err, ok := next()
			if !ok {
				break
			}
			if err != nil {
				t.Errorf("record %d: %v", got, err)
				break
			}
			got++
		}
		stop()
		drained <- got
	}()
	var got int
	waittest.For(t, "search drains with a delete queued on its chunk", func() bool {
		select {
		case got = <-drained:
			return true
		default:
			return false
		}
	})
	drainedCleanly = true
	if got != n {
		t.Fatalf("search returned %d records, want %d", got, n)
	}
	waittest.For(t, "delete completes once the search's cursor closes", func() bool {
		select {
		case err := <-deleted:
			if err != nil {
				t.Errorf("Delete: %v", err)
			}
			return true
		default:
			return false
		}
	})
}
