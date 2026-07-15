package file_test

import (
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
)

func newLazyTestManager(t *testing.T) *chunkfile.Manager {
	t.Helper()
	m, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            t.TempDir(),
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func lazyInfo(records int64) chunk.ExternalGLCBInfo {
	now := time.Now()
	return chunk.ExternalGLCBInfo{
		WriteStart:  now.Add(-time.Minute),
		WriteEnd:    now,
		RecordCount: records,
		Bytes:       records * 100,
		DiskBytes:   records * 80,
	}
}

// TestLazyExternalResolveOnMiss pins the on-miss contract: an unregistered
// chunk resolves through the installed resolver exactly once (memoized),
// and appears in Meta and List afterwards — registration as a cache
// (gastrolog-2kmgj6).
func TestLazyExternalResolveOnMiss(t *testing.T) {
	t.Parallel()
	m := newLazyTestManager(t)
	id := chunk.NewChunkID()
	glcbPath := filepath.Join(t.TempDir(), "data.glcb")

	var calls atomic.Int64
	m.SetExternalGLCBResolver(func(got chunk.ChunkID) (string, chunk.ExternalGLCBInfo, bool) {
		calls.Add(1)
		if got != id {
			return "", chunk.ExternalGLCBInfo{}, false
		}
		return glcbPath, lazyInfo(42), true
	})

	meta, err := m.Meta(id)
	if err != nil {
		t.Fatalf("Meta after lazy resolve: %v", err)
	}
	if meta.RecordCount != 42 || !meta.Sealed {
		t.Fatalf("resolved meta = %+v, want sealed with 42 records", meta)
	}
	if _, err := m.Meta(id); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("resolver invoked %d times, want 1 (memoized)", got)
	}

	metas, err := m.List()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, mm := range metas {
		if mm.ID == id {
			found = true
		}
	}
	if !found {
		t.Fatal("lazily resolved chunk missing from List")
	}
}

// TestLazyExternalListerSurfacesUnregistered pins the enumeration contract
// (gastrolog-3s26vr): List must surface an external chunk that has NEVER been
// looked up by ID — no prior Meta/OpenCursor to memoize it. A match-all search
// and the holder-scope gate enumerate the manager rather than naming a chunk,
// so lazy on-miss-by-ID resolution alone left a restarted home answering
// match-all with zero records until some other path registered the chunk. The
// lister closes that gap: List resolves each enumerated ID it does not already
// hold, then memoizes it.
func TestLazyExternalListerSurfacesUnregistered(t *testing.T) {
	t.Parallel()
	m := newLazyTestManager(t)
	id := chunk.NewChunkID()
	glcbPath := filepath.Join(t.TempDir(), "data.glcb")

	var resolverCalls atomic.Int64
	m.SetExternalGLCBResolver(func(got chunk.ChunkID) (string, chunk.ExternalGLCBInfo, bool) {
		resolverCalls.Add(1)
		if got != id {
			return "", chunk.ExternalGLCBInfo{}, false
		}
		return glcbPath, lazyInfo(10), true
	})
	m.SetExternalGLCBLister(func() []chunk.ChunkID { return []chunk.ChunkID{id} })

	// No Meta/OpenCursor first: the chunk is unregistered, exactly the
	// post-restart state. List alone must surface it.
	metas, err := m.List()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, mm := range metas {
		if mm.ID == id {
			found = true
			if mm.RecordCount != 10 || !mm.Sealed {
				t.Fatalf("listed meta = %+v, want sealed with 10 records", mm)
			}
		}
	}
	if !found {
		t.Fatal("external chunk absent from List despite an installed lister — enumeration gap")
	}

	// Memoized: a second List does not re-resolve.
	before := resolverCalls.Load()
	if _, err := m.List(); err != nil {
		t.Fatal(err)
	}
	if got := resolverCalls.Load(); got != before {
		t.Fatalf("List re-resolved a memoized chunk: %d calls after, %d before", got, before)
	}
}

// TestLazyExternalListerGatesOnResolver: the lister enumerates candidate IDs,
// but List must include one only when the resolver accepts it (the file is on
// this node and the entry is still sealed). A declined ID stays out of List
// and is not memoized.
func TestLazyExternalListerGatesOnResolver(t *testing.T) {
	t.Parallel()
	m := newLazyTestManager(t)
	present := chunk.NewChunkID()
	absent := chunk.NewChunkID()
	glcbPath := filepath.Join(t.TempDir(), "data.glcb")

	m.SetExternalGLCBResolver(func(got chunk.ChunkID) (string, chunk.ExternalGLCBInfo, bool) {
		if got == present {
			return glcbPath, lazyInfo(5), true
		}
		return "", chunk.ExternalGLCBInfo{}, false // absent: no bytes on this node
	})
	m.SetExternalGLCBLister(func() []chunk.ChunkID {
		return []chunk.ChunkID{present, absent}
	})

	metas, err := m.List()
	if err != nil {
		t.Fatal(err)
	}
	sawPresent, sawAbsent := false, false
	for _, mm := range metas {
		switch mm.ID {
		case present:
			sawPresent = true
		case absent:
			sawAbsent = true
		}
	}
	if !sawPresent {
		t.Fatal("resolvable chunk missing from List")
	}
	if sawAbsent {
		t.Fatal("declined chunk leaked into List — the resolver gate was bypassed")
	}
	// The declined ID must remain unregistered so it can resolve later.
	if _, err := m.Meta(absent); !errors.Is(err, chunk.ErrChunkNotFound) {
		t.Fatalf("declined chunk memoized by List: Meta err = %v, want ErrChunkNotFound", err)
	}
}

// TestLazyExternalResolverMissStaysNotFound: a resolver that declines must
// leave the lookup as ErrChunkNotFound, and the decline is NOT memoized —
// the chunk may become resolvable later (seal commits, file appears).
func TestLazyExternalResolverMissStaysNotFound(t *testing.T) {
	t.Parallel()
	m := newLazyTestManager(t)
	id := chunk.NewChunkID()

	resolvable := atomic.Bool{}
	m.SetExternalGLCBResolver(func(chunk.ChunkID) (string, chunk.ExternalGLCBInfo, bool) {
		if !resolvable.Load() {
			return "", chunk.ExternalGLCBInfo{}, false
		}
		return filepath.Join(t.TempDir(), "data.glcb"), lazyInfo(7), true
	})

	if _, err := m.Meta(id); !errors.Is(err, chunk.ErrChunkNotFound) {
		t.Fatalf("declined resolve: err = %v, want ErrChunkNotFound", err)
	}

	// The chunk becomes resolvable (e.g. its seal committed): the next
	// lookup must succeed — a lazy architecture has no permanent misses.
	resolvable.Store(true)
	if _, err := m.Meta(id); err != nil {
		t.Fatalf("resolvable chunk still not found: %v", err)
	}
}

// TestLazyExternalResolveConcurrent hammers the same unregistered chunk
// from many goroutines under -race: all lookups succeed and the manager
// ends with exactly one registration.
func TestLazyExternalResolveConcurrent(t *testing.T) {
	t.Parallel()
	m := newLazyTestManager(t)
	id := chunk.NewChunkID()
	glcbPath := filepath.Join(t.TempDir(), "data.glcb")
	m.SetExternalGLCBResolver(func(chunk.ChunkID) (string, chunk.ExternalGLCBInfo, bool) {
		return glcbPath, lazyInfo(9), true
	})

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for range 16 {
		wg.Go(func() {
			if _, err := m.Meta(id); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent lazy resolve: %v", err)
	}
	if !m.IsExternalGLCBAt(id, glcbPath) {
		t.Fatal("chunk not registered at the resolved path")
	}
}

// TestLazyExternalResolveDoesNotShadowLocal: a chunk the manager owns
// locally must never be re-routed through the resolver.
func TestLazyExternalResolveDoesNotShadowLocal(t *testing.T) {
	t.Parallel()
	m := newLazyTestManager(t)
	var calls atomic.Int64
	m.SetExternalGLCBResolver(func(chunk.ChunkID) (string, chunk.ExternalGLCBInfo, bool) {
		calls.Add(1)
		return "", chunk.ExternalGLCBInfo{}, false
	})

	rec := chunk.Record{IngestTS: time.Now(), Raw: []byte("local")}
	localID, _, err := m.Append(rec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Meta(localID); err != nil {
		t.Fatalf("local chunk lookup: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("resolver consulted %d times for a locally-owned chunk", calls.Load())
	}
}
