package file

// The cloud index is populated OFF the construction path: a store dial can
// block for minutes, and the constructor runs under the orchestrator
// registry lock inside the FSM apply — one blackholed endpoint there
// silently stops every ingest on the node. Construction must never touch
// the store; EnsureCloudIndex owns population, retried until the store
// answers and latched on success.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/waittest"
)

// blackholeStore embeds the in-memory store and makes every dialing verb
// (EnsureBucket, List) block until released — the shape of a service IP
// whose pod is gone: the dial neither succeeds nor refuses.
type blackholeStore struct {
	blobstore.Store
	mu       sync.Mutex
	released bool
	cond     *sync.Cond
	calls    atomic.Int32
}

func newBlackholeStore() *blackholeStore {
	b := &blackholeStore{Store: blobstore.NewMemory()}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *blackholeStore) release() {
	b.mu.Lock()
	b.released = true
	b.mu.Unlock()
	b.cond.Broadcast()
}

func (b *blackholeStore) List(ctx context.Context, prefix string, fn func(blobstore.BlobInfo) error) error {
	b.calls.Add(1)
	b.mu.Lock()
	for !b.released {
		b.cond.Wait()
	}
	b.mu.Unlock()
	return b.Store.List(ctx, prefix, fn)
}

func (b *blackholeStore) EnsureBucket(ctx context.Context) error {
	b.calls.Add(1)
	b.mu.Lock()
	for !b.released {
		b.cond.Wait()
	}
	b.mu.Unlock()
	return b.Store.EnsureBucket(ctx)
}

func newCloudManager(t *testing.T, store blobstore.Store) *Manager {
	t.Helper()
	m, err := NewManager(Config{
		Dir:            t.TempDir(),
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
		CloudStore:     store,
		VaultID:        glid.New(),
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// Construction returns while the store is still blackholed — it never
// touches the store at all.
func TestNewManagerNeverDialsTheCloudStore(t *testing.T) {
	t.Parallel()
	store := newBlackholeStore()
	defer store.release()

	m := newCloudManager(t, store) // pre-fix: blocks here for the dial's lifetime

	if got := store.calls.Load(); got != 0 {
		t.Fatalf("construction dialed the cloud store %d times, want 0", got)
	}
	if m.CloudIndexPopulated() {
		t.Fatal("index claims populated without any store contact")
	}
}

// EnsureCloudIndex retries until the store answers, latches on success, and
// never stacks concurrent attempts.
func TestEnsureCloudIndexRetriesAndLatches(t *testing.T) {
	t.Parallel()
	store := newBlackholeStore()
	m := newCloudManager(t, store)

	// While blackholed, an attempt blocks; a SECOND attempt must return
	// immediately (non-stacking) rather than queue behind it.
	done := make(chan error, 1)
	go func() { done <- m.EnsureCloudIndex() }()
	waittest.For(t, "first ensure attempt reaches the store", func() bool {
		return store.calls.Load() > 0
	})
	if err := m.EnsureCloudIndex(); err != nil {
		t.Fatalf("concurrent attempt should no-op, got %v", err)
	}
	if m.CloudIndexPopulated() {
		t.Fatal("populated while the store is still blackholed")
	}

	store.release()
	if err := <-done; err != nil {
		t.Fatalf("EnsureCloudIndex after release: %v", err)
	}
	if !m.CloudIndexPopulated() {
		t.Fatal("success did not latch")
	}

	// Latched: further calls never touch the store again.
	before := store.calls.Load()
	if err := m.EnsureCloudIndex(); err != nil {
		t.Fatal(err)
	}
	if store.calls.Load() != before {
		t.Fatal("a latched index dialed the store again")
	}
}

// failingStore errors on List — the fast-refusal outage shape.
type failingStore struct {
	blobstore.Store
	fail atomic.Bool
}

func (f *failingStore) List(ctx context.Context, prefix string, fn func(blobstore.BlobInfo) error) error {
	if f.fail.Load() {
		return errors.New("store unreachable")
	}
	return f.Store.List(ctx, prefix, fn)
}

// bucketFailStore errors on EnsureBucket while List would succeed — the
// first-contact-against-a-dead-endpoint shape for a brand-new vault, where
// bucket creation is the verb that fails.
type bucketFailStore struct {
	blobstore.Store
	fail atomic.Bool
}

func (f *bucketFailStore) EnsureBucket(ctx context.Context) error {
	if f.fail.Load() {
		return errors.New("store unreachable")
	}
	return f.Store.EnsureBucket(ctx)
}

func TestEnsureCloudIndexSurvivesABucketCreateOutage(t *testing.T) {
	t.Parallel()
	store := &bucketFailStore{Store: blobstore.NewMemory()}
	store.fail.Store(true)
	m := newCloudManager(t, store)

	if err := m.EnsureCloudIndex(); err == nil {
		t.Fatal("a failed bucket create populated the index")
	}
	if m.CloudIndexPopulated() {
		t.Fatal("failure latched as success")
	}
	if !m.CloudDegraded() {
		t.Fatal("a failed bucket create did not mark the store degraded")
	}

	store.fail.Store(false)
	if err := m.EnsureCloudIndex(); err != nil {
		t.Fatalf("retry after the outage: %v", err)
	}
	if !m.CloudIndexPopulated() {
		t.Fatal("recovery did not latch")
	}
	if m.CloudDegraded() {
		t.Fatal("recovery did not clear the degraded flag")
	}
}

func TestEnsureCloudIndexSurvivesAnOutage(t *testing.T) {
	t.Parallel()
	store := &failingStore{Store: blobstore.NewMemory()}
	store.fail.Store(true)
	m := newCloudManager(t, store)

	if err := m.EnsureCloudIndex(); err == nil {
		t.Fatal("an unreachable store populated the index")
	}
	if m.CloudIndexPopulated() {
		t.Fatal("failure latched as success")
	}
	if !m.CloudDegraded() {
		t.Fatal("a failed population did not mark the store degraded")
	}

	store.fail.Store(false)
	if err := m.EnsureCloudIndex(); err != nil {
		t.Fatalf("retry after the outage: %v", err)
	}
	if !m.CloudIndexPopulated() {
		t.Fatal("recovery did not latch")
	}
}
