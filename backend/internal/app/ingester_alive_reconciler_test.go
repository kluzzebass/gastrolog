package app

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// flakyStore wraps a real store and lets a test inject N failures before
// it starts succeeding — models a Raft apply that times out during
// startup and recovers once quorum is stable.
type flakyStore struct {
	system.Store
	mu             sync.Mutex
	failsRemaining int
	failErr        error
	successes      int32
}

func (f *flakyStore) SetIngesterAlive(ctx context.Context, ingesterID glid.GLID, nodeID string, alive bool) error {
	f.mu.Lock()
	if f.failsRemaining > 0 {
		f.failsRemaining--
		f.mu.Unlock()
		return f.failErr
	}
	f.mu.Unlock()
	if err := f.Store.SetIngesterAlive(ctx, ingesterID, nodeID, alive); err != nil {
		return err
	}
	atomic.AddInt32(&f.successes, 1)
	return nil
}

func TestAliveReconciler_RetriesAfterFailure(t *testing.T) {
	t.Parallel()
	inner := sysmem.NewStore()
	store := &flakyStore{Store: inner, failsRemaining: 3, failErr: errors.New("raft unavailable")}

	rec := NewAliveReconciler(store, "node-A", silentLogger())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	done := make(chan struct{})
	go func() {
		rec.Run(ctx)
		close(done)
	}()

	id := glid.New()
	rec.Enqueue(id, true)

	// Poll the inner store until the alive entry lands. The reconciler
	// retries with backoff (200ms, 400ms, 800ms after the 3 failures).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		alive, _ := inner.GetIngesterAlive(context.Background(), id)
		if alive["node-A"] {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	alive, _ := inner.GetIngesterAlive(context.Background(), id)
	if !alive["node-A"] {
		t.Fatalf("alive map for %s did not converge to {node-A:true}; got %v", id, alive)
	}
	if atomic.LoadInt32(&store.successes) != 1 {
		t.Errorf("expected exactly 1 successful apply, got %d", store.successes)
	}
}

func TestAliveReconciler_PreservesEventOrder(t *testing.T) {
	t.Parallel()
	inner := sysmem.NewStore()
	rec := NewAliveReconciler(inner, "node-A", silentLogger())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go rec.Run(ctx)

	id := glid.New()
	// true → false should converge to false; reverse order would converge to true.
	rec.Enqueue(id, true)
	rec.Enqueue(id, false)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		alive, _ := inner.GetIngesterAlive(context.Background(), id)
		// Memory store: an absent entry IS the false state; alive=false delete clears it.
		if _, present := alive["node-A"]; !present {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	alive, _ := inner.GetIngesterAlive(context.Background(), id)
	t.Fatalf("expected alive=false (absent entry) for %s; got %v", id, alive)
}

func TestAliveReconciler_DropsOnFullQueue(t *testing.T) {
	t.Parallel()
	store := sysmem.NewStore()
	rec := NewAliveReconciler(store, "node-A", silentLogger())
	// Don't call Run — events stay in the channel and we fill it past capacity.

	id := glid.New()
	for range aliveQueueCapacity {
		rec.Enqueue(id, true)
	}
	// The next Enqueue would block in the without-default branch; the
	// non-blocking select must drop it instead so a backpressure storm
	// never stalls the orchestrator goroutine.
	rec.Enqueue(id, false) // should be dropped, no deadlock
}
