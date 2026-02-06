package index_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
)

// mockIndexer is a configurable test double for index.Indexer.
type mockIndexer struct {
	name    string
	buildFn func(ctx context.Context, chunkID chunk.ChunkID) error
}

func (m *mockIndexer) Name() string { return m.name }
func (m *mockIndexer) Build(ctx context.Context, chunkID chunk.ChunkID) error {
	return m.buildFn(ctx, chunkID)
}

func TestBuildHelperDeduplication(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{})

	idx := &mockIndexer{
		name: "slow",
		buildFn: func(ctx context.Context, _ chunk.ChunkID) error {
			calls.Add(1)
			close(started)
			time.Sleep(50 * time.Millisecond)
			return nil
		},
	}

	helper := index.NewBuildHelper()
	chunkID := chunk.NewChunkID()

	const n = 10
	var wg sync.WaitGroup
	errs := make([]error, n)

	// Launch first caller to establish the in-flight build.
	wg.Go(func() {
		errs[0] = helper.Build(context.Background(), chunkID, []index.Indexer{idx})
	})

	// Wait for the build to start, then launch remaining callers.
	<-started
	for i := 1; i < n; i++ {
		wg.Go(func() {
			errs[i] = helper.Build(context.Background(), chunkID, []index.Indexer{idx})
		})
	}

	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d got error: %v", i, err)
		}
	}

	if got := calls.Load(); got != 1 {
		t.Errorf("indexer invoked %d times, want 1", got)
	}
}

func TestBuildHelperParallelism(t *testing.T) {
	var (
		mu     sync.Mutex
		starts []time.Time
	)

	makeIndexer := func(name string) index.Indexer {
		return &mockIndexer{
			name: name,
			buildFn: func(ctx context.Context, _ chunk.ChunkID) error {
				mu.Lock()
				starts = append(starts, time.Now())
				mu.Unlock()
				time.Sleep(50 * time.Millisecond)
				return nil
			},
		}
	}

	helper := index.NewBuildHelper()
	chunkID := chunk.NewChunkID()

	err := helper.Build(context.Background(), chunkID, []index.Indexer{
		makeIndexer("a"),
		makeIndexer("b"),
	})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	if len(starts) != 2 {
		t.Fatalf("expected 2 start times, got %d", len(starts))
	}

	diff := starts[1].Sub(starts[0])
	if diff < 0 {
		diff = -diff
	}
	if diff > 30*time.Millisecond {
		t.Errorf("indexers started %v apart, expected concurrent execution", diff)
	}
}

func TestBuildHelperErrorPropagation(t *testing.T) {
	sentinel := errors.New("build failed")

	idx := &mockIndexer{
		name: "failing",
		buildFn: func(ctx context.Context, _ chunk.ChunkID) error {
			return sentinel
		},
	}

	helper := index.NewBuildHelper()
	chunkID := chunk.NewChunkID()

	err := helper.Build(context.Background(), chunkID, []index.Indexer{idx})
	if !errors.Is(err, sentinel) {
		t.Errorf("got error %v, want %v", err, sentinel)
	}
}

func TestBuildHelperWaiterContextCancellation(t *testing.T) {
	buildStarted := make(chan struct{})
	buildDone := make(chan struct{})

	idx := &mockIndexer{
		name: "slow",
		buildFn: func(ctx context.Context, _ chunk.ChunkID) error {
			close(buildStarted)
			<-buildDone
			return nil
		},
	}

	helper := index.NewBuildHelper()
	chunkID := chunk.NewChunkID()

	// Caller A: initiates the build.
	var wg sync.WaitGroup
	var errA error
	wg.Go(func() {
		errA = helper.Build(context.Background(), chunkID, []index.Indexer{idx})
	})

	<-buildStarted

	// Caller B: waits with a cancellable context.
	ctxB, cancelB := context.WithCancel(context.Background())
	var errB error
	wg.Go(func() {
		errB = helper.Build(ctxB, chunkID, []index.Indexer{idx})
	})

	// Give B time to enter the callgroup wait, then cancel it.
	time.Sleep(10 * time.Millisecond)
	cancelB()

	// B should return promptly with context.Canceled.
	time.Sleep(20 * time.Millisecond)

	// Let the build finish so A can return.
	close(buildDone)
	wg.Wait()

	if errA != nil {
		t.Errorf("caller A got error: %v", errA)
	}
	if !errors.Is(errB, context.Canceled) {
		t.Errorf("caller B got error %v, want context.Canceled", errB)
	}
}

func TestBuildHelperIndependentChunkIDs(t *testing.T) {
	var calls atomic.Int32

	idx := &mockIndexer{
		name: "counter",
		buildFn: func(ctx context.Context, _ chunk.ChunkID) error {
			calls.Add(1)
			return nil
		},
	}

	helper := index.NewBuildHelper()
	id1 := chunk.ChunkIDFromTime(time.Now())
	id2 := chunk.ChunkIDFromTime(time.Now().Add(time.Second))

	var wg sync.WaitGroup
	for _, id := range []chunk.ChunkID{id1, id2} {
		wg.Go(func() {
			if err := helper.Build(context.Background(), id, []index.Indexer{idx}); err != nil {
				t.Errorf("Build(%s) error: %v", id, err)
			}
		})
	}

	wg.Wait()

	if got := calls.Load(); got != 2 {
		t.Errorf("indexer invoked %d times, want 2 (one per chunkID)", got)
	}
}
