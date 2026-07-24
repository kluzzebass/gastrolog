package applywait

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestWaitZeroTargetReturnsImmediately(t *testing.T) {
	t.Parallel()
	tr := New()
	if err := tr.Wait(context.Background(), 0); err != nil {
		t.Fatalf("Wait(0): %v", err)
	}
}

func TestWaitAlreadySatisfied(t *testing.T) {
	t.Parallel()
	tr := New()
	tr.Advance(5)
	if err := tr.Wait(context.Background(), 5); err != nil {
		t.Fatalf("Wait(5) after Advance(5): %v", err)
	}
	if err := tr.Wait(context.Background(), 3); err != nil {
		t.Fatalf("Wait(3) after Advance(5): %v", err)
	}
	if got := tr.Applied(); got != 5 {
		t.Fatalf("Applied() = %d, want 5", got)
	}
}

func TestWaitWokenByAdvance(t *testing.T) {
	t.Parallel()
	tr := New()
	done := make(chan error, 1)
	go func() { done <- tr.Wait(context.Background(), 7) }()
	// Advance below the target must not wake the waiter; advance to the
	// target must. Ordering is guaranteed by the tracker's own completion:
	// the goroutine only sends on done once Wait returns.
	tr.Advance(6)
	select {
	case err := <-done:
		t.Fatalf("Wait(7) returned after Advance(6): %v", err)
	default:
	}
	tr.Advance(7)
	if err := <-done; err != nil {
		t.Fatalf("Wait(7) after Advance(7): %v", err)
	}
}

func TestWaitMultipleWaitersDifferentTargets(t *testing.T) {
	t.Parallel()
	tr := New()
	results := make(chan uint64, 3)
	var wg sync.WaitGroup
	for _, target := range []uint64{2, 4, 6} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := tr.Wait(context.Background(), target); err != nil {
				t.Errorf("Wait(%d): %v", target, err)
				return
			}
			results <- target
		}()
	}
	// A single advance past every target wakes all waiters.
	tr.Advance(10)
	wg.Wait()
	close(results)
	seen := map[uint64]bool{}
	for target := range results {
		seen[target] = true
	}
	for _, target := range []uint64{2, 4, 6} {
		if !seen[target] {
			t.Errorf("waiter for target %d never woke", target)
		}
	}
}

func TestWaitContextCancelled(t *testing.T) {
	t.Parallel()
	tr := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := tr.Wait(ctx, 9)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait with cancelled ctx = %v, want context.Canceled", err)
	}
	// The aborted waiter must be unregistered — a later advance must not
	// panic (double close) and Applied must reflect the advance.
	tr.Advance(9)
	if got := tr.Applied(); got != 9 {
		t.Fatalf("Applied() = %d, want 9", got)
	}
}

func TestWaitSatisfiedConcurrentWithCancelReportsSuccess(t *testing.T) {
	t.Parallel()
	// If Advance closed the waiter channel before the aborting waiter
	// unregisters, Wait reports success — the write is visible.
	tr := New()
	w := &waiter{target: 4, ch: make(chan struct{})}
	tr.mu.Lock()
	tr.waiters = append(tr.waiters, w)
	tr.mu.Unlock()
	tr.Advance(4) // closes and removes w
	if !tr.removeSatisfied(w) {
		t.Fatal("removeSatisfied = false for a waiter Advance already woke")
	}
}

func TestAdvanceIsMonotonic(t *testing.T) {
	t.Parallel()
	tr := New()
	tr.Advance(8)
	tr.Advance(3) // stale — must not regress
	if got := tr.Applied(); got != 8 {
		t.Fatalf("Applied() = %d, want 8", got)
	}
}

func TestConcurrentAdvanceAndWait(t *testing.T) {
	t.Parallel()
	tr := New()
	const last = 200
	var wg sync.WaitGroup
	for target := uint64(1); target <= last; target++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := tr.Wait(context.Background(), target); err != nil {
				t.Errorf("Wait(%d): %v", target, err)
			}
		}()
	}
	go func() {
		for i := uint64(1); i <= last; i++ {
			tr.Advance(i)
		}
	}()
	wg.Wait()
	if got := tr.Applied(); got != last {
		t.Fatalf("Applied() = %d, want %d", got, last)
	}
}
