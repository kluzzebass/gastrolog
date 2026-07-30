// Package applywait provides an event-driven applied-index tracker for Raft
// FSMs. The FSM advances the tracker as it applies committed log entries;
// forward paths block on Wait until the local FSM has caught up to the
// index the leader reported — the read-after-write barrier for forwarded
// mutations.
//
// The tracker is fed from the FSM Apply path rather than from
// raft.AppliedIndex(): hashicorp/raft advances AppliedIndex when it
// *enqueues* a committed entry for the FSM goroutine, before FSM.Apply has
// actually run, so a barrier polling AppliedIndex can release a reader
// while the store still shows pre-mutation state. An FSM-fed tracker
// advances only after the mutation is visible.
package applywait

import (
	"context"
	"sync"
)

// waiter is a single blocked Wait call: a target index and the channel
// closed when the tracker advances to (or past) it.
type waiter struct {
	target uint64
	ch     chan struct{}
}

// Tracker tracks the highest Raft log index the owning FSM has applied and
// wakes waiters when their target index is reached. The zero value is not
// ready for use; construct with New.
type Tracker struct {
	mu      sync.Mutex
	applied uint64
	waiters []*waiter
}

// New returns a Tracker with no applied entries.
func New() *Tracker {
	return &Tracker{}
}

// Advance records that the FSM has applied the entry at index and wakes
// every waiter whose target is now satisfied. Monotonic: an index at or
// below the current applied index is a no-op. Call after the FSM state
// mutation is visible (end of Apply, or after a snapshot restore swap).
func (t *Tracker) Advance(index uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if index <= t.applied {
		return
	}
	t.applied = index
	kept := t.waiters[:0]
	for _, w := range t.waiters {
		if w.target <= index {
			close(w.ch)
			continue
		}
		kept = append(kept, w)
	}
	// Nil out the tail so released waiters don't linger in the backing array.
	for i := len(kept); i < len(t.waiters); i++ {
		t.waiters[i] = nil
	}
	t.waiters = kept
}

// Applied returns the highest applied index observed so far.
func (t *Tracker) Applied() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.applied
}

// Wait blocks until the tracker has advanced to at least target, or ctx is
// done. A target of 0 (no meaningful index) returns immediately. If the
// tracker reaches the target concurrently with ctx cancellation, Wait
// reports success — the caller's read-after-write guarantee holds either
// way. Callers own the deadline: wrap ctx with the timeout that should
// bound the wait.
func (t *Tracker) Wait(ctx context.Context, target uint64) error {
	if target == 0 {
		return nil
	}
	t.mu.Lock()
	if t.applied >= target {
		t.mu.Unlock()
		return nil
	}
	w := &waiter{target: target, ch: make(chan struct{})}
	t.waiters = append(t.waiters, w)
	t.mu.Unlock()

	select {
	case <-w.ch:
		return nil
	case <-ctx.Done():
		if t.removeSatisfied(w) {
			return nil
		}
		return ctx.Err()
	}
}

// removeSatisfied unregisters w after a context abort and reports whether
// the target was reached anyway (Advance closed the channel concurrently).
func (t *Tracker) removeSatisfied(w *waiter) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, cand := range t.waiters {
		if cand == w {
			t.waiters = append(t.waiters[:i], t.waiters[i+1:]...)
			return false
		}
	}
	// Not registered anymore: Advance already woke this waiter.
	return true
}
