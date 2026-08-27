package raftstore

import (
	"context"
	"sync"
)

// barrierRound is one catch-up commit shared by every caller enrolled in it.
type barrierRound struct {
	done chan struct{}
	err  error
}

// barrierGate collapses concurrent catch-up requests onto shared rounds.
//
// A barrier is a Raft write — an entry appended, fsynced, replicated and
// applied cluster-wide — so one per request would let anything that misses a
// local read drive consensus at request rate. Coalescing bounds that by
// elapsed time instead: every caller arriving during one round-trip commits a
// single entry between them.
//
// A caller joins the *next* round, never one already running. A round that
// began before the caller read stale state proves nothing about that read;
// only a commit ordered after the caller arrived does.
type barrierGate struct {
	mu      sync.Mutex
	pending *barrierRound
	running bool
}

// join enrols the caller in the round it must wait for, and reports whether
// the caller is the one that has to start it.
func (g *barrierGate) join() (*barrierRound, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.pending == nil {
		g.pending = &barrierRound{done: make(chan struct{})}
	}
	r := g.pending
	if g.running {
		return r, false
	}
	g.running = true
	g.pending = nil
	return r, true
}

// run commits one round, then hands off to whoever queued behind it. base
// carries the starting caller's context values with its cancellation stripped:
// a caller that gives up must not cancel the round its peers wait on, and with
// no deadline of its own commit falls back to the store's apply timeout.
func (g *barrierGate) run(base context.Context, r *barrierRound, commit func(context.Context) error) {
	for {
		r.err = commit(base)
		close(r.done)

		g.mu.Lock()
		next := g.pending
		g.pending = nil
		g.running = next != nil
		g.mu.Unlock()

		if next == nil {
			return
		}
		r = next
	}
}

// Do runs a catch-up round, sharing one with any callers that arrived
// alongside it, and returns when that round has committed.
func (g *barrierGate) Do(ctx context.Context, commit func(context.Context) error) error {
	r, start := g.join()
	if start {
		go g.run(context.WithoutCancel(ctx), r, commit)
	}
	select {
	case <-r.done:
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}
