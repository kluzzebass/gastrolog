// Package lifecycle exposes a shared shutdown signal used by every component
// that needs to react to "we are shutting down" as fast as possible.
//
// Phase is the single authority for the cluster-wide shutdown gate. It combines
// a cancellable context (for select-based drain handlers) and an atomic bool
// (for fast-path per-request checks) behind a single toggle. BeginShutdown
// flips both atomically and is idempotent; subsequent calls only update the
// current phase label.
//
// The one-phase-per-process design means every subsystem agrees on the same
// moment of transition — there is no race where the orchestrator thinks it is
// draining but the cluster forwarder has not yet been told.
package lifecycle

import (
	"context"
	"sync/atomic"
)

// Phase publishes the shutdown signal to every subsystem that subscribes.
//
// Construction: exactly one Phase is created at process start (typically by
// app.go) and passed by pointer to every component that needs to react to
// shutdown.
//
// Read paths:
//   - ShuttingDown() — O(1) atomic load, safe for per-request hot paths
//   - Context()      — a context that is Done() once BeginShutdown fires
//   - Label()        — the current phase label (empty before BeginShutdown)
//
// Write paths (typically app.go's shutdown sequence):
//   - BeginShutdown(label) — flips the flag AND cancels the context
//   - Set(label)           — updates the label without re-cancelling the ctx
//
// Phase is safe for concurrent use. All methods are lock-free.
type Phase struct {
	ctx    context.Context
	cancel context.CancelFunc
	flag   atomic.Bool
	label  atomic.Pointer[string]
}

// New creates a Phase in the "running" state. The returned Phase's
// ShuttingDown() returns false and its Context() is not yet cancelled.
func New() *Phase {
	ctx, cancel := context.WithCancel(context.Background())
	return &Phase{ctx: ctx, cancel: cancel}
}

// ShuttingDown reports whether BeginShutdown has been called. This is the
// cheapest check available — a single atomic load — and is safe to call from
// hot paths like per-record replication fanout.
func (p *Phase) ShuttingDown() bool {
	return p.flag.Load()
}

// Context returns a context that is Done() after BeginShutdown. Components
// that already select on other contexts can add this one as an extra case.
func (p *Phase) Context() context.Context {
	return p.ctx
}

// Label returns the current phase label. Returns "" before BeginShutdown.
// Useful for logging which shutdown step the process is in.
func (p *Phase) Label() string {
	if s := p.label.Load(); s != nil {
		return *s
	}
	return ""
}

// BeginShutdown flips the shutdown flag, cancels the context, and sets the
// initial label in one sequence. Idempotent: subsequent calls only update
// the label, leaving cancellation untouched.
func (p *Phase) BeginShutdown(label string) {
	p.label.Store(&label)
	if p.flag.CompareAndSwap(false, true) {
		p.cancel()
	}
}

// Set updates the phase label without re-triggering cancellation. Use this
// to record progression between shutdown steps — e.g. "draining orchestrator"
// then "closing cluster server".
func (p *Phase) Set(label string) {
	p.label.Store(&label)
}
