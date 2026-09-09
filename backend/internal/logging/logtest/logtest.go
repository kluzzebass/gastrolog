// Package logtest provides slog handlers for tests that need to read what a
// component logged, or to make a component panic where it logs.
//
// The panicking handler exists so a test can prove a goroutine's panic guard
// without corrupting shared state: every component already holds a logger,
// so a handler that panics on a chosen message raises a panic exactly where
// that component runs — inside a connection handler, inside a listener loop
// — and the guard is exercised on the real call path.
package logtest

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Recorder collects the lines a test's logger emitted. It is safe to read
// while the component under test keeps logging from other goroutines.
type Recorder struct {
	mu     sync.Mutex
	cond   *sync.Cond
	lines  []string
	closed bool
}

func newRecorder() *Recorder {
	r := &Recorder{}
	r.cond = sync.NewCond(&r.mu)
	return r
}

func (r *Recorder) add(line string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines = append(r.lines, line)
	r.cond.Broadcast()
}

// Contains reports whether one emitted line contains every substring.
func (r *Recorder) Contains(subs ...string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, line := range r.lines {
		if containsAll(line, subs) {
			return true
		}
	}
	return false
}

// Wait blocks until one emitted line contains every substring, and reports
// whether that happened before timeout. A component logs on its own
// goroutine, so a test that merely samples Contains races the write it is
// asserting on.
func (r *Recorder) Wait(timeout time.Duration, subs ...string) bool {
	timer := time.AfterFunc(timeout, func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.closed = true
		r.cond.Broadcast()
	})
	defer timer.Stop()

	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		for _, line := range r.lines {
			if containsAll(line, subs) {
				return true
			}
		}
		if r.closed {
			return false
		}
		r.cond.Wait()
	}
}

// String returns every emitted line, for failure messages.
func (r *Recorder) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.lines, "\n")
}

func containsAll(line string, subs []string) bool {
	for _, sub := range subs {
		if !strings.Contains(line, sub) {
			return false
		}
	}
	return true
}

// New returns a logger that records every level and the recorder holding
// what it wrote.
func New() (*slog.Logger, *Recorder) {
	rec := newRecorder()
	return slog.New(&handler{rec: rec}), rec
}

// NewPanicking returns a logger that panics the first times times it is
// asked to log the message panicOn, and records everything either way —
// including the lines written after the panic is recovered.
func NewPanicking(panicOn string, times int64) (*slog.Logger, *Recorder) {
	rec := newRecorder()
	remaining := &atomic.Int64{}
	remaining.Store(times)
	return slog.New(&handler{rec: rec, panicOn: panicOn, remaining: remaining}), rec
}

type handler struct {
	rec       *Recorder
	attrs     []slog.Attr
	panicOn   string
	remaining *atomic.Int64
}

func (h *handler) Enabled(context.Context, slog.Level) bool { return true }

func (h *handler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	b.WriteString(r.Level.String())
	b.WriteString(" ")
	b.WriteString(r.Message)
	for _, a := range h.attrs {
		fmt.Fprintf(&b, " %s=%v", a.Key, a.Value)
	}
	r.Attrs(func(a slog.Attr) bool {
		fmt.Fprintf(&b, " %s=%v", a.Key, a.Value)
		return true
	})
	// Record before panicking so the triggering line is visible to the test.
	h.rec.add(b.String())

	if h.panicOn != "" && r.Message == h.panicOn && h.remaining.Add(-1) >= 0 {
		panic("logtest: injected panic on " + h.panicOn)
	}
	return nil
}

func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := *h
	next.attrs = append(append([]slog.Attr(nil), h.attrs...), attrs...)
	return &next
}

// WithGroup returns the handler unchanged: tests here assert on message and
// attribute text, and grouping would only reshape that text.
func (h *handler) WithGroup(string) slog.Handler { return h }
