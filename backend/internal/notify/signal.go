// Package notify provides broadcast notification primitives.
package notify

import "sync"

// Signal is a broadcast notification mechanism using a close-and-recreate
// channel pattern. Callers wait on C(), and any call to Notify() wakes
// all waiters by closing the channel and creating a fresh one.
//
// Optionally carries a monotonically increasing version counter, set via
// NotifyWithVersion. Callers read the latest version with Version().
//
// Caller contract: after receiving from C(), the channel reference is stale
// (it stays closed forever). Callers must re-call C() after each wakeup
// to obtain the next notification channel. A typical select loop:
//
//	for {
//	    select {
//	    case <-signal.C():
//	        // handle notification
//	    case <-ctx.Done():
//	        return
//	    }
//	}
type Signal struct {
	mu      sync.Mutex
	ch      chan struct{}
	version uint64
}

// NewSignal creates a ready-to-use Signal.
func NewSignal() *Signal { return &Signal{ch: make(chan struct{})} }

// Notify wakes all goroutines currently waiting on C(). Safe to call
// concurrently and from multiple goroutines. No-op if no waiters exist
// (the channel is still closed and replaced).
func (s *Signal) Notify() {
	s.mu.Lock()
	close(s.ch)
	s.ch = make(chan struct{})
	s.mu.Unlock()
}

// NotifyWithVersion is like Notify but also updates the version counter.
// The version should be monotonically increasing (e.g. Raft log index).
func (s *Signal) NotifyWithVersion(v uint64) {
	s.mu.Lock()
	if v > s.version {
		s.version = v
	}
	close(s.ch)
	s.ch = make(chan struct{})
	s.mu.Unlock()
}

// Version returns the latest version set by NotifyWithVersion.
// Returns 0 if only Notify (no version) has been used.
func (s *Signal) Version() uint64 {
	s.mu.Lock()
	v := s.version
	s.mu.Unlock()
	return v
}

// C returns a channel that is closed on the next Notify() call.
// The returned channel is a snapshot: once closed, it remains closed.
// Callers must re-call C() after each receive to get the next channel.
func (s *Signal) C() <-chan struct{} {
	s.mu.Lock()
	ch := s.ch
	s.mu.Unlock()
	return ch
}
