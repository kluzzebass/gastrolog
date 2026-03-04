// Package notify provides broadcast notification primitives.
package notify

import "sync"

// Signal is a broadcast notification mechanism using a close-and-recreate
// channel pattern. Callers wait on C(), and any call to Notify() wakes
// all waiters by closing the channel and creating a fresh one.
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
	mu sync.Mutex
	ch chan struct{}
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

// C returns a channel that is closed on the next Notify() call.
// The returned channel is a snapshot: once closed, it remains closed.
// Callers must re-call C() after each receive to get the next channel.
func (s *Signal) C() <-chan struct{} {
	s.mu.Lock()
	ch := s.ch
	s.mu.Unlock()
	return ch
}
