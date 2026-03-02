// Package notify provides broadcast notification primitives.
package notify

import "sync"

// Signal is a broadcast notification mechanism. Callers wait on C(),
// and any call to Notify() wakes all waiters by closing the channel
// and creating a fresh one.
type Signal struct {
	mu sync.Mutex
	ch chan struct{}
}

// NewSignal creates a ready-to-use Signal.
func NewSignal() *Signal { return &Signal{ch: make(chan struct{})} }

// Notify wakes all current waiters.
func (s *Signal) Notify() {
	s.mu.Lock()
	close(s.ch)
	s.ch = make(chan struct{})
	s.mu.Unlock()
}

// C returns a channel that is closed on the next Notify() call.
// Callers should re-call C() after each wakeup to get the next channel.
func (s *Signal) C() <-chan struct{} {
	s.mu.Lock()
	ch := s.ch
	s.mu.Unlock()
	return ch
}
