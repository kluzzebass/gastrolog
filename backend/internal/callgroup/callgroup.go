// Package callgroup provides call deduplication by key.
//
// If multiple goroutines request the same key concurrently, only one
// executes the function. The others wait and receive the same result.
// Once the function returns, the key is forgotten and future calls
// trigger a new execution.
package callgroup

import "sync"

// Group deduplicates concurrent function calls by key.
type Group[K comparable] struct {
	mu    sync.Mutex
	calls map[K]*call
}

type call struct {
	done chan struct{}
	err  error
}

// DoChan executes fn if no call is in flight for key. If a call is
// already in flight, the returned channel will receive the result of
// that existing call. The channel receives exactly one value and is
// never closed.
func (g *Group[K]) DoChan(key K, fn func() error) <-chan error {
	g.mu.Lock()
	if g.calls == nil {
		g.calls = make(map[K]*call)
	}
	if c, ok := g.calls[key]; ok {
		g.mu.Unlock()
		ch := make(chan error, 1)
		go func() {
			<-c.done
			ch <- c.err
		}()
		return ch
	}

	c := &call{done: make(chan struct{})}
	g.calls[key] = c
	g.mu.Unlock()

	go func() {
		c.err = fn()
		close(c.done)

		g.mu.Lock()
		delete(g.calls, key)
		g.mu.Unlock()
	}()

	ch := make(chan error, 1)
	go func() {
		<-c.done
		ch <- c.err
	}()
	return ch
}
