// Package pipeline holds the small pieces shared by the write-path stage
// managers (ingest → digest → route → segment → distribute → collect →
// chunk); the stages themselves live in the subpackages.
package pipeline

import "sync"

// RunWorkerPool fans a stage's input queue out to workers goroutines, each
// draining in with handle until the queue closes, and blocks until all
// workers exit. Shutdown is close-driven (gastrolog-5kcq5q): the producer
// closes in on every exit path, so workers receive with a plain range
// instead of per-record ctx selects.
func RunWorkerPool[T any](workers int, in <-chan T, handle func(T)) {
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for item := range in {
				handle(item)
			}
		})
	}
	wg.Wait()
}
