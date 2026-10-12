package server

import (
	"context"
	"sync"
	"sync/atomic"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/notify"
)

// statusSnapshots holds the last WatchSystemStatus payload, built off the
// request path. Building one reads the orchestrator, which a stuck
// subsystem can hold for as long as it is stuck; a stream that waited on a
// build would hang with it. Streams send the last snapshot instead, and a
// build that is held up only delays the next one.
type statusSnapshots struct {
	mu       sync.Mutex
	build    func(context.Context) *apiv1.WatchSystemStatusResponse
	building bool
	again    bool

	latest  atomic.Pointer[apiv1.WatchSystemStatusResponse]
	updated *notify.Signal
}

func newStatusSnapshots() *statusSnapshots {
	return &statusSnapshots{updated: notify.NewSignal()}
}

// bind sets how a snapshot is built; the first binding stays.
func (c *statusSnapshots) bind(build func(context.Context) *apiv1.WatchSystemStatusResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.build == nil {
		c.build = build
	}
}

// refresh starts a build unless one is running, in which case that build is
// followed by another, so the newest request is never lost. It never waits
// on the build.
func (c *statusSnapshots) refresh() {
	c.mu.Lock()
	if c.build == nil {
		c.mu.Unlock()
		return
	}
	if c.building {
		c.again = true
		c.mu.Unlock()
		return
	}
	c.building = true
	build := c.build
	c.mu.Unlock()

	go func() {
		for {
			c.latest.Store(build(context.Background()))
			c.updated.Notify()
			c.mu.Lock()
			if !c.again {
				c.building = false
				c.mu.Unlock()
				return
			}
			c.again = false
			c.mu.Unlock()
		}
	}()
}

// refreshOn rebuilds after every signal until ctx ends.
func (c *statusSnapshots) refreshOn(ctx context.Context, signal *notify.Signal) {
	for {
		ch := signal.C()
		select {
		case <-ctx.Done():
			return
		case <-ch:
			c.refresh()
		}
	}
}
