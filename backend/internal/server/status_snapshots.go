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
//
// Snapshots are rebuilt on each stats update only while someone subscribes:
// a node nobody is watching builds none.
type statusSnapshots struct {
	mu          sync.Mutex
	build       func(context.Context) *apiv1.WatchSystemStatusResponse
	stats       *notify.Signal
	subscribers int
	stopTicks   context.CancelFunc
	building    bool
	again       bool

	latest  atomic.Pointer[apiv1.WatchSystemStatusResponse]
	updated *notify.Signal
}

func newStatusSnapshots() *statusSnapshots {
	return &statusSnapshots{updated: notify.NewSignal()}
}

// bind sets how a snapshot is built and which signal marks a stats update;
// the first binding stays.
func (c *statusSnapshots) bind(build func(context.Context) *apiv1.WatchSystemStatusResponse, stats *notify.Signal) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.build == nil {
		c.build, c.stats = build, stats
	}
}

// subscribe registers a subscriber and asks for a fresh snapshot for it.
// The first subscriber starts the rebuilds on stats updates; the returned
// function unregisters, and the last one to leave stops them.
func (c *statusSnapshots) subscribe() (unsubscribe func()) {
	c.mu.Lock()
	c.subscribers++
	if c.subscribers == 1 && c.stats != nil {
		ctx, cancel := context.WithCancel(context.Background())
		c.stopTicks = cancel
		// The first wait is taken here, so no update after subscribe
		// returns can slip past the rebuilds.
		go c.refreshOn(ctx, c.stats, c.stats.C())
	}
	c.mu.Unlock()
	c.refresh()

	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.subscribers--
			if c.subscribers == 0 && c.stopTicks != nil {
				c.stopTicks()
				c.stopTicks = nil
			}
		})
	}
}

// refreshing reports whether snapshots are being rebuilt on stats updates.
func (c *statusSnapshots) refreshing() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stopTicks != nil
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

// refreshOn rebuilds after every signal, starting with the wait on next,
// until ctx ends; an update that arrives after that builds nothing.
func (c *statusSnapshots) refreshOn(ctx context.Context, signal *notify.Signal, next <-chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-next:
			if ctx.Err() != nil {
				return
			}
			next = signal.C()
			c.refresh()
		}
	}
}
