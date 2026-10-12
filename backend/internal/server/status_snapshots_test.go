package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/notify"
)

// A node nobody watches builds no status snapshots, however many stats
// updates pass; the first subscriber gets one built for it and then one per
// update, and when the last subscriber leaves the rebuilding stops.
func TestStatusSnapshotsAreBuiltOnlyWhileSubscribed(t *testing.T) {
	t.Parallel()
	var builds atomic.Int32
	stats := notify.NewSignal()
	c := newStatusSnapshots()
	c.bind(func(context.Context) *apiv1.WatchSystemStatusResponse {
		builds.Add(1)
		return &apiv1.WatchSystemStatusResponse{}
	}, stats)

	waitBuilt := func(updated <-chan struct{}, want int32) {
		t.Helper()
		select {
		case <-updated:
		case <-time.After(5 * time.Second):
			t.Fatalf("no snapshot built; want build %d", want)
		}
		if got := builds.Load(); got != want {
			t.Fatalf("%d builds, want %d", got, want)
		}
	}

	idle := c.updated.C()
	for range 3 {
		stats.Notify()
	}
	if c.refreshing() || builds.Load() != 0 {
		t.Fatalf("with no subscriber: refreshing=%v, %d builds; want neither", c.refreshing(), builds.Load())
	}
	// A build would land within microseconds of the updates; this wait can
	// only fail when one happens, never pass because the build was slow.
	select {
	case <-idle:
		t.Fatalf("a snapshot was built with no subscriber (%d builds)", builds.Load())
	case <-time.After(100 * time.Millisecond):
	}

	updated := c.updated.C()
	unsubscribe := c.subscribe()
	waitBuilt(updated, 1)
	if !c.refreshing() {
		t.Fatal("a subscriber does not start the rebuilds")
	}
	for want := int32(2); want <= 3; want++ {
		updated = c.updated.C()
		stats.Notify()
		waitBuilt(updated, want)
	}

	unsubscribe()
	if c.refreshing() {
		t.Fatal("the rebuilds outlive the last subscriber")
	}
	for range 3 {
		stats.Notify()
	}
	if got := builds.Load(); got != 3 {
		t.Fatalf("%d builds after the last subscriber left, want them to stop at 3", got)
	}
}
