package cluster

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
)

func testServer() *Server {
	s := &Server{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	s.stopCtx, s.stopCancel = context.WithCancel(context.Background())
	return s
}

// notifyEviction's eviction handler must NOT fire when the cluster server
// is already shutting down. Without this gate, a k8s rolling restart races
// the preStop hook against the cluster's RemoveNode → NotifyEviction
// broadcast, the evicted node tries to re-bootstrap itself as a fresh
// single-node cluster mid-shutdown, and the pod hangs in Terminating until
// kubelet force-kills it — blocking the rollout. See gastrolog-5z7l8.

func TestNotifyEviction_SkipsHandlerWhenShuttingDown(t *testing.T) {
	t.Parallel()

	var fired atomic.Int32
	s := testServer()
	s.SetEvictionHandler(func() { fired.Add(1) })
	s.stopCancel() // simulate Stop() already called

	_, err := s.notifyEviction(context.Background(), &gastrologv1.NotifyEvictionRequest{Reason: "test"})
	if err != nil {
		t.Fatalf("notifyEviction: %v", err)
	}

	// Give the handler goroutine a chance to fire if the gate were broken.
	time.Sleep(20 * time.Millisecond)
	if got := fired.Load(); got != 0 {
		t.Errorf("eviction handler fired during shutdown: got %d calls, want 0", got)
	}
}

func TestNotifyEviction_FiresHandlerWhenRunning(t *testing.T) {
	t.Parallel()

	done := make(chan struct{})
	s := testServer()
	defer s.stopCancel()
	s.SetEvictionHandler(func() { close(done) })

	_, err := s.notifyEviction(context.Background(), &gastrologv1.NotifyEvictionRequest{Reason: "test"})
	if err != nil {
		t.Fatalf("notifyEviction: %v", err)
	}

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("eviction handler did not fire within 500ms during normal operation")
	}
}
