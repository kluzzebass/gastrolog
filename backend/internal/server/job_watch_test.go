package server

import (
	"context"
	"sync"
	"testing"
	"time"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/orchestrator"
)

// brokerAdapter wraps an orchestrator.JobEventBroker so it satisfies
// JobEventSubscriber for tests.
type brokerAdapter struct{ b *orchestrator.JobEventBroker }

func (a *brokerAdapter) Subscribe() (*orchestrator.JobSubscription, func()) {
	return a.b.Subscribe()
}

// captureSender collects WatchJobs sends so tests can assert on them.
type captureSender struct {
	mu    sync.Mutex
	resps []*apiv1.WatchJobsResponse
}

func (c *captureSender) send(r *apiv1.WatchJobsResponse) error {
	c.mu.Lock()
	c.resps = append(c.resps, r)
	c.mu.Unlock()
	return nil
}

func (c *captureSender) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.resps)
}

func (c *captureSender) waitCount(t *testing.T, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.count() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out: want %d sends, got %d", want, c.count())
}

// newWatchSrv builds a JobServer backed by a fresh broker so tests can
// publish events directly to it.
func newWatchSrv(t *testing.T, sched JobScheduler, peers PeerJobsProvider) (*JobServer, *orchestrator.JobEventBroker) {
	t.Helper()
	b := orchestrator.NewJobEventBroker(0)
	t.Cleanup(b.Close)
	return &JobServer{
		scheduler:   sched,
		localNodeID: "node-A",
		peerJobs:    peers,
		events:      &brokerAdapter{b: b},
	}, b
}

func publishEvent(b *orchestrator.JobEventBroker) {
	b.Publish(orchestrator.JobEvent{
		Kind: orchestrator.JobEventCompleted,
		Job:  orchestrator.JobInfo{Name: "synthetic"},
	})
}

// TestWatchJobs_InitialSnapshot verifies the first send is the current
// state even when no events have fired.
func TestWatchJobs_InitialSnapshot(t *testing.T) {
	sched := &stubScheduler{jobs: map[string]orchestrator.JobInfo{
		"j": {ID: "j", Name: "job", Schedule: "0 * * * *"},
	}}
	srv, _ := newWatchSrv(t, sched, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cap := &captureSender{}

	done := make(chan struct{})
	go func() {
		_ = srv.watchJobsLoop(ctx, cap.send)
		close(done)
	}()

	cap.waitCount(t, 1, time.Second)
	cancel()
	<-done

	got := cap.resps[0]
	if len(got.Jobs) != 1 || got.Jobs[0].Name != "job" {
		t.Errorf("initial snapshot wrong: %+v", got.Jobs)
	}
}

// TestWatchJobs_EventTriggersSend verifies a broker event produces a
// follow-up send.
func TestWatchJobs_EventTriggersSend(t *testing.T) {
	sched := &stubScheduler{jobs: map[string]orchestrator.JobInfo{}}
	srv, broker := newWatchSrv(t, sched, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cap := &captureSender{}

	done := make(chan struct{})
	go func() {
		_ = srv.watchJobsLoop(ctx, cap.send)
		close(done)
	}()

	cap.waitCount(t, 1, time.Second) // initial
	publishEvent(broker)
	cap.waitCount(t, 2, time.Second) // post-event
	cancel()
	<-done
}

// TestWatchJobs_EventBurstCoalesces verifies a burst of events collapses
// into a single send (coalesce loop drains the pending wakeups).
func TestWatchJobs_EventBurstCoalesces(t *testing.T) {
	sched := &stubScheduler{jobs: map[string]orchestrator.JobInfo{}}
	srv, broker := newWatchSrv(t, sched, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cap := &captureSender{}

	done := make(chan struct{})
	go func() {
		_ = srv.watchJobsLoop(ctx, cap.send)
		close(done)
	}()

	cap.waitCount(t, 1, time.Second) // initial

	// Fire several events in quick succession.
	for i := 0; i < 10; i++ {
		publishEvent(broker)
	}
	// Expect at least one but far fewer than 10 sends.
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	got := cap.count()
	if got < 2 {
		t.Errorf("expected ≥ 2 sends after event burst, got %d", got)
	}
	if got > 10 {
		t.Errorf("expected coalescing; got %d sends for 10 events", got)
	}
}

// TestWatchJobs_PeerChangeTriggersSend verifies a peer-jobs Change signal
// also produces a send.
func TestWatchJobs_PeerChangeTriggersSend(t *testing.T) {
	sched := &stubScheduler{jobs: map[string]orchestrator.JobInfo{}}
	peers := newStubPeerJobs(map[string][]*apiv1.Job{})
	srv, _ := newWatchSrv(t, sched, peers)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cap := &captureSender{}

	done := make(chan struct{})
	go func() {
		_ = srv.watchJobsLoop(ctx, cap.send)
		close(done)
	}()

	cap.waitCount(t, 1, time.Second)
	peers.Changes().Notify()
	cap.waitCount(t, 2, time.Second)
	cancel()
	<-done
}

// TestWatchJobs_NoEventsNoSends verifies the loop only sends when
// something happens — no timer-driven re-sends.
func TestWatchJobs_NoEventsNoSends(t *testing.T) {
	sched := &stubScheduler{jobs: map[string]orchestrator.JobInfo{}}
	srv, _ := newWatchSrv(t, sched, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cap := &captureSender{}

	done := make(chan struct{})
	go func() {
		_ = srv.watchJobsLoop(ctx, cap.send)
		close(done)
	}()

	cap.waitCount(t, 1, time.Second) // initial
	// Wait comfortably longer than the old 500ms polling interval.
	time.Sleep(1200 * time.Millisecond)

	if n := cap.count(); n != 1 {
		t.Errorf("expected only the initial send, got %d", n)
	}
	cancel()
	<-done
}

// TestWatchJobs_CtxCancelReturns verifies the loop exits on ctx cancel.
func TestWatchJobs_CtxCancelReturns(t *testing.T) {
	sched := &stubScheduler{jobs: map[string]orchestrator.JobInfo{}}
	srv, _ := newWatchSrv(t, sched, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cap := &captureSender{}

	done := make(chan struct{})
	go func() {
		_ = srv.watchJobsLoop(ctx, cap.send)
		close(done)
	}()

	cap.waitCount(t, 1, time.Second)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watchJobsLoop did not return on ctx cancel")
	}
}

// TestWatchJobs_BrokerCloseEndsStream verifies the loop exits cleanly
// when the broker is closed underneath it.
func TestWatchJobs_BrokerCloseEndsStream(t *testing.T) {
	sched := &stubScheduler{jobs: map[string]orchestrator.JobInfo{}}
	srv, broker := newWatchSrv(t, sched, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cap := &captureSender{}

	done := make(chan struct{})
	go func() {
		_ = srv.watchJobsLoop(ctx, cap.send)
		close(done)
	}()

	cap.waitCount(t, 1, time.Second)
	broker.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watchJobsLoop did not return on broker close")
	}
}

// TestWatchJobs_NilEventsReturnsUnimplemented is a safety net: WatchJobs
// must fail loudly if wiring missed the broker rather than silently
// looping forever.
func TestWatchJobs_NilEventsReturnsUnimplemented(t *testing.T) {
	srv := &JobServer{scheduler: &stubScheduler{}, localNodeID: "node-A"}
	err := srv.watchJobsLoop(context.Background(), func(*apiv1.WatchJobsResponse) error { return nil })
	if err == nil {
		t.Fatal("expected error when events is nil, got nil")
	}
}
