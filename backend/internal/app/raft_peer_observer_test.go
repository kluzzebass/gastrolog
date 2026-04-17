package app

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
)

// fakeEvictor records Delete calls for later assertion.
type fakeEvictor struct {
	mu    sync.Mutex
	calls []string
}

func (f *fakeEvictor) Delete(id string) {
	f.mu.Lock()
	f.calls = append(f.calls, id)
	f.mu.Unlock()
}

func (f *fakeEvictor) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.calls))
	copy(out, f.calls)
	return out
}

func (f *fakeEvictor) wait(t *testing.T, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, c := range f.snapshot() {
			if c == want {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for Delete(%q); saw %v", want, f.snapshot())
}

func quietAppLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// peerObs builds a PeerObservation for the given node + removed flag.
func peerObs(id string, removed bool) hraft.Observation {
	return hraft.Observation{
		Data: hraft.PeerObservation{
			Peer:    hraft.Server{ID: hraft.ServerID(id)},
			Removed: removed,
		},
	}
}

// TestRunPeerRemovalLoop_DeletesOnRemoval verifies the happy path: a
// PeerObservation with Removed=true triggers Delete on both supplied caches
// with the correct node ID.
func TestRunPeerRemovalLoop_DeletesOnRemoval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan hraft.Observation, 4)
	ps, pjs := &fakeEvictor{}, &fakeEvictor{}

	go runPeerRemovalLoop(ctx, ch, ps, pjs, quietAppLogger())

	ch <- peerObs("dead-node", true)

	ps.wait(t, "dead-node", time.Second)
	pjs.wait(t, "dead-node", time.Second)
}

// TestRunPeerRemovalLoop_IgnoresAddEvents verifies that Added events
// (Removed=false) don't trigger Delete — we only evict on removal.
func TestRunPeerRemovalLoop_IgnoresAddEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan hraft.Observation, 4)
	ps, pjs := &fakeEvictor{}, &fakeEvictor{}

	go runPeerRemovalLoop(ctx, ch, ps, pjs, quietAppLogger())

	ch <- peerObs("new-node", false)
	// Give the goroutine a chance to process.
	time.Sleep(50 * time.Millisecond)

	if got := ps.snapshot(); len(got) != 0 {
		t.Errorf("peer-state Delete called on add event: %v", got)
	}
	if got := pjs.snapshot(); len(got) != 0 {
		t.Errorf("peer-job-state Delete called on add event: %v", got)
	}
}

// TestRunPeerRemovalLoop_IgnoresNonPeerObservations verifies that other
// observation types (e.g. LeaderObservation) are silently skipped.
func TestRunPeerRemovalLoop_IgnoresNonPeerObservations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan hraft.Observation, 4)
	ps, pjs := &fakeEvictor{}, &fakeEvictor{}

	go runPeerRemovalLoop(ctx, ch, ps, pjs, quietAppLogger())

	ch <- hraft.Observation{Data: hraft.LeaderObservation{LeaderID: "leader"}}
	time.Sleep(50 * time.Millisecond)

	if got := ps.snapshot(); len(got) != 0 {
		t.Errorf("Delete called on leader observation: %v", got)
	}
}

// TestRunPeerRemovalLoop_StopsOnCtxDone verifies the loop exits when ctx is
// cancelled — no goroutine leak.
func TestRunPeerRemovalLoop_StopsOnCtxDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan hraft.Observation, 4)
	ps, pjs := &fakeEvictor{}, &fakeEvictor{}

	done := make(chan struct{})
	go func() {
		runPeerRemovalLoop(ctx, ch, ps, pjs, quietAppLogger())
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop did not exit on ctx cancellation")
	}
}

// TestRunPeerRemovalLoop_MultipleRemovals verifies that a sequence of
// removal events each evict the matching node.
func TestRunPeerRemovalLoop_MultipleRemovals(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan hraft.Observation, 8)
	ps, pjs := &fakeEvictor{}, &fakeEvictor{}

	go runPeerRemovalLoop(ctx, ch, ps, pjs, quietAppLogger())

	for _, id := range []string{"a", "b", "c"} {
		ch <- peerObs(id, true)
	}
	for _, id := range []string{"a", "b", "c"} {
		ps.wait(t, id, time.Second)
		pjs.wait(t, id, time.Second)
	}
}
