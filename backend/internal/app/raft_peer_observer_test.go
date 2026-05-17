package app

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"

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

// fakeLeader implements leaderChecker for tests. The leader flag is
// mutable so a single test can simulate leadership changes mid-flight.
type fakeLeader struct {
	mu       sync.Mutex
	isLeader bool
}

func (f *fakeLeader) IsLeader() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.isLeader
}

func (f *fakeLeader) set(v bool) {
	f.mu.Lock()
	f.isLeader = v
	f.mu.Unlock()
}

// waitForNode polls cfgStore until a NodeConfig with the given ID
// appears or the deadline elapses.
func waitForNode(t *testing.T, store system.Store, id glid.GLID, timeout time.Duration) *system.NodeConfig {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		n, err := store.GetNode(context.Background(), id)
		if err == nil && n != nil {
			return n
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for NodeConfig(%s)", id)
	return nil
}

// TestRunPeerAdditionLoop_WritesPlaceholder verifies the happy path: on
// PeerObservation.Removed=false while leader, a placeholder NodeConfig
// is written with the new peer's GLID and a non-empty Name.
func TestRunPeerAdditionLoop_WritesPlaceholder(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := sysmem.NewStore()
	leader := &fakeLeader{isLeader: true}
	ch := make(chan hraft.Observation, 4)

	go runPeerAdditionLoop(ctx, ch, leader, store, quietAppLogger())

	id := glid.New()
	ch <- peerObs(id.String(), false)

	n := waitForNode(t, store, id, time.Second)
	if n.Name == "" {
		t.Fatal("expected placeholder NodeConfig to have non-empty Name")
	}
	if n.EffectiveState() != system.NodeStateLive {
		t.Fatalf("expected State=Live, got %s", n.EffectiveState())
	}
	if n.StateSince.IsZero() {
		t.Fatal("expected StateSince to be set")
	}
}

// TestRunPeerAdditionLoop_SkipsWhenNotLeader verifies that followers
// observe the addition but defer the FSM write to the leader.
func TestRunPeerAdditionLoop_SkipsWhenNotLeader(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := sysmem.NewStore()
	leader := &fakeLeader{isLeader: false}
	ch := make(chan hraft.Observation, 4)

	go runPeerAdditionLoop(ctx, ch, leader, store, quietAppLogger())

	id := glid.New()
	ch <- peerObs(id.String(), false)
	time.Sleep(50 * time.Millisecond)

	if n, _ := store.GetNode(ctx, id); n != nil {
		t.Fatalf("follower wrote NodeConfig despite IsLeader()=false: %+v", n)
	}
}

// TestRunPeerAdditionLoop_PreservesExistingName verifies the idempotent
// property: rejoining peers (or those whose own ensureNodeConfig already
// landed) keep their existing Name; the observer does not overwrite.
func TestRunPeerAdditionLoop_PreservesExistingName(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := sysmem.NewStore()
	leader := &fakeLeader{isLeader: true}
	ch := make(chan hraft.Observation, 4)

	id := glid.New()
	preferredName := "gastrolog-0"
	if err := store.PutNode(ctx, system.NodeConfig{
		ID: id, Name: preferredName, State: system.NodeStateLive, StateSince: time.Now(),
	}); err != nil {
		t.Fatalf("seed PutNode: %v", err)
	}

	go runPeerAdditionLoop(ctx, ch, leader, store, quietAppLogger())

	ch <- peerObs(id.String(), false)
	time.Sleep(50 * time.Millisecond)

	n, err := store.GetNode(ctx, id)
	if err != nil || n == nil {
		t.Fatalf("GetNode: %v / %v", n, err)
	}
	if n.Name != preferredName {
		t.Fatalf("observer overwrote existing Name: got %q, want %q", n.Name, preferredName)
	}
}

// TestRunPeerAdditionLoop_IgnoresRemovals verifies the addition loop
// does NOT react to Removed=true events — that's the removal loop's
// job, and overlapping handling would cause double-writes.
func TestRunPeerAdditionLoop_IgnoresRemovals(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := sysmem.NewStore()
	leader := &fakeLeader{isLeader: true}
	ch := make(chan hraft.Observation, 4)

	go runPeerAdditionLoop(ctx, ch, leader, store, quietAppLogger())

	id := glid.New()
	ch <- peerObs(id.String(), true)
	time.Sleep(50 * time.Millisecond)

	if n, _ := store.GetNode(ctx, id); n != nil {
		t.Fatalf("addition loop wrote NodeConfig on removal event: %+v", n)
	}
}

// TestRunPeerAdditionLoop_StopsOnCtxDone verifies clean shutdown.
func TestRunPeerAdditionLoop_StopsOnCtxDone(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	store := sysmem.NewStore()
	leader := &fakeLeader{isLeader: true}
	ch := make(chan hraft.Observation, 4)

	done := make(chan struct{})
	go func() {
		runPeerAdditionLoop(ctx, ch, leader, store, quietAppLogger())
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("addition loop did not exit on ctx cancellation")
	}
}
