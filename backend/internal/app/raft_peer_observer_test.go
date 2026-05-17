package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"

	hraft "github.com/hashicorp/raft"
)

var errFakeMember = errors.New("fake Servers() error")

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

	go runPeerRemovalLoop(ctx, ch, quietAppLogger(), ps, pjs)

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

	go runPeerRemovalLoop(ctx, ch, quietAppLogger(), ps, pjs)

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

	go runPeerRemovalLoop(ctx, ch, quietAppLogger(), ps, pjs)

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
		runPeerRemovalLoop(ctx, ch, quietAppLogger(), ps, pjs)
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

	go runPeerRemovalLoop(ctx, ch, quietAppLogger(), ps, pjs)

	for _, id := range []string{"a", "b", "c"} {
		ch <- peerObs(id, true)
	}
	for _, id := range []string{"a", "b", "c"} {
		ps.wait(t, id, time.Second)
		pjs.wait(t, id, time.Second)
	}
}

// TestRunPeerRemovalLoop_VariadicEvictors verifies that every supplied
// evictor is called on a removal — the variadic signature is what lets
// the production wiring (app.go) thread all six per-peer caches through
// the same loop (gastrolog-9ohip).
func TestRunPeerRemovalLoop_VariadicEvictors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan hraft.Observation, 4)
	// Six fake evictors — matches production's actual fan-out:
	// PeerState, PeerJobState, PeerByteMetrics, Broadcaster,
	// StatsCollector, RecordForwarder.
	evictors := []*fakeEvictor{{}, {}, {}, {}, {}, {}}
	args := make([]peerEvictor, len(evictors))
	for i, e := range evictors {
		args[i] = e
	}

	go runPeerRemovalLoop(ctx, ch, quietAppLogger(), args...)

	ch <- peerObs("dead-node", true)

	for i, e := range evictors {
		e.wait(t, "dead-node", time.Second)
		if got := e.snapshot(); len(got) != 1 {
			t.Errorf("evictor[%d]: expected 1 Delete, got %v", i, got)
		}
	}
}

// fakeReconcilable records ReconcilePeers calls + a tiny internal map
// so tests can assert what was actually removed.
type fakeReconcilable struct {
	mu      sync.Mutex
	entries map[string]struct{}
	calls   int
}

func newFakeReconcilable(initial ...string) *fakeReconcilable {
	f := &fakeReconcilable{entries: make(map[string]struct{})}
	for _, id := range initial {
		f.entries[id] = struct{}{}
	}
	return f
}

func (f *fakeReconcilable) ReconcilePeers(keep map[string]struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	for id := range f.entries {
		if _, ok := keep[id]; !ok {
			delete(f.entries, id)
		}
	}
}

func (f *fakeReconcilable) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.entries))
	for id := range f.entries {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// fakeMemberSource implements memberSource with a swappable membership list.
type fakeMemberSource struct {
	mu      sync.Mutex
	servers []cluster.RaftServer
	err     error
}

func (f *fakeMemberSource) Servers() ([]cluster.RaftServer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	out := make([]cluster.RaftServer, len(f.servers))
	copy(out, f.servers)
	return out, nil
}

func (f *fakeMemberSource) set(ids ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.servers = make([]cluster.RaftServer, len(ids))
	for i, id := range ids {
		f.servers[i] = cluster.RaftServer{ID: id}
	}
}

// TestReconcilePeerCachesOnce_PurgesNonMembers verifies the happy
// path: caches contain some entries; the reconcile pass drops any
// whose peer isn't in the current Raft membership set, keeps the
// rest.
func TestReconcilePeerCachesOnce_PurgesNonMembers(t *testing.T) {
	src := &fakeMemberSource{}
	src.set("alive-1", "alive-2")
	cache := newFakeReconcilable("alive-1", "alive-2", "ghost-1", "ghost-2")

	reconcilePeerCachesOnce(src, quietAppLogger(), cache)

	got := cache.snapshot()
	if len(got) != 2 || got[0] != "alive-1" || got[1] != "alive-2" {
		t.Fatalf("expected only alive-1 + alive-2 preserved, got %v", got)
	}
}

// TestReconcilePeerCachesOnce_NoOpWhenAllMembersPresent verifies
// stable state: every cache entry matches a current member, no
// deletions.
func TestReconcilePeerCachesOnce_NoOpWhenAllMembersPresent(t *testing.T) {
	src := &fakeMemberSource{}
	src.set("a", "b", "c")
	cache := newFakeReconcilable("a", "b", "c")

	reconcilePeerCachesOnce(src, quietAppLogger(), cache)

	if got := cache.snapshot(); len(got) != 3 {
		t.Errorf("expected 3 members preserved, got %v", got)
	}
}

// TestReconcilePeerCachesOnce_SkipsOnSourceError verifies that a
// Servers() failure does NOT wipe caches — the reconciler logs and
// returns without touching them. Without this, a transient cluster
// hiccup could empty every peer cache cluster-wide.
func TestReconcilePeerCachesOnce_SkipsOnSourceError(t *testing.T) {
	src := &fakeMemberSource{err: errFakeMember}
	cache := newFakeReconcilable("only-entry")

	reconcilePeerCachesOnce(src, quietAppLogger(), cache)

	if got := cache.snapshot(); len(got) != 1 {
		t.Errorf("expected entry preserved on Servers() error, got %v", got)
	}
}

// fakeScheduler implements peerReconcileScheduler — just records
// what AddJob and Describe were called with so we can assert the job
// got registered with the right name + cron and a non-empty
// description (i.e. visible in the operator inspector).
type fakeScheduler struct {
	mu               sync.Mutex
	addJobName       string
	addJobCron       string
	addJobTaskFn     any
	addJobErr        error
	describeName     string
	describeMessage  string
}

func (f *fakeScheduler) AddJob(name, cronExpr string, taskFn any, _ ...any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addJobName = name
	f.addJobCron = cronExpr
	f.addJobTaskFn = taskFn
	return f.addJobErr
}

func (f *fakeScheduler) Describe(name, description string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.describeName = name
	f.describeMessage = description
}

// TestStartPeerCacheReconcile_RegistersOperatorVisibleJob verifies
// that startPeerCacheReconcile registers the reconcile work as a
// proper scheduled job (visible in the inspector's Scheduled view)
// rather than a hidden goroutine. The job MUST get a non-empty
// Describe() so operators can see what it does. Calling the
// captured taskFn should drive the cache reconcile end-to-end.
func TestStartPeerCacheReconcile_RegistersOperatorVisibleJob(t *testing.T) {
	src := &fakeMemberSource{}
	src.set("alive-1")
	cache := newFakeReconcilable("alive-1", "ghost-1")
	sched := &fakeScheduler{}

	if err := startPeerCacheReconcile(sched, src, quietAppLogger(), cache); err != nil {
		t.Fatalf("startPeerCacheReconcile: %v", err)
	}

	if sched.addJobName != peerCacheReconcileJobName {
		t.Errorf("AddJob name: got %q, want %q", sched.addJobName, peerCacheReconcileJobName)
	}
	if sched.addJobCron != peerCacheReconcileSchedule {
		t.Errorf("AddJob cron: got %q, want %q", sched.addJobCron, peerCacheReconcileSchedule)
	}
	if sched.describeName != peerCacheReconcileJobName {
		t.Errorf("Describe name: got %q, want %q", sched.describeName, peerCacheReconcileJobName)
	}
	if sched.describeMessage == "" {
		t.Error("Describe message empty — operator inspector will show no context")
	}

	// Run the captured task as the scheduler would.
	if task, ok := sched.addJobTaskFn.(func()); ok {
		task()
	} else {
		t.Fatalf("expected captured task of type func(), got %T", sched.addJobTaskFn)
	}

	if got := cache.snapshot(); len(got) != 1 || got[0] != "alive-1" {
		t.Errorf("after task run: expected alive-1 only, got %v", got)
	}
}

// TestStartPeerCacheReconcile_PropagatesAddJobError verifies the
// caller sees the AddJob failure (e.g. duplicate name) so it can
// log or fatal.
func TestStartPeerCacheReconcile_PropagatesAddJobError(t *testing.T) {
	sched := &fakeScheduler{addJobErr: errFakeMember}
	src := &fakeMemberSource{}
	cache := newFakeReconcilable()

	err := startPeerCacheReconcile(sched, src, quietAppLogger(), cache)
	if err == nil {
		t.Fatal("expected AddJob error to propagate")
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
