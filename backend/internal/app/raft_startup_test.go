package app

import (
	"context"
	"io"
	"testing"
	"time"

	"gastrolog/internal/system/command"
	"gastrolog/internal/system/raftfsm"
	"gastrolog/internal/system/raftstore"

	hraft "github.com/hashicorp/raft"
)

// discardLogger is defined in startup_test.go (same package).

// newInmemRaft creates a single-node in-memory raft instance. When bootstrap
// is true it is bootstrapped as a lone voter and becomes leader; otherwise it
// stays a leaderless follower (Apply returns ErrNotLeader), which is what the
// leader-wait and forward paths need.
func newInmemRaft(t *testing.T, id string, bootstrap bool) (*hraft.Raft, *raftfsm.FSM) {
	t.Helper()

	fsm := raftfsm.New()

	conf := hraft.DefaultConfig()
	conf.LocalID = hraft.ServerID(id)
	conf.LogOutput = io.Discard
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond

	logStore := hraft.NewInmemStore()
	stableStore := hraft.NewInmemStore()
	snapStore := hraft.NewInmemSnapshotStore()
	_, transport := hraft.NewInmemTransport(hraft.ServerAddress(id))

	r, err := hraft.NewRaft(conf, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}
	t.Cleanup(func() { _ = r.Shutdown().Error() })

	if bootstrap {
		boot := hraft.Configuration{
			Servers: []hraft.Server{{ID: hraft.ServerID(id), Address: transport.LocalAddr()}},
		}
		if err := r.BootstrapCluster(boot).Error(); err != nil {
			t.Fatalf("BootstrapCluster: %v", err)
		}
	}
	return r, fsm
}

// appMockForwarder is a raftstore.Forwarder that reports a fixed leader index
// without a real leader, so the follower forward-and-wait path can be driven
// deterministically.
type appMockForwarder struct{ appliedIndex uint64 }

func (m *appMockForwarder) Forward(_ context.Context, _ []byte) (uint64, error) {
	return m.appliedIndex, nil
}

// TestWaitForLeaderAlreadyElected covers the fast path: a leader already
// exists when WaitForLeader is called, so it returns immediately via the
// post-registration LeaderWithID check.
func TestWaitForLeaderAlreadyElected(t *testing.T) {
	t.Parallel()
	r, _ := newInmemRaft(t, "solo", true)
	// Ensure leadership is established before we call.
	select {
	case <-r.LeaderCh():
	case <-time.After(2 * time.Second):
		t.Fatal("node never became leader")
	}

	rcs := &raftClusterCtlStore{raft: r}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rcs.WaitForLeader(ctx, discardLogger()); err != nil {
		t.Fatalf("WaitForLeader (already elected): %v", err)
	}
}

// TestWaitForLeaderElectedWhileWaiting covers the event-driven path: the call
// blocks on a leaderless node, and returns once an election makes a leader
// known. The observer wake (or the registration-time re-check) releases it —
// never a poll. Synchronization is the returned error channel; no sleeps.
func TestWaitForLeaderElectedWhileWaiting(t *testing.T) {
	t.Parallel()
	r, _ := newInmemRaft(t, "late", false)
	rcs := &raftClusterCtlStore{raft: r}

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- rcs.WaitForLeader(ctx, discardLogger())
	}()

	// Bootstrap after the wait has begun; the ensuing election fires a
	// LeaderObservation that wakes the waiter. The inmem transport's
	// LocalAddr equals the node id (see newInmemRaft).
	boot := hraft.Configuration{
		Servers: []hraft.Server{{ID: "late", Address: hraft.ServerAddress("late")}},
	}
	if err := r.BootstrapCluster(boot).Error(); err != nil {
		t.Fatalf("BootstrapCluster: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitForLeader (elected while waiting): %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForLeader did not return after leader elected")
	}
}

// TestWaitForLeaderContextCancelled pins that a cancelled context aborts the
// wait with ctx.Err() rather than hanging on a leaderless node.
func TestWaitForLeaderContextCancelled(t *testing.T) {
	t.Parallel()
	r, _ := newInmemRaft(t, "leaderless", false)
	rcs := &raftClusterCtlStore{raft: r}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rcs.WaitForLeader(ctx, discardLogger()) }()
	cancel()

	select {
	case err := <-done:
		if err == nil || err != context.Canceled {
			t.Fatalf("WaitForLeader after cancel = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitForLeader did not observe context cancellation")
	}
}

// TestWaitForFSMCatchupOnLeader covers the leader path: the barrier is
// committed synchronously via raft.Apply and WaitForFSMCatchup returns with
// the FSM current.
func TestWaitForFSMCatchupOnLeader(t *testing.T) {
	t.Parallel()
	r, fsm := newInmemRaft(t, "solo-catchup", true)
	select {
	case <-r.LeaderCh():
	case <-time.After(2 * time.Second):
		t.Fatal("node never became leader")
	}
	store := raftstore.New(r, fsm, 5*time.Second)
	rcs := &raftClusterCtlStore{raft: r, raftStore: store}

	before := fsm.ApplyWait().Applied()
	if err := rcs.WaitForFSMCatchup(context.Background(), 5*time.Second, discardLogger()); err != nil {
		t.Fatalf("WaitForFSMCatchup on leader: %v", err)
	}
	if got := fsm.ApplyWait().Applied(); got <= before {
		t.Fatalf("catch-up barrier did not advance the FSM: before=%d after=%d", before, got)
	}
}

// TestWaitForFSMCatchupFollowerEventDriven covers the follower path: the
// barrier is forwarded to the leader, and WaitForFSMCatchup blocks on the FSM
// apply-wait tracker until the local FSM applies the barrier's index. The
// wake is driven by the FSM apply notification (replication simulated by a
// direct FSM.Apply of the committed barrier), never by polling. No sleeps.
func TestWaitForFSMCatchupFollowerEventDriven(t *testing.T) {
	t.Parallel()
	r, fsm := newInmemRaft(t, "follower-catchup", false)
	store := raftstore.New(r, fsm, 5*time.Second)
	const leaderIndex = 9
	store.SetForwarder(&appMockForwarder{appliedIndex: leaderIndex})
	rcs := &raftClusterCtlStore{raft: r, raftStore: store}

	done := make(chan error, 1)
	go func() {
		done <- rcs.WaitForFSMCatchup(context.Background(), 5*time.Second, discardLogger())
	}()

	data, err := command.Marshal(command.NewCatchupBarrier())
	if err != nil {
		t.Fatalf("marshal catchup barrier: %v", err)
	}
	fsm.Apply(&hraft.Log{Index: leaderIndex, Data: data})

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitForFSMCatchup (follower): %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForFSMCatchup did not release after local barrier apply")
	}
}

// TestWaitForFSMCatchupFollowerTimeout pins the bounded-timeout behaviour: a
// follower that never applies the forwarded barrier index surfaces an error
// rather than hanging.
func TestWaitForFSMCatchupFollowerTimeout(t *testing.T) {
	t.Parallel()
	r, fsm := newInmemRaft(t, "stuck-follower", false)
	store := raftstore.New(r, fsm, 100*time.Millisecond)
	store.SetForwarder(&appMockForwarder{appliedIndex: 99}) // never applied
	rcs := &raftClusterCtlStore{raft: r, raftStore: store}

	err := rcs.WaitForFSMCatchup(context.Background(), 200*time.Millisecond, discardLogger())
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}
