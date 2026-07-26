package raftstore

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	"gastrolog/internal/system/command"
	"gastrolog/internal/system/raftfsm"
	"gastrolog/internal/system/storetest"

	hraft "github.com/hashicorp/raft"
)

// testMaxAge satisfies RotationPolicyConfig's requirement for at least one
// rotation rule in the apply-wait tests below.
var testMaxAge = "1m"

// newTestRaft creates a single-node in-memory raft instance that becomes
// leader immediately. No cluster, no network — just raft's log + FSM
// machinery for persistence testing.
func newTestRaft(t *testing.T) (*hraft.Raft, *raftfsm.FSM) {
	t.Helper()

	fsm := raftfsm.New()

	conf := hraft.DefaultConfig()
	conf.LocalID = "test-node"
	conf.LogOutput = io.Discard
	// Tight timeouts so single-node election is near-instant.
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond

	logStore := hraft.NewInmemStore()
	stableStore := hraft.NewInmemStore()
	snapStore := hraft.NewInmemSnapshotStore()
	_, transport := hraft.NewInmemTransport("test-node")

	r, err := hraft.NewRaft(conf, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}
	t.Cleanup(func() {
		future := r.Shutdown()
		if err := future.Error(); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})

	// Bootstrap as single voter so this node becomes leader.
	boot := hraft.Configuration{
		Servers: []hraft.Server{
			{ID: "test-node", Address: transport.LocalAddr()},
		},
	}
	if err := r.BootstrapCluster(boot).Error(); err != nil {
		t.Fatalf("BootstrapCluster: %v", err)
	}

	// Wait for leadership.
	select {
	case <-r.LeaderCh():
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for leadership")
	}

	return r, fsm
}

func TestConformance(t *testing.T) {
	t.Parallel()
	storetest.TestStore(t, func(t *testing.T) system.Store {
		r, fsm := newTestRaft(t)
		return New(r, fsm, 5*time.Second)
	})
}

func TestApplyBadData(t *testing.T) {
	t.Parallel()
	r, fsm := newTestRaft(t)
	s := New(r, fsm, 5*time.Second)

	// Apply garbage through raft — FSM returns an unmarshal error
	// which surfaces via future.Response().
	future := s.raft.Apply([]byte("not a valid protobuf"), s.applyTimeout)
	if err := future.Error(); err != nil {
		t.Fatalf("unexpected raft-level error: %v", err)
	}
	resp := future.Response()
	if resp == nil {
		t.Fatal("expected error response from FSM, got nil")
	}
	if _, ok := resp.(error); !ok {
		t.Fatalf("expected error, got %T: %v", resp, resp)
	}
}

// mockForwarder records Forward calls for testing.
type mockForwarder struct {
	called       bool
	data         []byte
	appliedIndex uint64
	err          error
}

func (m *mockForwarder) Forward(ctx context.Context, data []byte) (uint64, error) {
	m.called = true
	m.data = data
	return m.appliedIndex, m.err
}

func TestApplyForwardsOnNotLeader(t *testing.T) {
	t.Parallel()
	// Create a raft instance that is NOT the leader: bootstrap but
	// immediately add a second non-existent node so this node steps down.
	// Simpler approach: create a non-bootstrapped raft that returns ErrNotLeader.
	fsm := raftfsm.New()

	conf := hraft.DefaultConfig()
	conf.LocalID = "follower"
	conf.LogOutput = io.Discard
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond

	logStore := hraft.NewInmemStore()
	stableStore := hraft.NewInmemStore()
	snapStore := hraft.NewInmemSnapshotStore()
	_, transport := hraft.NewInmemTransport("follower")

	r, err := hraft.NewRaft(conf, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}
	t.Cleanup(func() { _ = r.Shutdown().Error() })

	// Don't bootstrap — this node has no leader, so Apply returns ErrNotLeader.

	s := New(r, fsm, 5*time.Second)
	fwd := &mockForwarder{}
	s.SetForwarder(fwd)

	// ApplyRaw should detect ErrNotLeader and forward.
	testData := []byte("test-command-data")
	_, err = s.ApplyRaw(testData)
	if err != nil {
		t.Fatalf("ApplyRaw returned error: %v", err)
	}
	if !fwd.called {
		t.Fatal("forwarder was not called")
	}
	if string(fwd.data) != string(testData) {
		t.Errorf("forwarder got %q, want %q", fwd.data, testData)
	}
}

func TestApplyNoForwarderReturnsError(t *testing.T) {
	t.Parallel()
	// Non-bootstrapped raft, no forwarder set.
	fsm := raftfsm.New()

	conf := hraft.DefaultConfig()
	conf.LocalID = "follower"
	conf.LogOutput = io.Discard
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond

	logStore := hraft.NewInmemStore()
	stableStore := hraft.NewInmemStore()
	snapStore := hraft.NewInmemSnapshotStore()
	_, transport := hraft.NewInmemTransport("follower")

	r, err := hraft.NewRaft(conf, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}
	t.Cleanup(func() { _ = r.Shutdown().Error() })

	s := New(r, fsm, 5*time.Second)
	// No forwarder set.

	_, err = s.ApplyRaw([]byte("test-command-data"))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// The error should contain "not leader" (wrapped by "raft apply: ...").
	if got := err.Error(); got == "" {
		t.Fatal("expected non-empty error")
	}
}

func TestApplyForwarderError(t *testing.T) {
	t.Parallel()
	fsm := raftfsm.New()

	conf := hraft.DefaultConfig()
	conf.LocalID = "follower"
	conf.LogOutput = io.Discard
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond

	logStore := hraft.NewInmemStore()
	stableStore := hraft.NewInmemStore()
	snapStore := hraft.NewInmemSnapshotStore()
	_, transport := hraft.NewInmemTransport("follower")

	r, err := hraft.NewRaft(conf, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}
	t.Cleanup(func() { _ = r.Shutdown().Error() })

	s := New(r, fsm, 5*time.Second)
	fwdErr := errors.New("leader unreachable")
	s.SetForwarder(&mockForwarder{err: fwdErr})

	_, err = s.ApplyRaw([]byte("test-data"))
	if !errors.Is(err, fwdErr) {
		t.Fatalf("expected forwarder error, got: %v", err)
	}
}

// TestApplyReturnsAppliedIndexOnLeader pins that the leader path returns the
// raft log index from the future, not zero. The mutation handlers in
// SystemServer don't read this index, but the cluster ForwardApply handler
// does (returning it to the forwarding follower) — gastrolog-2nxij.
func TestApplyReturnsAppliedIndexOnLeader(t *testing.T) {
	t.Parallel()
	r, fsm := newTestRaft(t)
	s := New(r, fsm, 5*time.Second)

	// Apply a raw byte payload — FSM will reject deserialization, but
	// future.Index() is set on the resp side regardless. The goal of this
	// test is to pin that the leader path returns a non-zero index, which
	// is what the cluster ForwardApply handler ships back to followers.
	idx, _ := s.ApplyRaw([]byte("noop-command"))
	if idx == 0 {
		t.Errorf("expected non-zero applied index on leader path, got 0")
	}
}

// TestApplyWaitsForLocalApplyAfterForward is the regression test for
// gastrolog-2nxij. After Forward returns the leader's applied index, the
// follower's applyRaw must block until its local raft.AppliedIndex catches
// up. The mock forwarder reports an index in the future; the test asserts
// applyRaw times out rather than returning while still behind.
//
// This proves the wait loop is wired and bounded, without needing a real
// two-node cluster (which the in-process harness doesn't simulate).
func TestApplyWaitsForLocalApplyAfterForward(t *testing.T) {
	t.Parallel()
	fsm := raftfsm.New()

	conf := hraft.DefaultConfig()
	conf.LocalID = "follower"
	conf.LogOutput = io.Discard
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond

	logStore := hraft.NewInmemStore()
	stableStore := hraft.NewInmemStore()
	snapStore := hraft.NewInmemSnapshotStore()
	_, transport := hraft.NewInmemTransport("follower")

	r, err := hraft.NewRaft(conf, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}
	t.Cleanup(func() { _ = r.Shutdown().Error() })

	// Local AppliedIndex starts at 0; mock forwarder reports the leader
	// applied at index 99 — which the follower will never reach because
	// it has no leader.
	s := New(r, fsm, 100*time.Millisecond)
	s.SetForwarder(&mockForwarder{appliedIndex: 99})

	start := time.Now()
	_, err = s.ApplyRaw([]byte("forwarded-command"))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected wait-for-local-apply timeout, got nil error")
	}
	if !contains(err.Error(), "wait for local FSM apply") {
		t.Errorf("expected wait-for-local-apply timeout, got: %v", err)
	}
	if elapsed < 50*time.Millisecond {
		t.Errorf("returned too fast (%s), wait loop appears bypassed", elapsed)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("took too long (%s), timeout bound not enforced", elapsed)
	}
}

// TestApplyReturnsImmediatelyOnZeroAppliedIndex covers the edge case where
// the leader's response carries applied_index=0 (e.g. legacy peer that
// hasn't been upgraded, or a synthetic command). The follower must not
// wait — there's nothing meaningful to wait for. Returning immediately
// preserves backward compatibility during a rolling upgrade.
func TestApplyReturnsImmediatelyOnZeroAppliedIndex(t *testing.T) {
	t.Parallel()
	fsm := raftfsm.New()

	conf := hraft.DefaultConfig()
	conf.LocalID = "follower"
	conf.LogOutput = io.Discard
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond

	logStore := hraft.NewInmemStore()
	stableStore := hraft.NewInmemStore()
	snapStore := hraft.NewInmemSnapshotStore()
	_, transport := hraft.NewInmemTransport("follower")

	r, err := hraft.NewRaft(conf, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}
	t.Cleanup(func() { _ = r.Shutdown().Error() })

	s := New(r, fsm, 5*time.Second)
	s.SetForwarder(&mockForwarder{appliedIndex: 0})

	start := time.Now()
	_, err = s.ApplyRaw([]byte("forwarded-command"))
	if err != nil {
		t.Fatalf("ApplyRaw unexpectedly errored: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("wait fired despite applied_index=0 (took %s)", elapsed)
	}
}

// newFollowerRaft creates a non-bootstrapped raft instance that always
// returns ErrNotLeader from Apply, for exercising the forward path.
func newFollowerRaft(t *testing.T, fsm *raftfsm.FSM) *hraft.Raft {
	t.Helper()

	conf := hraft.DefaultConfig()
	conf.LocalID = "follower"
	conf.LogOutput = io.Discard
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond

	logStore := hraft.NewInmemStore()
	stableStore := hraft.NewInmemStore()
	snapStore := hraft.NewInmemSnapshotStore()
	_, transport := hraft.NewInmemTransport("follower")

	r, err := hraft.NewRaft(conf, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}
	t.Cleanup(func() { _ = r.Shutdown().Error() })
	return r
}

// TestApplyWakesOnLocalFSMApply is the event-driven regression test for
// gastrolog-3klg1: the post-forward wait must release the moment the local
// FSM applies the leader's index — driven by the FSM apply notification,
// not by polling raft.AppliedIndex. The test simulates replication by
// applying the committed entry to the FSM directly; the concurrent write
// call must then return with the mutation locally readable. No sleeps —
// synchronization is the barrier's own completion.
func TestApplyWakesOnLocalFSMApply(t *testing.T) {
	t.Parallel()
	fsm := raftfsm.New()
	r := newFollowerRaft(t, fsm)

	s := New(r, fsm, 5*time.Second)
	const leaderIndex = 7
	s.SetForwarder(&mockForwarder{appliedIndex: leaderIndex})

	ctx := context.Background()
	cfg := system.RotationPolicyConfig{ID: glid.New(), Name: "event-driven-probe", MaxAge: &testMaxAge}

	done := make(chan error, 1)
	go func() { done <- s.PutRotationPolicy(ctx, cfg) }()

	// Simulate the follower's raft delivering the committed entry to the
	// FSM. Whether this lands before or after the waiter registers, the
	// tracker guarantees the wake.
	data, err := command.Marshal(command.NewPutRotationPolicy(cfg))
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	fsm.Apply(&hraft.Log{Index: leaderIndex, Data: data})

	if err := <-done; err != nil {
		t.Fatalf("PutRotationPolicy via forward: %v", err)
	}
	got, err := s.GetRotationPolicy(ctx, cfg.ID)
	if err != nil {
		t.Fatalf("GetRotationPolicy after barrier released: %v", err)
	}
	if got == nil {
		t.Fatal("barrier released before the local FSM reflected the write")
	}
}

// TestApplyReturnsImmediatelyWhenAlreadyApplied covers the no-op forward:
// the local FSM already applied the leader's index (e.g. replication beat
// the forward response), so the barrier must not block at all.
func TestApplyReturnsImmediatelyWhenAlreadyApplied(t *testing.T) {
	t.Parallel()
	fsm := raftfsm.New()
	r := newFollowerRaft(t, fsm)

	cfg := system.RotationPolicyConfig{ID: glid.New(), Name: "pre-applied-probe", MaxAge: &testMaxAge}
	data, err := command.Marshal(command.NewPutRotationPolicy(cfg))
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	const leaderIndex = 3
	fsm.Apply(&hraft.Log{Index: leaderIndex, Data: data})

	// applyTimeout is irrelevant here — the wait must not engage. A hang
	// would trip the test binary timeout, not a flaky bound.
	s := New(r, fsm, 5*time.Second)
	s.SetForwarder(&mockForwarder{appliedIndex: leaderIndex})

	if _, err := s.ApplyRaw(data); err != nil {
		t.Fatalf("ApplyRaw with already-applied index: %v", err)
	}
}

// TestApplyWaitCancelledContext pins ctx cancellation semantics: a caller
// cancelling mid-wait gets ctx.Err(), not the timeout error.
func TestApplyWaitCancelledContext(t *testing.T) {
	t.Parallel()
	fsm := raftfsm.New()
	r := newFollowerRaft(t, fsm)

	s := New(r, fsm, 5*time.Second)
	s.SetForwarder(&mockForwarder{appliedIndex: 42}) // never applied locally

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.applyRaw(ctx, []byte("forwarded-command"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("applyRaw with cancelled ctx = %v, want context.Canceled", err)
	}
}

// TestBarrierOnLeaderApplies pins the leader path of the startup catch-up
// barrier (gastrolog-1go57): on the leader raft.Apply is synchronous, so
// Barrier commits a no-op entry and returns with the FSM current. The
// apply-wait tracker must reflect the barrier's index on return.
func TestBarrierOnLeaderApplies(t *testing.T) {
	t.Parallel()
	r, fsm := newTestRaft(t)
	s := New(r, fsm, 5*time.Second)

	before := fsm.ApplyWait().Applied()
	if err := s.Barrier(context.Background()); err != nil {
		t.Fatalf("Barrier on leader: %v", err)
	}
	if got := fsm.ApplyWait().Applied(); got <= before {
		t.Fatalf("apply-wait tracker did not advance: before=%d after=%d", before, got)
	}
}

// TestBarrierFollowerWakesOnLocalApply is the event-driven follower path
// (gastrolog-1go57): the barrier is forwarded to the leader and Barrier
// blocks until the local FSM applies up to the leader's index — released by
// the FSM apply notification, not a poll. Replication is simulated by
// applying the committed barrier entry to the FSM directly; no sleeps.
func TestBarrierFollowerWakesOnLocalApply(t *testing.T) {
	t.Parallel()
	fsm := raftfsm.New()
	r := newFollowerRaft(t, fsm)

	const leaderIndex = 11
	s := New(r, fsm, 5*time.Second)
	s.SetForwarder(&mockForwarder{appliedIndex: leaderIndex})

	done := make(chan error, 1)
	go func() { done <- s.Barrier(context.Background()) }()

	// The follower's raft delivers the committed barrier entry to the FSM.
	// Whether this lands before or after the waiter registers, the tracker
	// guarantees the wake.
	data, err := command.Marshal(command.NewCatchupBarrier())
	if err != nil {
		t.Fatalf("marshal catchup barrier: %v", err)
	}
	fsm.Apply(&hraft.Log{Index: leaderIndex, Data: data})

	if err := <-done; err != nil {
		t.Fatalf("Barrier via forward: %v", err)
	}
}

// TestBarrierFollowerTimeout pins that a follower that never catches up to
// the forwarded barrier index surfaces a bounded timeout error, not a hang.
func TestBarrierFollowerTimeout(t *testing.T) {
	t.Parallel()
	fsm := raftfsm.New()
	r := newFollowerRaft(t, fsm)

	s := New(r, fsm, 100*time.Millisecond)
	s.SetForwarder(&mockForwarder{appliedIndex: 99}) // never applied locally

	start := time.Now()
	err := s.Barrier(context.Background())
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected timeout, got nil")
	}
	if !contains(err.Error(), "wait for local FSM apply") {
		t.Errorf("expected wait-for-local-apply timeout, got: %v", err)
	}
	if elapsed < 50*time.Millisecond {
		t.Errorf("returned too fast (%s), wait loop appears bypassed", elapsed)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("took too long (%s), timeout bound not enforced", elapsed)
	}
}

// TestBarrierFollowerCancelled pins ctx cancellation: a caller cancelling
// mid-wait gets ctx.Err(), not the timeout error.
func TestBarrierFollowerCancelled(t *testing.T) {
	t.Parallel()
	fsm := raftfsm.New()
	r := newFollowerRaft(t, fsm)

	s := New(r, fsm, 5*time.Second)
	s.SetForwarder(&mockForwarder{appliedIndex: 42}) // never applied locally

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Barrier(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Barrier with cancelled ctx = %v, want context.Canceled", err)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
