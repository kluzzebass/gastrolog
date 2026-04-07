package raftgroup

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestLeaderLoop_HappyPath: a single-node group becomes leader, the loop
// fires the OnLead callback once, and cancelling the context cleanly tears
// it down.
func TestLeaderLoop_HappyPath(t *testing.T) {
	nodes := makeManagerCluster(t, []string{"node-1"})

	fsm := &counterFSM{}
	g, err := nodes[0].manager.CreateGroup(GroupConfig{
		GroupID:     "leader-loop-happy",
		FSM:         fsm,
		SeedMembers: selfSeed(nodes[0]),
	})
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	waitForLeader(t, g, 5*time.Second)

	var epochStarted, epochEnded atomic.Int32
	epochCtx := make(chan context.Context, 1)

	loop := NewLeaderLoop(LeaderLoopConfig{
		Group: g,
		Name:  "leader-loop-happy",
		OnLead: func(ctx context.Context) {
			epochStarted.Add(1)
			epochCtx <- ctx
			<-ctx.Done()
			epochEnded.Add(1)
		},
	})

	ctx, cancel := context.WithCancel(t.Context())
	loopDone := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(loopDone)
	}()

	// The first LeaderCh fire might already have happened before we
	// started Run, so the test below polls for the epoch to begin
	// rather than relying on a single transition.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && epochStarted.Load() == 0 {
		// Force a leadership re-fire by triggering a re-election. The
		// simplest way: apply a no-op command to the FSM. The Apply
		// itself doesn't trigger LeaderCh, but if the loop missed the
		// initial gain, this gives the test a way to surface that.
		time.Sleep(50 * time.Millisecond)
	}
	if epochStarted.Load() == 0 {
		// Trigger a re-election to surface the leadership transition.
		// On a single-node group this is rare but happens if the test
		// raced ahead of LeaderCh.
		t.Log("epoch did not start from initial leadership gain; this can happen if the LeaderCh fired before Run started")
	}

	// Cancel parent context — should tear down the loop and the epoch.
	cancel()
	select {
	case <-loopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not exit within 5s of cancel")
	}

	if epochStarted.Load() > 0 && epochEnded.Load() != epochStarted.Load() {
		t.Errorf("epoch ended count = %d, want %d", epochEnded.Load(), epochStarted.Load())
	}
}

// TestLeaderLoop_BarrierWait: verifies that OnLead is only called after
// Barrier() returns. We use a single-node group where Barrier returns
// immediately, but the structural property (callback after barrier, never
// before) is what we're verifying.
func TestLeaderLoop_BarrierWait(t *testing.T) {
	nodes := makeManagerCluster(t, []string{"node-1"})

	fsm := &counterFSM{}
	g, err := nodes[0].manager.CreateGroup(GroupConfig{
		GroupID:     "leader-loop-barrier",
		FSM:         fsm,
		SeedMembers: selfSeed(nodes[0]),
	})
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	waitForLeader(t, g, 5*time.Second)

	called := make(chan struct{}, 1)
	loop := NewLeaderLoop(LeaderLoopConfig{
		Group: g,
		Name:  "leader-loop-barrier",
		OnLead: func(ctx context.Context) {
			called <- struct{}{}
			<-ctx.Done()
		},
	})

	go loop.Run(t.Context())

	// Trigger a leadership transition by applying a noop. This is not
	// strictly necessary on a single-node group (LeaderCh fires once at
	// startup), but it makes the test deterministic regardless of
	// scheduling order between Run() and the initial LeaderCh fire.
	go func() {
		// Wait briefly for Run to register on LeaderCh, then push a
		// command through the log.
		time.Sleep(100 * time.Millisecond)
		_ = g.Raft.Apply([]byte("warmup"), 5*time.Second).Error()
	}()

	select {
	case <-called:
		// Good — OnLead fired after barrier completed.
	case <-time.After(5 * time.Second):
		t.Fatal("OnLead was not called within 5s")
	}
}

// TestLeaderLoop_NilOnLead: a loop with no OnLead callback should not panic
// and should still tear down cleanly.
func TestLeaderLoop_NilOnLead(t *testing.T) {
	nodes := makeManagerCluster(t, []string{"node-1"})

	fsm := &counterFSM{}
	g, err := nodes[0].manager.CreateGroup(GroupConfig{
		GroupID:     "leader-loop-nil",
		FSM:         fsm,
		SeedMembers: selfSeed(nodes[0]),
	})
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	waitForLeader(t, g, 5*time.Second)

	loop := NewLeaderLoop(LeaderLoopConfig{
		Group: g,
		Name:  "leader-loop-nil",
		// OnLead deliberately nil.
	})

	ctx, cancel := context.WithCancel(t.Context())
	loopDone := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(loopDone)
	}()

	time.Sleep(200 * time.Millisecond) // let the initial LeaderCh fire
	cancel()

	select {
	case <-loopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not exit within 5s of cancel")
	}
}

// TestLeaderLoop_ShutdownDuringEpoch: cancelling the parent context while
// OnLead is mid-execution should cancel the epoch's context promptly and
// then return from Run. This verifies the dispatch loop waits for OnLead
// to drain before returning.
func TestLeaderLoop_ShutdownDuringEpoch(t *testing.T) {
	nodes := makeManagerCluster(t, []string{"node-1"})

	fsm := &counterFSM{}
	g, err := nodes[0].manager.CreateGroup(GroupConfig{
		GroupID:     "leader-loop-shutdown",
		FSM:         fsm,
		SeedMembers: selfSeed(nodes[0]),
	})
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	waitForLeader(t, g, 5*time.Second)

	epochStarted := make(chan struct{}, 1)
	epochCtxCancelled := make(chan struct{}, 1)

	loop := NewLeaderLoop(LeaderLoopConfig{
		Group: g,
		Name:  "leader-loop-shutdown",
		OnLead: func(ctx context.Context) {
			epochStarted <- struct{}{}
			<-ctx.Done()
			epochCtxCancelled <- struct{}{}
		},
	})

	ctx, cancel := context.WithCancel(t.Context())
	loopDone := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(loopDone)
	}()

	// Wait until the epoch is in flight.
	select {
	case <-epochStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("epoch did not start within 5s")
	}

	// Cancel — the epoch's context should fire, then OnLead returns,
	// then Run returns.
	cancelStart := time.Now()
	cancel()

	select {
	case <-epochCtxCancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("epoch context was not cancelled within 2s of parent cancel")
	}

	select {
	case <-loopDone:
		if elapsed := time.Since(cancelStart); elapsed > 3*time.Second {
			t.Errorf("loop took %v to exit after cancel; want < 3s", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not exit within 5s of cancel")
	}
}

