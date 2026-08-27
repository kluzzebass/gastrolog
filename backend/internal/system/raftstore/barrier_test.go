package raftstore

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// commitRecorder stands in for the Raft commit a barrier round performs. Each
// round hands back a channel the test completes by hand, so the sequencing is
// driven by the test rather than by timing.
type commitRecorder struct {
	started chan chan error
}

func newCommitRecorder() *commitRecorder {
	return &commitRecorder{started: make(chan chan error, 8)}
}

func (c *commitRecorder) commit(ctx context.Context) error {
	finish := make(chan error)
	c.started <- finish
	return <-finish
}

// awaitRound blocks until the next round starts and returns its handle.
func (c *commitRecorder) awaitRound() chan error {
	return <-c.started
}

// noRoundStarted reports that nothing has begun committing. Sound only at a
// point where the gate cannot be about to start one — the caller establishes
// that, the channel peek only confirms it.
func (c *commitRecorder) noRoundStarted() bool {
	return len(c.started) == 0
}

// TestBarrierGateCoalescesConcurrentCallers proves the two properties the
// bound rests on: callers arriving during one round share a single commit
// between them, and they wait for the round that follows rather than the one
// already running — which began before they read and so proves nothing.
func TestBarrierGateCoalescesConcurrentCallers(t *testing.T) {
	t.Parallel()
	commits := newCommitRecorder()
	var g barrierGate

	first, start := g.join()
	if !start {
		t.Fatal("the first caller must start a round")
	}
	go g.run(context.Background(), first, commits.commit)
	firstFinish := commits.awaitRound()

	// Everyone arriving while that round is in flight joins the next one.
	const arrivals = 5
	rounds := make([]*barrierRound, arrivals)
	for i := range arrivals {
		r, start := g.join()
		if start {
			t.Fatalf("caller %d started a round while one was in flight", i)
		}
		if r == first {
			t.Fatalf("caller %d joined a round that began before it arrived", i)
		}
		rounds[i] = r
	}
	for i, r := range rounds {
		if r != rounds[0] {
			t.Fatalf("caller %d got its own round; concurrent callers must share one", i)
		}
	}

	if !commits.noRoundStarted() {
		t.Fatal("the shared round must not commit until the one in flight finishes")
	}

	firstFinish <- nil
	<-first.done

	// Finishing the first round starts exactly one more, for all five.
	secondFinish := commits.awaitRound()
	secondFinish <- nil
	<-rounds[0].done

	if !commits.noRoundStarted() {
		t.Fatal("five coalesced callers should have cost exactly one further commit")
	}
	for i, r := range rounds {
		if r.err != nil {
			t.Fatalf("caller %d: %v", i, r.err)
		}
	}
}

// TestBarrierGateSharesTheRoundsError proves a coalesced caller is told what
// happened rather than being handed a silent success.
func TestBarrierGateSharesTheRoundsError(t *testing.T) {
	t.Parallel()
	commits := newCommitRecorder()
	var g barrierGate

	first, _ := g.join()
	go g.run(context.Background(), first, commits.commit)
	firstFinish := commits.awaitRound()

	joined, _ := g.join()
	firstFinish <- nil
	<-first.done

	want := errors.New("no known leader")
	commits.awaitRound() <- want
	<-joined.done

	if !errors.Is(joined.err, want) {
		t.Fatalf("coalesced caller got %v, want %v", joined.err, want)
	}
}

// TestBarrierGateRunsAgainAfterQuiescing proves the gate does not latch: once
// the queue drains, the next caller starts a fresh round of its own.
func TestBarrierGateRunsAgainAfterQuiescing(t *testing.T) {
	t.Parallel()
	commits := newCommitRecorder()
	var g barrierGate
	ctx := context.Background()

	for round := range 3 {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := g.Do(ctx, commits.commit); err != nil {
				t.Errorf("round %d: %v", round, err)
			}
		}()
		(<-commits.started) <- nil
		wg.Wait()
	}
}

// TestBarrierGateCallerGivesUpWithoutCancellingTheRound proves one caller's
// deadline does not take the shared round down with it.
func TestBarrierGateCallerGivesUpWithoutCancellingTheRound(t *testing.T) {
	t.Parallel()
	commits := newCommitRecorder()
	var g barrierGate

	abandoned, cancel := context.WithCancel(context.Background())
	gaveUp := make(chan error, 1)
	go func() { gaveUp <- g.Do(abandoned, commits.commit) }()
	finish := <-commits.started

	cancel()
	if err := <-gaveUp; !errors.Is(err, context.Canceled) {
		t.Fatalf("abandoning caller got %v, want context.Canceled", err)
	}

	// The round is still live for everyone else.
	joined, start := g.join()
	if start {
		t.Fatal("the round should still be running")
	}
	finish <- nil
	commits.awaitRound() <- nil
	<-joined.done
	if joined.err != nil {
		t.Fatalf("remaining caller: %v", joined.err)
	}
}
