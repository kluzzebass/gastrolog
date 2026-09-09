package raftstore

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// commitCall is one round's commit, captured mid-flight: the context it was
// handed, and the channel the test finishes it through.
type commitCall struct {
	ctx    context.Context
	finish chan error
}

// commitRecorder stands in for the Raft commit a barrier round performs. Each
// round hands back a handle the test completes by hand, so the sequencing is
// driven by the test rather than by timing.
type commitRecorder struct {
	started chan commitCall
}

func newCommitRecorder() *commitRecorder {
	return &commitRecorder{started: make(chan commitCall, 8)}
}

func (c *commitRecorder) commit(ctx context.Context) error {
	call := commitCall{ctx: ctx, finish: make(chan error)}
	c.started <- call
	return <-call.finish
}

// awaitRound blocks until the next round starts and returns its finish channel.
func (c *commitRecorder) awaitRound() chan error {
	return (<-c.started).finish
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
	go g.run(first, commits.commit)
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
	go g.run(first, commits.commit)
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
		(<-commits.started).finish <- nil
		wg.Wait()
	}
}

// TestBarrierGateRoundRunsOnNoCallersContext proves the round is detached from
// whichever caller happened to start it. A round bound to that caller's
// context would fail the moment the caller gave up — and because the same
// context would carry into every round of the handoff chain, it would keep
// rejecting live sessions for the whole busy period.
func TestBarrierGateRoundRunsOnNoCallersContext(t *testing.T) {
	t.Parallel()
	commits := newCommitRecorder()
	var g barrierGate

	starter, cancel := context.WithCancel(context.Background())
	go func() { _ = g.Do(starter, commits.commit) }()
	first := <-commits.started

	// A caller queued behind the starter, so the handoff chain continues past
	// the round the abandoning caller kicked off.
	queued, start := g.join()
	if start {
		t.Fatal("the queued caller should not have started a round")
	}

	cancel()
	if err := first.ctx.Err(); err != nil {
		t.Fatalf("the round died with the caller that started it: %v", err)
	}

	first.finish <- nil
	second := <-commits.started
	if err := second.ctx.Err(); err != nil {
		t.Fatalf("the round after it inherited the same dead context: %v", err)
	}
	second.finish <- nil
	<-queued.done
	if queued.err != nil {
		t.Fatalf("queued caller: %v", queued.err)
	}
}

// TestBarrierGateWaitPrefersAFinishedRound proves a round that has already
// committed is reported as such even when the caller's context expired in the
// same breath — a spurious cancellation here rejects a live session.
func TestBarrierGateWaitPrefersAFinishedRound(t *testing.T) {
	t.Parallel()
	var g barrierGate

	r, _ := g.join()
	close(r.done)

	expired, cancel := context.WithCancel(context.Background())
	cancel()

	if err := g.wait(expired, r); err != nil {
		t.Fatalf("a committed round reported %v", err)
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
	finish := (<-commits.started).finish

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
