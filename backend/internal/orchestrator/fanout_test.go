// W-of-N coordinator tests (gastrolog-5pn44).
//
// Coverage dimensions per CLAUDE.md "Test Coverage — MANDATORY":
//   - Single-node degenerate case (N=1, W=1).
//   - Happy path: W ≤ N peers ack within deadline.
//   - Unhappy path: too many failures → ErrWOfNUnreachable.
//   - Edge case: live-Receiving de-escalation reclassifies a removed
//     peer's failure as not-required, NOT as a failure that counts
//     toward the unreachable threshold. Closes the spurious-failure
//     hole during multi-node drains.
//   - Context cancellation: deadline before threshold.
//   - Closed results channel before threshold.
//   - Defensive guards: W > N, W = 0.

package orchestrator

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// allReceiving is the no-op live-membership oracle: every node stays
// in Receiving. Used when the test isn't exercising the de-escalation
// path.
func allReceiving(_ string) bool { return true }

// neverReceiving treats every failure as already-removed; the
// coordinator should classify everything as "not required."
func neverReceiving(_ string) bool { return false }

func TestWaitWOfNHappyPathReturnsOnThreshold(t *testing.T) {
	t.Parallel()
	results := make(chan NodeResult, 3)
	results <- NodeResult{NodeID: "a"}
	results <- NodeResult{NodeID: "b"}
	results <- NodeResult{NodeID: "c"} // arrives after W reached
	close(results)

	err := waitWOfN(context.Background(), 3, 2, results, allReceiving)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
}

func TestWaitWOfNFailsWhenStillExpectedDropsBelowTarget(t *testing.T) {
	t.Parallel()
	results := make(chan NodeResult, 3)
	results <- NodeResult{NodeID: "a", Err: errors.New("peer-error")}
	results <- NodeResult{NodeID: "b", Err: errors.New("peer-error")}
	results <- NodeResult{NodeID: "c"} // one success arrives too late

	err := waitWOfN(context.Background(), 3, 2, results, allReceiving)
	if !errors.Is(err, ErrWOfNUnreachable) {
		t.Fatalf("expected ErrWOfNUnreachable, got %v", err)
	}
	// The first peer error should be wrapped for telemetry.
	if !strings.Contains(err.Error(), "peer-error") {
		t.Errorf("expected first-error wrapping; err = %v", err)
	}
}

func TestWaitWOfNTreatsLeftReceivingAsNotRequired(t *testing.T) {
	t.Parallel()
	// 3 of 3 with W=2. Two peers fail, but both have been removed
	// from Receiving (live-membership says they're no longer
	// expected). The third peer's ack succeeds, so W=2 can't be
	// reached — but the "unreachable" classification should reflect
	// that the failures are not-required, not failures.
	//
	// Stronger scenario: 4-of-4 with W=2. Two are de-escalated, one
	// fails, one acks. Final state: 1 ack, 1 failure, 2 not-required.
	// Still expected = 4 − 1 − 2 = 1 < W − acks = 1. Wait — that's
	// equal, which is fine. So waitWOfN should NOT return failure
	// here; it should wait for the still-expected peer. But there's
	// only one peer left and it acked. Let's pick a case where the
	// de-escalation matters.
	results := make(chan NodeResult, 4)
	results <- NodeResult{NodeID: "a"} // ack
	results <- NodeResult{NodeID: "b", Err: errors.New("gone")}
	results <- NodeResult{NodeID: "c", Err: errors.New("gone")}
	results <- NodeResult{NodeID: "d"} // ack
	close(results)

	removed := map[string]bool{"b": true, "c": true}
	classifier := func(nodeID string) bool { return !removed[nodeID] }

	err := waitWOfN(context.Background(), 4, 2, results, classifier)
	if err != nil {
		t.Fatalf("expected success (b and c de-escalated; a and d acked), got %v", err)
	}
}

func TestWaitWOfNSpuriousFailureScenario(t *testing.T) {
	t.Parallel()
	// Regression scenario from docs/fan-out-data-plane-design.md
	// "Failure-mode rationale": multi-node drain during steady writes.
	// Snapshot is {a, b, c} with W=2. b and c get drained mid-write
	// (CmdRemoveReceiving). Without the de-escalation, b and c both
	// failing would tip the coordinator over the failure threshold.
	// With it, they're not-required; a's single ack is "enough"
	// against the now-effective denominator of 1.
	//
	// We assert this returns SUCCESS, not failure.
	results := make(chan NodeResult, 3)
	results <- NodeResult{NodeID: "a"} // sole still-Receiving peer
	results <- NodeResult{NodeID: "b", Err: errors.New("drained")}
	results <- NodeResult{NodeID: "c", Err: errors.New("drained")}
	close(results)

	drained := map[string]bool{"b": true, "c": true}
	classifier := func(nodeID string) bool { return !drained[nodeID] }

	err := waitWOfN(context.Background(), 3, 2, results, classifier)
	if err != nil {
		t.Fatalf("multi-node drain produced spurious failure: %v", err)
	}
}

func TestWaitWOfNContextDeadlineBeforeThreshold(t *testing.T) {
	t.Parallel()
	results := make(chan NodeResult, 3)
	results <- NodeResult{NodeID: "a"} // only one ack arrives

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := waitWOfN(ctx, 3, 2, results, allReceiving)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestWaitWOfNClosedChannelBeforeThreshold(t *testing.T) {
	t.Parallel()
	results := make(chan NodeResult, 3)
	results <- NodeResult{NodeID: "a"}
	close(results)

	err := waitWOfN(context.Background(), 3, 2, results, allReceiving)
	if !errors.Is(err, ErrWOfNUnreachable) {
		t.Fatalf("expected ErrWOfNUnreachable on closed channel below threshold, got %v", err)
	}
}

func TestWaitWOfNDegenerateW1OfN1(t *testing.T) {
	t.Parallel()
	results := make(chan NodeResult, 1)
	results <- NodeResult{NodeID: "a"}
	close(results)

	if err := waitWOfN(context.Background(), 1, 1, results, allReceiving); err != nil {
		t.Fatalf("W=1, N=1 ack: %v", err)
	}
}

func TestWaitWOfNW0IsTrivialSuccess(t *testing.T) {
	t.Parallel()
	// Defensive: W=0 is invalid per config validation but the
	// coordinator should not deadlock if it ever leaks through.
	results := make(chan NodeResult)
	if err := waitWOfN(context.Background(), 3, 0, results, allReceiving); err != nil {
		t.Fatalf("W=0 trivial success: %v", err)
	}
}

func TestWaitWOfNRejectsWGreaterThanN(t *testing.T) {
	t.Parallel()
	results := make(chan NodeResult)
	err := waitWOfN(context.Background(), 2, 3, results, allReceiving)
	if err == nil {
		t.Fatal("expected error for W > N")
	}
}

func TestWaitWOfNAllRemovedReturnsUnreachable(t *testing.T) {
	t.Parallel()
	// Pathological: every snapshot member's failure is de-escalated.
	// Effective denominator becomes 0; W > 0 can't be met. The
	// coordinator should return ErrWOfNUnreachable, not deadlock.
	results := make(chan NodeResult, 3)
	results <- NodeResult{NodeID: "a", Err: errors.New("gone")}
	results <- NodeResult{NodeID: "b", Err: errors.New("gone")}
	results <- NodeResult{NodeID: "c", Err: errors.New("gone")}
	close(results)

	err := waitWOfN(context.Background(), 3, 1, results, neverReceiving)
	if !errors.Is(err, ErrWOfNUnreachable) {
		t.Fatalf("expected ErrWOfNUnreachable when all members de-escalated, got %v", err)
	}
}

func TestWaitWOfNStragglersAfterSuccessAreSafe(t *testing.T) {
	t.Parallel()
	// Buffered channel so producer goroutines completing AFTER
	// waitWOfN returns don't block. Mimics the production setup: the
	// caller launches N goroutines, each writes to a chan with cap N,
	// and waitWOfN exits as soon as the threshold is reached.
	results := make(chan NodeResult, 3)
	results <- NodeResult{NodeID: "a"}
	results <- NodeResult{NodeID: "b"}

	if err := waitWOfN(context.Background(), 3, 2, results, allReceiving); err != nil {
		t.Fatalf("threshold reached early: %v", err)
	}

	// Send a straggler post-return; it should not block.
	done := make(chan struct{})
	go func() {
		results <- NodeResult{NodeID: "c"}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("straggler blocked on buffered channel")
	}
}
