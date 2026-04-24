package cluster

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Regression test for gastrolog-5oofa: the runWithCtx helper is what
// prevents TierReplicator.send from blocking forever on a paused peer.
// Before this helper was added, send called stream.SendMsg / RecvMsg
// directly with no ctx handling — a SIGSTOPed follower's stream would
// block RecvMsg indefinitely, cascading into an orchestrator-wide
// deadlock. runWithCtx races the blocking operation against ctx.Done,
// so a caller deadline bounds the worst-case wait.

// TestRunWithCtx_ReturnsOnCtxCancel verifies the helper's core contract:
// a long-running fn returns ctx.Err when the context fires first.
func TestRunWithCtx_ReturnsOnCtxCancel(t *testing.T) {
	t.Parallel()

	tr := &TierReplicator{}

	// fn that would run for a "very long time" — model of SendMsg /
	// RecvMsg against a paused peer.
	fn := func() error {
		time.Sleep(5 * time.Second)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := tr.runWithCtx(ctx, fn)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
	// Generous bound: the fn should NOT delay us more than ~25ms past the
	// deadline. If runWithCtx were still blocking on fn, elapsed would be
	// close to 5s.
	if elapsed > 100*time.Millisecond {
		t.Errorf("runWithCtx ignored ctx: elapsed=%v, expected ~50ms", elapsed)
	}
}

// TestRunWithCtx_ReturnsFnResult verifies the happy path: when fn returns
// before ctx fires, the fn's error is returned (not ctx's).
func TestRunWithCtx_ReturnsFnResult(t *testing.T) {
	t.Parallel()

	tr := &TierReplicator{}

	// fn returns a specific sentinel error immediately.
	sentinel := errors.New("fn-specific")
	fn := func() error { return sentinel }

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := tr.runWithCtx(ctx, fn)
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel, got %v", err)
	}
}

// TestRunWithCtx_ZeroDeadline verifies behavior when ctx is already past
// its deadline at call time. runWithCtx should return ctx.Err without
// calling fn (or at most allowing fn a single scheduler tick).
func TestRunWithCtx_ZeroDeadline(t *testing.T) {
	t.Parallel()

	tr := &TierReplicator{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled before call

	fnCalled := false
	fn := func() error {
		fnCalled = true
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	start := time.Now()
	err := tr.runWithCtx(ctx, fn)
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected Canceled, got %v", err)
	}
	if elapsed > 50*time.Millisecond {
		t.Errorf("runWithCtx blocked after ctx was already cancelled: %v", elapsed)
	}
	// fnCalled may be true or false depending on scheduler — the key
	// property is that runWithCtx returned promptly regardless.
	_ = fnCalled
}
