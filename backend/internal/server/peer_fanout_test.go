package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

// TestPeerFanOutHonorsPerPeerTimeout pins the gastrolog-csspr invariant
// that one paused peer cannot block the whole inspector handler. A peer
// whose fn never returns must be elided within peerInspectorTimeout
// while healthy peers' results land normally.
//
// The "paused" peer simulates SIGSTOP on a real node: its TCP connection
// stays open, gRPC keepalive doesn't fire for many minutes, and the
// callee never receives anything. We model that as a fn that blocks on
// its peer-context's Done channel — the bounded context delivers Done
// when the timeout fires, and the goroutine returns the ctx error.
func TestPeerFanOutHonorsPerPeerTimeout(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	nodes := []string{"node-fast-1", "node-paused", "node-fast-2"}

	// "Paused" peer: blocks on its peer-context until the per-peer
	// timeout cancels it.
	var pausedReturned atomic.Int32
	fn := func(peerCtx context.Context, nodeID string) (string, error) {
		if nodeID == "node-paused" {
			<-peerCtx.Done()
			pausedReturned.Add(1)
			return "", peerCtx.Err()
		}
		return "ok-" + nodeID, nil
	}

	start := time.Now()
	results, ok := peerFanOut(context.Background(), logger, "test", nodes, fn)
	elapsed := time.Since(start)

	// The whole call should finish within peerInspectorTimeout + a small
	// scheduler-jitter margin. "Sequential blocking" (the bug) would
	// take ~3× peerInspectorTimeout because each peer would wait its
	// own timeout in series; "no timeout at all" would hang forever.
	maxAllowed := peerInspectorTimeout + 1*time.Second
	if elapsed > maxAllowed {
		t.Errorf("fan-out took %v, want <= %v (paused peer should not delay healthy peers)",
			elapsed, maxAllowed)
	}

	// The two fast peers must have succeeded; the paused one must be
	// elided (ok=false) but the slot must exist (results length matches
	// nodes). The exact identity check pins that node-order is preserved.
	if len(results) != len(nodes) || len(ok) != len(nodes) {
		t.Fatalf("results/ok length = (%d, %d), want both %d", len(results), len(ok), len(nodes))
	}
	if !ok[0] || results[0] != "ok-node-fast-1" {
		t.Errorf("node-fast-1: ok=%v, value=%q; want true, ok-node-fast-1", ok[0], results[0])
	}
	if ok[1] {
		t.Errorf("node-paused: ok=true, want false (paused peer must be elided)")
	}
	if !ok[2] || results[2] != "ok-node-fast-2" {
		t.Errorf("node-fast-2: ok=%v, value=%q; want true, ok-node-fast-2", ok[2], results[2])
	}

	// The paused-peer goroutine must have observed the timeout.
	if pausedReturned.Load() != 1 {
		t.Errorf("paused peer goroutine returned %d times, want 1 (timeout should have cancelled it)",
			pausedReturned.Load())
	}
}

// TestPeerFanOutPreservesNodeOrder pins that results[i] corresponds to
// nodes[i]. Inspector handlers rely on this to pair successful results
// with their reporting node ID for the merged-view dedup pass — losing
// that correspondence would break replica-residency tracking.
func TestPeerFanOutPreservesNodeOrder(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	nodes := []string{"alpha", "bravo", "charlie", "delta"}

	fn := func(_ context.Context, nodeID string) (string, error) {
		// Random-ish "work" so goroutines complete out of order. Each
		// returns its own node ID so any cross-wire shows up immediately.
		switch nodeID {
		case "alpha":
			time.Sleep(30 * time.Millisecond)
		case "bravo":
			time.Sleep(5 * time.Millisecond)
		case "charlie":
			time.Sleep(20 * time.Millisecond)
		case "delta":
			time.Sleep(10 * time.Millisecond)
		}
		return nodeID, nil
	}

	results, ok := peerFanOut(context.Background(), logger, "test", nodes, fn)

	for i, n := range nodes {
		if !ok[i] {
			t.Errorf("nodes[%d] = %q: ok=false, want true", i, n)
			continue
		}
		if results[i] != n {
			t.Errorf("nodes[%d] = %q: results[%d] = %q (out-of-order)", i, n, i, results[i])
		}
	}
}

// TestPeerFanOutErroringPeerIsElided pins that fn returning an error
// is treated identically to fn timing out: the peer is elided (ok=false)
// without affecting siblings or the helper's overall return.
func TestPeerFanOutErroringPeerIsElided(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	nodes := []string{"good", "bad"}
	wantErr := errors.New("simulated remote failure")

	fn := func(_ context.Context, nodeID string) (string, error) {
		if nodeID == "bad" {
			return "", wantErr
		}
		return "ok", nil
	}

	results, ok := peerFanOut(context.Background(), logger, "test", nodes, fn)

	if !ok[0] || results[0] != "ok" {
		t.Errorf("good: ok=%v, value=%q; want true, ok", ok[0], results[0])
	}
	if ok[1] {
		t.Errorf("bad: ok=true, want false (errored peer must be elided)")
	}
}

// TestPeerFanOutEmptyNodes pins the no-fan-out case. Empty node list
// must short-circuit without spawning goroutines and return empty
// parallel slices that the caller can range over harmlessly.
func TestPeerFanOutEmptyNodes(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	results, ok := peerFanOut(context.Background(), logger, "test", nil,
		func(_ context.Context, _ string) (struct{}, error) {
			t.Fatal("fn should never be called for an empty node list")
			return struct{}{}, nil
		})

	if len(results) != 0 || len(ok) != 0 {
		t.Errorf("results/ok = (len %d, len %d), want both 0", len(results), len(ok))
	}
}
