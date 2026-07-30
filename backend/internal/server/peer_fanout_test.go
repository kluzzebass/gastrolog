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

// TestPeerFanOutHonorsPerPeerTimeout pins the invariant that one paused
// peer cannot block the whole inspector handler. A peer whose fn never
// returns must be elided within peerInspectorTimeout while healthy
// peers' results land normally.
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
	results, ok, report := peerFanOut(context.Background(), logger, "test", nodes, fn)
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

	// The merge must report itself as partial, naming the paused peer with
	// a "timeout" reason (deadline overrun collapses to that single word).
	if report == nil {
		t.Fatalf("report = nil, want a ContributionReport naming node-paused")
	}
	if len(report.Degraded) != 1 {
		t.Fatalf("report.Degraded = %d entries, want 1", len(report.Degraded))
	}
	if report.Degraded[0].NodeId != "node-paused" {
		t.Errorf("degraded node = %q, want node-paused", report.Degraded[0].NodeId)
	}
	if report.Degraded[0].Reason != "timeout" {
		t.Errorf("degraded reason = %q, want %q", report.Degraded[0].Reason, "timeout")
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

	results, ok, report := peerFanOut(context.Background(), logger, "test", nodes, fn)

	for i, n := range nodes {
		if !ok[i] {
			t.Errorf("nodes[%d] = %q: ok=false, want true", i, n)
			continue
		}
		if results[i] != n {
			t.Errorf("nodes[%d] = %q: results[%d] = %q (out-of-order)", i, n, i, results[i])
		}
	}

	// Every peer contributed — the happy path must omit the report entirely
	// so the UI stays quiet.
	if report != nil {
		t.Errorf("report = %+v, want nil (all peers contributed)", report)
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

	results, ok, report := peerFanOut(context.Background(), logger, "test", nodes, fn)

	if !ok[0] || results[0] != "ok" {
		t.Errorf("good: ok=%v, value=%q; want true, ok", ok[0], results[0])
	}
	if ok[1] {
		t.Errorf("bad: ok=true, want false (errored peer must be elided)")
	}

	// The errored peer must appear in the contribution report carrying the
	// transport error text verbatim (non-timeout failures keep their cause).
	if report == nil || len(report.Degraded) != 1 {
		t.Fatalf("report = %+v, want exactly one degraded peer", report)
	}
	if report.Degraded[0].NodeId != "bad" {
		t.Errorf("degraded node = %q, want bad", report.Degraded[0].NodeId)
	}
	if report.Degraded[0].Reason != wantErr.Error() {
		t.Errorf("degraded reason = %q, want %q", report.Degraded[0].Reason, wantErr.Error())
	}
}

// TestPeerFanOutEmptyNodes pins the no-fan-out case. Empty node list
// must short-circuit without spawning goroutines and return empty
// parallel slices that the caller can range over harmlessly.
func TestPeerFanOutEmptyNodes(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	results, ok, report := peerFanOut(context.Background(), logger, "test", nil,
		func(_ context.Context, _ string) (struct{}, error) {
			t.Fatal("fn should never be called for an empty node list")
			return struct{}{}, nil
		})

	if len(results) != 0 || len(ok) != 0 {
		t.Errorf("results/ok = (len %d, len %d), want both 0", len(results), len(ok))
	}
	if report != nil {
		t.Errorf("report = %+v, want nil (no peers to fan out to)", report)
	}
}

// TestPeerFanOutPlacementChurnNotDegraded pins that a benign
// placement-churn error (peer no longer owns the vault during
// reconfiguration) is elided from results WITHOUT being counted as
// degradation — it is expected reconfiguration, not an operational
// failure, so it must never make a merge read as partial.
func TestPeerFanOutPlacementChurnNotDegraded(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	nodes := []string{"good", "churning", "broken"}
	realErr := errors.New("connection refused")

	fn := func(_ context.Context, nodeID string) (string, error) {
		switch nodeID {
		case "churning":
			// A cross-RPC placement-churn error shape (peer reconfigured
			// out of the vault); IsPlacementChurnErr recognises this.
			return "", errors.New("follower rejected command: seal failed: vault instance not registered on this node: vault V")
		case "broken":
			return "", realErr
		default:
			return "ok", nil
		}
	}

	results, ok, report := peerFanOut(context.Background(), logger, "test", nodes, fn)

	if !ok[0] || results[0] != "ok" {
		t.Errorf("good: ok=%v value=%q, want true ok", ok[0], results[0])
	}
	if ok[1] || ok[2] {
		t.Errorf("churning/broken peers must be elided: ok=%v", ok)
	}

	// Only the genuinely broken peer is degradation; the churning peer is
	// benign and must not appear.
	if report == nil || len(report.Degraded) != 1 {
		t.Fatalf("report = %+v, want exactly one degraded peer (broken only)", report)
	}
	if report.Degraded[0].NodeId != "broken" {
		t.Errorf("degraded node = %q, want broken (churning must be excluded)", report.Degraded[0].NodeId)
	}
}
