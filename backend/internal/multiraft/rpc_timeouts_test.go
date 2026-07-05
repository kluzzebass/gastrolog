package multiraft

import (
	"testing"
	"time"
)

// TestConfigureRPCTimeouts: per-RPC deadlines derive from the configured
// failure-detector timing and never undercut it (the gastrolog-1io54g
// inversion) or shrink below the shipped defaults. Not parallel: mutates
// package-level deadlines.
func TestConfigureRPCTimeouts(t *testing.T) {
	restore := func() { ConfigureRPCTimeouts(2*time.Second, 2*time.Second) }
	t.Cleanup(restore)

	// Widened detector: deadlines scale with it.
	ConfigureRPCTimeouts(5*time.Second, 5*time.Second)
	if heartbeatRPCTimeout != 5*time.Second {
		t.Fatalf("heartbeatRPCTimeout = %v, want 5s (must cover the detector window)", heartbeatRPCTimeout)
	}
	if appendEntriesRPCTimeout != 6*time.Second {
		t.Fatalf("appendEntriesRPCTimeout = %v, want 6s (heartbeat+1s)", appendEntriesRPCTimeout)
	}
	if requestVoteRPCTimeout != 5*time.Second || requestPreVoteRPCTimeout != 5*time.Second || timeoutNowRPCTimeout != 5*time.Second {
		t.Fatalf("vote-path timeouts = %v/%v/%v, want 5s each", requestVoteRPCTimeout, requestPreVoteRPCTimeout, timeoutNowRPCTimeout)
	}

	// Tightened detector: deadlines floor at the shipped defaults — a
	// paused peer must still fail fast, and the transport being LOOSER
	// than the detector is harmless (only the detector declares death).
	ConfigureRPCTimeouts(500*time.Millisecond, 500*time.Millisecond)
	if heartbeatRPCTimeout != 2*time.Second || appendEntriesRPCTimeout != 3*time.Second || requestVoteRPCTimeout != 2*time.Second {
		t.Fatalf("floors: got %v/%v/%v, want 2s/3s/2s", heartbeatRPCTimeout, appendEntriesRPCTimeout, requestVoteRPCTimeout)
	}
}
