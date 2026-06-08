package orchestrator

import (
	"context"
	"testing"
)

// TestStopWaitsForAuxGoroutines verifies that Stop() blocks until the
// watchdog goroutine has exited.
func TestStopWaitsForAuxGoroutines(t *testing.T) {
	t.Parallel()

	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Stop must not hang — the auxWg tracks the watchdog goroutine.
	// A test timeout catches the failure if it blocks.
	if err := orch.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
