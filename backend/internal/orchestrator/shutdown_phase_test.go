package orchestrator

import (
	"context"
	"testing"

	"gastrolog/internal/lifecycle"
)

// TestOrchestratorStopFlipsPhase verifies that Orchestrator.Stop() itself
// flips the phase at stage 0, even when no external caller has. This is
// defence-in-depth for the case where a component (other than the top-level
// shutdown) stops the orchestrator first — the phase should still reach
// "shutting down" state so subsequent shutdown-aware work skips.
func TestOrchestratorStopFlipsPhase(t *testing.T) {
	t.Parallel()

	phase := lifecycle.New()
	orch := newTestOrch(t, Config{LocalNodeID: "local", Phase: phase})

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if phase.ShuttingDown() {
		t.Fatal("phase should not be shutting down after Start")
	}
	if err := orch.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if !phase.ShuttingDown() {
		t.Error("Stop should flip the phase to shutting down")
	}
	if phase.Label() == "" {
		t.Error("Stop should set a phase label")
	}
}
