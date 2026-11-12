package orchestrator

import (
	"context"
	"gastrolog/internal/glid"
)

// TryLockForTest reports whether the orchestrator lock is free right now,
// releasing it again when it was: a test's probe that a long-running
// operation is not holding it.
func (o *Orchestrator) TryLockForTest() bool {
	if !o.mu.TryLock() {
		return false
	}
	o.mu.Unlock()
	return true
}

// AwaitVaultTeardownForTest waits until the vault's background teardown, if
// any, has finished.
func (o *Orchestrator) AwaitVaultTeardownForTest(ctx context.Context, id glid.GLID) error {
	return o.awaitVaultTeardown(ctx, id)
}

// RunsPipelineForTest reports whether this node currently runs the vault's
// pipeline (holds a registration for it).
func (o *Orchestrator) RunsPipelineForTest(id glid.GLID) bool {
	return o.isPipelineIngestVault(id)
}
