package orchestrator

// finishPendingPipelineCtlRestore is invoked from reloadPipelineFromConfig
// while the caller (reconcileFilters/ReloadFilters) holds o.mu WRITE-locked,
// and its reconcile chain takes o.mu.RLock (isPipelineIngestVault). Inline
// execution self-deadlocked the entire orchestrator the first time a node
// rejoined via vault-ctl snapshot restore — every o.mu user in the process
// queued behind the poisoned writer. This test pins the contract: the finish
// must be safe to call under the write lock.

import (
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/waittest"
)

func TestFinishPendingCtlRestoreSafeUnderWriteLock(t *testing.T) {
	t.Parallel()

	orch, err := New(Config{LocalNodeID: "node-A"})
	if err != nil {
		t.Fatal(err)
	}

	vid := glid.New()
	// The vault must look pipeline-registered so the reconcile chain reaches
	// isPipelineIngestVault's RLock — the deadlock edge.
	orch.mu.Lock()
	orch.setPipelineVaultLocked(vid, pipelineVaultReg{})
	orch.mu.Unlock()
	orch.pendingPipelineCtlRestore.Store(vid, struct{}{})

	// Reproduce the caller's exact shape: write lock held across the finish,
	// as reconcileFilters -> reloadPipelineFromConfig does on the rejoin path.
	done := make(chan struct{})
	go func() {
		orch.mu.Lock()
		orch.finishPendingPipelineCtlRestore(vid)
		orch.mu.Unlock()
		close(done)
	}()

	waittest.Recv(t, "finishPendingPipelineCtlRestore returns under the o.mu write lock", done, nil)

	// Exactly-once: the pending entry must be consumed.
	if _, still := orch.pendingPipelineCtlRestore.Load(vid); still {
		t.Fatal("pending ctl-restore entry not consumed")
	}
}
