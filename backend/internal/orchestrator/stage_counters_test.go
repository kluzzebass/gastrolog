package orchestrator

import (
	"sync"
	"testing"

	"gastrolog/internal/glid"
)

// TestStageEventCounters_PerVaultMonotonic verifies the orchestrator-owned
// GLCB-pull and retention-delete counters accumulate per vault and snapshot
// cleanly.
func TestStageEventCounters_PerVaultMonotonic(t *testing.T) {
	t.Parallel()
	s := newStageEventCounters()
	vaultA := glid.New()
	vaultB := glid.New()

	s.recordGLCBPullAttempt(vaultA)
	s.recordGLCBPullAttempt(vaultA)
	s.recordGLCBPullFailed(vaultA)
	s.recordRetentionDelete(vaultA)
	s.recordRetentionDelete(vaultA)
	s.recordRetentionDelete(vaultA)
	s.recordGLCBPullAttempt(vaultB)

	byVault := map[glid.GLID]VaultStageEventSnapshot{}
	for _, snap := range s.snapshot() {
		byVault[snap.VaultID] = snap
	}
	a := byVault[vaultA]
	if a.GLCBPullsAttempted != 2 || a.GLCBPullsFailed != 1 || a.RetentionDeletes != 3 {
		t.Fatalf("vaultA = att %d fail %d del %d, want 2/1/3",
			a.GLCBPullsAttempted, a.GLCBPullsFailed, a.RetentionDeletes)
	}
	b := byVault[vaultB]
	if b.GLCBPullsAttempted != 1 || b.GLCBPullsFailed != 0 || b.RetentionDeletes != 0 {
		t.Fatalf("vaultB = att %d fail %d del %d, want 1/0/0",
			b.GLCBPullsAttempted, b.GLCBPullsFailed, b.RetentionDeletes)
	}

	// Forget drops the vault entirely.
	s.Forget(vaultA)
	for _, snap := range s.snapshot() {
		if snap.VaultID == vaultA {
			t.Fatalf("vaultA still present after Forget")
		}
	}
}

// TestStageEventCounters_ConcurrentRecord exercises the counters under
// concurrent writers to prove the per-vault atomics + map lock are race-free.
func TestStageEventCounters_ConcurrentRecord(t *testing.T) {
	t.Parallel()
	s := newStageEventCounters()
	vault := glid.New()
	const workers, perWorker = 8, 500
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWorker {
				s.recordGLCBPullAttempt(vault)
				s.recordRetentionDelete(vault)
			}
		}()
	}
	wg.Wait()
	snaps := s.snapshot()
	if len(snaps) != 1 {
		t.Fatalf("snapshot len = %d, want 1", len(snaps))
	}
	want := uint64(workers * perWorker)
	if snaps[0].GLCBPullsAttempted != want || snaps[0].RetentionDeletes != want {
		t.Fatalf("counts = att %d del %d, want %d each",
			snaps[0].GLCBPullsAttempted, snaps[0].RetentionDeletes, want)
	}
}
