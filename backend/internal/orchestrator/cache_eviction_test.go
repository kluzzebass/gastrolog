package orchestrator

import (
	"sync/atomic"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// evictCountingChunkManager is a minimal ChunkManager (reusing
// retentionFakeChunkManager's no-op method set) that also implements
// CacheEvictor, counting how many times EvictCache is called. Used to
// pin down the rule that the dedicated cache-eviction job is the only
// path that may ever call EvictCache — the retention sweep must not.
type evictCountingChunkManager struct {
	retentionFakeChunkManager
	evictCalls atomic.Int64
}

func (f *evictCountingChunkManager) EvictCache() (int, int64) {
	f.evictCalls.Add(1)
	return 0, 0
}

// newEvictionTestOrch builds a single-node orchestrator with one leader
// vault instance whose chunk manager counts EvictCache calls, wired with
// a real SystemLoader so retentionSweepAll runs its full production path
// (not just an early-return due to a missing loader).
func newEvictionTestOrch(t *testing.T) (*Orchestrator, *evictCountingChunkManager) {
	t.Helper()
	orch := newTestOrch(t, Config{LocalNodeID: "node1"})

	vaultID := glid.New()
	store := newTestStore(&system.Config{
		Vaults: []system.VaultConfig{{ID: vaultID, Name: "v", Type: system.VaultTypeMemory}},
	}, "node1")
	orch.setSystemLoader(&transitionSystemLoader{store: store, nodeID: "node1"})

	evictor := &evictCountingChunkManager{}
	vaultInst := &VaultInstance{VaultID: vaultID, Type: "memory", Chunks: evictor}
	orch.RegisterVault(NewVault(vaultID, vaultInst))

	return orch, evictor
}

// TestCacheEvictionSweepAll_FiresEvictCache is the survivor-path test: the
// dedicated cache-eviction job must still evict on every tick.
func TestCacheEvictionSweepAll_FiresEvictCache(t *testing.T) {
	orch, evictor := newEvictionTestOrch(t)

	orch.cacheEvictionSweepAll()
	if got := evictor.evictCalls.Load(); got != 1 {
		t.Fatalf("EvictCache calls after one cacheEvictionSweepAll = %d, want 1", got)
	}

	orch.cacheEvictionSweepAll()
	if got := evictor.evictCalls.Load(); got != 2 {
		t.Fatalf("EvictCache calls after two cacheEvictionSweepAll ticks = %d, want 2", got)
	}
}

// TestRetentionSweepAll_DoesNotCallEvictCache pins down the rule that
// the retention sweep must never call EvictCache — that is the
// cache-eviction job's sole responsibility. Regression guard for the
// duplicate-eviction bug the two independent scheduler jobs used to
// produce.
func TestRetentionSweepAll_DoesNotCallEvictCache(t *testing.T) {
	orch, evictor := newEvictionTestOrch(t)

	orch.retentionSweepAll()
	if got := evictor.evictCalls.Load(); got != 0 {
		t.Fatalf("EvictCache calls after retentionSweepAll = %d, want 0 (retention must not evict caches)", got)
	}

	// Run it a few more times — still zero. A prior duplicate-path bug
	// would have fired once per call.
	orch.retentionSweepAll()
	orch.retentionSweepAll()
	if got := evictor.evictCalls.Load(); got != 0 {
		t.Fatalf("EvictCache calls after 3x retentionSweepAll = %d, want 0", got)
	}
}

// TestCacheEvictionNotDuplicatedAcrossSweeps runs one retention sweep and
// one cache-eviction sweep — simulating one minute of both scheduled jobs
// ticking — and asserts EvictCache fired exactly once total, not twice.
func TestCacheEvictionNotDuplicatedAcrossSweeps(t *testing.T) {
	orch, evictor := newEvictionTestOrch(t)

	orch.retentionSweepAll()
	orch.cacheEvictionSweepAll()

	if got := evictor.evictCalls.Load(); got != 1 {
		t.Fatalf("EvictCache calls after one retention + one cache-eviction sweep = %d, want exactly 1 (no double-eviction)", got)
	}
}

// TestCacheEvictionSweepAll_SkipsNonLeaderInstance verifies the survivor
// path's existing leader gate: cacheEvictionSweepAll only evicts on
// leader instances (followers never got a runner via retentionSweepAll's
// leader-only path either, so parity is preserved).
func TestCacheEvictionSweepAll_SkipsNonLeaderInstance(t *testing.T) {
	orch := newTestOrch(t, Config{LocalNodeID: "node1"})
	vaultID := glid.New()
	evictor := &evictCountingChunkManager{}
	vaultInst := &VaultInstance{VaultID: vaultID, Type: "memory", Chunks: evictor, IsFollower: true}
	orch.RegisterVault(NewVault(vaultID, vaultInst))

	orch.cacheEvictionSweepAll()

	if got := evictor.evictCalls.Load(); got != 0 {
		t.Fatalf("EvictCache calls on a follower instance = %d, want 0", got)
	}
}
