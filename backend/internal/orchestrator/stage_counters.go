package orchestrator

import (
	"sync"
	"sync/atomic"

	"gastrolog/internal/glid"
)

// vaultStageEvents holds the orchestrator-owned per-vault pipeline stage
// counters that don't originate in a pipeline manager: GLCB replica catch-up
// pulls and retention chunk deletes. Managers own their own stage counters
// (segmentation completed, distribution published, chunking
// planned/built/sealed/released/purged); these two live here because their
// event sites are orchestrator methods (runGLCBPull, retentionRunner.expireChunk).
//
// All counters are monotonic. Retention deletes are counted only on the
// instance-leader delete path so cluster totals reflect expiration decisions,
// not follower delete-cascade applications.
type vaultStageEvents struct {
	glcbPullsAttempted atomic.Uint64
	glcbPullsFailed    atomic.Uint64
	retentionDeletes   atomic.Uint64
}

// stageEventCounters is the orchestrator's lazily-populated per-vault map of
// stage-event counters. Guarded by its own mutex so hot counter reads/writes
// never contend on the orchestrator's main lock.
type stageEventCounters struct {
	mu     sync.Mutex
	vaults map[glid.GLID]*vaultStageEvents
}

func newStageEventCounters() *stageEventCounters {
	return &stageEventCounters{vaults: make(map[glid.GLID]*vaultStageEvents)}
}

func (s *stageEventCounters) get(vaultID glid.GLID) *vaultStageEvents {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.vaults[vaultID]
	if !ok {
		v = &vaultStageEvents{}
		s.vaults[vaultID] = v
	}
	return v
}

// Forget drops a removed vault's counters.
func (s *stageEventCounters) Forget(vaultID glid.GLID) {
	s.mu.Lock()
	delete(s.vaults, vaultID)
	s.mu.Unlock()
}

func (s *stageEventCounters) recordGLCBPullAttempt(vaultID glid.GLID) {
	s.get(vaultID).glcbPullsAttempted.Add(1)
}

func (s *stageEventCounters) recordGLCBPullFailed(vaultID glid.GLID) {
	s.get(vaultID).glcbPullsFailed.Add(1)
}

func (s *stageEventCounters) recordRetentionDelete(vaultID glid.GLID) {
	s.get(vaultID).retentionDeletes.Add(1)
}

// VaultStageEventSnapshot is one vault's orchestrator-owned stage-event
// counters for the stats broadcast.
type VaultStageEventSnapshot struct {
	VaultID            glid.GLID
	GLCBPullsAttempted uint64
	GLCBPullsFailed    uint64
	RetentionDeletes   uint64
}

// snapshot returns a stable copy of every tracked vault's stage-event counters.
func (s *stageEventCounters) snapshot() []VaultStageEventSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]VaultStageEventSnapshot, 0, len(s.vaults))
	for vaultID, v := range s.vaults {
		out = append(out, VaultStageEventSnapshot{
			VaultID:            vaultID,
			GLCBPullsAttempted: v.glcbPullsAttempted.Load(),
			GLCBPullsFailed:    v.glcbPullsFailed.Load(),
			RetentionDeletes:   v.retentionDeletes.Load(),
		})
	}
	return out
}
