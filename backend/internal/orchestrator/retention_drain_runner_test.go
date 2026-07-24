package orchestrator

// Coverage for gastrolog-aop1yc: memory-budget enforcement must retire
// chunks through the sweep's cached retention runner, not a bare one minted
// per chunk. The inline runner carried no disposition (so a delete-vault got
// fanned out anyway) and a fresh unreadable map on every iteration (so the
// retry backoff never accumulated and the operator's "Retry unreadable"
// action could not see the chunks at all).

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/logging"
	"gastrolog/internal/system"
)

func drainFixture(t *testing.T, disposition string) (*Orchestrator, *retentionRunner, *retentionFakeChunkManager, chunk.ChunkID) {
	t.Helper()
	o := newTestOrch(t, Config{LocalNodeID: "node-A"})
	id := chunk.NewChunkID()
	cm := &retentionFakeChunkManager{
		chunks: []chunk.ChunkMeta{{ID: id, Sealed: true, Bytes: 1024, WriteStart: time.Now()}},
	}
	im := &retentionFakeIndexManager{}
	vaultID := glid.New()
	vaultInst := &VaultInstance{VaultID: vaultID, Chunks: cm, Indexes: im}
	rec := NewVaultLifecycleReconciler(o, vaultID, vaultInst, "node-A", slog.Default())
	vaultInst.Reconciler = rec
	r := &retentionRunner{
		isLeader:    true,
		vaultID:     vaultID,
		cm:          cm,
		im:          im,
		orch:        o,
		reconciler:  rec,
		now:         o.now,
		logger:      slog.Default(),
		idleLog:     logging.Throttle{Interval: time.Minute},
		disposition: disposition,
	}
	return o, r, cm, id
}

// A vault configured "delete" — the safe default — must not stream records
// through the routing engine, on this path as much as on the TTL path. The
// records drop and the storage frees.
func TestDrainExcessChunksHonorsDeleteDisposition(t *testing.T) {
	t.Parallel()
	o, r, cm, id := drainFixture(t, system.RetentionDispositionDelete)

	o.drainExcessChunks(r, cm, 512)

	cm.mu.Lock()
	deleted := append([]chunk.ChunkID(nil), cm.deleted...)
	cm.mu.Unlock()
	if len(deleted) != 1 || deleted[0] != id {
		t.Fatalf("deleted %v, want exactly [%s]: a delete-disposition vault retires without routing", deleted, id)
	}
}

// A vault configured "route" must not have its chunks destroyed when the
// fan-out could not deliver a single record — here because no vault instance
// is registered, so the records cannot even be read. Before gastrolog-aop1yc
// this path ignored the verdict entirely and destroyed regardless.
func TestDrainExcessChunksRetainsWhenRouteFanOutCannotRun(t *testing.T) {
	t.Parallel()
	o, r, cm, _ := drainFixture(t, system.RetentionDispositionRoute)

	o.drainExcessChunks(r, cm, 512)

	cm.mu.Lock()
	deleted := len(cm.deleted)
	cm.mu.Unlock()
	if deleted != 0 {
		t.Fatalf("deleted %d chunks, want 0: a route-disposition chunk whose fan-out never ran must survive for a later sweep", deleted)
	}
}

// The runner is per (vault, storage) and its per-chunk state must
// accumulate. A fresh runner per chunk reset failCount to 1 every time, so
// the exponential backoff never lengthened and a permanently unreadable
// chunk was retried on every single sweep.
func TestRetentionRunnerForCachesAndAccumulatesUnreadableState(t *testing.T) {
	t.Parallel()
	o := newTestOrch(t, Config{LocalNodeID: "node-A"})
	vaultID := glid.New()
	inst := newMemoryInstance(t, vaultID)
	o.RegisterVault(&Vault{ID: vaultID, Instance: inst})
	vaultCfg := system.VaultConfig{ID: vaultID, Name: "v", Type: "memory"}

	active := map[string]bool{}
	o.mu.Lock()
	first := o.retentionRunnerFor(vaultCfg, inst, active)
	o.mu.Unlock()

	id := chunk.NewChunkID()
	first.markUnreadable(id, errors.New("io error"))
	first.markUnreadable(id, errors.New("io error again"))

	o.mu.Lock()
	second := o.retentionRunnerFor(vaultCfg, inst, active)
	o.mu.Unlock()

	if first != second {
		t.Fatal("retentionRunnerFor must return the cached runner, not a new one: per-chunk state only means anything if it accumulates")
	}
	second.mu.Lock()
	entry := second.unreadable[id]
	second.mu.Unlock()
	if entry == nil {
		t.Fatal("unreadable state must survive re-resolution of the runner")
	}
	if entry.failCount != 2 {
		t.Fatalf("failCount = %d, want 2: the backoff must lengthen across sweeps, not reset", entry.failCount)
	}
}

// A vault with no retention rules still needs a runner: memory-budget
// enforcement retires its chunks and must go through the same gates. Before
// this fix the rules check returned early, so no runner was ever cached and
// the drain path minted its own.
func TestRetentionRunnerExistsForVaultWithoutRetentionRules(t *testing.T) {
	t.Parallel()
	o := newTestOrch(t, Config{LocalNodeID: "node-A"})
	vaultID := glid.New()
	inst := newMemoryInstance(t, vaultID)
	o.RegisterVault(&Vault{ID: vaultID, Instance: inst})
	// No RetentionRules, and a route disposition that must reach the runner.
	vaultCfg := system.VaultConfig{
		ID:                   vaultID,
		Name:                 "v",
		Type:                 "memory",
		RetentionDisposition: system.RetentionDispositionRoute,
	}

	active := map[string]bool{}
	o.mu.Lock()
	target := o.retentionTargetForInstance(&system.Config{}, vaultCfg, inst, active)
	runner := o.retention[retentionKey(vaultID, inst.StorageID)]
	o.mu.Unlock()

	if target != nil {
		t.Fatal("a vault with no retention rules is not a sweep target")
	}
	if runner == nil {
		t.Fatal("a vault with no retention rules must still have a cached runner for memory-budget enforcement")
	}
	if runner.disposition != system.RetentionDispositionRoute {
		t.Fatalf("runner disposition = %q, want %q: an unset disposition silently disables routing",
			runner.disposition, system.RetentionDispositionRoute)
	}
	if !active[retentionKey(vaultID, inst.StorageID)] {
		t.Fatal("the runner must be marked active or the sweep's GC deletes it every tick")
	}
}
