package orchestrator

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// The chunk tombstone sweep's purge is what a chunk purge confirmation
// vouches for: when it reports success, this node holds no copy of a
// tombstoned chunk. These tests pin that a success report is never issued
// over a copy that is still there.

// stagingCases seeds a tombstoned, a live, a pending-delete and an unknown
// chunk dir under the fixture's chunk staging root.
type stagingCases struct {
	tombstoned, live, pending, unknown string
}

func seedStagingCases(t *testing.T, f *stagingSweepFixture) stagingCases {
	t.Helper()
	now := time.Now()
	tombstoned := chunk.NewChunkID()
	f.apply(t, vaultctlfsm.MarshalFinalizeDelete(tombstoned))
	live := chunk.NewChunkID()
	f.apply(t, vaultctlfsm.MarshalCreateChunk(live, now, now, now))
	pending := chunk.NewChunkID()
	f.apply(t, vaultctlfsm.MarshalCreateChunk(pending, now, now, now))
	f.apply(t, vaultctlfsm.MarshalRequestDelete(pending, now, "test", []string{"node-B"}))
	return stagingCases{
		tombstoned: f.writeChunkDir(t, tombstoned),
		live:       f.writeChunkDir(t, live),
		pending:    f.writeChunkDir(t, pending),
		unknown:    f.writeChunkDir(t, chunk.NewChunkID()),
	}
}

// A node that is not a home for the vault — never was, or no longer is — can
// still hold a chunk's staging dir. The purge reaches it whether or not the
// pipeline runs the vault here, and still deletes only tombstoned chunks.
func TestChunkTombstonePurgeReachesStagingOnNonHomes(t *testing.T) {
	t.Parallel()
	f := newStagingSweepFixture(t)
	f.orch.deletePipelineVaultLocked(f.vaultID)
	c := seedStagingCases(t, f)

	if _, err := f.orch.purgeTombstonedChunks(nil, f.vaultID, f.fsm); err != nil {
		t.Fatalf("purge: %v", err)
	}
	if exists(c.tombstoned) {
		t.Error("tombstoned chunk staging dir survived on a non-home")
	}
	for name, dir := range map[string]string{"live": c.live, "pending-delete": c.pending, "unknown": c.unknown} {
		if !exists(dir) {
			t.Errorf("%s chunk staging dir was deleted", name)
		}
	}
}

// A tombstoned chunk dir the purge cannot delete fails the purge, so the
// sweep does not confirm while the copy is still on disk.
func TestChunkTombstonePurgeReportsAnUndeletableStagingCopy(t *testing.T) {
	t.Parallel()
	f := newStagingSweepFixture(t)
	c := seedStagingCases(t, f)
	chunksDir := filepath.Dir(c.tombstoned)
	if err := os.Chmod(chunksDir, 0o555); err != nil {
		t.Fatalf("chmod chunks/: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(chunksDir, 0o755) })

	if _, err := f.orch.purgeTombstonedChunks(nil, f.vaultID, f.fsm); err == nil {
		t.Fatal("purge reported success with the tombstoned copy still on disk")
	}
	if !exists(c.tombstoned) {
		t.Fatal("premise: the read-only chunks/ dir did not keep the tombstoned copy")
	}

	if err := os.Chmod(chunksDir, 0o755); err != nil {
		t.Fatalf("chmod chunks/ back: %v", err)
	}
	if _, err := f.orch.purgeTombstonedChunks(nil, f.vaultID, f.fsm); err != nil {
		t.Fatalf("purge once deletable: %v", err)
	}
	if exists(c.tombstoned) {
		t.Error("tombstoned copy survived once deletable")
	}
}

// A chunk store's copy of a tombstoned chunk counts too: the vault
// instance's local-orphan pass runs in the purge, and a copy it could not
// delete fails the purge.
func TestChunkTombstonePurgeCoversTheVaultInstanceChunkStore(t *testing.T) {
	t.Parallel()
	tombstoned := chunk.NewChunkID()

	run := func(t *testing.T, cm chunk.ChunkManager) error {
		t.Helper()
		fsm := vaultctlfsm.New()
		applyTo(t, fsm, vaultctlfsm.MarshalFinalizeDelete(tombstoned))
		vaultID := glid.New()
		inst := &VaultInstance{VaultID: vaultID, Chunks: cm}
		orch := &Orchestrator{vaults: map[glid.GLID]*Vault{vaultID: {Instance: inst}}}
		inst.Reconciler = NewVaultLifecycleReconciler(orch, vaultID, inst, "node-A", slog.Default())
		inst.Reconciler.Wire(fsm)
		_, err := orch.purgeTombstonedChunks(nil, vaultID, fsm)
		return err
	}

	t.Run("deleted", func(t *testing.T) {
		t.Parallel()
		cm := &reconcilerFakeChunkManager{}
		cm.chunks = []chunk.ChunkMeta{{ID: tombstoned, Sealed: true}}
		if err := run(t, cm); err != nil {
			t.Fatalf("purge: %v", err)
		}
		if !slices.Contains(cm.deleted, tombstoned) {
			t.Fatal("tombstoned chunk store copy not deleted")
		}
	})
	t.Run("delete fails", func(t *testing.T) {
		t.Parallel()
		cm := &failingDeleteChunkManager{}
		cm.chunks = []chunk.ChunkMeta{{ID: tombstoned, Sealed: true}}
		if err := run(t, cm); err == nil {
			t.Fatal("purge reported success over a chunk store copy it could not delete")
		}
	})
	t.Run("active without a seal ensurer", func(t *testing.T) {
		t.Parallel()
		cm := &reconcilerFakeChunkManager{}
		cm.chunks = []chunk.ChunkMeta{{ID: tombstoned, Sealed: false}}
		if err := run(t, cm); err == nil {
			t.Fatal("purge reported success over an active tombstoned copy it could not demote")
		}
	})
}

// Before the reconciler is bound to the vault-ctl FSM it has not looked at
// the chunk store, so it cannot vouch for it.
func TestLocalOrphanPurgeFailsBeforeTheFSMIsBound(t *testing.T) {
	t.Parallel()
	inst := &VaultInstance{VaultID: glid.New(), Chunks: &reconcilerFakeChunkManager{}}
	rec := NewVaultLifecycleReconciler(nil, inst.VaultID, inst, "node-A", slog.Default())
	if err := rec.purgeLocalOrphans(); err == nil {
		t.Fatal("unbound reconciler reported a complete local-orphan pass")
	}
}

type failingDeleteChunkManager struct {
	reconcilerFakeChunkManager
}

func (f *failingDeleteChunkManager) Delete(chunk.ChunkID) error {
	return errors.New("disk says no")
}

func applyTo(t *testing.T, fsm *vaultctlfsm.FSM, data []byte) {
	t.Helper()
	if err, ok := fsm.Apply(&hraft.Log{Data: data}).(error); ok && err != nil {
		t.Fatalf("apply: %v", err)
	}
}

// After a placement moves a vault onto a file storage, its chunks live under
// that storage's pipeline root, not the node home's. The purge must reach
// them there, or a confirmation would vouch for a copy that is still on disk.
func TestChunkTombstonePurgeReachesEveryStorageRoot(t *testing.T) {
	t.Parallel()
	f := newStagingSweepFixture(t)
	storageDir := t.TempDir()
	sys := &system.System{Runtime: system.Runtime{NodeStorageConfigs: []system.NodeStorageConfig{{
		NodeID:       f.orch.localNodeID,
		FileStorages: []system.FileStorage{{ID: glid.New(), StorageClass: 3, Name: "archive", Path: storageDir}},
	}}}}
	tombstoned := chunk.NewChunkID()
	f.apply(t, vaultctlfsm.MarshalFinalizeDelete(tombstoned))
	onStorage := filepath.Join(pipelineChunkRoot(storagePipelineRoot(storageDir, f.vaultID)), tombstoned.String())
	if err := os.MkdirAll(onStorage, 0o750); err != nil {
		t.Fatal(err)
	}

	if _, err := f.orch.purgeTombstonedChunks(sys, f.vaultID, f.fsm); err != nil {
		t.Fatalf("purge: %v", err)
	}
	if exists(onStorage) {
		t.Error("tombstoned chunk dir on a file storage's pipeline root survived the purge")
	}
}
