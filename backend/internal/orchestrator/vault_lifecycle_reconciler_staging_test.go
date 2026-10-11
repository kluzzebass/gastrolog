package orchestrator

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// SweepStagingOrphans reconciles chunks/ on disk against the replicated FSM
// with positive evidence only. These tests pin the safety contract: tombstoned
// chunk dirs go, everything else — live, unknown, pending-delete — stays. The
// failure mode of a stale FSM must be a delayed cleanup, never a deleted live
// file (the cardinal rule).

// stagingSweepFixture builds an on-disk vault staging root, an
// orchestrator that resolves it, and a wired reconciler.
type stagingSweepFixture struct {
	vaultID glid.GLID
	root    string
	orch    *Orchestrator
	fsm     *vaultctlfsm.FSM
	rec     *VaultLifecycleReconciler
}

func newStagingSweepFixture(t *testing.T) *stagingSweepFixture {
	t.Helper()
	vaultID := glid.New()
	base := t.TempDir()
	root := filepath.Join(base, vaultID.String())
	for _, dir := range []string{paths.CompletedDir(root), paths.HeadDir(root), paths.PreHeadDir(root), filepath.Join(root, "chunks")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	orch := &Orchestrator{segmentsDir: base}
	orch.setPipelineVaultLocked(vaultID, pipelineVaultReg{home: true})
	fsm := vaultctlfsm.New()
	rec := NewVaultLifecycleReconciler(orch, vaultID, &VaultInstance{VaultID: vaultID}, "node-A", slog.Default())
	rec.Wire(fsm)
	return &stagingSweepFixture{vaultID: vaultID, root: root, orch: orch, fsm: fsm, rec: rec}
}

func (f *stagingSweepFixture) apply(t *testing.T, data []byte) {
	t.Helper()
	if err, ok := f.fsm.Apply(&hraft.Log{Data: data}).(error); ok && err != nil {
		t.Fatalf("apply: %v", err)
	}
}

func (f *stagingSweepFixture) writeChunkDir(t *testing.T, id chunk.ChunkID) string {
	t.Helper()
	dir := filepath.Join(f.root, "chunks", id.String())
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "data.glcb"), []byte("glcb"), 0o644); err != nil {
		t.Fatalf("write glcb: %v", err)
	}
	return dir
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func TestStagingSweepRemovesOnlyTombstonedChunkDirs(t *testing.T) {
	t.Parallel()
	f := newStagingSweepFixture(t)
	now := time.Now()

	// Tombstoned: full delete cycle finalized while "offline". Dir must go.
	tombstoned := chunk.NewChunkID()
	f.apply(t, vaultctlfsm.MarshalCreateChunk(tombstoned, now, now, now))
	f.apply(t, vaultctlfsm.MarshalSealChunk(tombstoned, now, 1, 1, now, now, now, false, now, [32]byte{}))
	f.apply(t, vaultctlfsm.MarshalRequestDelete(tombstoned, now, "test", []string{"node-B"}))
	f.apply(t, vaultctlfsm.MarshalAckDelete(tombstoned, "node-B"))
	f.apply(t, vaultctlfsm.MarshalFinalizeDelete(tombstoned))
	tombstonedDir := f.writeChunkDir(t, tombstoned)

	// Live sealed chunk: FSM manifest entry exists. Must survive.
	live := chunk.NewChunkID()
	f.apply(t, vaultctlfsm.MarshalCreateChunk(live, now, now, now))
	f.apply(t, vaultctlfsm.MarshalSealChunk(live, now, 1, 1, now, now, now, false, now, [32]byte{}))
	liveDir := f.writeChunkDir(t, live)

	// Pending delete: receipt protocol owns cleanup. Must survive here.
	pending := chunk.NewChunkID()
	f.apply(t, vaultctlfsm.MarshalCreateChunk(pending, now, now, now))
	f.apply(t, vaultctlfsm.MarshalSealChunk(pending, now, 1, 1, now, now, now, false, now, [32]byte{}))
	f.apply(t, vaultctlfsm.MarshalRequestDelete(pending, now, "test", []string{"node-B"}))
	pendingDir := f.writeChunkDir(t, pending)

	// Unknown: no entry, no tombstone — recovery surface, preserved
	// (no-auto-delete-of-unknown-orphans invariant).
	unknown := chunk.NewChunkID()
	unknownDir := f.writeChunkDir(t, unknown)

	// Non-chunk debris in chunks/ is ignored.
	if err := os.WriteFile(filepath.Join(f.root, "chunks", "not-a-chunk-id"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write debris: %v", err)
	}

	f.rec.SweepStagingOrphans()

	if exists(tombstonedDir) {
		t.Error("tombstoned chunk staging dir survived the sweep")
	}
	if !exists(liveDir) {
		t.Error("live chunk staging dir was deleted")
	}
	if !exists(pendingDir) {
		t.Error("pending-delete chunk staging dir was deleted (receipt protocol owns it)")
	}
	if !exists(unknownDir) {
		t.Error("unknown chunk staging dir was deleted — violates no-auto-delete-of-unknown-orphans")
	}
}

func TestStagingSweepNoopBeforeFSMReady(t *testing.T) {
	t.Parallel()
	f := newStagingSweepFixture(t)

	// A chunk dir on disk, FSM has applied nothing (not Ready). Positive
	// evidence cannot exist yet; the sweep must not touch anything.
	dir := f.writeChunkDir(t, chunk.NewChunkID())

	f.rec.SweepStagingOrphans()

	if !exists(dir) {
		t.Error("sweep acted before the FSM was Ready")
	}
}

// TestReconcileFromSnapshotRunsStagingOrphanCleanup pins the staging-orphan
// category's event source: snapshot install. Like local orphans, staging
// orphans are stranded when a finalize applied while this node was offline
// and the rejoin came via snapshot (not command replay).
// ReconcileFromSnapshot — the after-restore hook — must run the staging
// reconcile on that event, not leave it for the periodic backstop tick.
// Fires ONLY the restore event; never calls SweepStagingOrphans /
// ReconcileTick.
func TestReconcileFromSnapshotRunsStagingOrphanCleanup(t *testing.T) {
	t.Parallel()
	f := newStagingSweepFixture(t)
	now := time.Now()

	// Tombstoned chunk staging dir: full delete cycle finalized "offline".
	tombstoned := chunk.NewChunkID()
	f.apply(t, vaultctlfsm.MarshalCreateChunk(tombstoned, now, now, now))
	f.apply(t, vaultctlfsm.MarshalSealChunk(tombstoned, now, 1, 1, now, now, now, false, now, [32]byte{}))
	f.apply(t, vaultctlfsm.MarshalRequestDelete(tombstoned, now, "test", []string{"node-B"}))
	f.apply(t, vaultctlfsm.MarshalAckDelete(tombstoned, "node-B"))
	f.apply(t, vaultctlfsm.MarshalFinalizeDelete(tombstoned))
	tombstonedDir := f.writeChunkDir(t, tombstoned)

	// Live sealed chunk dir: FSM manifest entry exists; must survive.
	live := chunk.NewChunkID()
	f.apply(t, vaultctlfsm.MarshalCreateChunk(live, now, now, now))
	f.apply(t, vaultctlfsm.MarshalSealChunk(live, now, 1, 1, now, now, now, false, now, [32]byte{}))
	liveDir := f.writeChunkDir(t, live)

	// Fire ONLY the snapshot-restore event.
	f.rec.ReconcileFromSnapshot(f.fsm)

	if exists(tombstonedDir) {
		t.Error("ReconcileFromSnapshot did not remove the tombstoned chunk dir on the restore event")
	}
	if !exists(liveDir) {
		t.Error("ReconcileFromSnapshot deleted a live chunk dir")
	}
}
