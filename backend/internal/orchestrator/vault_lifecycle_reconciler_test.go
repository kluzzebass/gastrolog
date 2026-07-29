package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// captureCatchupReplicator records the most recent RequestReplicaCatchup
// call so SweepMissingReplicas tests can assert what the follower asked
// the leader to push. Other ChunkReplicator methods are no-ops — the
// missing-replica sweep only exercises the one inverse method.
type captureCatchupReplicator struct {
	calls         atomic.Int32
	lastLeader    string
	lastVault     glid.GLID
	lastChunks    []chunk.ChunkID
	lastRequester string
	scheduledRet  uint32
	failNextWith  error
}

func (c *captureCatchupReplicator) AppendRecords(_ context.Context, _ string, _ glid.GLID, _ chunk.ChunkID, _ []chunk.Record) error {
	return nil
}
func (c *captureCatchupReplicator) SealVault(_ context.Context, _ string, _ glid.GLID, _ chunk.ChunkID) error {
	return nil
}
func (c *captureCatchupReplicator) ImportSealedChunk(_ context.Context, _ string, _ glid.GLID, _ chunk.ChunkID, _ chunk.RecordIterator) error {
	return nil
}
func (c *captureCatchupReplicator) RequestReplicaCatchup(_ context.Context, leaderNodeID string, vaultID glid.GLID, chunkIDs []chunk.ChunkID, requesterNodeID string) (uint32, error) {
	c.calls.Add(1)
	c.lastLeader = leaderNodeID
	c.lastVault = vaultID
	c.lastChunks = append([]chunk.ChunkID(nil), chunkIDs...)
	c.lastRequester = requesterNodeID
	if c.failNextWith != nil {
		err := c.failNextWith
		c.failNextWith = nil
		return 0, err
	}
	return c.scheduledRet, nil
}

// gastrolog-51gme step 4 — receipt protocol integration via reconciler.

// reconcilerFakeChunkManager is a chunk-manager stub that records
// delete calls so the reconciler tests can assert local-file deletion
// happened (or didn't) without needing a real on-disk manager.
type reconcilerFakeChunkManager struct {
	retentionFakeChunkManager
}

// reconcilerFakeSealEnsurerChunkManager extends the fake chunk manager
// with the chunk.SealEnsurer interface so onSeal / ReconcileFromSnapshot
// projection tests can observe EnsureSealed calls. See gastrolog-51gme step 8.
type reconcilerFakeSealEnsurerChunkManager struct {
	retentionFakeChunkManager
	ensured []chunk.ChunkID
}

func (f *reconcilerFakeSealEnsurerChunkManager) EnsureSealed(id chunk.ChunkID) error {
	f.ensured = append(f.ensured, id)
	return nil
}

// TestReconcilerOnRequestDeleteDeletesLocalAndAcks pins the receiver-side
// invariant: when CmdRequestDelete commits and this node is in
// expectedFrom, the reconciler deletes its local copy and proposes
// CmdAckDelete. Failure to either delete or ack must leave the FSM
// obligation in place for retry.
func TestReconcilerOnRequestDeleteDeletesLocalAndAcks(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	cm := &reconcilerFakeChunkManager{}

	var ackedID chunk.ChunkID
	var ackedNode string
	var ackCount atomic.Int32
	vaultInst := &VaultInstance{
		VaultID: glid.New(),
		Chunks:  cm,
		RaftApplyFacet: RaftApplyFacet{
			ApplyRaftAckDelete: func(id chunk.ChunkID, nodeID string) error {
				ackedID = id
				ackedNode = nodeID
				ackCount.Add(1)
				return nil
			},
		},
	}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())
	rec.Wire(fsm)

	chunkID := chunk.NewChunkID()
	if err := fsm.Apply(&hraft.Log{
		Data: vaultctlfsm.MarshalRequestDelete(chunkID, time.Now(), "retention-ttl",
			[]string{"node-A", "node-B"}),
	}); err != nil {
		t.Fatalf("apply CmdRequestDelete: %v", err)
	}

	// onRequestDelete dispatches the local delete + ack in a goroutine to
	// avoid deadlocking the FSM apply pump (CmdAckDelete on the leader
	// posts to the same Raft apply queue we're currently draining). Wait
	// for the goroutine to drain before asserting.
	deadline := time.After(2 * time.Second)
	for ackCount.Load() < 1 {
		select {
		case <-deadline:
			t.Fatalf("ack did not fire within deadline (count=%d)", ackCount.Load())
		case <-time.After(10 * time.Millisecond):
		}
	}

	if len(cm.deleted) != 1 || cm.deleted[0] != chunkID {
		t.Errorf("local delete = %v, want [%s]", cm.deleted, chunkID)
	}
	if ackedID != chunkID || ackedNode != "node-A" {
		t.Errorf("ack = (%s, %s), want (%s, node-A)", ackedID, ackedNode, chunkID)
	}
}

// TestReconcilerOnRequestDeleteIgnoresNotInExpectedFrom verifies that
// nodes outside expectedFrom never delete and never ack — the entire
// callback is a no-op for them. Without this guarantee a node that lost
// its placement (rebalanced away) could try to re-ack a delete it
// doesn't owe and confuse the leader's finalization decision.
func TestReconcilerOnRequestDeleteIgnoresNotInExpectedFrom(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	cm := &reconcilerFakeChunkManager{}

	var ackCount atomic.Int32
	vaultInst := &VaultInstance{
		VaultID: glid.New(),
		Chunks:  cm,
		RaftApplyFacet: RaftApplyFacet{
			ApplyRaftAckDelete: func(_ chunk.ChunkID, _ string) error {
				ackCount.Add(1)
				return nil
			},
		},
	}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-Z", slog.Default())
	rec.Wire(fsm)

	chunkID := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{
		Data: vaultctlfsm.MarshalRequestDelete(chunkID, time.Now(), "retention-ttl",
			[]string{"node-A", "node-B"}),
	})

	// Give a goroutine a chance to fire if the expectedFrom-skip check fails.
	time.Sleep(50 * time.Millisecond)

	if len(cm.deleted) != 0 {
		t.Errorf("non-expected node must not delete locally, got %v", cm.deleted)
	}
	if ackCount.Load() != 0 {
		t.Errorf("non-expected node must not ack, got %d acks", ackCount.Load())
	}
}

// TestReconcilerOnAckDeleteAutoFinalizesInsideApply pins the
// gastrolog-15fm8 invariant: when CmdAckDelete drains ExpectedFrom to
// empty, the FSM finalizes atomically inside the same apply — no
// leader-only callback proposes CmdFinalizeDelete. The reconciler's
// onAckDelete is audit-only post-fix; any leader-only proposal would
// re-introduce the leader-transfer leak the fix closes.
func TestReconcilerOnAckDeleteAutoFinalizesInsideApply(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()

	var (
		proposedFinalize atomic.Int32
		fsmFinalized     atomic.Int32
		finalizedID      chunk.ChunkID
		idMu             sync.Mutex
	)
	vaultInst := &VaultInstance{
		VaultID: glid.New(),
		Chunks:  &reconcilerFakeChunkManager{},
		RaftLeadershipFacet: RaftLeadershipFacet{
			IsRaftLeader: func() bool { return true },
		},
		RaftApplyFacet: RaftApplyFacet{
			ApplyRaftAckDelete: func(_ chunk.ChunkID, _ string) error { return nil },
			// ApplyRaftFinalizeDelete MUST NOT be called by the post-fix
			// onAckDelete — finalize happens inline in applyAckDelete.
			ApplyRaftFinalizeDelete: func(_ chunk.ChunkID) error {
				proposedFinalize.Add(1)
				return nil
			},
		},
	}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())
	rec.Wire(fsm)
	// Set the test hook AFTER Wire so the reconciler's audit-only
	// onFinalizeDelete doesn't steal the callback.
	fsm.SetOnFinalizeDelete(func(id chunk.ChunkID) {
		idMu.Lock()
		finalizedID = id
		idMu.Unlock()
		fsmFinalized.Add(1)
	})

	chunkID := chunk.NewChunkID()
	now := time.Now()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(chunkID, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(chunkID, now, 1, 1, now, now, now, false, now)})
	_ = fsm.Apply(&hraft.Log{
		Data: vaultctlfsm.MarshalRequestDelete(chunkID, now, "retention-ttl",
			[]string{"node-A", "node-B"}),
	})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalAckDelete(chunkID, "node-A")})

	if fsmFinalized.Load() != 0 {
		t.Errorf("must not finalize while node-B still owes ack, got %d", fsmFinalized.Load())
	}

	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalAckDelete(chunkID, "node-B")})

	// FSM-side finalize is synchronous within applyAckDelete; the
	// onFinalizeDelete callback fires inside fire() right after apply
	// returns. No goroutine to wait for; either it fired or it didn't.
	if fsmFinalized.Load() != 1 {
		t.Errorf("FSM onFinalizeDelete fires = %d, want 1 (atomic finalize on draining ack)", fsmFinalized.Load())
	}
	idMu.Lock()
	if finalizedID != chunkID {
		t.Errorf("finalize id = %s, want %s", finalizedID, chunkID)
	}
	idMu.Unlock()

	// The reconciler must NOT propose CmdFinalizeDelete in any branch —
	// that's the leader-only-callback shape the fix eliminated.
	if proposedFinalize.Load() != 0 {
		t.Errorf("ApplyRaftFinalizeDelete must not be called from onAckDelete post-fix, got %d", proposedFinalize.Load())
	}
}

// TestReconcilerDeleteChunkSingleNodeFallback pins the path that runs
// when no Raft applier is wired (single-node / memory mode): deleteChunk
// performs the local delete directly without going through the FSM.
// Without this fallback, single-node retention would become a no-op when
// the receipt protocol replaces the legacy CmdDeleteChunk path.
func TestReconcilerDeleteChunkSingleNodeFallback(t *testing.T) {
	t.Parallel()

	cm := &reconcilerFakeChunkManager{}
	vaultInst := &VaultInstance{
		VaultID: glid.New(),
		Chunks:  cm,
		// ApplyRaftRequestDelete deliberately nil — single-node mode.
	}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())

	chunkID := chunk.NewChunkID()
	if err := rec.deleteChunk(chunkID, "retention-ttl", []string{"node-A"}); err != nil {
		t.Fatalf("deleteChunk: %v", err)
	}
	if len(cm.deleted) != 1 || cm.deleted[0] != chunkID {
		t.Errorf("single-node deleteChunk delete = %v, want [%s]", cm.deleted, chunkID)
	}
}

// TestReconcilerOnPruneNodeAutoFinalizesInsideApply pins the
// gastrolog-15fm8 invariant: when CmdPruneNode drains a pendingDelete's
// ExpectedFrom to empty, the FSM finalizes atomically inside the same
// apply — no leader-only callback proposes CmdFinalizeDelete. The
// onFinalizeDelete callback fires once per finalized chunk through the
// FSM's apply dispatch; the reconciler's onPruneNode is audit-only
// post-fix. Pre-fix, the leader proposed finalize in a goroutine that
// could drop on leadership transfer mid-prune.
func TestReconcilerOnPruneNodeAutoFinalizesInsideApply(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	now := time.Now()

	// Three pendingDeletes; pruning node-A drains the second.
	idStillOwed := chunk.NewChunkID()
	idEmptied := chunk.NewChunkID()
	idUntouched := chunk.NewChunkID()
	for _, id := range []chunk.ChunkID{idStillOwed, idEmptied, idUntouched} {
		_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(id, now, now, now)})
		_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(id, now, 1, 1, now, now, now, false, now)})
	}
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalRequestDelete(idStillOwed, now, "test", []string{"node-A", "node-B"})})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalRequestDelete(idEmptied, now, "test", []string{"node-A"})})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalRequestDelete(idUntouched, now, "test", []string{"node-B"})})

	var proposedFinalize atomic.Int32
	vaultInst := &VaultInstance{
		VaultID: glid.New(),
		Chunks:  &reconcilerFakeChunkManager{},
		RaftLeadershipFacet: RaftLeadershipFacet{
			IsRaftLeader: func() bool { return true },
		},
		RaftApplyFacet: RaftApplyFacet{
			// ApplyRaftFinalizeDelete MUST NOT be called by the post-fix
			// onPruneNode — finalize happens inline in applyPruneNode.
			ApplyRaftFinalizeDelete: func(_ chunk.ChunkID) error {
				proposedFinalize.Add(1)
				return nil
			},
		},
	}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-B", slog.Default())
	rec.Wire(fsm)
	// Set the test hook AFTER Wire so the reconciler's audit-only
	// onFinalizeDelete doesn't steal the callback.
	finalized := map[chunk.ChunkID]int{}
	var finalizedMu sync.Mutex
	fsm.SetOnFinalizeDelete(func(id chunk.ChunkID) {
		finalizedMu.Lock()
		finalized[id]++
		finalizedMu.Unlock()
	})

	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalPruneNode("node-A")}); err != nil {
		t.Fatalf("apply prune: %v", err)
	}

	finalizedMu.Lock()
	defer finalizedMu.Unlock()
	if finalized[idEmptied] != 1 {
		t.Errorf("onFinalizeDelete for idEmptied = %d, want 1", finalized[idEmptied])
	}
	if finalized[idStillOwed] != 0 || finalized[idUntouched] != 0 {
		t.Errorf("non-drained chunks must not fire onFinalizeDelete: idStillOwed=%d idUntouched=%d",
			finalized[idStillOwed], finalized[idUntouched])
	}
	// FSM state must reflect the atomic finalize.
	if got := fsm.PendingDelete(idEmptied); got != nil {
		t.Errorf("idEmptied: pendingDeletes entry should be gone post-prune, got %+v", got)
	}
	if e := fsm.Get(idEmptied); e != nil {
		t.Errorf("idEmptied: manifest entry should be gone post-prune, got %+v", e)
	}
	if !fsm.IsTombstoned(idEmptied) {
		t.Error("idEmptied: tombstone should be present post-prune")
	}
	// The reconciler must NOT propose CmdFinalizeDelete — that's the
	// leader-only-callback shape the fix eliminated.
	if proposedFinalize.Load() != 0 {
		t.Errorf("ApplyRaftFinalizeDelete must not be called from onPruneNode post-fix, got %d", proposedFinalize.Load())
	}
}

// TestReconcilerOnSealProjectsToLocalManager pins the gastrolog-51gme step 8
// invariant: when CmdSealChunk applies, the reconciler asks the local chunk
// Manager to project the FSM-sealed state via the SealEnsurer interface. The
// Manager's EnsureSealed contract handles the no-op cases internally; the
// test just asserts the projection method was invoked with the right ID.
func TestReconcilerOnSealProjectsToLocalManager(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	cm := &reconcilerFakeSealEnsurerChunkManager{}
	vaultInst := &VaultInstance{
		VaultID: glid.New(),
		Chunks:  cm,
	}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())
	rec.Wire(fsm)

	id := chunk.NewChunkID()
	now := time.Now()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(id, now, now, now)}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(id, now, 100, 1234, now, now, now, false, now)}); err != nil {
		t.Fatalf("seal: %v", err)
	}

	if len(cm.ensured) != 1 || cm.ensured[0] != id {
		t.Errorf("EnsureSealed = %v, want [%s]", cm.ensured, id)
	}
}

// TestReconcileFromSnapshotProjectsAllSealedEntries pins that after FSM
// Restore, every sealed entry in the FSM is projected to the local
// Manager. This is the catchup pass that replaces the deleted
// "multiple unsealed → seal all but newest" startup heuristic. See
// gastrolog-51gme step 8 / gastrolog-uccg6.
func TestReconcileFromSnapshotProjectsAllSealedEntries(t *testing.T) {
	t.Parallel()

	src := vaultctlfsm.New()

	// Seed the source FSM: 3 chunks created, 2 sealed, 1 still active.
	now := time.Now()
	idSealed1 := chunk.NewChunkID()
	idSealed2 := chunk.NewChunkID()
	idActive := chunk.NewChunkID()
	for _, id := range []chunk.ChunkID{idSealed1, idSealed2, idActive} {
		_ = src.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(id, now, now, now)})
	}
	_ = src.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(idSealed1, now, 1, 1, now, now, now, false, now)})
	_ = src.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(idSealed2, now, 1, 1, now, now, now, false, now)})

	cm := &reconcilerFakeSealEnsurerChunkManager{}
	vaultInst := &VaultInstance{
		VaultID: glid.New(),
		Chunks:  cm,
	}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())

	rec.ReconcileFromSnapshot(src)

	if len(cm.ensured) != 2 {
		t.Fatalf("EnsureSealed call count = %d, want 2 (only sealed entries projected)", len(cm.ensured))
	}
	got := map[chunk.ChunkID]bool{cm.ensured[0]: true, cm.ensured[1]: true}
	if !got[idSealed1] || !got[idSealed2] {
		t.Errorf("EnsureSealed = %v, want both sealed IDs (%s, %s)", cm.ensured, idSealed1, idSealed2)
	}
	if got[idActive] {
		t.Errorf("EnsureSealed must not be called for the still-active chunk %s", idActive)
	}
}

// TestReconcileFromSnapshotProcessesPendingObligations pins the catchup
// invariant that motivated the receipt protocol in the first place: a
// node that joins (or restores) when pending deletes already exist must
// process its obligations from the FSM state alone, with no individual
// CmdRequestDelete entry to replay.
func TestReconcileFromSnapshotProcessesPendingObligations(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()

	// Seed two pending deletes — node-A owes both, node-B owes only the second.
	id1 := chunk.NewChunkID()
	id2 := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{
		Data: vaultctlfsm.MarshalRequestDelete(id1, time.Now(), "retention-ttl", []string{"node-A"}),
	})
	_ = fsm.Apply(&hraft.Log{
		Data: vaultctlfsm.MarshalRequestDelete(id2, time.Now(), "retention-ttl", []string{"node-A", "node-B"}),
	})

	cm := &reconcilerFakeChunkManager{}
	ackCh := make(chan chunk.ChunkID, 4)
	vaultInst := &VaultInstance{
		VaultID: glid.New(),
		Chunks:  cm,
		RaftApplyFacet: RaftApplyFacet{
			ApplyRaftAckDelete: func(id chunk.ChunkID, _ string) error {
				ackCh <- id
				return nil
			},
		},
	}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())

	// Reconcile from the FSM's pending state — does NOT require Wire().
	rec.ReconcileFromSnapshot(fsm)

	// ReconcileFromSnapshot dispatches the obligations on a goroutine
	// to avoid deadlocking the Raft apply-pump (which is what fires
	// the after-restore hook in production). Wait for both acks to
	// drain before asserting.
	var ackedIDs []chunk.ChunkID
	deadline := time.After(2 * time.Second)
	for len(ackedIDs) < 2 {
		select {
		case id := <-ackCh:
			ackedIDs = append(ackedIDs, id)
		case <-deadline:
			t.Fatalf("acks did not drain within deadline (got %d/2)", len(ackedIDs))
		}
	}

	if len(cm.deleted) != 2 {
		t.Errorf("expected 2 local deletes from reconcile, got %d (%v)", len(cm.deleted), cm.deleted)
	}
	if len(ackedIDs) != 2 {
		t.Errorf("expected 2 acks from reconcile, got %d (%v)", len(ackedIDs), ackedIDs)
	}
}

// TestSweepLocalOrphansDeletesOnlyTombstonedAbsentEntries pins the
// snapshot-restore catchup invariant: the orphan sweep is the only
// recovery path when a delete cycle finalized while this node was
// offline (snapshot install brings the FSM forward to "tombstone
// present, manifest absent, pendingDeletes absent" but the local
// file survived).
//
// The four safety gates — sealed locally, absent from manifest,
// absent from pendingDeletes, present in tombstones — each guard a
// distinct failure mode the sweep must NOT trip into:
//
//   - active (unsealed) chunks must be left alone (mid-rotation race)
//   - manifest-known chunks must be left alone (FSM-known live)
//   - pendingDeletes-tracked chunks must be left alone (receipt
//     protocol owns those via SweepPendingObligations)
//   - chunks WITHOUT a tombstone must be left alone (could be a
//     fresh chunk with announce in flight; deleting would lose data)
//
// The test seeds one chunk for each gate plus a positive case, runs
// the sweep, and asserts only the positive case is deleted.
func TestSweepLocalOrphansDeletesOnlyTombstonedAbsentEntries(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	cm := &reconcilerFakeChunkManager{}

	now := time.Now()

	// Case 1 (positive): tombstoned-absent. Drive the full receipt
	// protocol to commit a tombstone, then leave the local file behind.
	idTombstoned := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idTombstoned, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(idTombstoned, now, 1, 1, now, now, now, false, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalRequestDelete(idTombstoned, now, "test", []string{"node-A"})})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalAckDelete(idTombstoned, "node-A")})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalFinalizeDelete(idTombstoned)})

	// Case 2 (negative): live in manifest. Created + sealed, no deletes.
	idLiveSealed := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idLiveSealed, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(idLiveSealed, now, 1, 1, now, now, now, false, now)})

	// Case 3 (negative): pendingDeletes — receipt protocol owns it.
	idPending := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idPending, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(idPending, now, 1, 1, now, now, now, false, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalRequestDelete(idPending, now, "test", []string{"node-A"})})

	// Case 4 (negative): on disk, FSM has nothing about it (no tombstone,
	// no manifest, no pending). Could be announce-in-flight; must not delete.
	idUnknown := chunk.NewChunkID()

	// Case 5 (negative): unsealed local file. The chunk-manager fake
	// here does NOT implement chunk.SealEnsurer, so the sweep can't
	// force-demote and falls back to the safe path: log + skip. The
	// "demote-then-delete" happy path with a real SealEnsurer is
	// covered by TestSweepLocalOrphansDemotesActiveTombstonedChunk.
	// See gastrolog-533l9.
	idUnsealed := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idUnsealed, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalRequestDelete(idUnsealed, now, "test", []string{"node-A"})})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalAckDelete(idUnsealed, "node-A")})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalFinalizeDelete(idUnsealed)})

	// Seed the local chunk manager with each case as if the file is
	// still on disk regardless of FSM state.
	cm.chunks = []chunk.ChunkMeta{
		{ID: idTombstoned, Sealed: true},
		{ID: idLiveSealed, Sealed: true},
		{ID: idPending, Sealed: true},
		{ID: idUnknown, Sealed: true},
		{ID: idUnsealed, Sealed: false},
	}

	vaultInst := &VaultInstance{VaultID: glid.New(), Chunks: cm}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())
	rec.Wire(fsm)

	rec.SweepLocalOrphans()

	if len(cm.deleted) != 1 || cm.deleted[0] != idTombstoned {
		t.Errorf("orphan sweep deleted = %v, want only [%s] (tombstoned-absent positive case)",
			cm.deleted, idTombstoned)
	}
}

// TestSweepLocalOrphansPreservesDataBearingUnknownOrphans pins the
// no-auto-delete-of-unknown-orphans invariant from
// docs/disk-authority-audit.md / gastrolog-3y8py. A sealed chunk with
// real records but no FSM record, no tombstone, and no pendingDelete
// is exactly the recovery surface FSM-glitch scenarios need preserved.
// The sweep must alert (the alert side is exercised in
// TestAlertUnknownOrphanRaisesAlert below) and MUST NOT delete.
//
// Distinct from idUnknown in the previous test, which has
// RecordCount=0 and WriteEnd zero — that's an announce-in-flight or
// rotation artifact, handled by SweepLocalOrphans's rotation-ghost
// branch (gastrolog-66b7x). Data-bearing chunks are different.
func TestSweepLocalOrphansPreservesDataBearingUnknownOrphans(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	cm := &reconcilerFakeChunkManager{}
	now := time.Now()

	// Sealed locally, records on disk, FSM doesn't recognize it
	// (no Create, no Seal, no Delete, no tombstone).
	idUnknown := chunk.NewChunkID()

	cm.chunks = []chunk.ChunkMeta{
		{
			ID:          idUnknown,
			Sealed:      true,
			RecordCount: 42,                      // load-bearing: > 0 triggers the preserve path
			WriteEnd:    now.Add(-1 * time.Hour), // old enough to be a "ghost" if records were 0
		},
	}

	vaultInst := &VaultInstance{VaultID: glid.New(), Chunks: cm}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())
	rec.Wire(fsm)

	rec.SweepLocalOrphans()

	if len(cm.deleted) != 0 {
		t.Errorf("expected unknown orphan with records to be preserved, but sweep deleted %v",
			cm.deleted)
	}
}

// TestSweepLocalOrphansDeletesEmptyRotationGhost verifies the
// rotation-artifact branch still fires for chunks with zero records
// (gastrolog-66b7x). Required because the split in
// gastrolog-3y8py is "preserve data, delete artifacts" — and the
// artifact path must not regress under the new guard.
func TestSweepLocalOrphansDeletesEmptyRotationGhost(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	cm := &reconcilerFakeChunkManager{}
	now := time.Now()

	idGhost := chunk.NewChunkID()

	cm.chunks = []chunk.ChunkMeta{
		{
			ID:          idGhost,
			Sealed:      true,
			RecordCount: 0,                       // artifact: never received records
			WriteEnd:    now.Add(-1 * time.Hour), // old enough to be past the ghost threshold
		},
	}

	vaultInst := &VaultInstance{VaultID: glid.New(), Chunks: cm}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())
	rec.Wire(fsm)

	rec.SweepLocalOrphans()

	if len(cm.deleted) != 1 || cm.deleted[0] != idGhost {
		t.Errorf("expected empty rotation ghost to be deleted; got deleted=%v", cm.deleted)
	}
}

// TestSweepMissingReplicasRequestsOnlySealedAndAbsentEntries pins the
// invariant that the missing-replica sweep filters the FSM-vs-disk diff
// to exactly the chunks a follower is allowed to request: sealed, not
// cloud-backed, present in the FSM, missing locally. Active chunks,
// cloud-backed chunks, and chunks already on disk must be excluded.
//
// Each gate represents a distinct failure mode the sweep must NOT trip:
//   - active (unsealed) entries lack a stable on-disk identity, so we
//     must not chase them across the wire mid-rotation
//   - cloud-backed chunks live in shared object storage; pulling
//     records to a follower's local disk would defeat the cloud-instance
//     contract and waste bandwidth
//   - chunks already present locally are not missing — re-requesting
//     would create unbounded re-push amplification on every sweep tick
func TestSweepMissingReplicasRequestsOnlySealedAndAbsentEntries(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	cm := &reconcilerFakeChunkManager{}
	now := time.Now()

	// Case 1 (positive): sealed in FSM, missing on disk → must be requested.
	idMissing := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idMissing, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(idMissing, now, 1, 1, now, now, now, false, now)})

	// Case 2 (negative): sealed in FSM, present locally → must NOT be requested.
	idPresent := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idPresent, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(idPresent, now, 1, 1, now, now, now, false, now)})

	// Case 3 (negative): in FSM but unsealed (active) → must NOT be requested.
	idActive := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idActive, now, now, now)})

	// Case 4 (negative): sealed and cloud-backed → must NOT be requested
	// (lives in shared bucket; not a local-replica concern).
	idCloud := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idCloud, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(idCloud, now, 1, 1, now, now, now, false, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalUploadChunk(idCloud, 1, 0, 0, 0, 0, [32]byte{}, glid.GLID{}, 0)})

	cm.chunks = []chunk.ChunkMeta{
		{ID: idPresent, Sealed: true},
	}

	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	fake := &captureCatchupReplicator{scheduledRet: 1}
	orch.SetChunkReplicator(fake)

	vaultInst := &VaultInstance{
		VaultID:      glid.New(),
		Type:         "memory",
		Chunks:       cm,
		IsFollower:   true,
		LeaderNodeID: "node-leader",
	}
	rec := NewVaultLifecycleReconciler(orch, glid.New(), vaultInst, "node-A", slog.Default())
	rec.Wire(fsm)

	rec.SweepMissingReplicas()

	if fake.calls.Load() != 1 {
		t.Fatalf("RequestReplicaCatchup call count = %d, want 1", fake.calls.Load())
	}
	if len(fake.lastChunks) != 1 || fake.lastChunks[0] != idMissing {
		t.Errorf("requested chunks = %v, want only [%s] (sealed-and-missing positive case)",
			fake.lastChunks, idMissing)
	}
	if fake.lastLeader != "node-leader" {
		t.Errorf("leader = %q, want %q", fake.lastLeader, "node-leader")
	}
	if fake.lastRequester != "node-A" {
		t.Errorf("requester = %q, want %q", fake.lastRequester, "node-A")
	}
}

// SweepMissingReplicas must not request the entire missing set in one tick —
// batching keeps catchup from storming the replication streams.
func TestSweepMissingReplicasBatchesCatchupRequests(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	cm := &reconcilerFakeChunkManager{}
	base := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)

	const total = maxMissingReplicaCatchupPerSweep + 4
	ids := make([]chunk.ChunkID, total)
	for i := range total {
		id := chunk.NewChunkID()
		ids[i] = id
		sealedAt := base.Add(time.Duration(i) * time.Minute)
		_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(id, sealedAt, sealedAt, sealedAt)})
		_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(id, sealedAt, 1, 1, sealedAt, sealedAt, sealedAt, false, sealedAt)})
	}

	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	fake := &captureCatchupReplicator{scheduledRet: 1}
	orch.SetChunkReplicator(fake)

	vaultInst := &VaultInstance{
		VaultID:      glid.New(),
		Type:         "memory",
		Chunks:       cm,
		IsFollower:   true,
		LeaderNodeID: "node-leader",
	}
	rec := NewVaultLifecycleReconciler(orch, vaultInst.VaultID, vaultInst, "node-A", slog.Default())
	rec.Wire(fsm)

	rec.SweepMissingReplicas()

	if got := len(fake.lastChunks); got != maxMissingReplicaCatchupPerSweep {
		t.Fatalf("requested %d chunks, want batch cap %d", got, maxMissingReplicaCatchupPerSweep)
	}
	wantOldest := ids[:maxMissingReplicaCatchupPerSweep]
	for i, id := range wantOldest {
		if fake.lastChunks[i] != id {
			t.Errorf("batch[%d] = %s, want oldest %s", i, fake.lastChunks[i], id)
		}
	}
}

// gastrolog-19241: when leadership transfers to a node that doesn't
// have historical sealed chunks (e.g. a scale-out joiner that became
// leader), SweepMissingReplicas on the LEADER must ask its follower
// targets to push the missing chunks back. Without this, the new
// leader is permanently under-replicated until the stale-fsm sweep
// deletes the chunks as "unrecoverable" — silent data loss.
//
// This test pins the leader-side direction of the now-symmetric peer-
// to-peer catchup: leader has empty disk, FollowerTargets enumerates
// two peers, the sweep must dial both.
func TestSweepMissingReplicasFromLeaderAsksEveryFollower(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	cm := &reconcilerFakeChunkManager{}
	now := time.Now()

	idMissing := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idMissing, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(idMissing, now, 1, 1, now, now, now, false, now)})

	orch := newTestOrch(t, Config{LocalNodeID: "node-leader"})
	fake := &captureCatchupReplicator{scheduledRet: 1}
	orch.SetChunkReplicator(fake)

	vaultInst := &VaultInstance{
		VaultID:    glid.New(),
		Type:       "memory",
		Chunks:     cm,
		IsFollower: false, // this node IS the leader
		FollowerTargets: []system.ReplicationTarget{
			{NodeID: "node-follower-1"},
			{NodeID: "node-follower-2"},
		},
	}
	rec := NewVaultLifecycleReconciler(orch, glid.New(), vaultInst, "node-leader", slog.Default())
	rec.Wire(fsm)

	rec.SweepMissingReplicas()

	if got := fake.calls.Load(); got != 2 {
		t.Fatalf("leader must request catchup from every follower target, got %d call(s)", got)
	}
	// Last-call wins on the recorder; that's fine — both peers were called
	// with the same chunk set, and the test asserts the count + the chunk
	// identity. (Per-call ordering is exercised separately if it matters.)
	if len(fake.lastChunks) != 1 || fake.lastChunks[0] != idMissing {
		t.Errorf("requested chunks = %v, want only [%s]", fake.lastChunks, idMissing)
	}
	if fake.lastRequester != "node-leader" {
		t.Errorf("requester = %q, want %q", fake.lastRequester, "node-leader")
	}
}

// gastrolog-19241: a leader with no FollowerTargets (single-node
// placement, or placement just collapsed mid-failover) must not dial
// anywhere. The next placement tick will re-populate FollowerTargets.
func TestSweepMissingReplicasFromLeaderWithNoFollowersIsNoOp(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	cm := &reconcilerFakeChunkManager{}
	now := time.Now()

	idMissing := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idMissing, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(idMissing, now, 1, 1, now, now, now, false, now)})

	orch := newTestOrch(t, Config{LocalNodeID: "node-leader"})
	fake := &captureCatchupReplicator{}
	orch.SetChunkReplicator(fake)

	vaultInst := &VaultInstance{
		VaultID:         glid.New(),
		Type:            "memory",
		Chunks:          cm,
		IsFollower:      false,
		FollowerTargets: nil, // RF=1, or placement in flux
	}
	rec := NewVaultLifecycleReconciler(orch, glid.New(), vaultInst, "node-leader", slog.Default())
	rec.Wire(fsm)

	rec.SweepMissingReplicas()

	if got := fake.calls.Load(); got != 0 {
		t.Errorf("leader with no follower targets must not dial; got %d call(s)", got)
	}
}

// gastrolog-19241: a transient failure to reach one peer must not
// short-circuit the sweep. The leader keeps dialing remaining peers;
// the next sweep tick retries the failed one. This is the failure-path
// mirror of TestSweepMissingReplicasFromLeaderAsksEveryFollower.
func TestSweepMissingReplicasFromLeaderContinuesPastPeerError(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	cm := &reconcilerFakeChunkManager{}
	now := time.Now()

	idMissing := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idMissing, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(idMissing, now, 1, 1, now, now, now, false, now)})

	orch := newTestOrch(t, Config{LocalNodeID: "node-leader"})
	fake := &captureCatchupReplicator{scheduledRet: 1, failNextWith: errors.New("transient")}
	orch.SetChunkReplicator(fake)

	vaultInst := &VaultInstance{
		VaultID:    glid.New(),
		Type:       "memory",
		Chunks:     cm,
		IsFollower: false,
		FollowerTargets: []system.ReplicationTarget{
			{NodeID: "node-follower-1"}, // first call fails
			{NodeID: "node-follower-2"}, // sweep must still try this one
		},
	}
	rec := NewVaultLifecycleReconciler(orch, glid.New(), vaultInst, "node-leader", slog.Default())
	rec.Wire(fsm)

	rec.SweepMissingReplicas()

	if got := fake.calls.Load(); got != 2 {
		t.Errorf("after one peer error, sweep must still dial the rest; got %d call(s)", got)
	}
}

// FollowerTargets containing the leader's own ID (a placement-state
// edge during reconfiguration) must be filtered out — the leader must
// not ask itself.
func TestSweepMissingReplicasFromLeaderSkipsSelfInFollowerTargets(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	cm := &reconcilerFakeChunkManager{}
	now := time.Now()

	idMissing := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idMissing, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(idMissing, now, 1, 1, now, now, now, false, now)})

	orch := newTestOrch(t, Config{LocalNodeID: "node-leader"})
	fake := &captureCatchupReplicator{scheduledRet: 1}
	orch.SetChunkReplicator(fake)

	vaultInst := &VaultInstance{
		VaultID:    glid.New(),
		Type:       "memory",
		Chunks:     cm,
		IsFollower: false,
		FollowerTargets: []system.ReplicationTarget{
			{NodeID: "node-leader"}, // self — must be skipped
			{NodeID: "node-follower-1"},
		},
	}
	rec := NewVaultLifecycleReconciler(orch, glid.New(), vaultInst, "node-leader", slog.Default())
	rec.Wire(fsm)

	rec.SweepMissingReplicas()

	if got := fake.calls.Load(); got != 1 {
		t.Errorf("must skip self in FollowerTargets, got %d call(s)", got)
	}
}

// TestSweepMissingReplicasSkipsWhenLeaderUnknown pins the early-exit
// when LeaderNodeID is empty. This happens during placement transitions
// where a follower has lost its leader (election in progress, leader
// just demoted) — sending a catchup request would land on no one.
// The next sweep tick runs after the new leader is observed.
func TestSweepMissingReplicasSkipsWhenLeaderUnknown(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	cm := &reconcilerFakeChunkManager{}
	now := time.Now()

	idMissing := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idMissing, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(idMissing, now, 1, 1, now, now, now, false, now)})

	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	fake := &captureCatchupReplicator{}
	orch.SetChunkReplicator(fake)

	vaultInst := &VaultInstance{
		VaultID:      glid.New(),
		Type:         "memory",
		Chunks:       cm,
		IsFollower:   true,
		LeaderNodeID: "", // unknown — election in progress
	}
	rec := NewVaultLifecycleReconciler(orch, glid.New(), vaultInst, "node-A", slog.Default())
	rec.Wire(fsm)

	rec.SweepMissingReplicas()

	if fake.calls.Load() != 0 {
		t.Errorf("must not request when leader unknown, got %d call(s)", fake.calls.Load())
	}
}

// fakeSealEnsurerThatDemotesActive is a SealEnsurer fake that mimics
// the real Manager's force-demote semantics: when EnsureSealed is
// called for the chunk recorded as "active", that chunk transitions
// to sealed (active cleared) so a subsequent Delete on the same
// chunk doesn't return ErrActiveChunk. Used to test that
// fulfillObligation calls EnsureSealed BEFORE deleteLocalCopy so the
// receipt-protocol delete path handles still-active chunks cleanly.
type fakeSealEnsurerThatDemotesActive struct {
	retentionFakeChunkManager
	activeID         chunk.ChunkID // chunk currently "active"; cleared on EnsureSealed
	ensureSealedSeen []chunk.ChunkID
}

func (f *fakeSealEnsurerThatDemotesActive) EnsureSealed(id chunk.ChunkID) error {
	f.ensureSealedSeen = append(f.ensureSealedSeen, id)
	if f.activeID == id {
		// Demote: remove from "active" so subsequent Delete succeeds.
		f.activeID = chunk.ChunkID{}
	}
	return nil
}

func (f *fakeSealEnsurerThatDemotesActive) Delete(id chunk.ChunkID) error {
	if f.activeID == id {
		return chunk.ErrActiveChunk
	}
	f.deleted = append(f.deleted, id)
	return nil
}

// TestFulfillObligationDemotesLocalActiveBeforeDelete pins
// gastrolog-2yeht: the receipt-protocol delete obligation MUST call
// EnsureSealed before deleteLocalCopy so a chunk that's still local
// active on a follower (downstream instance with no continuous record
// stream → no natural active swap) gets force-demoted and then
// deleted, instead of bouncing off ErrActiveChunk every periodic
// sweep tick.
//
// Pre-fix: fulfillObligation called deleteLocalCopy directly;
// receipt protocol stuck forever on vaults with no record stream
// because deleteInternal returned ErrActiveChunk.
//
// Post-fix: fulfillObligation calls EnsureSealed first; the
// EnsureSealed contract demotes local-active chunks; deleteLocalCopy
// then succeeds because the chunk is no longer active; the ack
// fires; finalize lands; orphan sweep can clean up downstream.
func TestFulfillObligationDemotesLocalActiveBeforeDelete(t *testing.T) {
	t.Parallel()

	chunkID := chunk.NewChunkID()

	cm := &fakeSealEnsurerThatDemotesActive{
		activeID: chunkID, // simulate a stuck-active chunk on a downstream-instance follower
	}

	var ackedID chunk.ChunkID
	var ackCount atomic.Int32
	vaultInst := &VaultInstance{
		VaultID: glid.New(),
		Chunks:  cm,
		RaftApplyFacet: RaftApplyFacet{
			ApplyRaftAckDelete: func(id chunk.ChunkID, _ string) error {
				ackedID = id
				ackCount.Add(1)
				return nil
			},
		},
	}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())

	rec.fulfillObligation(chunkID, "retention-ttl", "test")

	// Acceptance #1: EnsureSealed was called before Delete (otherwise
	// Delete would have returned ErrActiveChunk and we'd never get
	// here).
	if len(cm.ensureSealedSeen) != 1 || cm.ensureSealedSeen[0] != chunkID {
		t.Errorf("EnsureSealed calls = %v, want exactly [%s]", cm.ensureSealedSeen, chunkID)
	}

	// Acceptance #2: Delete succeeded (chunk was demoted; not active anymore).
	if len(cm.deleted) != 1 || cm.deleted[0] != chunkID {
		t.Errorf("deleted = %v, want [%s]", cm.deleted, chunkID)
	}

	// Acceptance #3: Ack fired — the obligation fulfilled cleanly.
	if ackCount.Load() != 1 || ackedID != chunkID {
		t.Errorf("ack count = %d, id = %s; want 1, %s", ackCount.Load(), ackedID, chunkID)
	}
}

// TestSweepLocalOrphansDemotesActiveTombstonedChunk pins
// gastrolog-533l9: when a chunk is the local Manager's active
// pointer AND the FSM has only a tombstone for it (no manifest
// entry, no pendingDeletes entry), SweepLocalOrphans must
// force-demote the active first via EnsureSealed and then delete
// the local files. Failure mode: a node SIGBUS-crashes with chunk
// X active; while offline, the cluster seals → retention-deletes
// → finalizes X; node restarts; FSM has only the tombstone; pre-
// fix the orphan sweep skipped X because it was !Sealed locally.
//
// The pre-fix orphan sweep only handled sealed-on-disk chunks
// (the snapshot-restore-after-finalize case). The post-fix sweep
// also handles the local-active-after-finalize case by demoting
// first.
func TestSweepLocalOrphansDemotesActiveTombstonedChunk(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	now := time.Now()

	// Drive the FSM to: chunk fully finalized, leaving only a
	// tombstone (no manifest entry, no pendingDeletes entry).
	idTombstoned := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idTombstoned, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(idTombstoned, now, 1, 1, now, now, now, false, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalRequestDelete(idTombstoned, now, "test", []string{"node-A"})})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalAckDelete(idTombstoned, "node-A")})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalFinalizeDelete(idTombstoned)})

	// Local state: chunk is the active pointer (unsealed on disk).
	// fakeSealEnsurerThatDemotesActive mimics real-Manager
	// EnsureSealed semantics: demotes the active to sealed.
	cm := &fakeSealEnsurerThatDemotesActive{
		activeID: idTombstoned,
	}
	cm.chunks = []chunk.ChunkMeta{
		{ID: idTombstoned, Sealed: false}, // active = unsealed
	}

	vaultInst := &VaultInstance{VaultID: glid.New(), Chunks: cm}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())
	rec.Wire(fsm)

	rec.SweepLocalOrphans()

	// EnsureSealed was called (demote pre-step) before Delete.
	if len(cm.ensureSealedSeen) != 1 || cm.ensureSealedSeen[0] != idTombstoned {
		t.Errorf("EnsureSealed calls = %v, want exactly [%s]", cm.ensureSealedSeen, idTombstoned)
	}
	// Delete was called and succeeded (chunk no longer active after demote).
	if len(cm.deleted) != 1 || cm.deleted[0] != idTombstoned {
		t.Errorf("deleted = %v, want [%s]", cm.deleted, idTombstoned)
	}
}

// ---------- gastrolog-2ob86: WatchChunks signal on follower-side events ----------

// recordingSilentDeleter implements chunk.SilentDeleter on top of the
// shared fake chunk manager so the rest of chunk.ChunkManager is
// satisfied by embedding. Used by gastrolog-2ob86 tests that need to
// observe wireVaultFSMOnDelete's local-delete behavior alongside the
// orchestrator-level signal.
type recordingSilentDeleter struct {
	retentionFakeChunkManager
	silentDeleted []chunk.ChunkID
	failNext      error
}

func (r *recordingSilentDeleter) DeleteSilent(id chunk.ChunkID) error {
	r.silentDeleted = append(r.silentDeleted, id)
	if r.failNext != nil {
		err := r.failNext
		r.failNext = nil
		return err
	}
	return nil
}

// waitForChunkSignal blocks until the orchestrator's chunk signal fires
// or the timeout elapses. Returns true on signal, false on timeout.
func waitForChunkSignal(ch <-chan struct{}, timeout time.Duration) bool {
	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		return false
	}
}

// startThrottleForTest runs the orchestrator's progress-throttle
// goroutine for the duration of the test. NotifyChunkChange enqueues
// to progressTrigger; without this goroutine, the chunkSignal never
// fans out and any test asserting the signal will hang. Uses a tight
// 10ms window so leading-edge fires land promptly within test
// timeouts. See gastrolog-4y03v.
func startThrottleForTest(t *testing.T, orch *Orchestrator) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go orch.runProgressNotifier(ctx, 10*time.Millisecond)
}

// TestReconcilerOnSealNotifiesChunkChange pins the gastrolog-2ob86 fix:
// when CmdSealChunk applies on this node (originating from any node in
// the cluster), the WatchChunks signal must fire so subscribers refetch.
// Pre-fix the FSM seal projected to the local Manager but the inspector
// view never knew about the seal, leaving follower caches stale.
func TestReconcilerOnSealNotifiesChunkChange(t *testing.T) {
	t.Parallel()

	orch, err := New(Config{LocalNodeID: "node-A"})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	startThrottleForTest(t, orch)
	signalCh := orch.ChunkSignal().C()

	fsm := vaultctlfsm.New()
	cm := &reconcilerFakeSealEnsurerChunkManager{}
	vaultInst := &VaultInstance{VaultID: glid.New(), Chunks: cm}
	rec := NewVaultLifecycleReconciler(orch, glid.New(), vaultInst, "node-A", slog.Default())
	rec.Wire(fsm)

	id := chunk.NewChunkID()
	now := time.Now()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(id, now, now, now)}); err != nil {
		t.Fatalf("apply create: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(id, now, 100, 1234, now, now, now, false, now)}); err != nil {
		t.Fatalf("apply seal: %v", err)
	}

	if !waitForChunkSignal(signalCh, time.Second) {
		t.Fatal("expected chunk signal after CmdSealChunk apply, got timeout")
	}
	if len(cm.ensured) != 1 || cm.ensured[0] != id {
		t.Errorf("EnsureSealed = %v, want [%s] (signal must not gate state projection)", cm.ensured, id)
	}
}

// reconcilerFailEnsurerChunkManager forces EnsureSealed to fail so the
// test can pin the invariant that the WatchChunks signal still fires
// when the local on-disk projection cannot be applied. The FSM is
// authoritative about whether the chunk is sealed, not the on-disk
// header — so the inspector must refetch regardless of local outcome.
type reconcilerFailEnsurerChunkManager struct {
	retentionFakeChunkManager
	ensureErr error
}

func (f *reconcilerFailEnsurerChunkManager) EnsureSealed(chunk.ChunkID) error {
	return f.ensureErr
}

// TestReconcilerOnSealNotifiesEvenWhenEnsureSealedFails pins that the
// signal fires unconditionally. EnsureSealed errors are logged and the
// FSM apply moves on; the inspector view must still refresh because the
// FSM's authoritative seal flag flipped regardless of what the local
// chunk file looks like. See gastrolog-2ob86.
func TestReconcilerOnSealNotifiesEvenWhenEnsureSealedFails(t *testing.T) {
	t.Parallel()

	orch, err := New(Config{LocalNodeID: "node-A"})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	startThrottleForTest(t, orch)
	signalCh := orch.ChunkSignal().C()

	fsm := vaultctlfsm.New()
	cm := &reconcilerFailEnsurerChunkManager{ensureErr: errors.New("disk gone")}
	vaultInst := &VaultInstance{VaultID: glid.New(), Chunks: cm}
	rec := NewVaultLifecycleReconciler(orch, glid.New(), vaultInst, "node-A", slog.Default())
	rec.Wire(fsm)

	id := chunk.NewChunkID()
	now := time.Now()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(id, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(id, now, 1, 1, now, now, now, false, now)})

	if !waitForChunkSignal(signalCh, time.Second) {
		t.Fatal("expected chunk signal even when EnsureSealed errors, got timeout")
	}
}

// TestReconcilerOnFinalizeDeleteEmitsChunkDeleted pins that the receipt-
// protocol finalize path (CmdAckDelete draining ExpectedFrom) emits a
// typed DELETED event on every node, not just nodes that ran
// deleteLocalCopy. Without this, inspector clients connected to a node that
// only saw the chunk via ListChunks fan-out keep stale retention-pending
// rows until reload.
func TestReconcilerOnFinalizeDeleteEmitsChunkDeleted(t *testing.T) {
	t.Parallel()

	orch, err := New(Config{LocalNodeID: "node-A"})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	fsm := vaultctlfsm.New()
	vaultID := glid.New()
	vaultInst := &VaultInstance{
		VaultID: vaultID,
		Chunks:  &reconcilerFakeChunkManager{},
		RaftLeadershipFacet: RaftLeadershipFacet{
			IsRaftLeader: func() bool { return true },
		},
		RaftApplyFacet: RaftApplyFacet{
			ApplyRaftAckDelete: func(_ chunk.ChunkID, _ string) error { return nil },
		},
	}
	rec := NewVaultLifecycleReconciler(orch, vaultID, vaultInst, "node-A", slog.Default())
	rec.Wire(fsm)

	chunkID := chunk.NewChunkID()
	now := time.Now()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(chunkID, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(chunkID, now, 1, 1, now, now, now, false, now)})
	_ = fsm.Apply(&hraft.Log{
		Data: vaultctlfsm.MarshalRequestDelete(chunkID, now, "retention-ttl", []string{"node-B"}),
	})

	bus := orch.ChunkBus()
	subID, events, _ := bus.Subscribe()
	defer bus.Unsubscribe(subID)

	// node-A is not in ExpectedFrom — simulates a node that only rendered
	// the chunk via cluster-wide ListChunks, not local storage.
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalAckDelete(chunkID, "node-B")})

	select {
	case msg := <-events:
		if msg.Event.Op != ChunkChangeOpDeleted {
			t.Fatalf("event op = %v, want Deleted", msg.Event.Op)
		}
		if msg.Event.ChunkID != chunkID {
			t.Fatalf("event chunk = %s, want %s", msg.Event.ChunkID, chunkID)
		}
		if msg.Event.VaultID != vaultID {
			t.Fatalf("event vault = %s, want %s", msg.Event.VaultID, vaultID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for DELETED event on finalize")
	}
}

// TestWireInstanceFSMOnUploadFiresNotifyChunkChange pins that follower
// nodes, on receiving a CmdUploadChunk via Raft (the leader's
// AnnounceUpload propagated through), refresh their inspector view.
// Pre-fix the cloud-backed transition was invisible until manual
// reload. See gastrolog-2ob86. (Cloud-index registration is no longer an
// onUpload effect — the chunk manager's lazy cloud-backed resolver fills
// the index from the FSM at first lookup; gastrolog-5bnxc.)
func TestWireInstanceFSMOnUploadFiresNotifyChunkChange(t *testing.T) {
	t.Parallel()

	orch, err := New(Config{LocalNodeID: "node-A"})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	startThrottleForTest(t, orch)
	signalCh := orch.ChunkSignal().C()

	fsm := vaultctlfsm.New()
	vaultID := glid.New()
	g := &raftgroup.Group{FSM: fsm}
	wireVaultFSMOnUpload(g, vaultID, orch)

	id := chunk.NewChunkID()
	now := time.Now()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(id, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(id, now, 1, 1, now, now, now, false, now)})
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalUploadChunk(id, 1024, 0, 0, 0, 0, [32]byte{}, glid.GLID{}, 0)}); err != nil {
		t.Fatalf("apply upload: %v", err)
	}

	if !waitForChunkSignal(signalCh, time.Second) {
		t.Fatal("expected chunk signal after CmdUploadChunk apply, got timeout")
	}
}

// TestReconcileFromSnapshotResumesSealingChunks pins the Phase 3
// crash-recovery invariant: when a leader crashes between CmdBeginSeal
// (Active → Sealing) and CmdSealChunk (Sealing → Sealed), the FSM is
// left holding a Sealing entry. After restore, the reconciler must
// re-schedule PostSealProcess so sealToGLCB completes, AnnounceSeal
// fires, and downstream steps that gate on Sealed (cloud upload,
// retention, replication catchup) can proceed.
//
// Setup: seed the FSM with three chunks — one Active (BeginSeal NOT
// applied), one Sealing (Created + BeginSeal, no SealChunk), one
// Sealed (full lifecycle). The local Manager pretends to hold all
// three with on-disk sealed flags matching the real flow:
// sealActiveLocked closes the files (sealed bit set) BEFORE the
// post-seal pipeline runs, so the Sealing chunk has Sealed=true on
// disk despite still being mid-Sealing on the FSM.
//
// Asserts: only the Sealing chunk's ID is scheduled. Active is too
// early (no GLCB to assemble); Sealed is too late (already done).
// gastrolog-1huz5.
func TestReconcileFromSnapshotResumesSealingChunks(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	now := time.Now()

	idActive := chunk.NewChunkID()
	idSealing := chunk.NewChunkID()
	idSealed := chunk.NewChunkID()
	for _, id := range []chunk.ChunkID{idActive, idSealing, idSealed} {
		if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(id, now, now, now)}); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalBeginSeal(idSealing)}); err != nil {
		t.Fatalf("begin-seal sealing: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalBeginSeal(idSealed)}); err != nil {
		t.Fatalf("begin-seal sealed: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(idSealed, now, 1, 1, now, now, now, false, now)}); err != nil {
		t.Fatalf("seal-chunk sealed: %v", err)
	}

	cm := &reconcilerFakeChunkManager{}
	cm.chunks = []chunk.ChunkMeta{
		{ID: idActive, Sealed: false},
		{ID: idSealing, Sealed: true},
		{ID: idSealed, Sealed: true},
	}

	vaultInst := &VaultInstance{
		VaultID: glid.New(),
		Chunks:  cm,
	}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())

	var scheduled []chunk.ChunkID
	rec.postSealHook = func(_ glid.GLID, _ chunk.ChunkManager, id chunk.ChunkID) {
		scheduled = append(scheduled, id)
	}

	rec.ReconcileFromSnapshot(fsm)

	if len(scheduled) != 1 {
		t.Fatalf("postSealHook calls = %d (%v), want 1 (only the Sealing chunk)", len(scheduled), scheduled)
	}
	if scheduled[0] != idSealing {
		t.Errorf("postSealHook called with %s, want %s (Sealing chunk)", scheduled[0], idSealing)
	}
}

// TestReconcileFromSnapshotSkipsSealingWithNoLocalChunk pins the
// follower-or-stranded-leader case: when the FSM has a Sealing entry
// but the local Manager doesn't hold the chunk, the reconciler must
// NOT schedule PostSealProcess — there are no active-form files to
// assemble from. Dispatching would just hand the chunk Manager an
// ID it can't find. The Sealing entry stays in the FSM until either
// a peer with the local files becomes leader and resumes it, or
// stale-fsm cleanup retires it.
func TestReconcileFromSnapshotSkipsSealingWithNoLocalChunk(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	now := time.Now()
	idSealing := chunk.NewChunkID()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idSealing, now, now, now)}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalBeginSeal(idSealing)}); err != nil {
		t.Fatalf("begin-seal: %v", err)
	}

	cm := &reconcilerFakeChunkManager{}
	vaultInst := &VaultInstance{
		VaultID: glid.New(),
		Chunks:  cm,
	}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())

	var calls int
	rec.postSealHook = func(_ glid.GLID, _ chunk.ChunkManager, _ chunk.ChunkID) {
		calls++
	}

	rec.ReconcileFromSnapshot(fsm)

	if calls != 0 {
		t.Errorf("postSealHook called %d times, want 0 (no local chunk to resume from)", calls)
	}
}

// TestReconcileFromSnapshotSkipsSealingWithUnsealedLocalChunk pins
// the deeper-bug case: a Sealing FSM entry whose local on-disk chunk
// hasn't actually been sealed (active-form flush didn't complete or
// the seal flag rewrite was lost). PostSealProcess would error with
// ErrChunkNotSealed — silently skip rather than fire a confusing
// pipeline failure. Logged at warn so the inconsistency is visible.
func TestReconcileFromSnapshotSkipsSealingWithUnsealedLocalChunk(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	now := time.Now()
	idSealing := chunk.NewChunkID()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idSealing, now, now, now)}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalBeginSeal(idSealing)}); err != nil {
		t.Fatalf("begin-seal: %v", err)
	}

	cm := &reconcilerFakeChunkManager{}
	cm.chunks = []chunk.ChunkMeta{{ID: idSealing, Sealed: false}}
	vaultInst := &VaultInstance{
		VaultID: glid.New(),
		Chunks:  cm,
	}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())

	var calls int
	rec.postSealHook = func(_ glid.GLID, _ chunk.ChunkManager, _ chunk.ChunkID) {
		calls++
	}

	rec.ReconcileFromSnapshot(fsm)

	if calls != 0 {
		t.Errorf("postSealHook called %d times, want 0 (local chunk unsealed)", calls)
	}
}

// TestSweepStaleLeaderFSMEntriesProposesDeleteForStrandedSealingChunk pins
// the Phase 3 (gastrolog-1huz5) follow-on for SweepStaleLeaderFSMEntries:
// when the FSM holds a Sealing entry whose chunk this leader does not have
// locally — the classic "leader transferred mid-PostSealProcess" case in
// 1:1:1 placement — the sweep must propose CmdRequestDelete after grace
// period, not skip it. Without this, a stranded Sealing entry would sit in
// the FSM forever, blocking nothing useful but accumulating as garbage.
//
// Setup seeds three chunks with carefully aged WriteStarts so the grace
// period anchors are easy to reason about:
//   - sealedFresh: Sealed, just sealed → must skip (within grace period)
//   - sealedStale: Sealed, past grace → must propose delete (positive control)
//   - sealingStranded: Sealing, past grace, no local files → must propose delete
//   - activeStranded: Active, past grace, no local files → must skip (Active
//     is not the sweep's responsibility; ingest reroute handles it)
func TestSweepStaleLeaderFSMEntriesProposesDeleteForStrandedSealingChunk(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	now := time.Now()
	old := now.Add(-2 * time.Hour) // well past the 1h grace period

	idSealedFresh := chunk.NewChunkID()
	idSealedStale := chunk.NewChunkID()
	idSealingStranded := chunk.NewChunkID()
	idActiveStranded := chunk.NewChunkID()

	// Fresh Sealed: created and sealed now.
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idSealedFresh, now, now, now)}); err != nil {
		t.Fatalf("create fresh sealed: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(idSealedFresh, now, 1, 1, now, now, now, false, now)}); err != nil {
		t.Fatalf("seal fresh: %v", err)
	}

	// Stale Sealed: created and sealed 2h ago.
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idSealedStale, old, old, old)}); err != nil {
		t.Fatalf("create stale sealed: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(idSealedStale, old, 1, 1, old, old, old, false, old)}); err != nil {
		t.Fatalf("seal stale: %v", err)
	}

	// Stranded Sealing: created 2h ago, BeginSeal applied, no SealChunk
	// (PostSealProcess never finished — leader transferred away).
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idSealingStranded, old, old, old)}); err != nil {
		t.Fatalf("create stranded sealing: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalBeginSeal(idSealingStranded)}); err != nil {
		t.Fatalf("begin-seal stranded: %v", err)
	}

	// Stranded Active: created 2h ago, no BeginSeal yet.
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idActiveStranded, old, old, old)}); err != nil {
		t.Fatalf("create stranded active: %v", err)
	}

	// Local chunk Manager has only the fresh Sealed chunk (the leader
	// for that chunk is this node and the seal recently completed).
	cm := &reconcilerFakeChunkManager{}
	cm.chunks = []chunk.ChunkMeta{{ID: idSealedFresh, Sealed: true}}

	var deletedRequests []chunk.ChunkID
	var deleteReasons []string
	vaultInst := &VaultInstance{
		VaultID:    glid.New(),
		Chunks:     cm,
		IsFollower: false,
		RaftApplyFacet: RaftApplyFacet{
			ApplyRaftRequestDelete: func(id chunk.ChunkID, reason string, _ []string) error {
				deletedRequests = append(deletedRequests, id)
				deleteReasons = append(deleteReasons, reason)
				return nil
			},
		},
	}

	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())
	rec.fsm = fsm

	rec.SweepStaleLeaderFSMEntries()

	if len(deletedRequests) != 2 {
		t.Fatalf("delete proposals = %d (%v), want 2 (sealedStale + sealingStranded)",
			len(deletedRequests), deletedRequests)
	}
	got := map[chunk.ChunkID]bool{deletedRequests[0]: true, deletedRequests[1]: true}
	if !got[idSealedStale] {
		t.Errorf("expected delete proposal for stale Sealed chunk %s; got %v", idSealedStale, deletedRequests)
	}
	if !got[idSealingStranded] {
		t.Errorf("expected delete proposal for stranded Sealing chunk %s; got %v", idSealingStranded, deletedRequests)
	}
	if got[idSealedFresh] {
		t.Errorf("must NOT propose delete for fresh Sealed chunk %s (within grace period)", idSealedFresh)
	}
	if got[idActiveStranded] {
		t.Errorf("must NOT propose delete for stranded Active chunk %s (Active is out of scope)", idActiveStranded)
	}
	for _, reason := range deleteReasons {
		if reason != "stale-fsm-leader-missing" {
			t.Errorf("delete reason = %q, want stale-fsm-leader-missing", reason)
		}
	}
}

func TestSweepStaleLeaderFSMEntriesSkipsPipelineVault(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	old := time.Now().Add(-2 * time.Hour)
	id := chunk.NewChunkID()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(id, old, old, old)}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(id, old, 1, 1, old, old, old, false, old)}); err != nil {
		t.Fatalf("seal: %v", err)
	}

	vaultID := glid.New()
	orch := &Orchestrator{segmentsDir: t.TempDir()}
	orch.setPipelineVaultLocked(vaultID, pipelineVaultReg{home: true, hasHandle: true})
	var deleted []chunk.ChunkID
	vaultInst := &VaultInstance{
		VaultID:    vaultID,
		Chunks:     &reconcilerFakeChunkManager{},
		IsFollower: false,
		RaftLeadershipFacet: RaftLeadershipFacet{
			HasRaftLeader: func() bool { return true },
		},
		RaftApplyFacet: RaftApplyFacet{
			ApplyRaftRequestDelete: func(id chunk.ChunkID, _ string, _ []string) error {
				deleted = append(deleted, id)
				return nil
			},
		},
	}
	rec := NewVaultLifecycleReconciler(orch, vaultID, vaultInst, "node-A", slog.Default())
	rec.fsm = fsm

	rec.SweepStaleLeaderFSMEntries()

	if len(deleted) != 0 {
		t.Fatalf("pipeline vault must skip stale-fsm sweep; got deletes %v", deleted)
	}
}

func TestSweepStaleLeaderFSMEntriesRespectsSealedAtGrace(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	now := time.Now()
	oldWrite := now.Add(-3 * time.Hour)
	recentSeal := now.Add(-10 * time.Minute)
	id := chunk.NewChunkID()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(id, oldWrite, oldWrite, oldWrite)}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(id, oldWrite, 1, 1, oldWrite, oldWrite, oldWrite, false, recentSeal)}); err != nil {
		t.Fatalf("seal: %v", err)
	}

	var deleted []chunk.ChunkID
	vaultInst := &VaultInstance{
		VaultID:    glid.New(),
		Chunks:     &reconcilerFakeChunkManager{},
		IsFollower: false,
		RaftLeadershipFacet: RaftLeadershipFacet{
			HasRaftLeader: func() bool { return true },
		},
		RaftApplyFacet: RaftApplyFacet{
			ApplyRaftRequestDelete: func(id chunk.ChunkID, _ string, _ []string) error {
				deleted = append(deleted, id)
				return nil
			},
		},
	}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())
	rec.fsm = fsm

	rec.SweepStaleLeaderFSMEntries()

	if len(deleted) != 0 {
		t.Fatalf("recent SealedAt must keep chunk within grace despite old WriteEnd; deleted=%v", deleted)
	}
}

// TestSweepStalePendingDeleteAcksPrunesNonPlacementNodes pins the
// self-healing receipt-protocol-unstick path: after a vault placement
// change (kubectl scale, vault rebalance), pendingDelete entries can
// carry stale ExpectedFrom node IDs for nodes no longer in the
// placement set. Those nodes have no vault instance running so they
// can never ack the delete. The sweep proposes CmdPruneNode to drop
// them from every entry's ExpectedFrom; the FSM's applyPruneNode then
// atomically finalizes any entries whose ExpectedFrom drained.
//
// Setup mirrors the live K8s incident from gastrolog-2eclw-cascade-fix
// follow-up: first-vault has chunks stuck retention-pending with
// ExpectedFrom containing only stale-node, while current placement is
// {leader-node + follower-node}.
func TestSweepStalePendingDeleteAcksPrunesNonPlacementNodes(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	chunkID := chunk.NewChunkID()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(chunkID, time.Now(), time.Now(), time.Now())}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{
		Data: vaultctlfsm.MarshalRequestDelete(chunkID, time.Now(), "retention-ttl",
			[]string{"leader-node", "follower-node", "stale-node"}),
	}); err != nil {
		t.Fatalf("request delete: %v", err)
	}

	var prunedNodes []string
	vaultInst := &VaultInstance{
		VaultID:    glid.New(),
		IsFollower: false,
		FollowerTargets: []system.ReplicationTarget{
			{NodeID: "follower-node"},
		},
		RaftApplyFacet: RaftApplyFacet{
			ApplyRaftPruneNode: func(nodeID string) error {
				prunedNodes = append(prunedNodes, nodeID)
				return nil
			},
		},
	}

	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "leader-node", slog.Default())
	rec.fsm = fsm

	rec.SweepStalePendingDeleteAcks()

	if len(prunedNodes) != 1 {
		t.Fatalf("expected 1 prune proposal for stale-node, got %d (%v)", len(prunedNodes), prunedNodes)
	}
	if prunedNodes[0] != "stale-node" {
		t.Errorf("expected prune for stale-node, got %q", prunedNodes[0])
	}
}

// TestSweepStalePendingDeleteAcksSkipsCurrentPlacementMembers pins the
// negative case: nodes in the current placement (leader + followers)
// MUST NOT be pruned, even if they haven't acked yet. Live deletes
// in-flight to current placement members are the receipt protocol's
// normal operation, not stale references.
func TestSweepStalePendingDeleteAcksSkipsCurrentPlacementMembers(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	chunkID := chunk.NewChunkID()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(chunkID, time.Now(), time.Now(), time.Now())}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{
		Data: vaultctlfsm.MarshalRequestDelete(chunkID, time.Now(), "retention-ttl",
			[]string{"leader-node", "follower-node"}),
	}); err != nil {
		t.Fatalf("request delete: %v", err)
	}

	var prunedNodes []string
	vaultInst := &VaultInstance{
		VaultID:    glid.New(),
		IsFollower: false,
		FollowerTargets: []system.ReplicationTarget{
			{NodeID: "follower-node"},
		},
		RaftApplyFacet: RaftApplyFacet{
			ApplyRaftPruneNode: func(nodeID string) error {
				prunedNodes = append(prunedNodes, nodeID)
				return nil
			},
		},
	}

	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "leader-node", slog.Default())
	rec.fsm = fsm

	rec.SweepStalePendingDeleteAcks()

	if len(prunedNodes) != 0 {
		t.Errorf("MUST NOT prune current placement members; got %v", prunedNodes)
	}
}

// TestSweepStalePendingDeleteAcksFollowersAreNoOp pins the leader-only
// gate: only the vault-ctl leader should propose CmdPruneNode, mirroring
// SweepStaleLeaderFSMEntries' leader-only design. Without this, every
// node would race to propose the same prune on every sweep tick.
func TestSweepStalePendingDeleteAcksFollowersAreNoOp(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	chunkID := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(chunkID, time.Now(), time.Now(), time.Now())})
	_ = fsm.Apply(&hraft.Log{
		Data: vaultctlfsm.MarshalRequestDelete(chunkID, time.Now(), "retention-ttl",
			[]string{"leader-node", "stale-node"}),
	})

	var prunedNodes []string
	vaultInst := &VaultInstance{
		VaultID:    glid.New(),
		IsFollower: true,
		RaftApplyFacet: RaftApplyFacet{
			ApplyRaftPruneNode: func(nodeID string) error {
				prunedNodes = append(prunedNodes, nodeID)
				return nil
			},
		},
	}

	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-X", slog.Default())
	rec.fsm = fsm

	rec.SweepStalePendingDeleteAcks()

	if len(prunedNodes) != 0 {
		t.Errorf("followers MUST NOT propose CmdPruneNode; got %v", prunedNodes)
	}
}

// idleActiveSweepFakeManager extends the fake chunk manager with the
// surface the idle-active sweep needs: per-id Meta lookups, a Seal()
// counter that records which m.active was sealed, an EnsureSealed
// counter for the metadata-only-unsealed branch, and an AnnouncerGetter
// for the manual CmdSealChunk path. The announcer captures every
// AnnounceSeal call for assertion.
type idleActiveSweepFakeManager struct {
	retentionFakeChunkManager

	metas     map[chunk.ChunkID]chunk.ChunkMeta
	active    *chunk.ChunkMeta
	sealCalls []chunk.ChunkID
	ensured   []chunk.ChunkID
	announcer *captureAnnouncer
}

func (f *idleActiveSweepFakeManager) Meta(id chunk.ChunkID) (chunk.ChunkMeta, error) {
	if m, ok := f.metas[id]; ok {
		return m, nil
	}
	return chunk.ChunkMeta{}, chunk.ErrChunkNotFound
}
func (f *idleActiveSweepFakeManager) Active() *chunk.ChunkMeta {
	return f.active
}
func (f *idleActiveSweepFakeManager) Seal() error {
	if f.active == nil {
		return nil
	}
	f.sealCalls = append(f.sealCalls, f.active.ID)
	return nil
}
func (f *idleActiveSweepFakeManager) EnsureSealed(id chunk.ChunkID) error {
	f.ensured = append(f.ensured, id)
	if m, ok := f.metas[id]; ok {
		m.Sealed = true
		f.metas[id] = m
	}
	return nil
}
func (f *idleActiveSweepFakeManager) GetAnnouncer() chunk.MetadataAnnouncer {
	return f.announcer
}

// captureAnnouncer records every AnnounceSeal payload. Other methods
// are no-ops — the sweep only calls AnnounceSeal.
type captureAnnouncer struct {
	sealed []capturedSeal
}

type capturedSeal struct {
	id          chunk.ChunkID
	writeEnd    time.Time
	recordCount int64
	bytes       int64
}

func (a *captureAnnouncer) AnnounceCreate(chunk.ChunkID, time.Time, time.Time, time.Time) {}
func (a *captureAnnouncer) AnnounceBeginSeal(chunk.ChunkID)                               {}
func (a *captureAnnouncer) AnnounceSeal(id chunk.ChunkID, writeEnd time.Time, recordCount, bytes int64, _ time.Time, _ time.Time, _ time.Time, _ bool) {
	a.sealed = append(a.sealed, capturedSeal{id: id, writeEnd: writeEnd, recordCount: recordCount, bytes: bytes})
}
func (a *captureAnnouncer) AnnounceCompress(chunk.ChunkID, int64) {}
func (a *captureAnnouncer) AnnounceAttachOffsets(chunk.ChunkID, int64, int64, int64, int64) {
}
func (a *captureAnnouncer) AnnounceUpload(chunk.ChunkID, int64, int64, int64, int64, int64, [32]byte, glid.GLID, uint8) {
}

func (a *captureAnnouncer) AnnounceArchived(chunk.ChunkID, string) {}

// TestSweepIdleActiveSealsLocalActivePastThreshold pins the m.active
// branch: when an FSM-Active entry is the local m.active and has been
// idle past the threshold, the sweep calls Chunks.Seal() (which the
// chunk manager turns into AnnounceSeal internally). No manual
// announcer call from the sweep in this branch.
func TestSweepIdleActiveSealsLocalActivePastThreshold(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	now := time.Now()
	idleStart := now.Add(-2 * time.Hour)

	id := chunk.NewChunkID()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(id, idleStart, idleStart, idleStart)}); err != nil {
		t.Fatalf("create: %v", err)
	}

	staleEnd := now.Add(-30 * time.Minute) // > idleActiveThreshold (10m)
	cm := &idleActiveSweepFakeManager{
		metas: map[chunk.ChunkID]chunk.ChunkMeta{
			id: {ID: id, WriteEnd: staleEnd, RecordCount: 10, Bytes: 1024},
		},
		active:    &chunk.ChunkMeta{ID: id, WriteEnd: staleEnd, RecordCount: 10, Bytes: 1024},
		announcer: &captureAnnouncer{},
	}

	vaultInst := &VaultInstance{VaultID: glid.New(), Chunks: cm}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())
	rec.fsm = fsm

	rec.SweepIdleActiveChunks()

	if len(cm.sealCalls) != 1 || cm.sealCalls[0] != id {
		t.Errorf("expected Chunks.Seal() once for %s, got %v", id, cm.sealCalls)
	}
	if len(cm.announcer.sealed) != 0 {
		t.Errorf("m.active path must not fire AnnounceSeal directly; got %d announces", len(cm.announcer.sealed))
	}
}

// TestSweepIdleActiveSealsMetadataOnlyOrphan pins the non-m.active
// branch: when an FSM-Active entry exists locally but isn't m.active
// (multiple unsealed local chunks on startup: only the newest opens
// as m.active, the rest sit in m.metas), the sweep must EnsureSealed
// the local files AND manually fire AnnounceSeal so CmdSealChunk
// propagates cluster-wide.
func TestSweepIdleActiveSealsMetadataOnlyOrphan(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	now := time.Now()
	idleStart := now.Add(-2 * time.Hour)

	orphanID := chunk.NewChunkID()
	currentID := chunk.NewChunkID()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(orphanID, idleStart, idleStart, idleStart)}); err != nil {
		t.Fatalf("create orphan: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(currentID, now, now, now)}); err != nil {
		t.Fatalf("create current: %v", err)
	}

	staleEnd := now.Add(-30 * time.Minute)
	cm := &idleActiveSweepFakeManager{
		metas: map[chunk.ChunkID]chunk.ChunkMeta{
			orphanID:  {ID: orphanID, WriteEnd: staleEnd, RecordCount: 100, Bytes: 4096},
			currentID: {ID: currentID, WriteEnd: now, RecordCount: 1, Bytes: 32},
		},
		// m.active is the current (newest) chunk; the orphan is
		// metadata-only-unsealed in m.metas.
		active:    &chunk.ChunkMeta{ID: currentID, WriteEnd: now, RecordCount: 1, Bytes: 32},
		announcer: &captureAnnouncer{},
	}

	vaultInst := &VaultInstance{VaultID: glid.New(), Chunks: cm}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())
	rec.fsm = fsm

	rec.SweepIdleActiveChunks()

	if len(cm.sealCalls) != 0 {
		t.Errorf("metadata-only orphan must not call Chunks.Seal() (that targets m.active); got %v", cm.sealCalls)
	}
	if len(cm.ensured) != 1 || cm.ensured[0] != orphanID {
		t.Errorf("expected EnsureSealed for orphan %s, got %v", orphanID, cm.ensured)
	}
	if len(cm.announcer.sealed) != 1 || cm.announcer.sealed[0].id != orphanID {
		t.Errorf("expected manual AnnounceSeal for orphan %s, got %v", orphanID, cm.announcer.sealed)
	}
	if got := cm.announcer.sealed[0].recordCount; got != 100 {
		t.Errorf("AnnounceSeal carried recordCount=%d, want 100 (from local meta)", got)
	}
}

// TestSweepIdleActiveSkipsFreshActiveEntries pins the negative
// case — an FSM-Active chunk whose local WriteEnd is recent must NOT
// be sealed (it's a live, currently-being-appended-to chunk).
func TestSweepIdleActiveSkipsFreshActiveEntries(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	now := time.Now()

	id := chunk.NewChunkID()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(id, now, now, now)}); err != nil {
		t.Fatalf("create: %v", err)
	}

	cm := &idleActiveSweepFakeManager{
		metas: map[chunk.ChunkID]chunk.ChunkMeta{
			id: {ID: id, WriteEnd: now.Add(-30 * time.Second), RecordCount: 50, Bytes: 2048},
		},
		active:    &chunk.ChunkMeta{ID: id, WriteEnd: now.Add(-30 * time.Second), RecordCount: 50, Bytes: 2048},
		announcer: &captureAnnouncer{},
	}

	vaultInst := &VaultInstance{VaultID: glid.New(), Chunks: cm}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())
	rec.fsm = fsm

	rec.SweepIdleActiveChunks()

	if len(cm.sealCalls) != 0 {
		t.Errorf("fresh Active must not be sealed; got %v", cm.sealCalls)
	}
	if len(cm.ensured) != 0 {
		t.Errorf("fresh Active must not be EnsureSealed; got %v", cm.ensured)
	}
	if len(cm.announcer.sealed) != 0 {
		t.Errorf("fresh Active must not fire AnnounceSeal; got %v", cm.announcer.sealed)
	}
}

// TestSweepIdleActiveSkipsChunksNotHeldLocally pins the cross-node
// invariant: an FSM-Active entry whose chunk this node doesn't hold
// locally must be skipped silently. Some other holder will propose
// the seal — every node runs this sweep on every tick.
func TestSweepIdleActiveSkipsChunksNotHeldLocally(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	now := time.Now()
	idleStart := now.Add(-2 * time.Hour)

	id := chunk.NewChunkID()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(id, idleStart, idleStart, idleStart)}); err != nil {
		t.Fatalf("create: %v", err)
	}

	cm := &idleActiveSweepFakeManager{
		// Empty metas — this node doesn't hold the chunk.
		metas:     map[chunk.ChunkID]chunk.ChunkMeta{},
		active:    nil,
		announcer: &captureAnnouncer{},
	}

	vaultInst := &VaultInstance{VaultID: glid.New(), Chunks: cm}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())
	rec.fsm = fsm

	rec.SweepIdleActiveChunks()

	if len(cm.sealCalls)+len(cm.ensured)+len(cm.announcer.sealed) != 0 {
		t.Errorf("must not touch a chunk this node doesn't hold; seal=%v ensure=%v announce=%v",
			cm.sealCalls, cm.ensured, cm.announcer.sealed)
	}
}

// TestSweepIdleActiveSkipsAlreadyLocallySealedChunks pins the
// idempotency invariant that caught us in K8s: after the first sweep
// flips the local sealed flag (via EnsureSealed or sealActiveLocked),
// subsequent ticks MUST NOT re-fire AnnounceSeal or postSealWork even
// though the FSM may still say Active.
//
// Why this happens: AnnounceSeal forwards Apply to the vault-ctl
// leader. If the leader doesn't fully converge the apply (forward
// RPCs failing, "no raft leader", a stale group member etc.), the
// FSM stays Active on this node forever. Without this guard, the
// sweep re-runs the entire post-seal pipeline (sealToGLCB +
// index rebuild + replication) every 20s on each orphan — multi-GB
// memory churn per tick.
//
// gastrolog-2eclw follow-up: the first implementation shipped without
// this check and was caught live by 2866 sweep firings on a single
// chunk on gastrolog-6, peaking VmRSS at 15GB.
func TestSweepIdleActiveSkipsAlreadyLocallySealedChunks(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	now := time.Now()
	idleStart := now.Add(-2 * time.Hour)

	id := chunk.NewChunkID()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(id, idleStart, idleStart, idleStart)}); err != nil {
		t.Fatalf("create: %v", err)
	}
	// FSM stays Active — Apply did not converge on this node.

	staleEnd := now.Add(-30 * time.Minute)
	cm := &idleActiveSweepFakeManager{
		metas: map[chunk.ChunkID]chunk.ChunkMeta{
			// Crucially, Sealed=true: a prior tick already sealed it
			// locally; FSM hasn't caught up.
			id: {ID: id, WriteEnd: staleEnd, RecordCount: 50, Bytes: 2048, Sealed: true},
		},
		// Not the m.active (would otherwise hit the m.active branch).
		active:    nil,
		announcer: &captureAnnouncer{},
	}

	vaultInst := &VaultInstance{VaultID: glid.New(), Chunks: cm}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())
	rec.fsm = fsm

	rec.SweepIdleActiveChunks()

	if len(cm.sealCalls)+len(cm.ensured)+len(cm.announcer.sealed) != 0 {
		t.Errorf("locally-sealed chunk with FSM still Active MUST short-circuit; seal=%v ensure=%v announce=%v",
			cm.sealCalls, cm.ensured, cm.announcer.sealed)
	}
}

// TestSweepIdleActiveSkipsSealingAndSealedEntries pins state-filter
// scope: only state=Active is in scope. Sealing entries are recovered
// by resumeSealingFromFSM; Sealed entries are out of the seal lifecycle
// entirely.
func TestSweepIdleActiveSkipsSealingAndSealedEntries(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	now := time.Now()
	idleStart := now.Add(-2 * time.Hour)

	idSealing := chunk.NewChunkID()
	idSealed := chunk.NewChunkID()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idSealing, idleStart, idleStart, idleStart)}); err != nil {
		t.Fatalf("create sealing: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalBeginSeal(idSealing)}); err != nil {
		t.Fatalf("begin-seal: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idSealed, idleStart, idleStart, idleStart)}); err != nil {
		t.Fatalf("create sealed: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(idSealed, idleStart, 1, 1, idleStart, idleStart, idleStart, false, idleStart)}); err != nil {
		t.Fatalf("seal: %v", err)
	}

	staleEnd := now.Add(-30 * time.Minute)
	cm := &idleActiveSweepFakeManager{
		metas: map[chunk.ChunkID]chunk.ChunkMeta{
			idSealing: {ID: idSealing, WriteEnd: staleEnd},
			idSealed:  {ID: idSealed, WriteEnd: staleEnd, Sealed: true},
		},
		announcer: &captureAnnouncer{},
	}

	vaultInst := &VaultInstance{VaultID: glid.New(), Chunks: cm}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())
	rec.fsm = fsm

	rec.SweepIdleActiveChunks()

	if len(cm.sealCalls)+len(cm.ensured)+len(cm.announcer.sealed) != 0 {
		t.Errorf("non-Active entries must be skipped; seal=%v ensure=%v announce=%v",
			cm.sealCalls, cm.ensured, cm.announcer.sealed)
	}
}

// ---- gastrolog-3fu9t: event-driven reconcile wakes ----
//
// These tests pin the doctrine of gastrolog-3fu9t: each reconcile
// category that has a genuine upstream event now converges on that event,
// WITHOUT waiting for the periodic backstop tick. Each fires only the
// upstream event (ReconcileFromSnapshot for the snapshot-install edge,
// ReconcileMembershipCatchup for the lead-gained edge) and asserts the
// reconcile action happened — never calling ReconcileTick /
// vaultCatchupSweepAll. The periodic tick's own coverage stays in the
// Sweep* / ReconcileTick tests above; these prove the events are wired.

// TestReconcileFromSnapshotDeletesTombstonedLocalOrphan pins the
// local-orphan category's event source: snapshot install. A delete cycle
// that finalized while this node was offline leaves the restored FSM with
// only a tombstone (no manifest entry, no pendingDeletes) and the local
// bytes orphaned. ReconcileFromSnapshot — fired from the vault-ctl FSM's
// after-restore hook — must clean it up on that event, not on the tick.
func TestReconcileFromSnapshotDeletesTombstonedLocalOrphan(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	cm := &reconcilerFakeChunkManager{}
	now := time.Now()

	// Tombstoned-absent (positive): full receipt cycle finalized while
	// "offline"; local file survives with no obligation.
	idTombstoned := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idTombstoned, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(idTombstoned, now, 1, 1, now, now, now, false, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalRequestDelete(idTombstoned, now, "test", []string{"node-A"})})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalAckDelete(idTombstoned, "node-A")})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalFinalizeDelete(idTombstoned)})

	// Live sealed (negative control): must survive the restore reconcile.
	idLive := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idLive, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(idLive, now, 1, 1, now, now, now, false, now)})

	cm.chunks = []chunk.ChunkMeta{
		{ID: idTombstoned, Sealed: true},
		{ID: idLive, Sealed: true},
	}

	vaultInst := &VaultInstance{VaultID: glid.New(), Chunks: cm}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())
	rec.Wire(fsm)

	// Fire ONLY the snapshot-restore event. No tick, no SweepLocalOrphans.
	rec.ReconcileFromSnapshot(fsm)

	if len(cm.deleted) != 1 || cm.deleted[0] != idTombstoned {
		t.Errorf("ReconcileFromSnapshot deleted = %v, want only [%s] (tombstoned orphan cleaned on the restore event, live chunk preserved)",
			cm.deleted, idTombstoned)
	}
}

// TestReconcileMembershipCatchupPrunesStalePendingAcks pins the
// stale-pending-ack category's event source: gaining vault-ctl leadership
// (ReconcileMembershipCatchup, wired to onVaultCtlLeadGained). A leader
// that inherits a pendingDelete whose ExpectedFrom references a node
// dropped from placement must prune it on the leadership edge, not wait
// for the periodic backstop.
func TestReconcileMembershipCatchupPrunesStalePendingAcks(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	chunkID := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(chunkID, time.Now(), time.Now(), time.Now())})
	_ = fsm.Apply(&hraft.Log{
		Data: vaultctlfsm.MarshalRequestDelete(chunkID, time.Now(), "retention-ttl",
			[]string{"leader-node", "follower-node", "stale-node"}),
	})

	var prunedNodes []string
	vaultInst := &VaultInstance{
		VaultID:    glid.New(),
		IsFollower: false,
		FollowerTargets: []system.ReplicationTarget{
			{NodeID: "follower-node"},
		},
		RaftApplyFacet: RaftApplyFacet{
			ApplyRaftPruneNode: func(nodeID string) error {
				prunedNodes = append(prunedNodes, nodeID)
				return nil
			},
		},
	}

	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "leader-node", slog.Default())
	rec.fsm = fsm

	// Fire ONLY the lead-gained catchup wake. No ReconcileTick.
	rec.ReconcileMembershipCatchup()

	if len(prunedNodes) != 1 || prunedNodes[0] != "stale-node" {
		t.Fatalf("membership catchup prune = %v, want [stale-node]", prunedNodes)
	}
}

// TestReconcileMembershipCatchupRequestsMissingReplicasFromFollowers is
// the multi-node convergence case for the replication category: a node
// that just gained vault-ctl leadership but joined the placement set late
// (gastrolog-19241) holds the FSM manifest without the historical bytes.
// The lead-gained catchup wake must ask every follower to re-push the
// missing sealed chunks — event-driven, without the backstop tick.
func TestReconcileMembershipCatchupRequestsMissingReplicasFromFollowers(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	cm := &reconcilerFakeChunkManager{}
	now := time.Now()

	idMissing := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idMissing, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(idMissing, now, 1, 1, now, now, now, false, now)})

	orch := newTestOrch(t, Config{LocalNodeID: "node-leader"})
	fake := &captureCatchupReplicator{scheduledRet: 1}
	orch.SetChunkReplicator(fake)

	vaultInst := &VaultInstance{
		VaultID:    glid.New(),
		Type:       "memory",
		Chunks:     cm,
		IsFollower: false, // just became the leader
		FollowerTargets: []system.ReplicationTarget{
			{NodeID: "node-follower-1"},
			{NodeID: "node-follower-2"},
		},
	}
	rec := NewVaultLifecycleReconciler(orch, glid.New(), vaultInst, "node-leader", slog.Default())
	rec.Wire(fsm)

	// Fire ONLY the lead-gained catchup wake. No ReconcileTick.
	rec.ReconcileMembershipCatchup()

	if got := fake.calls.Load(); got != 2 {
		t.Fatalf("new leader must request catchup from every follower on the lead-gained event, got %d call(s)", got)
	}
	if len(fake.lastChunks) != 1 || fake.lastChunks[0] != idMissing {
		t.Errorf("requested chunks = %v, want only [%s]", fake.lastChunks, idMissing)
	}
	if fake.lastRequester != "node-leader" {
		t.Errorf("requester = %q, want %q", fake.lastRequester, "node-leader")
	}
}
