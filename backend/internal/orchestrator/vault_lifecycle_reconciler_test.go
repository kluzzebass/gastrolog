package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/vaultraft/tierfsm"

	hraft "github.com/hashicorp/raft"
)

// captureCatchupReplicator records the most recent RequestReplicaCatchup
// call so SweepMissingReplicas tests can assert what the follower asked
// the leader to push. Other ChunkReplicator methods are no-ops — the
// missing-replica sweep only exercises the one inverse method.
type captureCatchupReplicator struct {
	calls          atomic.Int32
	lastLeader     string
	lastVault      glid.GLID
	lastTier       glid.GLID
	lastChunks     []chunk.ChunkID
	lastRequester  string
	scheduledRet   uint32
	failNextWith   error
}

func (c *captureCatchupReplicator) AppendRecords(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID, _ []chunk.Record) error {
	return nil
}
func (c *captureCatchupReplicator) SealVault(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID) error {
	return nil
}
func (c *captureCatchupReplicator) ImportSealedChunk(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID, _ []chunk.Record) error {
	return nil
}
func (c *captureCatchupReplicator) DeleteChunk(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID) error {
	return nil
}
func (c *captureCatchupReplicator) RequestReplicaCatchup(_ context.Context, leaderNodeID string, vaultID, tierID glid.GLID, chunkIDs []chunk.ChunkID, requesterNodeID string) (uint32, error) {
	c.calls.Add(1)
	c.lastLeader = leaderNodeID
	c.lastVault = vaultID
	c.lastTier = tierID
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

	fsm := tierfsm.New()
	cm := &reconcilerFakeChunkManager{}

	var ackedID chunk.ChunkID
	var ackedNode string
	var ackCount atomic.Int32
	tier := &VaultInstance{
		TierID: glid.New(),
		Chunks: cm,
		ApplyRaftAckDelete: func(id chunk.ChunkID, nodeID string) error {
			ackedID = id
			ackedNode = nodeID
			ackCount.Add(1)
			return nil
		},
	}
	rec := NewTierLifecycleReconciler(nil, glid.New(), tier.TierID, tier, "node-A", slog.Default())
	rec.Wire(fsm)

	chunkID := chunk.NewChunkID()
	if err := fsm.Apply(&hraft.Log{
		Data: tierfsm.MarshalRequestDelete(chunkID, time.Now(), "retention-ttl",
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

	fsm := tierfsm.New()
	cm := &reconcilerFakeChunkManager{}

	var ackCount atomic.Int32
	tier := &VaultInstance{
		TierID: glid.New(),
		Chunks: cm,
		ApplyRaftAckDelete: func(_ chunk.ChunkID, _ string) error {
			ackCount.Add(1)
			return nil
		},
	}
	rec := NewTierLifecycleReconciler(nil, glid.New(), tier.TierID, tier, "node-Z", slog.Default())
	rec.Wire(fsm)

	chunkID := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{
		Data: tierfsm.MarshalRequestDelete(chunkID, time.Now(), "retention-ttl",
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

// TestReconcilerOnAckDeleteFinalizesWhenAllAcked pins the leader-side
// invariant: when CmdAckDelete commits and the FSM's expectedFrom set
// becomes empty, the leader proposes CmdFinalizeDelete. Followers
// (IsRaftLeader == false) must NOT propose finalize — that's the
// leader-only convergence point.
func TestReconcilerOnAckDeleteFinalizesWhenAllAcked(t *testing.T) {
	t.Parallel()

	fsm := tierfsm.New()
	cm := &reconcilerFakeChunkManager{}

	var finalizeCount atomic.Int32
	var finalizedID chunk.ChunkID
	tier := &VaultInstance{
		TierID: glid.New(),
		Chunks: cm,
		IsRaftLeader: func() bool { return true },
		ApplyRaftAckDelete: func(_ chunk.ChunkID, _ string) error { return nil },
		ApplyRaftFinalizeDelete: func(id chunk.ChunkID) error {
			finalizedID = id
			finalizeCount.Add(1)
			return nil
		},
	}
	rec := NewTierLifecycleReconciler(nil, glid.New(), tier.TierID, tier, "node-A", slog.Default())
	rec.Wire(fsm)

	chunkID := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{
		Data: tierfsm.MarshalRequestDelete(chunkID, time.Now(), "retention-ttl",
			[]string{"node-A", "node-B"}),
	})
	// node-A acks (this fires through the local applier stub above and
	// also via direct Apply for node-B simulation below).
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalAckDelete(chunkID, "node-A")})

	if finalizeCount.Load() != 0 {
		t.Errorf("must not finalize while node-B still owes ack, got %d", finalizeCount.Load())
	}

	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalAckDelete(chunkID, "node-B")})

	// onAckDelete dispatches the finalize Apply in a goroutine to avoid
	// deadlocking the FSM apply pump. Wait for the goroutine to drain.
	deadline := time.After(2 * time.Second)
	for finalizeCount.Load() < 1 {
		select {
		case <-deadline:
			t.Fatalf("finalize did not fire within deadline (count=%d)", finalizeCount.Load())
		case <-time.After(10 * time.Millisecond):
		}
	}

	if finalizedID != chunkID {
		t.Errorf("finalize id = %s, want %s", finalizedID, chunkID)
	}
}

// TestReconcilerOnAckDeleteSkipsOnFollower verifies that a non-leader
// reconciler observing CmdAckDelete does NOT propose CmdFinalizeDelete.
// Only one node at a time may cleanly drive finalization — the leader.
func TestReconcilerOnAckDeleteSkipsOnFollower(t *testing.T) {
	t.Parallel()

	fsm := tierfsm.New()
	var finalizeCount atomic.Int32
	tier := &VaultInstance{
		TierID:                  glid.New(),
		Chunks:                  &reconcilerFakeChunkManager{},
		IsRaftLeader:            func() bool { return false },
		ApplyRaftAckDelete:      func(_ chunk.ChunkID, _ string) error { return nil },
		ApplyRaftFinalizeDelete: func(_ chunk.ChunkID) error { finalizeCount.Add(1); return nil },
	}
	rec := NewTierLifecycleReconciler(nil, glid.New(), tier.TierID, tier, "node-A", slog.Default())
	rec.Wire(fsm)

	chunkID := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{
		Data: tierfsm.MarshalRequestDelete(chunkID, time.Now(), "test", []string{"node-A"}),
	})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalAckDelete(chunkID, "node-A")})

	if finalizeCount.Load() != 0 {
		t.Errorf("follower must not finalize, got %d finalize calls", finalizeCount.Load())
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
	tier := &VaultInstance{
		TierID: glid.New(),
		Chunks: cm,
		// ApplyRaftRequestDelete deliberately nil — single-node mode.
	}
	rec := NewTierLifecycleReconciler(nil, glid.New(), tier.TierID, tier, "node-A", slog.Default())

	chunkID := chunk.NewChunkID()
	if err := rec.deleteChunk(chunkID, "retention-ttl", []string{"node-A"}); err != nil {
		t.Fatalf("deleteChunk: %v", err)
	}
	if len(cm.deleted) != 1 || cm.deleted[0] != chunkID {
		t.Errorf("single-node deleteChunk delete = %v, want [%s]", cm.deleted, chunkID)
	}
}

// TestReconcilerOnPruneNodeFinalizesEmptiedEntries pins the gastrolog-51gme
// step 10 invariant: when CmdPruneNode commits and the FSM reports a list
// of chunks whose ExpectedFrom became empty, the reconciler (leader-only)
// proposes CmdFinalizeDelete for each. Without this, removing a node from
// the voter set would orphan its outstanding deletes — onAckDelete only
// fires for actual CmdAckDelete applies, not for prune-induced empties.
func TestReconcilerOnPruneNodeFinalizesEmptiedEntries(t *testing.T) {
	t.Parallel()

	fsm := tierfsm.New()
	now := time.Now()

	// Three pendingDeletes; pruning node-A empties the second.
	idStillOwed := chunk.NewChunkID()
	idEmptied := chunk.NewChunkID()
	idUntouched := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalRequestDelete(idStillOwed, now, "test", []string{"node-A", "node-B"})})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalRequestDelete(idEmptied, now, "test", []string{"node-A"})})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalRequestDelete(idUntouched, now, "test", []string{"node-B"})})

	finalizedCh := make(chan chunk.ChunkID, 4)
	tier := &VaultInstance{
		TierID:                  glid.New(),
		Chunks:                  &reconcilerFakeChunkManager{},
		IsRaftLeader:            func() bool { return true },
		ApplyRaftFinalizeDelete: func(id chunk.ChunkID) error { finalizedCh <- id; return nil },
	}
	rec := NewTierLifecycleReconciler(nil, glid.New(), tier.TierID, tier, "node-B", slog.Default())
	rec.Wire(fsm)

	if err := fsm.Apply(&hraft.Log{Data: tierfsm.MarshalPruneNode("node-A")}); err != nil {
		t.Fatalf("apply prune: %v", err)
	}

	// onPruneNode dispatches finalize Applies in a goroutine to avoid
	// deadlocking the FSM apply pump (CmdFinalizeDelete on the leader
	// posts to the same Raft apply queue we're currently draining). The
	// test must wait for that goroutine to drain before asserting.
	var finalized []chunk.ChunkID
	deadline := time.After(2 * time.Second)
	for {
		select {
		case id := <-finalizedCh:
			finalized = append(finalized, id)
			if len(finalized) >= 1 {
				goto done
			}
		case <-deadline:
			goto done
		}
	}
done:
	if len(finalized) != 1 || finalized[0] != idEmptied {
		t.Errorf("finalized = %v, want [%s] (idEmptied only)", finalized, idEmptied)
	}
}

// TestReconcilerOnPruneNodeSkipsOnFollower pins that a non-leader reconciler
// observing CmdPruneNode does NOT propose CmdFinalizeDelete — finalization
// is leader-only, matching onAckDelete.
func TestReconcilerOnPruneNodeSkipsOnFollower(t *testing.T) {
	t.Parallel()

	fsm := tierfsm.New()
	now := time.Now()
	id := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalRequestDelete(id, now, "test", []string{"node-A"})})

	var finalizeCount atomic.Int32
	tier := &VaultInstance{
		TierID:                  glid.New(),
		Chunks:                  &reconcilerFakeChunkManager{},
		IsRaftLeader:            func() bool { return false },
		ApplyRaftFinalizeDelete: func(_ chunk.ChunkID) error { finalizeCount.Add(1); return nil },
	}
	rec := NewTierLifecycleReconciler(nil, glid.New(), tier.TierID, tier, "node-Z", slog.Default())
	rec.Wire(fsm)

	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalPruneNode("node-A")})

	// Give a goroutine a chance to fire if the follower-skip check fails.
	time.Sleep(50 * time.Millisecond)

	if finalizeCount.Load() != 0 {
		t.Errorf("follower must not finalize after prune, got %d calls", finalizeCount.Load())
	}
}

// TestReconcilerOnSealProjectsToLocalManager pins the gastrolog-51gme step 8
// invariant: when CmdSealChunk applies, the reconciler asks the local chunk
// Manager to project the FSM-sealed state via the SealEnsurer interface. The
// Manager's EnsureSealed contract handles the no-op cases internally; the
// test just asserts the projection method was invoked with the right ID.
func TestReconcilerOnSealProjectsToLocalManager(t *testing.T) {
	t.Parallel()

	fsm := tierfsm.New()
	cm := &reconcilerFakeSealEnsurerChunkManager{}
	tier := &VaultInstance{
		TierID: glid.New(),
		Chunks: cm,
	}
	rec := NewTierLifecycleReconciler(nil, glid.New(), tier.TierID, tier, "node-A", slog.Default())
	rec.Wire(fsm)

	id := chunk.NewChunkID()
	now := time.Now()
	if err := fsm.Apply(&hraft.Log{Data: tierfsm.MarshalCreateChunk(id, now, now, now)}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: tierfsm.MarshalSealChunk(id, now, 100, 1234, now, now, now, false)}); err != nil {
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

	src := tierfsm.New()

	// Seed the source FSM: 3 chunks created, 2 sealed, 1 still active.
	now := time.Now()
	idSealed1 := chunk.NewChunkID()
	idSealed2 := chunk.NewChunkID()
	idActive := chunk.NewChunkID()
	for _, id := range []chunk.ChunkID{idSealed1, idSealed2, idActive} {
		_ = src.Apply(&hraft.Log{Data: tierfsm.MarshalCreateChunk(id, now, now, now)})
	}
	_ = src.Apply(&hraft.Log{Data: tierfsm.MarshalSealChunk(idSealed1, now, 1, 1, now, now, now, false)})
	_ = src.Apply(&hraft.Log{Data: tierfsm.MarshalSealChunk(idSealed2, now, 1, 1, now, now, now, false)})

	cm := &reconcilerFakeSealEnsurerChunkManager{}
	tier := &VaultInstance{
		TierID: glid.New(),
		Chunks: cm,
	}
	rec := NewTierLifecycleReconciler(nil, glid.New(), tier.TierID, tier, "node-A", slog.Default())

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

	fsm := tierfsm.New()

	// Seed two pending deletes — node-A owes both, node-B owes only the second.
	id1 := chunk.NewChunkID()
	id2 := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{
		Data: tierfsm.MarshalRequestDelete(id1, time.Now(), "retention-ttl", []string{"node-A"}),
	})
	_ = fsm.Apply(&hraft.Log{
		Data: tierfsm.MarshalRequestDelete(id2, time.Now(), "retention-ttl", []string{"node-A", "node-B"}),
	})

	cm := &reconcilerFakeChunkManager{}
	ackCh := make(chan chunk.ChunkID, 4)
	tier := &VaultInstance{
		TierID: glid.New(),
		Chunks: cm,
		ApplyRaftAckDelete: func(id chunk.ChunkID, _ string) error {
			ackCh <- id
			return nil
		},
	}
	rec := NewTierLifecycleReconciler(nil, glid.New(), tier.TierID, tier, "node-A", slog.Default())

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

	fsm := tierfsm.New()
	cm := &reconcilerFakeChunkManager{}

	now := time.Now()

	// Case 1 (positive): tombstoned-absent. Drive the full receipt
	// protocol to commit a tombstone, then leave the local file behind.
	idTombstoned := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalCreateChunk(idTombstoned, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalSealChunk(idTombstoned, now, 1, 1, now, now, now, false)})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalRequestDelete(idTombstoned, now, "test", []string{"node-A"})})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalAckDelete(idTombstoned, "node-A")})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalFinalizeDelete(idTombstoned)})

	// Case 2 (negative): live in manifest. Created + sealed, no deletes.
	idLiveSealed := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalCreateChunk(idLiveSealed, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalSealChunk(idLiveSealed, now, 1, 1, now, now, now, false)})

	// Case 3 (negative): pendingDeletes — receipt protocol owns it.
	idPending := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalCreateChunk(idPending, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalSealChunk(idPending, now, 1, 1, now, now, now, false)})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalRequestDelete(idPending, now, "test", []string{"node-A"})})

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
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalCreateChunk(idUnsealed, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalRequestDelete(idUnsealed, now, "test", []string{"node-A"})})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalAckDelete(idUnsealed, "node-A")})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalFinalizeDelete(idUnsealed)})

	// Seed the local chunk manager with each case as if the file is
	// still on disk regardless of FSM state.
	cm.chunks = []chunk.ChunkMeta{
		{ID: idTombstoned, Sealed: true},
		{ID: idLiveSealed, Sealed: true},
		{ID: idPending, Sealed: true},
		{ID: idUnknown, Sealed: true},
		{ID: idUnsealed, Sealed: false},
	}

	tier := &VaultInstance{TierID: glid.New(), Chunks: cm}
	rec := NewTierLifecycleReconciler(nil, glid.New(), tier.TierID, tier, "node-A", slog.Default())
	rec.Wire(fsm)

	rec.SweepLocalOrphans()

	if len(cm.deleted) != 1 || cm.deleted[0] != idTombstoned {
		t.Errorf("orphan sweep deleted = %v, want only [%s] (tombstoned-absent positive case)",
			cm.deleted, idTombstoned)
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
//     records to a follower's local disk would defeat the cloud-tier
//     contract and waste bandwidth
//   - chunks already present locally are not missing — re-requesting
//     would create unbounded re-push amplification on every sweep tick
func TestSweepMissingReplicasRequestsOnlySealedAndAbsentEntries(t *testing.T) {
	t.Parallel()

	fsm := tierfsm.New()
	cm := &reconcilerFakeChunkManager{}
	now := time.Now()

	// Case 1 (positive): sealed in FSM, missing on disk → must be requested.
	idMissing := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalCreateChunk(idMissing, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalSealChunk(idMissing, now, 1, 1, now, now, now, false)})

	// Case 2 (negative): sealed in FSM, present locally → must NOT be requested.
	idPresent := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalCreateChunk(idPresent, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalSealChunk(idPresent, now, 1, 1, now, now, now, false)})

	// Case 3 (negative): in FSM but unsealed (active) → must NOT be requested.
	idActive := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalCreateChunk(idActive, now, now, now)})

	// Case 4 (negative): sealed and cloud-backed → must NOT be requested
	// (lives in shared bucket; not a local-replica concern).
	idCloud := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalCreateChunk(idCloud, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalSealChunk(idCloud, now, 1, 1, now, now, now, false)})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalUploadChunk(idCloud, 1, 0, 0, 0, 0, [32]byte{}, glid.GLID{}, 0)})

	cm.chunks = []chunk.ChunkMeta{
		{ID: idPresent, Sealed: true},
	}

	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	fake := &captureCatchupReplicator{scheduledRet: 1}
	orch.SetChunkReplicator(fake)

	tier := &VaultInstance{
		TierID:       glid.New(),
		Type:         "memory",
		Chunks:       cm,
		IsFollower:   true,
		LeaderNodeID: "node-leader",
	}
	rec := NewTierLifecycleReconciler(orch, glid.New(), tier.TierID, tier, "node-A", slog.Default())
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

// TestSweepMissingReplicasSkipsLeaderTier pins that the sweep is a
// follower-only operation. The leader's local store is by definition
// the source of truth — if a chunk is in its FSM but not on its disk,
// the chunk has been lost and no peer catchup will recover it. Running
// the sweep on the leader would waste an RPC and could mask a real
// disk-failure incident.
func TestSweepMissingReplicasSkipsLeaderTier(t *testing.T) {
	t.Parallel()

	fsm := tierfsm.New()
	cm := &reconcilerFakeChunkManager{}
	now := time.Now()

	// Sealed in FSM, missing on disk — the same shape that would trigger
	// a request on a follower. On a leader, the sweep must skip entirely.
	idMissing := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalCreateChunk(idMissing, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalSealChunk(idMissing, now, 1, 1, now, now, now, false)})

	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	fake := &captureCatchupReplicator{}
	orch.SetChunkReplicator(fake)

	tier := &VaultInstance{
		TierID:     glid.New(),
		Type:       "memory",
		Chunks:     cm,
		IsFollower: false, // this node IS the placement leader
	}
	rec := NewTierLifecycleReconciler(orch, glid.New(), tier.TierID, tier, "node-A", slog.Default())
	rec.Wire(fsm)

	rec.SweepMissingReplicas()

	if fake.calls.Load() != 0 {
		t.Errorf("leader must not request catchup, got %d call(s)", fake.calls.Load())
	}
}

// TestSweepMissingReplicasSkipsWhenLeaderUnknown pins the early-exit
// when LeaderNodeID is empty. This happens during placement transitions
// where a follower has lost its leader (election in progress, leader
// just demoted) — sending a catchup request would land on no one.
// The next sweep tick runs after the new leader is observed.
func TestSweepMissingReplicasSkipsWhenLeaderUnknown(t *testing.T) {
	t.Parallel()

	fsm := tierfsm.New()
	cm := &reconcilerFakeChunkManager{}
	now := time.Now()

	idMissing := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalCreateChunk(idMissing, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalSealChunk(idMissing, now, 1, 1, now, now, now, false)})

	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	fake := &captureCatchupReplicator{}
	orch.SetChunkReplicator(fake)

	tier := &VaultInstance{
		TierID:       glid.New(),
		Type:         "memory",
		Chunks:       cm,
		IsFollower:   true,
		LeaderNodeID: "", // unknown — election in progress
	}
	rec := NewTierLifecycleReconciler(orch, glid.New(), tier.TierID, tier, "node-A", slog.Default())
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
// active on a follower (downstream tier with no continuous record
// stream → no natural active swap) gets force-demoted and then
// deleted, instead of bouncing off ErrActiveChunk every periodic
// sweep tick.
//
// Pre-fix: fulfillObligation called deleteLocalCopy directly;
// receipt protocol stuck forever on tiers with no record stream
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
		activeID: chunkID, // simulate a stuck-active chunk on a downstream-tier follower
	}

	var ackedID chunk.ChunkID
	var ackCount atomic.Int32
	tier := &VaultInstance{
		TierID: glid.New(),
		Chunks: cm,
		ApplyRaftAckDelete: func(id chunk.ChunkID, _ string) error {
			ackedID = id
			ackCount.Add(1)
			return nil
		},
	}
	rec := NewTierLifecycleReconciler(nil, glid.New(), tier.TierID, tier, "node-A", slog.Default())

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

	fsm := tierfsm.New()
	now := time.Now()

	// Drive the FSM to: chunk fully finalized, leaving only a
	// tombstone (no manifest entry, no pendingDeletes entry).
	idTombstoned := chunk.NewChunkID()
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalCreateChunk(idTombstoned, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalSealChunk(idTombstoned, now, 1, 1, now, now, now, false)})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalRequestDelete(idTombstoned, now, "test", []string{"node-A"})})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalAckDelete(idTombstoned, "node-A")})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalFinalizeDelete(idTombstoned)})

	// Local state: chunk is the active pointer (unsealed on disk).
	// fakeSealEnsurerThatDemotesActive mimics real-Manager
	// EnsureSealed semantics: demotes the active to sealed.
	cm := &fakeSealEnsurerThatDemotesActive{
		activeID: idTombstoned,
	}
	cm.chunks = []chunk.ChunkMeta{
		{ID: idTombstoned, Sealed: false}, // active = unsealed
	}

	tier := &VaultInstance{TierID: glid.New(), Chunks: cm}
	rec := NewTierLifecycleReconciler(nil, glid.New(), tier.TierID, tier, "node-A", slog.Default())
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
// observe wireTierFSMOnDelete's local-delete behavior alongside the
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

// recordingCloudRegistrar adds chunk.CloudChunkRegistrar on top of the
// silent-deleter fake.
type recordingCloudRegistrar struct {
	recordingSilentDeleter
	registered []chunk.ChunkID
	registerErr error
}

func (r *recordingCloudRegistrar) RegisterCloudChunk(id chunk.ChunkID, _ chunk.CloudChunkInfo) error {
	r.registered = append(r.registered, id)
	return r.registerErr
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

	fsm := tierfsm.New()
	cm := &reconcilerFakeSealEnsurerChunkManager{}
	tier := &VaultInstance{TierID: glid.New(), Chunks: cm}
	rec := NewTierLifecycleReconciler(orch, glid.New(), tier.TierID, tier, "node-A", slog.Default())
	rec.Wire(fsm)

	id := chunk.NewChunkID()
	now := time.Now()
	if err := fsm.Apply(&hraft.Log{Data: tierfsm.MarshalCreateChunk(id, now, now, now)}); err != nil {
		t.Fatalf("apply create: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: tierfsm.MarshalSealChunk(id, now, 100, 1234, now, now, now, false)}); err != nil {
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

	fsm := tierfsm.New()
	cm := &reconcilerFailEnsurerChunkManager{ensureErr: errors.New("disk gone")}
	tier := &VaultInstance{TierID: glid.New(), Chunks: cm}
	rec := NewTierLifecycleReconciler(orch, glid.New(), tier.TierID, tier, "node-A", slog.Default())
	rec.Wire(fsm)

	id := chunk.NewChunkID()
	now := time.Now()
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalCreateChunk(id, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalSealChunk(id, now, 1, 1, now, now, now, false)})

	if !waitForChunkSignal(signalCh, time.Second) {
		t.Fatal("expected chunk signal even when EnsureSealed errors, got timeout")
	}
}

// TestWireTierFSMOnDeleteFiresNotifyChunkChange pins that the legacy
// FSM-driven delete callback (CmdDeleteChunk applied via Raft, not the
// receipt-protocol path) also fires NotifyChunkChange. Without this,
// nodes that don't own the chunk locally — but display it via the
// inspector's cluster-wide ListChunks fan-out — would never refresh
// after a remote delete. See gastrolog-2ob86.
func TestWireTierFSMOnDeleteFiresNotifyChunkChange(t *testing.T) {
	t.Parallel()

	orch, err := New(Config{LocalNodeID: "node-A"})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	startThrottleForTest(t, orch)
	signalCh := orch.ChunkSignal().C()

	fsm := tierfsm.New()
	cm := &recordingSilentDeleter{}
	tierID := glid.New()
	g := &raftgroup.Group{FSM: fsm}
	wireTierFSMOnDelete(g, tierID, cm, nil, orch, slog.Default())

	id := chunk.NewChunkID()
	now := time.Now()
	if err := fsm.Apply(&hraft.Log{Data: tierfsm.MarshalCreateChunk(id, now, now, now)}); err != nil {
		t.Fatalf("apply create: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: tierfsm.MarshalDeleteChunk(id)}); err != nil {
		t.Fatalf("apply delete: %v", err)
	}

	if !waitForChunkSignal(signalCh, time.Second) {
		t.Fatal("expected chunk signal after CmdDeleteChunk apply, got timeout")
	}
	if len(cm.silentDeleted) != 1 || cm.silentDeleted[0] != id {
		t.Errorf("DeleteSilent = %v, want [%s]", cm.silentDeleted, id)
	}
}

// TestWireTierFSMOnDeleteNotifiesEvenWhenDeleteSilentFails pins the
// FSM-state-is-authoritative principle for the delete callback: a
// failed local file delete (chunk missing, manager closed, etc.) must
// not gate the inspector signal. The chunks-map entry was removed from
// the FSM regardless. See gastrolog-2ob86.
func TestWireTierFSMOnDeleteNotifiesEvenWhenDeleteSilentFails(t *testing.T) {
	t.Parallel()

	orch, err := New(Config{LocalNodeID: "node-A"})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	startThrottleForTest(t, orch)
	signalCh := orch.ChunkSignal().C()

	fsm := tierfsm.New()
	cm := &recordingSilentDeleter{failNext: chunk.ErrChunkNotFound}
	tierID := glid.New()
	g := &raftgroup.Group{FSM: fsm}
	wireTierFSMOnDelete(g, tierID, cm, nil, orch, slog.Default())

	id := chunk.NewChunkID()
	now := time.Now()
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalCreateChunk(id, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalDeleteChunk(id)})

	if !waitForChunkSignal(signalCh, time.Second) {
		t.Fatal("expected chunk signal even when DeleteSilent fails, got timeout")
	}
}

// TestWireTierFSMOnUploadFiresNotifyChunkChange pins that follower
// nodes, on receiving a CmdUploadChunk via Raft (the leader's
// AnnounceUpload propagated through), refresh their inspector view.
// Pre-fix the cloud-backed transition was invisible until manual
// reload. See gastrolog-2ob86.
func TestWireTierFSMOnUploadFiresNotifyChunkChange(t *testing.T) {
	t.Parallel()

	orch, err := New(Config{LocalNodeID: "node-A"})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	startThrottleForTest(t, orch)
	signalCh := orch.ChunkSignal().C()

	fsm := tierfsm.New()
	cm := &recordingCloudRegistrar{}
	tierID := glid.New()
	g := &raftgroup.Group{FSM: fsm}
	wireTierFSMOnUpload(g, tierID, cm, orch, slog.Default())

	id := chunk.NewChunkID()
	now := time.Now()
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalCreateChunk(id, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalSealChunk(id, now, 1, 1, now, now, now, false)})
	if err := fsm.Apply(&hraft.Log{Data: tierfsm.MarshalUploadChunk(id, 1024, 0, 0, 0, 0, [32]byte{}, glid.GLID{}, 0)}); err != nil {
		t.Fatalf("apply upload: %v", err)
	}

	if !waitForChunkSignal(signalCh, time.Second) {
		t.Fatal("expected chunk signal after CmdUploadChunk apply, got timeout")
	}
	if len(cm.registered) != 1 || cm.registered[0] != id {
		t.Errorf("RegisterCloudChunk = %v, want [%s]", cm.registered, id)
	}
}
