package orchestrator

import (
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	tierfsm "gastrolog/internal/tier/raftfsm"

	hraft "github.com/hashicorp/raft"
)

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
	tier := &TierInstance{
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

	if len(cm.deleted) != 1 || cm.deleted[0] != chunkID {
		t.Errorf("local delete = %v, want [%s]", cm.deleted, chunkID)
	}
	if ackCount.Load() != 1 {
		t.Errorf("ack count = %d, want 1", ackCount.Load())
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
	tier := &TierInstance{
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
	tier := &TierInstance{
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

	if finalizeCount.Load() != 1 {
		t.Errorf("finalize count = %d, want 1 once expectedFrom empty", finalizeCount.Load())
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
	tier := &TierInstance{
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
	tier := &TierInstance{
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

	var finalized []chunk.ChunkID
	tier := &TierInstance{
		TierID:                  glid.New(),
		Chunks:                  &reconcilerFakeChunkManager{},
		IsRaftLeader:            func() bool { return true },
		ApplyRaftFinalizeDelete: func(id chunk.ChunkID) error { finalized = append(finalized, id); return nil },
	}
	rec := NewTierLifecycleReconciler(nil, glid.New(), tier.TierID, tier, "node-B", slog.Default())
	rec.Wire(fsm)

	if err := fsm.Apply(&hraft.Log{Data: tierfsm.MarshalPruneNode("node-A")}); err != nil {
		t.Fatalf("apply prune: %v", err)
	}

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
	tier := &TierInstance{
		TierID:                  glid.New(),
		Chunks:                  &reconcilerFakeChunkManager{},
		IsRaftLeader:            func() bool { return false },
		ApplyRaftFinalizeDelete: func(_ chunk.ChunkID) error { finalizeCount.Add(1); return nil },
	}
	rec := NewTierLifecycleReconciler(nil, glid.New(), tier.TierID, tier, "node-Z", slog.Default())
	rec.Wire(fsm)

	_ = fsm.Apply(&hraft.Log{Data: tierfsm.MarshalPruneNode("node-A")})

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
	tier := &TierInstance{
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
	if err := fsm.Apply(&hraft.Log{Data: tierfsm.MarshalSealChunk(id, now, 100, 1234, now, now)}); err != nil {
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
	_ = src.Apply(&hraft.Log{Data: tierfsm.MarshalSealChunk(idSealed1, now, 1, 1, now, now)})
	_ = src.Apply(&hraft.Log{Data: tierfsm.MarshalSealChunk(idSealed2, now, 1, 1, now, now)})

	cm := &reconcilerFakeSealEnsurerChunkManager{}
	tier := &TierInstance{
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
	var ackedIDs []chunk.ChunkID
	tier := &TierInstance{
		TierID: glid.New(),
		Chunks: cm,
		ApplyRaftAckDelete: func(id chunk.ChunkID, _ string) error {
			ackedIDs = append(ackedIDs, id)
			return nil
		},
	}
	rec := NewTierLifecycleReconciler(nil, glid.New(), tier.TierID, tier, "node-A", slog.Default())

	// Reconcile from the FSM's pending state — does NOT require Wire().
	rec.ReconcileFromSnapshot(fsm)

	if len(cm.deleted) != 2 {
		t.Errorf("expected 2 local deletes from reconcile, got %d (%v)", len(cm.deleted), cm.deleted)
	}
	if len(ackedIDs) != 2 {
		t.Errorf("expected 2 acks from reconcile, got %d (%v)", len(ackedIDs), ackedIDs)
	}
}
