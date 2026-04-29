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
