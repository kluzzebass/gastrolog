package vaultctlfsm

import (
	"bytes"
	"testing"
	"time"

	"gastrolog/internal/chunk"

	hraft "github.com/hashicorp/raft"
)

var tombstoneTestTime = time.Unix(0, 1_700_000_000_000).UTC()

// createAndDelete runs one chunk through the receipt-based delete: created,
// requested for deletion from node-a, finalized by node-a's ack.
func createAndDelete(id chunk.ChunkID) [][]byte {
	return [][]byte{
		MarshalCreateChunk(id, tombstoneTestTime, tombstoneTestTime, tombstoneTestTime),
		MarshalRequestDelete(id, tombstoneTestTime, "test", []string{"node-a"}),
		MarshalAckDelete(id, "node-a"),
	}
}

func applyAll(t *testing.T, fsm *FSM, cmds [][]byte) {
	t.Helper()
	for _, cmd := range cmds {
		applyCmd(t, fsm, cmd)
	}
}

// deleteLog tombstones chunks through every path that creates a chunk
// tombstone — the draining ack, an explicit FinalizeDelete, and a node prune
// that drains ExpectedFrom — interleaved with purge confirmations and prunes.
func deleteLog() [][]byte {
	acked, finalized, drained, kept := testChunkID(1), testChunkID(2), testChunkID(3), testChunkID(4)
	cmds := createAndDelete(acked)
	cmds = append(cmds,
		MarshalCreateChunk(finalized, tombstoneTestTime, tombstoneTestTime, tombstoneTestTime),
		MarshalFinalizeDelete(finalized),
		MarshalConfirmChunkPurge("node-a", 2),
		MarshalConfirmChunkPurge("node-b", 1),
		MarshalCreateChunk(drained, tombstoneTestTime, tombstoneTestTime, tombstoneTestTime),
		MarshalCreateChunk(kept, tombstoneTestTime, tombstoneTestTime, tombstoneTestTime),
		MarshalRequestDelete(drained, tombstoneTestTime, "test", []string{"node-a", "node-gone"}),
		MarshalAckDelete(drained, "node-a"),
		MarshalPruneNode("node-gone"),
		MarshalConfirmChunkPurge("node-c", 3),
		MarshalPruneChunkTombstones(threeMembers),
	)
	return cmds
}

// Two replicas applying the same log hold the same chunk tombstones: the
// apply reads nothing from the node it runs on, so the snapshots are
// byte-identical.
func TestChunkTombstonesAreIdenticalOnEveryReplica(t *testing.T) {
	t.Parallel()
	first, second := New(), New()
	applyAll(t, first, deleteLog())
	applyAll(t, second, deleteLog())

	if got := first.DeleteSeq(); got != 3 {
		t.Fatalf("DeleteSeq = %d, want 3 (one per tombstoning apply)", got)
	}
	if !first.IsTombstoned(testChunkID(2)) || !first.IsTombstoned(testChunkID(3)) {
		t.Fatal("a tombstone node-b has not confirmed was pruned")
	}
	if first.IsTombstoned(testChunkID(1)) {
		t.Fatal("the tombstone every member confirmed was not pruned")
	}
	if !bytes.Equal(snapshotBytes(t, first), snapshotBytes(t, second)) {
		t.Fatal("two replicas applying the same log hold different chunk tombstones")
	}
}

// A replica restored from a snapshot mid-log ends where a replica that
// replayed the whole log ends.
func TestChunkTombstoneStateSurvivesSnapshotAndReplaysDeterministically(t *testing.T) {
	t.Parallel()
	log := deleteLog()
	split := 7

	replayed := New()
	applyAll(t, replayed, log)

	mid := New()
	applyAll(t, mid, log[:split])
	restored := New()
	restored.RestoreProto(mid.SnapshotProto())
	if !bytes.Equal(snapshotBytes(t, restored), snapshotBytes(t, mid)) {
		t.Fatal("restored snapshot differs from its source")
	}
	if got := restored.ChunkPurgeConfirmed("node-b"); got != 1 {
		t.Fatalf("restored node-b confirmation = %d, want 1", got)
	}
	applyAll(t, restored, log[split:])

	if !bytes.Equal(snapshotBytes(t, restored), snapshotBytes(t, replayed)) {
		t.Fatal("snapshot-restored replica diverged from the replica that replayed the whole log")
	}
}

// A late command naming a deleted chunk — a create, an open-chunk manifest, a
// repatriation of a copy some node still holds — stays refused for as long as
// the tombstone lives, which is until every member has confirmed holding no
// copy of it.
func TestLateCommandsNamingATombstonedChunkAreRefusedUntilEveryMemberConfirms(t *testing.T) {
	t.Parallel()
	fsm := New()
	id := testChunkID(9)
	applyAll(t, fsm, createAndDelete(id))
	repatriate, err := MarshalRepatriateChunk(ManifestEntry{ID: id, RecordCount: 5, Bytes: 50})
	if err != nil {
		t.Fatal(err)
	}
	ackHolder, err := MarshalAckChunkHolders([]chunk.ChunkID{id}, "node-c")
	if err != nil {
		t.Fatal(err)
	}

	lateCommandsRefused := func(when string) {
		t.Helper()
		applyCmd(t, fsm, MarshalCreateChunk(id, tombstoneTestTime, tombstoneTestTime, tombstoneTestTime))
		applyCmd(t, fsm, MarshalOpenChunkManifest(id, tombstoneTestTime))
		applyExpectErr(t, fsm, repatriate)
		applyCmd(t, fsm, ackHolder)
		if fsm.Get(id) != nil {
			t.Fatalf("%s: a late command re-created the deleted chunk", when)
		}
		if m := fsm.OpenChunk(); m != nil && m.ChunkID == id {
			t.Fatalf("%s: a late open-chunk manifest reopened the deleted chunk", when)
		}
		if !fsm.IsTombstoned(id) {
			t.Fatalf("%s: tombstone gone", when)
		}
	}

	lateCommandsRefused("no confirmations")
	applyCmd(t, fsm, MarshalConfirmChunkPurge("node-a", fsm.DeleteSeq()))
	applyCmd(t, fsm, MarshalConfirmChunkPurge("node-b", fsm.DeleteSeq()))
	if fsm.ChunkTombstonesPrunable(threeMembers) {
		t.Fatal("prunable while node-c has confirmed nothing")
	}
	applyCmd(t, fsm, MarshalPruneChunkTombstones(threeMembers))
	lateCommandsRefused("after a prune node-c never confirmed")

	applyCmd(t, fsm, MarshalConfirmChunkPurge("node-c", fsm.DeleteSeq()))
	if !fsm.ChunkTombstonesPrunable(threeMembers) {
		t.Fatal("not prunable once every member confirmed")
	}
	applyCmd(t, fsm, MarshalPruneChunkTombstones(threeMembers))
	if fsm.IsTombstoned(id) {
		t.Fatal("tombstone survived a prune every member confirmed")
	}
	if got := fsm.ChunkTombstoneCount(); got != 0 {
		t.Fatalf("tombstones = %d, want 0", got)
	}
}

// A tombstone is pruned at its own sequence: a confirmation that predates a
// delete does not cover it, a confirmation past the log is clamped, and an
// older confirmation never lowers a newer one.
func TestChunkPurgeConfirmationsAreClampedAndMonotonic(t *testing.T) {
	t.Parallel()
	fsm := New()
	first, second := testChunkID(1), testChunkID(2)
	applyAll(t, fsm, createAndDelete(first))
	applyCmd(t, fsm, MarshalConfirmChunkPurge("node-a", 99))
	if got := fsm.ChunkPurgeConfirmed("node-a"); got != 1 {
		t.Fatalf("confirmation past the delete sequence = %d, want clamped to 1", got)
	}
	applyAll(t, fsm, createAndDelete(second))
	applyCmd(t, fsm, MarshalConfirmChunkPurge("node-b", 2))
	applyCmd(t, fsm, MarshalConfirmChunkPurge("node-b", 1))
	if got := fsm.ChunkPurgeConfirmed("node-b"); got != 2 {
		t.Fatalf("an older confirmation lowered node-b to %d, want 2", got)
	}
	applyCmd(t, fsm, MarshalPruneChunkTombstones([]string{"node-a", "node-b"}))
	if fsm.IsTombstoned(first) {
		t.Error("tombstone both members confirmed was not pruned")
	}
	if !fsm.IsTombstoned(second) {
		t.Error("a confirmation sent before the delete covered it")
	}

	applyExpectErr(t, fsm, MarshalConfirmChunkPurge("", 1))
	applyExpectErr(t, fsm, MarshalPruneChunkTombstones(nil))
}

// Re-finalizing a tombstoned chunk keeps its sequence: the purge obligation
// does not restart, and the delete sequence does not move.
func TestRefinalizingATombstonedChunkKeepsItsSequence(t *testing.T) {
	t.Parallel()
	fsm := New()
	id := testChunkID(1)
	applyAll(t, fsm, createAndDelete(id))
	applyCmd(t, fsm, MarshalFinalizeDelete(id))
	if got := fsm.DeleteSeq(); got != 1 {
		t.Fatalf("DeleteSeq = %d after re-finalizing a tombstoned chunk, want 1", got)
	}
	applyCmd(t, fsm, MarshalConfirmChunkPurge("node-a", 1))
	applyCmd(t, fsm, MarshalPruneChunkTombstones([]string{"node-a"}))
	if fsm.IsTombstoned(id) {
		t.Fatal("tombstone survived a prune its only member confirmed")
	}
}

// A node outside the members a prune names neither holds the tombstones nor
// keeps its confirmation: it has left the vault-ctl group.
func TestChunkTombstonePruneDropsConfirmationsOfDepartedMembers(t *testing.T) {
	t.Parallel()
	fsm := New()
	applyAll(t, fsm, createAndDelete(testChunkID(1)))
	applyCmd(t, fsm, MarshalConfirmChunkPurge("node-a", 1))
	applyCmd(t, fsm, MarshalConfirmChunkPurge("node-old", 1))
	members := []string{"node-a"}
	applyCmd(t, fsm, MarshalPruneChunkTombstones(members))
	if got := fsm.ChunkTombstoneCount(); got != 0 {
		t.Fatalf("tombstones = %d, want 0", got)
	}
	if got := fsm.ChunkPurgeConfirmed("node-old"); got != 0 {
		t.Fatalf("departed node's confirmation kept (%d)", got)
	}
	if fsm.ChunkTombstonesPrunable(members) {
		t.Error("prunable after the prune applied")
	}
}

// The vault-ctl snapshot stops growing with the number of deleted chunks:
// after every delete cycle is confirmed and pruned, the snapshot is no larger
// than after the first cycle.
func TestSnapshotStaysBoundedAcrossDeleteCycles(t *testing.T) {
	t.Parallel()
	fsm := New()
	cycle := func() {
		for range 8 {
			applyAll(t, fsm, createAndDelete(chunk.NewChunkID()))
		}
		for _, m := range threeMembers {
			applyCmd(t, fsm, MarshalConfirmChunkPurge(m, fsm.DeleteSeq()))
		}
		applyCmd(t, fsm, MarshalPruneChunkTombstones(threeMembers))
	}
	cycle()
	firstSize := len(snapshotBytes(t, fsm))
	for range 100 {
		cycle()
	}
	if got := fsm.ChunkTombstoneCount(); got != 0 {
		t.Fatalf("tombstones = %d after 101 confirmed cycles, want 0", got)
	}
	// The delete sequence and each member's confirmation are varints that
	// grow by a few bytes over the whole run, independent of how many chunks
	// were deleted.
	varintGrowth := (1 + len(threeMembers)) * 2
	if size := len(snapshotBytes(t, fsm)); size > firstSize+varintGrowth {
		t.Fatalf("snapshot grew from %d to %d bytes across 100 pruned delete cycles", firstSize, size)
	}
}

// A node prune that drains several deletes tombstones them all at one
// sequence, whatever order the apply visits them in.
func TestPruneNodeDrainTombstonesAtOneSequence(t *testing.T) {
	t.Parallel()
	fsm := New()
	ids := []chunk.ChunkID{testChunkID(1), testChunkID(2), testChunkID(3)}
	for _, id := range ids {
		applyCmd(t, fsm, MarshalCreateChunk(id, tombstoneTestTime, tombstoneTestTime, tombstoneTestTime))
		applyCmd(t, fsm, MarshalRequestDelete(id, tombstoneTestTime, "test", []string{"node-gone"}))
	}
	if result := fsm.Apply(&hraft.Log{Data: MarshalPruneNode("node-gone")}); result != nil {
		t.Fatalf("prune node: %v", result)
	}
	if got := fsm.DeleteSeq(); got != 1 {
		t.Fatalf("DeleteSeq = %d after one draining prune, want 1", got)
	}
	for _, ts := range fsm.SnapshotProto().GetTombstones() {
		if ts.GetDeleteSeq() != 1 {
			t.Fatalf("tombstone %x at sequence %d, want 1", ts.GetChunkId(), ts.GetDeleteSeq())
		}
	}
	if got := fsm.ChunkTombstoneCount(); got != len(ids) {
		t.Fatalf("tombstones = %d, want %d", got, len(ids))
	}
}
