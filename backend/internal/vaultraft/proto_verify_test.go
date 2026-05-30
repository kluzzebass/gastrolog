package vaultraft

// Cross-cutting verification for the vault-ctl protobuf migration
// (gastrolog-2q1xq, epic gastrolog-5lrg7):
//   - multi-node (5) InstallSnapshot/restore carrying every snapshot section,
//   - WAL-replay determinism (identical proto command stream => identical FSM
//     state and byte-identical snapshots).
// The seq-lease-reserve benchmark lives in vaultctlfsm/bench_seq_test.go.

import (
	"bytes"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// TestProtoVerify_FiveNode_InstallSnapshotAllSections seeds every snapshot
// section (manifest entries incl. a sealed chunk, a tombstone, an in-flight
// pending delete, an active seq lease, and a published fence) across two
// vaults on a 5-node cluster, forces a leader snapshot, advances the log past
// it, then wipes a follower so it can only rejoin via InstallSnapshot. The
// restored follower must carry all five sections, not just the fingerprinted
// subset.
func TestProtoVerify_FiveNode_InstallSnapshotAllSections(t *testing.T) {
	t.Parallel()
	h := newReliabilityHarness(t, 5)

	v1, v2 := glid.New(), glid.New()
	now := time.Now().Truncate(time.Nanosecond)

	sealedID := chunkIDWithPrefix(0x01)
	activeID := chunkIDWithPrefix(0x02)
	deadID := chunkIDWithPrefix(0x03)
	pendID := chunkIDWithPrefix(0x04)
	expectedAcks := []string{"node-2", "node-3", "node-4"}

	// v1: a sealed chunk, an active chunk, a tombstone (via finalize-delete,
	// the non-deprecated path), and an in-flight pending delete.
	h.applyInstanceCreate(v1, sealedID, now)
	h.applyVaultSeqCommand(v1, vaultctlfsm.MarshalBeginSeal(sealedID))
	h.applyVaultSeqCommand(v1, vaultctlfsm.MarshalSealChunk(sealedID, now, 5, 500, now, now, now, false))
	h.applyInstanceCreate(v1, activeID, now)
	h.applyVaultSeqCommand(v1, vaultctlfsm.MarshalFinalizeDelete(deadID))
	h.applyVaultSeqCommand(v1, vaultctlfsm.MarshalRequestDelete(pendID, now, "retention-ttl", expectedAcks))

	// v2: a seq lease and a published fence.
	h.applyReserveSeqRange(v2, "node-1", vaultctlfsm.InitialSeqEpoch, 40)
	h.applyVaultSeqCommand(v2, vaultctlfsm.MarshalPublishFence(123, now))

	h.assertAllFSMsConverged()

	// Force a snapshot on the leader and truncate the log behind it.
	leaderID := h.leaderID()
	if err := h.nodes[leaderID].raft.Snapshot().Error(); err != nil && !isBenignSnapshotErr(err) {
		t.Fatalf("force snapshot: %v", err)
	}

	// Advance the log past the snapshot so catch-up cannot be replay-only.
	for i := range byte(4) {
		h.applyInstanceCreate(v1, chunkIDWithPrefix(0x90+i), now)
	}

	// Wipe a follower: it must rejoin via InstallSnapshot + post-snapshot tail.
	var followerID string
	for _, id := range h.nodeIDs {
		if id != leaderID {
			followerID = id
			break
		}
	}
	h.stopNode(followerID)
	if err := removeDirContents(h.nodes[followerID].walDir); err != nil {
		t.Fatalf("wipe wal dir: %v", err)
	}
	h.startNode(followerID)
	h.wireTransports()

	h.assertAllFSMsConverged()

	// Fingerprint convergence does not cover tombstones, pending deletes, or
	// fences — assert those sections survived the InstallSnapshot directly on
	// the wiped follower.
	fn := h.nodes[followerID]
	fn.mu.Lock()
	followerFSM := fn.fsm
	fn.mu.Unlock()

	sub1 := followerFSM.VaultFSM(v1)
	if sub1 == nil {
		t.Fatal("wiped follower missing vault v1 after InstallSnapshot")
	}
	if e := sub1.Get(sealedID); e == nil || !e.IsSealed() {
		t.Errorf("sealed chunk not restored on follower: %+v", e)
	}
	if !sub1.IsTombstoned(deadID) {
		t.Error("tombstone not restored on follower")
	}
	if pd := sub1.PendingDelete(pendID); pd == nil || len(pd.ExpectedFrom) != len(expectedAcks) {
		t.Errorf("pending delete not restored on follower: %+v", pd)
	}

	sub2 := followerFSM.VaultFSM(v2)
	if sub2 == nil {
		t.Fatal("wiped follower missing vault v2 after InstallSnapshot")
	}
	if got := sub2.LatestFenceUpperBound(); got != 123 {
		t.Errorf("fence not restored on follower: upper=%d want 123", got)
	}
	alloc := sub2.SeqAllocatorState()
	if len(alloc.ActiveSwaths) != 1 || alloc.ActiveSwaths[0].HolderID != "node-1" {
		t.Errorf("seq lease not restored on follower: %+v", alloc.ActiveSwaths)
	}
}

// protoCmdStream returns a deterministic, delete-free stream of outer
// VaultRaftCommand byte payloads spanning entries, a pending delete, a seq
// lease, and a fence. Deletes are intentionally excluded: applyDelete /
// applyFinalizeDelete stamp tombstones with time.Now(), which is not
// reproducible across independent replays. Everything here derives its
// timestamps from the fixed `now`, so two replays must produce identical
// bytes.
func protoCmdStream(v1, v2 glid.GLID, now time.Time) [][]byte {
	mustOuter := func(b []byte, err error) []byte {
		if err != nil {
			panic(err)
		}
		return b
	}
	a := chunkIDWithPrefix(0x11)
	c := chunkIDWithPrefix(0x12)
	return [][]byte{
		MarshalVaultChunkCommand(v1, vaultctlfsm.MarshalCreateChunk(a, now, now, now)),
		MarshalVaultChunkCommand(v1, vaultctlfsm.MarshalBeginSeal(a)),
		MarshalVaultChunkCommand(v1, vaultctlfsm.MarshalSealChunk(a, now, 7, 700, now, now, now, true)),
		MarshalVaultChunkCommand(v1, vaultctlfsm.MarshalCreateChunk(c, now, now, now)),
		MarshalVaultChunkCommand(v1, vaultctlfsm.MarshalRequestDelete(chunkIDWithPrefix(0x13), now, "ttl", []string{"node-3", "node-1", "node-2"})),
		mustOuter(MarshalVaultReserveSeqRange(v2, "node-1", vaultctlfsm.InitialSeqEpoch, 40)),
		MarshalVaultPublishFence(v2, 200, now),
	}
}

// TestProtoVerify_WALReplayDeterministic verifies that replaying an identical
// proto command stream produces identical FSM state and byte-identical
// snapshots. This is the migration's "WAL replay yields identical FSM state"
// acceptance, checked at the encoding level (gastrolog-5lrg7 / gastrolog-2q1xq).
func TestProtoVerify_WALReplayDeterministic(t *testing.T) {
	t.Parallel()
	v1, v2 := glid.New(), glid.New()
	now := time.Unix(0, 1_700_000_000_123_456_789)

	build := func() *FSM {
		f := NewFSM()
		for i, cmd := range protoCmdStream(v1, v2, now) {
			if r := f.Apply(&hraft.Log{Data: cmd}); r != nil {
				if err, ok := r.(error); ok && err != nil {
					t.Fatalf("apply cmd %d: %v", i, err)
				}
			}
		}
		return f
	}

	f1, f2 := build(), build()

	if fp1, fp2 := vaultFSMFingerprint(f1), vaultFSMFingerprint(f2); fp1 != fp2 {
		t.Fatalf("fingerprints differ across replays:\n%s\n---\n%s", fp1, fp2)
	}

	snapBytes := func(f *FSM) []byte {
		snap, err := f.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		var buf bytes.Buffer
		if err := snap.Persist(&bufSink{Writer: &buf}); err != nil {
			t.Fatalf("Persist: %v", err)
		}
		return buf.Bytes()
	}
	if !bytes.Equal(snapBytes(f1), snapBytes(f2)) {
		t.Fatal("snapshot bytes differ across identical replays")
	}
}
