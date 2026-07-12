package vaultctlfsm

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"

	hraft "github.com/hashicorp/raft"
)

func newHolderTestFSM(t *testing.T, ids ...chunk.ChunkID) *FSM {
	t.Helper()
	f := New()
	for _, id := range ids {
		applyCmd(t, f, MarshalCreateChunk(id, time.Now(), time.Now(), time.Time{}))
	}
	return f
}

func applyHolderCmd(t *testing.T, f *FSM, data []byte) {
	t.Helper()
	if err, ok := f.Apply(&hraft.Log{Data: data}).(error); ok && err != nil {
		t.Fatalf("apply: %v", err)
	}
}

func residency(f *FSM, id chunk.ChunkID) []string {
	return f.ChunkResidency(id)
}

func TestChunkHolderAckRevokeLifecycle(t *testing.T) {
	t.Parallel()
	id := chunk.NewChunkID()
	f := newHolderTestFSM(t, id)

	// No receipts: residency is empty — never a placement assumption
	// (gastrolog-68wsli: the optimistic fallback made residency
	// non-monotonic and regressed sealed pips to amber).
	if got := residency(f, id); len(got) != 0 {
		t.Fatalf("pre-receipt residency = %v, want empty (no placement fallback)", got)
	}

	// Two homes earn claims; residency switches to receipts.
	applyHolderCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{id}, "node-1")))
	applyHolderCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{id}, "node-2")))
	got := residency(f, id)
	if len(got) != 2 || got[0] != "node-1" || got[1] != "node-2" {
		t.Fatalf("residency = %v, want [node-1 node-2]", got)
	}

	// Idempotent re-ack: no duplicate.
	applyHolderCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{id}, "node-1")))
	if got := residency(f, id); len(got) != 2 {
		t.Fatalf("re-ack duplicated holder: %v", got)
	}

	// Revoke drops the claim immediately (the incident shape: bytes lost,
	// count must stop lying). Idempotent for absent claims.
	applyHolderCmd(t, f, mustMarshalCommand(NewRevokeChunkHolders([]chunk.ChunkID{id}, "node-1")))
	if got := residency(f, id); len(got) != 1 || got[0] != "node-2" {
		t.Fatalf("post-revoke residency = %v, want [node-2]", got)
	}
	applyHolderCmd(t, f, mustMarshalCommand(NewRevokeChunkHolders([]chunk.ChunkID{id}, "node-1")))
	if got := residency(f, id); len(got) != 1 {
		t.Fatalf("double revoke corrupted holders: %v", got)
	}

	// Re-earn after recovery.
	applyHolderCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{id}, "node-1")))
	if got := residency(f, id); len(got) != 2 {
		t.Fatalf("re-earn failed: %v", got)
	}
}

func TestChunkHolderAckUnknownChunkSkipped(t *testing.T) {
	t.Parallel()
	f := newHolderTestFSM(t)
	// Sealed-then-expunged race: acking a chunk the FSM no longer lists
	// must be a silent no-op, not an error that fails the whole batch.
	applyHolderCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{chunk.NewChunkID()}, "node-1")))
}

func TestChunkHolderCopyOnWrite(t *testing.T) {
	t.Parallel()
	id := chunk.NewChunkID()
	f := newHolderTestFSM(t, id)
	applyHolderCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{id}, "node-1")))
	// A reader's entry copy must be immune to later applies: Get hands out
	// shallow copies, so apply must never mutate a stored Holders backing
	// array in place.
	before := f.Get(id)
	applyHolderCmd(t, f, mustMarshalCommand(NewRevokeChunkHolders([]chunk.ChunkID{id}, "node-1")))
	applyHolderCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{id}, "node-9")))
	if len(before.Holders) != 1 || before.Holders[0] != "node-1" {
		t.Fatalf("reader's copy mutated by later applies: %v", before.Holders)
	}
}

func TestChunkHoldersSurviveSnapshotRestore(t *testing.T) {
	t.Parallel()
	id := chunk.NewChunkID()
	f := newHolderTestFSM(t, id)
	applyHolderCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{id}, "node-1")))
	applyHolderCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{id}, "node-2")))

	snap := f.SnapshotProto()
	restored := New()
	restored.RestoreProto(snap)
	e := restored.Get(id)
	if e == nil {
		t.Fatal("entry lost in snapshot round-trip")
	}
	if len(e.Holders) != 2 || e.Holders[0] != "node-1" || e.Holders[1] != "node-2" {
		t.Fatalf("holders lost in snapshot round-trip: %v", e.Holders)
	}
}
