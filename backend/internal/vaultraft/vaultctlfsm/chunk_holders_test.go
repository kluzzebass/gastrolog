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

// onHoldersChanged is the live edge for receipt-only residency
// (gastrolog-68wsli): the WatchChunks bus subscribes so the inspector
// sees holder growth without a snapshot refetch. Fires once per chunk
// whose holder set actually changed; idempotent re-acks stay silent.
func TestChunkHolderChangeCallbackFiresPerActualChange(t *testing.T) {
	t.Parallel()
	id := chunk.NewChunkID()
	f := newHolderTestFSM(t, id)

	var fired []ManifestEntry
	f.SetOnHoldersChanged(func(e ManifestEntry) { fired = append(fired, e) })

	applyHolderCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{id}, "node-1")))
	if len(fired) != 1 {
		t.Fatalf("first ack should fire once, fired %d times", len(fired))
	}
	if len(fired[0].Holders) != 1 || fired[0].Holders[0] != "node-1" {
		t.Fatalf("callback entry holders = %v, want [node-1] (post-apply state)", fired[0].Holders)
	}

	// Idempotent re-ack: holder set unchanged, no fire.
	applyHolderCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{id}, "node-1")))
	if len(fired) != 1 {
		t.Fatalf("idempotent re-ack must not fire, fired %d times total", len(fired))
	}

	// Second holder: fires with the grown set.
	applyHolderCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{id}, "node-2")))
	if len(fired) != 2 || len(fired[1].Holders) != 2 {
		t.Fatalf("second ack should fire with grown holders, fired=%d holders=%v", len(fired), fired[len(fired)-1].Holders)
	}

	// Revoke: fires with the shrunk set (residency shrink must reach the UI).
	applyHolderCmd(t, f, mustMarshalCommand(NewRevokeChunkHolders([]chunk.ChunkID{id}, "node-1")))
	if len(fired) != 3 || len(fired[2].Holders) != 1 || fired[2].Holders[0] != "node-2" {
		t.Fatalf("revoke should fire with shrunk holders, fired=%d holders=%v", len(fired), fired[len(fired)-1].Holders)
	}

	// Revoke of an absent claim: no change, no fire. Unknown chunks skip too.
	applyHolderCmd(t, f, mustMarshalCommand(NewRevokeChunkHolders([]chunk.ChunkID{id}, "node-9")))
	applyHolderCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{chunk.NewChunkID()}, "node-1")))
	if len(fired) != 3 {
		t.Fatalf("no-op applies must not fire, fired %d times total", len(fired))
	}
}

// Batched acks (the sweep shape: one Raft apply covering many chunks)
// fire once per changed chunk in the batch.
func TestChunkHolderChangeCallbackBatched(t *testing.T) {
	t.Parallel()
	a, b := chunk.NewChunkID(), chunk.NewChunkID()
	f := newHolderTestFSM(t, a, b)

	// Pre-ack b so the batch is a change for a but a no-op for b.
	applyHolderCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{b}, "node-1")))

	var fired []ManifestEntry
	f.SetOnHoldersChanged(func(e ManifestEntry) { fired = append(fired, e) })

	applyHolderCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{a, b}, "node-1")))
	if len(fired) != 1 || fired[0].ID != a {
		t.Fatalf("batch should fire only for the actually-changed chunk, fired=%d", len(fired))
	}
}
