package orchestrator

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// TestGroundMetaFromEntryGroundsClusterFields pins the exact set of fields the
// grounded-read seam sources from the replicated manifest entry (State, Sealed,
// CloudBacked, Archived, SealedAt) — the same set the retired OverlayFromFSM
// callback overlaid.
func TestGroundMetaFromEntryGroundsClusterFields(t *testing.T) {
	t.Parallel()

	sealedAt := time.Now()
	e := vaultctlfsm.ManifestEntry{
		State:       chunk.ChunkStateSealed,
		CloudBacked: true,
		Archived:    true,
		SealedAt:    sealedAt,
	}
	local := chunk.ChunkMeta{
		State:       chunk.ChunkStateActive,
		Sealed:      false,
		CloudBacked: false,
		Archived:    false,
	}

	got := groundMetaFromEntry(local, e)

	if got.State != chunk.ChunkStateSealed {
		t.Errorf("State: got %v, want Sealed", got.State)
	}
	if !got.Sealed {
		t.Error("Sealed should be true when State == Sealed")
	}
	if !got.CloudBacked {
		t.Error("CloudBacked should be grounded from the FSM")
	}
	if !got.Archived {
		t.Error("Archived should be grounded from the FSM")
	}
	if !got.SealedAt.Equal(sealedAt) {
		t.Errorf("SealedAt: got %v, want %v", got.SealedAt, sealedAt)
	}
}

// TestGroundMetaFromEntrySealingReadsNotSealed pins that a Sealing FSM state
// grounds meta.Sealed to false — producer-side iteration must not treat a chunk
// mid-seal as done.
func TestGroundMetaFromEntrySealingReadsNotSealed(t *testing.T) {
	t.Parallel()

	got := groundMetaFromEntry(
		chunk.ChunkMeta{Sealed: true},
		vaultctlfsm.ManifestEntry{State: chunk.ChunkStateSealing},
	)
	if got.State != chunk.ChunkStateSealing {
		t.Errorf("State: got %v, want Sealing", got.State)
	}
	if got.Sealed {
		t.Error("Sealed must read false while the cluster is still Sealing")
	}
}

// TestGroundMetaFromEntryPreservesZeroSealedAt pins that a zero FSM SealedAt does
// not clobber a populated local SealedAt (the FSM anchor is applied only when
// present).
func TestGroundMetaFromEntryPreservesZeroSealedAt(t *testing.T) {
	t.Parallel()

	localSealedAt := time.Now()
	got := groundMetaFromEntry(
		chunk.ChunkMeta{SealedAt: localSealedAt, State: chunk.ChunkStateSealed},
		vaultctlfsm.ManifestEntry{State: chunk.ChunkStateSealed}, // zero SealedAt
	)
	if !got.SealedAt.Equal(localSealedAt) {
		t.Errorf("SealedAt: got %v, want local %v (FSM zero must not clobber)", got.SealedAt, localSealedAt)
	}
}

// TestGroundMetaFromEntryDoesNotClobberLocalDiskBytes pins the merge
// invariant: the merge only ever overrides the documented cluster-wide
// fields. DiskBytes is per-node live warm-cache state that only the local chunk
// Manager can know; the FSM's ManifestEntry has no equivalent fact (it carries
// CloudBytes, the cloud object size, not a local disk claim). If a future change
// wires ManifestEntry.CloudBytes — or anything else — into overwriting DiskBytes,
// this test catches the clobber.
func TestGroundMetaFromEntryDoesNotClobberLocalDiskBytes(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	id := chunk.NewChunkID()
	now := time.Now()

	applyLocal := func(data []byte) { fsm.Apply(&hraft.Log{Data: data}) }
	applyLocal(vaultctlfsm.MarshalCreateChunk(id, now, now, now))
	applyLocal(vaultctlfsm.MarshalSealChunk(id, now, 10, 5000, now, now, now, false, now))
	applyLocal(vaultctlfsm.MarshalUploadChunk(id, 1234, 0, 0, 0, 0, [32]byte{}, glid.GLID{}, 0))

	e := fsm.Get(id)
	if e == nil || !e.CloudBacked || e.CloudBytes != 1234 {
		t.Fatalf("fixture setup: entry = %+v", e)
	}

	// The local chunk Manager's live view: a warm cache of 777 bytes right
	// now, independent of the FSM's CloudBytes (the uploaded blob's transport
	// size).
	local := chunk.ChunkMeta{
		ID:          id,
		CloudBacked: false, // stale/absent locally (no CloudStore wired on this node)
		DiskBytes:   777,
		CloudBytes:  0,
	}

	got := groundMetaFromEntry(local, *e)

	if !got.CloudBacked {
		t.Error("grounding should set CloudBacked=true from the FSM (documented behavior)")
	}
	if got.DiskBytes != 777 {
		t.Errorf("grounding clobbered DiskBytes: got %d, want 777 (unchanged local warm-cache state)", got.DiskBytes)
	}
	if got.CloudBytes != 0 {
		t.Errorf("grounding must not inject CloudBytes from the FSM either: got %d, want 0 (unchanged)", got.CloudBytes)
	}
}

// TestGroundChunkMetaNoFSMReturnsUnchanged pins that groundChunkMeta is a no-op
// when this node has no vault-ctl FSM for the vault (memory mode / single-node):
// the local manager is already authoritative and nothing is grounded.
func TestGroundChunkMetaNoFSMReturnsUnchanged(t *testing.T) {
	t.Parallel()

	o := &Orchestrator{} // no groupMgr → vaultCtlFSMForVault is nil
	local := chunk.ChunkMeta{
		ID:          chunk.NewChunkID(),
		State:       chunk.ChunkStateActive,
		CloudBacked: false,
		DiskBytes:   999,
	}
	got := o.groundChunkMeta(glid.New(), local)
	if got != local {
		t.Errorf("groundChunkMeta with no FSM should return meta unchanged: got %+v, want %+v", got, local)
	}
}
