package orchestrator

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// TestOverlayFromFSMDoesNotClobberLocalDiskBytes pins the gastrolog-33ul6h
// invariant that the production overlayFromFSM callback (built by
// buildVaultRaftCallbacks, the real closure wired onto every VaultInstance
// with a Raft group — not a test double) only ever overrides the
// cluster-wide fields it documents (CloudBacked, Archived, State, Sealed,
// SealedAt). DiskBytes is per-node live warm-cache state that only the
// local chunk Manager can know; the FSM's replicated ManifestEntry has no
// equivalent fact to overlay it with (ManifestEntry carries CloudBytes,
// the cloud object size, not a local disk claim). If a future change wires
// ManifestEntry.CloudBytes (or anything else) into overlaying DiskBytes,
// this test catches the clobber.
func TestOverlayFromFSMDoesNotClobberLocalDiskBytes(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	id := chunk.NewChunkID()
	now := time.Now()

	applyLocal := func(data []byte) {
		fsm.Apply(&hraft.Log{Data: data})
	}
	applyLocal(vaultctlfsm.MarshalCreateChunk(id, now, now, now))
	applyLocal(vaultctlfsm.MarshalSealChunk(id, now, 10, 5000, now, now, now, false, now))
	applyLocal(vaultctlfsm.MarshalUploadChunk(id, 1234, 0, 0, 0, 0, [32]byte{}, glid.GLID{}, 0))

	e := fsm.Get(id)
	if e == nil || !e.CloudBacked || e.CloudBytes != 1234 {
		t.Fatalf("fixture setup: entry = %+v", e)
	}

	// buildVaultRaftCallbacks only dereferences its *hraft.Raft argument
	// inside closures this test never calls (hasLeader/isLeader/isFSMReady)
	// — overlayFromFSM only reads fsm, so nil is safe here.
	callbacks := buildVaultRaftCallbacks(nil, fsm, nil)
	if callbacks.overlayFromFSM == nil {
		t.Fatal("overlayFromFSM callback is nil")
	}

	// Simulate the local chunk Manager's live view: this node has a warm
	// cache of 777 bytes for the chunk right now (independent of whatever
	// the FSM's CloudBytes says about the uploaded blob's transport size).
	local := chunk.ChunkMeta{
		ID:          id,
		CloudBacked: false, // stale/absent locally (e.g. no CloudStore wired on this node)
		DiskBytes:   777,
		CloudBytes:  0,
	}

	overlaid := callbacks.overlayFromFSM(local)

	if !overlaid.CloudBacked {
		t.Error("overlay should set CloudBacked=true from the FSM (documented behavior)")
	}
	if overlaid.DiskBytes != 777 {
		t.Errorf("overlay clobbered DiskBytes: got %d, want 777 (unchanged local warm-cache state)", overlaid.DiskBytes)
	}
	if overlaid.CloudBytes != 0 {
		t.Errorf("overlay must not inject CloudBytes from the FSM either: got %d, want 0 (unchanged)", overlaid.CloudBytes)
	}
}
