package orchestrator

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// The restart path, against a real vault-ctl FSM.
//
// resumeSealingFromFSM is what runs after a snapshot restore, and it is the
// path that carried the pipeline guard — the steady-state category never had
// one (see reconcileSealingResume). So this is where the gap lived: a node
// killed between AnnounceBeginSeal and PostSealProcess came back, took the
// early return because the vault ran a pipeline, and left its chunk in Sealing
// forever.
//
// Driving a real FSM rather than a stubbed predicate is the point. The
// discriminator is a property of FSM state — a sealed open-chunk manifest is
// present for exactly the window an entry awaits its build — and a fake would
// assert my understanding of that rather than the behaviour.

// fsmWithBothSealingPopulations returns an FSM holding one chunk of each kind,
// both sitting in ChunkStateSealing.
func fsmWithBothSealingPopulations(t *testing.T) (*vaultctlfsm.FSM, chunk.ChunkID, chunk.ChunkID) {
	t.Helper()
	fsm := vaultctlfsm.New()
	now := time.Now()

	// (i) chunk-manager chunk caught mid-seal: created, then BeginSeal, and no
	// CmdSealChunk ever arrives.
	stranded := chunk.NewChunkID()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(stranded, now, now, now)}); err != nil {
		t.Fatalf("create stranded: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalBeginSeal(stranded)}); err != nil {
		t.Fatalf("begin seal stranded: %v", err)
	}

	// (ii) pipeline chunk whose manifest is sealed but whose GLCB is unbuilt:
	// open the manifest, then seal it. The FSM marks this Sealing too.
	building := chunk.NewChunkID()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalOpenChunkManifest(building, now)}); err != nil {
		t.Fatalf("open manifest: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealOpenChunkManifest(building, now)}); err != nil {
		t.Fatalf("seal manifest: %v", err)
	}
	return fsm, stranded, building
}

// The premise the whole fix rests on: both really do land in the same state, so
// a resume that reads state alone cannot tell them apart. If this ever stops
// being true the discriminator is unnecessary and should go.
func TestFSM_BothPopulationsShareSealing(t *testing.T) {
	t.Parallel()
	fsm, stranded, building := fsmWithBothSealingPopulations(t)

	byID := map[chunk.ChunkID]chunk.ChunkState{}
	for _, e := range fsm.List() {
		byID[e.ID] = e.State
	}
	if byID[stranded] != chunk.ChunkStateSealing {
		t.Errorf("stranded chunk state = %v, want Sealing", byID[stranded])
	}
	if byID[building] != chunk.ChunkStateSealing {
		t.Errorf("awaiting-build chunk state = %v, want Sealing — if these differ, "+
			"state alone would be enough and AwaitingBuild is dead weight", byID[building])
	}
}

// And the discriminator separates them from that same state.
func TestFSM_AwaitingBuildSeparatesThem(t *testing.T) {
	t.Parallel()
	fsm, stranded, building := fsmWithBothSealingPopulations(t)

	if fsm.AwaitingBuild(stranded) {
		t.Error("a chunk-manager strand reported as awaiting build; it would never be resumed")
	}
	if !fsm.AwaitingBuild(building) {
		t.Error("a sealed-but-unbuilt manifest reported as resumable; sealToGLCB would run " +
			"over a blob the pipeline is already building")
	}
}

// The manifest is popped when the chunk reaches Sealed, so the discriminator is
// exact rather than approximate — it stops answering true the moment the entry
// leaves the Sealing population.
func TestFSM_AwaitingBuildClearsOnSeal(t *testing.T) {
	t.Parallel()
	fsm, _, building := fsmWithBothSealingPopulations(t)
	now := time.Now()

	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(
		building, now, 10, 100, now, now, now, true, now)}); err != nil {
		t.Fatalf("seal chunk: %v", err)
	}
	if fsm.AwaitingBuild(building) {
		t.Error("still awaiting build after reaching Sealed; the manifest was not popped")
	}
}

// The restart path end to end: only the strand is scheduled, and it is
// scheduled even though the vault runs a pipeline — which is the whole gap.
func TestResumeSealingFromFSM_RecoversTheStrandOnly(t *testing.T) {
	t.Parallel()
	fsm, stranded, building := fsmWithBothSealingPopulations(t)

	r := newResumeReconciler(t)
	r.fsm = fsm
	var scheduled []chunk.ChunkID
	r.postSealHook = func(_ glid.GLID, _ chunk.ChunkManager, id chunk.ChunkID) {
		scheduled = append(scheduled, id)
	}
	// Both chunks are present and sealed locally, so nothing but the
	// discriminator can separate them.
	r.resumeSealingEntries(fsm.List(),
		[]chunk.ChunkMeta{sealedLocal(stranded), sealedLocal(building)},
		r.postSealHook, "restart", true, r.awaitingBuild)

	if len(scheduled) != 1 || scheduled[0] != stranded {
		t.Fatalf("scheduled %v, want only %s — the strand must recover and the "+
			"awaiting-build manifest must be left to the pipeline", scheduled, stranded)
	}
}
