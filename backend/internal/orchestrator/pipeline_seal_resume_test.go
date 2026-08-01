package orchestrator

import (
	"testing"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/memtest"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// ChunkStateSealing carries two populations, and telling them apart is the
// whole point of this file:
//
//   - a chunk-manager chunk caught between BeginSeal and the seal completing.
//     Nothing else can recover it; a crash there strands records short of
//     Sealed forever.
//   - a PIPELINE chunk whose manifest is sealed but whose GLCB is not built.
//     That is work in flight, and sealToGLCB over it would rebuild a blob the
//     pipeline is already producing.
//
// The resume used to bail out on pipeline vaults entirely rather than risk the
// second, which left the first unrecoverable. The discriminator is the sealed
// open-chunk manifest: present for exactly the window an entry sits in Sealing
// awaiting a build, because applySeal pops it in the same step it moves the
// entry to Sealed.

// newResumeReconciler builds the minimum a resume needs: the scheduling call
// passes r.vaultInst.Chunks through to the hook, so the instance has to exist
// even though these tests capture the call instead of running it.
func newResumeReconciler(t *testing.T) *VaultLifecycleReconciler {
	t.Helper()
	s := memtest.MustNewVault(t, chunkmem.Config{})
	return &VaultLifecycleReconciler{
		vaultID:   glid.New(),
		logger:    discardLogger(),
		vaultInst: &VaultInstance{Chunks: s.CM},
	}
}

// resumeCase drives resumeSealingEntries with an explicit awaitingBuild
// predicate and reports which chunks were scheduled.
func resumeCase(t *testing.T, entries []vaultctlfsm.ManifestEntry, local []chunk.ChunkMeta, awaiting map[chunk.ChunkID]bool) []chunk.ChunkID {
	t.Helper()
	r := newResumeReconciler(t)
	var scheduled []chunk.ChunkID
	r.resumeSealingEntries(entries, local,
		func(_ glid.GLID, _ chunk.ChunkManager, id chunk.ChunkID) { scheduled = append(scheduled, id) },
		"test", false,
		func(id chunk.ChunkID) bool { return awaiting[id] })
	return scheduled
}

func sealingEntry(id chunk.ChunkID) vaultctlfsm.ManifestEntry {
	return vaultctlfsm.ManifestEntry{ID: id, State: chunk.ChunkStateSealing}
}

func sealedLocal(id chunk.ChunkID) chunk.ChunkMeta {
	return chunk.ChunkMeta{ID: id, Sealed: true}
}

// The gap this issue exists for: a chunk stranded mid-seal on a vault that also
// runs a pipeline must be resumed, not skipped along with the pipeline's own
// entries.
func TestSealResume_StrandedChunkIsResumedEvenOnAPipelineVault(t *testing.T) {
	t.Parallel()
	stranded := chunk.NewChunkID()
	building := chunk.NewChunkID()

	got := resumeCase(t,
		[]vaultctlfsm.ManifestEntry{sealingEntry(stranded), sealingEntry(building)},
		[]chunk.ChunkMeta{sealedLocal(stranded), sealedLocal(building)},
		map[chunk.ChunkID]bool{building: true})

	if len(got) != 1 || got[0] != stranded {
		t.Fatalf("scheduled %v, want only the stranded chunk %s — the pipeline entry beside it "+
			"must not make it unrecoverable", got, stranded)
	}
}

// The other half, and the reason the old guard existed: a manifest awaiting its
// build must be left alone even though it wears the same state and is present
// locally.
func TestSealResume_ManifestAwaitingBuildIsNotResealed(t *testing.T) {
	t.Parallel()
	building := chunk.NewChunkID()

	got := resumeCase(t,
		[]vaultctlfsm.ManifestEntry{sealingEntry(building)},
		[]chunk.ChunkMeta{sealedLocal(building)},
		map[chunk.ChunkID]bool{building: true})

	if len(got) != 0 {
		t.Fatalf("scheduled %v; re-sealing a manifest awaiting build rebuilds a GLCB the "+
			"pipeline is already producing", got)
	}
}

// Without a predicate the behaviour is what it was before the distinction
// existed, so a reconciler with no FSM cannot start skipping work.
func TestSealResume_NoPredicateResumesEverythingResumable(t *testing.T) {
	t.Parallel()
	a, b := chunk.NewChunkID(), chunk.NewChunkID()

	r := newResumeReconciler(t)
	var scheduled []chunk.ChunkID
	r.resumeSealingEntries(
		[]vaultctlfsm.ManifestEntry{sealingEntry(a), sealingEntry(b)},
		[]chunk.ChunkMeta{sealedLocal(a), sealedLocal(b)},
		func(_ glid.GLID, _ chunk.ChunkManager, id chunk.ChunkID) { scheduled = append(scheduled, id) },
		"test", false, nil)

	if len(scheduled) != 2 {
		t.Fatalf("scheduled %v, want both: a nil predicate must not change behaviour", scheduled)
	}
}

// The pre-existing preconditions still gate everything. A chunk this node never
// held cannot be assembled here whatever population it belongs to.
func TestSealResume_StillRequiresASealedLocalChunk(t *testing.T) {
	t.Parallel()
	absent := chunk.NewChunkID()
	unsealed := chunk.NewChunkID()

	got := resumeCase(t,
		[]vaultctlfsm.ManifestEntry{sealingEntry(absent), sealingEntry(unsealed)},
		[]chunk.ChunkMeta{{ID: unsealed, Sealed: false}},
		nil)

	if len(got) != 0 {
		t.Fatalf("scheduled %v; neither a missing local chunk nor an unsealed one can be resumed", got)
	}
}

// Only Sealing is resumable — a Sealed entry is finished and an Active one has
// not begun.
func TestSealResume_IgnoresEntriesInOtherStates(t *testing.T) {
	t.Parallel()
	active := chunk.NewChunkID()
	sealed := chunk.NewChunkID()

	got := resumeCase(t,
		[]vaultctlfsm.ManifestEntry{
			{ID: active, State: chunk.ChunkStateActive},
			{ID: sealed, State: chunk.ChunkStateSealed},
		},
		[]chunk.ChunkMeta{sealedLocal(active), sealedLocal(sealed)},
		nil)

	if len(got) != 0 {
		t.Fatalf("scheduled %v, want none", got)
	}
}
