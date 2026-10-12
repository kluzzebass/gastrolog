package vaultctlfsm

import (
	"bytes"
	"slices"
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

func TestHolderQuorumAndPlacedHolderCount(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		holders    []string
		placement  []string
		wantPlaced int
		wantQuorum int
	}{
		{"empty placement disables the gate", []string{"a", "b"}, nil, 0, 0},
		{"single home", []string{"a"}, []string{"a"}, 1, 1},
		{"single home, holder elsewhere", []string{"b"}, []string{"a"}, 0, 1},
		{"no holders", nil, []string{"a", "b"}, 0, 2},
		{"all placed", []string{"a", "b"}, []string{"a", "b", "c"}, 2, 2},
		{"holder outside placement ignored", []string{"b", "a"}, []string{"a", "c"}, 1, 2},
		{"quorum caps at two", []string{"a", "b", "c", "d"}, []string{"a", "b", "c", "d"}, 4, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := PlacedHolderCount(tc.holders, tc.placement); got != tc.wantPlaced {
				t.Errorf("PlacedHolderCount = %d, want %d", got, tc.wantPlaced)
			}
			if got := HolderQuorum(tc.placement); got != tc.wantQuorum {
				t.Errorf("HolderQuorum = %d, want %d", got, tc.wantQuorum)
			}
		})
	}
}

// TestSegmentSupersededCountsOnlyPlacedHolders: a vault on homes {A,B}; B
// builds chunk K and acks; B's disk dies and the vault is re-placed to {A,C};
// A acks. The raw holder set {B,A} meets RF 2, but only A holds a managed
// copy — K must not supersede its segment (which would release and purge the
// raw transport everywhere) until C holds it too.
func TestSegmentSupersededCountsOnlyPlacedHolders(t *testing.T) {
	t.Parallel()
	f := New()
	segID, chunkID := buildChunkedSegment(t, f)
	before := []string{"A", "B"}
	after := []string{"A", "C"}

	applyCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{chunkID}, "B")))
	applyCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{chunkID}, "A")))

	if !f.SegmentSuperseded(segID, before) {
		t.Fatal("fixture: two placed holders under {A,B} must supersede")
	}
	if f.SegmentSuperseded(segID, after) {
		t.Fatal("holder B, outside placement {A,C}, counted toward supersession")
	}
	if _, ok := f.SnapshotReleaseScan(after).Superseded[segID]; ok {
		t.Fatal("release scan marked the segment superseded on a holder outside placement")
	}

	applyCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{chunkID}, "C")))
	if !f.SegmentSuperseded(segID, after) {
		t.Fatal("segment not superseded once two placed homes hold the chunk")
	}
	if _, ok := f.SnapshotReleaseScan(after).Superseded[segID]; !ok {
		t.Fatal("release scan missed supersession once two placed homes hold the chunk")
	}
}

// TestSegmentSupersededSingleHomePlacement: a one-home vault supersedes on
// its own home's receipt and never on a receipt from elsewhere.
func TestSegmentSupersededSingleHomePlacement(t *testing.T) {
	t.Parallel()
	f := New()
	segID, chunkID := buildChunkedSegment(t, f)
	placement := []string{"A"}

	applyCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{chunkID}, "B")))
	if f.SegmentSuperseded(segID, placement) {
		t.Fatal("a non-home receipt superseded a single-home vault's segment")
	}
	applyCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{chunkID}, "A")))
	if !f.SegmentSuperseded(segID, placement) {
		t.Fatal("the home's own receipt must supersede a single-home vault's segment")
	}
}

// pruneFixture is an FSM with chunk and segment holder claims from nodes A
// and B: chunks k (A,B), l (B), m (A); segment s (A,B).
type pruneFixture struct {
	f       *FSM
	k, l, m chunk.ChunkID
	s       glid.GLID
}

func newPruneFixture(t *testing.T) pruneFixture {
	t.Helper()
	fx := pruneFixture{k: chunk.NewChunkID(), l: chunk.NewChunkID(), m: chunk.NewChunkID()}
	fx.f = newHolderTestFSM(t, fx.k, fx.l, fx.m)
	applyHolderCmd(t, fx.f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{fx.k, fx.m}, "A")))
	applyHolderCmd(t, fx.f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{fx.k, fx.l}, "B")))
	fx.s, _ = buildChunkedSegment(t, fx.f)
	applyHolderCmd(t, fx.f, MarshalAckSegmentHolders([]glid.GLID{fx.s}, "A"))
	applyHolderCmd(t, fx.f, MarshalAckSegmentHolders([]glid.GLID{fx.s}, "B"))
	return fx
}

func (fx pruneFixture) assertBPruned(t *testing.T, f *FSM) {
	t.Helper()
	for _, c := range []struct {
		id   chunk.ChunkID
		want []string
	}{{fx.k, []string{"A"}}, {fx.l, []string{}}, {fx.m, []string{"A"}}} {
		e := f.Get(c.id)
		if e == nil {
			t.Fatalf("chunk %s lost", c.id)
		}
		if !slices.Equal(e.Holders, c.want) {
			t.Errorf("chunk %s holders = %v, want %v", c.id, e.Holders, c.want)
		}
	}
	seg := f.GetCompletedSegment(fx.s)
	if seg == nil {
		t.Fatal("segment entry lost")
	}
	if !slices.Equal(seg.Holders, []string{"A"}) {
		t.Errorf("segment holders = %v, want [A]", seg.Holders)
	}
}

// TestPruneNodeDropsChunkAndSegmentHolders: a node out of the vault's scope
// can never revoke its own claims, so CmdPruneNode removes it from every
// chunk and segment holder set and fires the holder-change edge once per
// chunk whose set actually changed.
func TestPruneNodeDropsChunkAndSegmentHolders(t *testing.T) {
	t.Parallel()
	fx := newPruneFixture(t)
	var fired []chunk.ChunkID
	fx.f.SetOnHoldersChanged(func(e ManifestEntry) { fired = append(fired, e.ID) })

	applyHolderCmd(t, fx.f, MarshalPruneNode("B"))
	fx.assertBPruned(t, fx.f)
	slices.SortFunc(fired, func(a, b chunk.ChunkID) int { return bytes.Compare(a[:], b[:]) })
	want := []chunk.ChunkID{fx.k, fx.l}
	slices.SortFunc(want, func(a, b chunk.ChunkID) int { return bytes.Compare(a[:], b[:]) })
	if !slices.Equal(fired, want) {
		t.Fatalf("holder-change edge fired for %v, want exactly %v", fired, want)
	}

	// Idempotent: a second prune changes nothing and fires nothing.
	fired = nil
	applyHolderCmd(t, fx.f, MarshalPruneNode("B"))
	fx.assertBPruned(t, fx.f)
	if len(fired) != 0 {
		t.Fatalf("repeat prune fired the holder-change edge for %v", fired)
	}

	// A pruned node that returns to the placement re-earns its claim.
	applyHolderCmd(t, fx.f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{fx.l}, "B")))
	if got := fx.f.Get(fx.l).Holders; !slices.Equal(got, []string{"B"}) {
		t.Fatalf("re-earned holders = %v, want [B]", got)
	}
}

// TestPruneNodeHolderDropSurvivesSnapshot: the pruned holder sets persist
// through a snapshot, and a follower that restores the pre-prune snapshot
// and then replays the prune converges on the same state as the leader.
func TestPruneNodeHolderDropSurvivesSnapshot(t *testing.T) {
	t.Parallel()
	fx := newPruneFixture(t)
	prePrune := fx.f.SnapshotProto()

	applyHolderCmd(t, fx.f, MarshalPruneNode("B"))
	restored := New()
	restored.RestoreProto(fx.f.SnapshotProto())
	fx.assertBPruned(t, restored)

	follower := New()
	follower.RestoreProto(prePrune)
	applyHolderCmd(t, follower, MarshalPruneNode("B"))
	fx.assertBPruned(t, follower)
}
