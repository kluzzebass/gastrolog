package orchestrator

import (
	"testing"

	"gastrolog/internal/vaultraft/vaultctlfsm"
)

func TestClassifyFenceHolesAssignedMissing(t *testing.T) {
	t.Parallel()
	fence := vaultctlfsm.FenceRecord{PrevBoundSeq: 0, UpperBoundSeq: 5}
	present := map[uint64]bool{1: true, 2: true, 4: true, 5: true}
	holes := ClassifyFenceHoles(fence, vaultctlfsm.SeqAllocatorSnapshot{}, func(seq uint64) bool {
		return present[seq]
	})
	missing := assignedMissingHoles(holes)
	if len(missing) != 1 || missing[0] != 3 {
		t.Fatalf("assigned missing = %v, want [3]", missing)
	}
	if gaps := unassignedGapHoles(holes); len(gaps) != 0 {
		t.Fatalf("unassigned gaps = %v, want none", gaps)
	}
}

func TestClassifyFenceHolesUnassignedGap(t *testing.T) {
	t.Parallel()
	fence := vaultctlfsm.FenceRecord{PrevBoundSeq: 10, UpperBoundSeq: 15}
	alloc := vaultctlfsm.SeqAllocatorSnapshot{
		BurnedTails: []vaultctlfsm.SeqBurnedTail{{Start: 12, End: 13, Epoch: 1}},
	}
	holes := ClassifyFenceHoles(fence, alloc, func(uint64) bool { return false })
	gaps := unassignedGapHoles(holes)
	if len(gaps) != 2 || gaps[0] != 12 || gaps[1] != 13 {
		t.Fatalf("gaps = %v, want [12 13]", gaps)
	}
	missing := assignedMissingHoles(holes)
	if len(missing) != 3 {
		t.Fatalf("assigned missing = %v, want [11 14 15]", missing)
	}
}
