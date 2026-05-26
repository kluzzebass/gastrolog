package orchestrator

import (
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// HoleClass labels a missing sequence in a fence range.
type HoleClass string

const (
	HoleClassAssignedMissing HoleClass = "assigned_missing"
	HoleClassUnassignedGap    HoleClass = "unassigned_gap"
)

// ClassifiedHole is one missing sequence with reconcile authority classification.
type ClassifiedHole struct {
	Seq   uint64
	Class HoleClass
}

// seqInBurnedTail reports whether seq falls in a documented unassigned gap.
func seqInBurnedTail(seq uint64, tails []vaultctlfsm.SeqBurnedTail) bool {
	for _, tail := range tails {
		if seq >= tail.Start && seq <= tail.End {
			return true
		}
	}
	return false
}

// ClassifyFenceHoles compares the fence range against allocator burned tails
// and a local presence probe. Sequences in burned tails are unassigned gaps;
// other missing sequences are assigned-missing holes that reconcile must heal.
func ClassifyFenceHoles(
	fence vaultctlfsm.FenceRecord,
	alloc vaultctlfsm.SeqAllocatorSnapshot,
	localPresent func(seq uint64) bool,
) []ClassifiedHole {
	if localPresent == nil {
		localPresent = func(uint64) bool { return false }
	}
	var holes []ClassifiedHole
	for seq := fence.PrevBoundSeq + 1; seq <= fence.UpperBoundSeq; seq++ {
		if localPresent(seq) {
			continue
		}
		class := HoleClassAssignedMissing
		if seqInBurnedTail(seq, alloc.BurnedTails) {
			class = HoleClassUnassignedGap
		}
		holes = append(holes, ClassifiedHole{Seq: seq, Class: class})
	}
	return holes
}

func assignedMissingHoles(holes []ClassifiedHole) []uint64 {
	var out []uint64
	for _, h := range holes {
		if h.Class == HoleClassAssignedMissing {
			out = append(out, h.Seq)
		}
	}
	return out
}

func unassignedGapHoles(holes []ClassifiedHole) []uint64 {
	var out []uint64
	for _, h := range holes {
		if h.Class == HoleClassUnassignedGap {
			out = append(out, h.Seq)
		}
	}
	return out
}
