package chunking

import (
	"slices"

	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// manifestSegmentIDs returns the unique segment IDs referenced by a sealed
// manifest awaiting local GLCB build.
func manifestSegmentIDs(m *vaultctlfsm.OpenChunkManifest) []glid.GLID {
	if m == nil || len(m.Refs) == 0 {
		return nil
	}
	seen := make(map[glid.GLID]struct{}, len(m.Refs))
	out := make([]glid.GLID, 0, len(m.Refs))
	for _, ref := range m.Refs {
		if ref.SegmentID == glid.Nil {
			continue
		}
		if _, ok := seen[ref.SegmentID]; ok {
			continue
		}
		seen[ref.SegmentID] = struct{}{}
		out = append(out, ref.SegmentID)
	}
	return out
}

// releasableSegmentIDs returns manifest segment IDs whose EventID-order records
// are fully consumed (resume cursor reached RecordCount). Partial slices must
// stay in the completed-segment registry so the leader planner can continue the
// segment in the next open manifest.
func releasableSegmentIDs(fsm *vaultctlfsm.FSM, m *vaultctlfsm.OpenChunkManifest) []glid.GLID {
	ids := manifestSegmentIDs(m)
	if fsm == nil || len(ids) == 0 {
		return nil
	}
	out := make([]glid.GLID, 0, len(ids))
	for _, id := range ids {
		entry := fsm.GetCompletedSegment(id)
		if entry == nil {
			continue
		}
		if segmentExhaustedForPlanning(fsm, *entry) {
			out = append(out, id)
		}
	}
	return out
}

// segmentReadyForRegistryRelease reports whether a segment may be dropped from
// the completed-segment registry: fully consumed and every required vault home
// has committed a holder receipt.
func segmentReadyForRegistryRelease(fsm *vaultctlfsm.FSM, segmentID glid.GLID, requiredHolders []string) bool {
	entry := fsm.GetCompletedSegment(segmentID)
	if entry == nil {
		return false
	}
	if !segmentExhaustedForPlanning(fsm, *entry) {
		return false
	}
	return holdersCover(entry.Holders, requiredHolders)
}

func holdersCover(holders, required []string) bool {
	if len(required) == 0 {
		return true
	}
	for _, node := range required {
		if !slices.Contains(holders, node) {
			return false
		}
	}
	return true
}

// partitionPendingRelease splits queued segment IDs into those ready for
// ReleaseSegments now and those still awaiting holder receipts.
func partitionPendingRelease(fsm *vaultctlfsm.FSM, pending []glid.GLID, requiredHolders []string) (ready, stillPending []glid.GLID) {
	for _, id := range pending {
		if segmentReadyForRegistryRelease(fsm, id, requiredHolders) {
			ready = append(ready, id)
			continue
		}
		if fsm.GetCompletedSegment(id) != nil {
			stillPending = append(stillPending, id)
		}
	}
	return ready, stillPending
}

// appendUniqueGLIDs appends ids not already present in pending.
func appendUniqueGLIDs(pending []glid.GLID, ids []glid.GLID) []glid.GLID {
	if len(ids) == 0 {
		return pending
	}
	seen := make(map[glid.GLID]struct{}, len(pending)+len(ids))
	for _, id := range pending {
		seen[id] = struct{}{}
	}
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		pending = append(pending, id)
	}
	return pending
}
