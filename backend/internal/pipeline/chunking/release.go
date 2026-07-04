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
	if fsm != nil && fsm.SegmentReferencedInManifest(segmentID) {
		return false
	}
	entry := fsm.GetCompletedSegment(segmentID)
	if entry == nil {
		return false
	}
	if !segmentExhaustedForPlanning(fsm, *entry) {
		return false
	}
	return holdersCover(entry.Holders, requiredHolders)
}

// mayPurgeHeadAfterBuild reports whether this home may drop its head/ copy of a
// segment after local GLCB build. Cluster vaults keep origin head/ and completed/
// until every placement holder has ack'd; tests without
// RequiredHolders wired keep the legacy immediate purge.
//
// A segment referenced by the open chunk or ANY queued sealed manifest must
// survive: "exhausted for planning" means fully assigned to manifests, not
// fully built. Purging on exhaustion alone deleted segments that later queued
// chunks still needed, pinning those chunks in Sealing forever
// (gastrolog-67c9b0).
func mayPurgeHeadAfterBuild(fsm *vaultctlfsm.FSM, segmentID glid.GLID, requiredHolders []string, holdersWired bool) bool {
	if fsm != nil && fsm.SegmentReferencedInManifest(segmentID) {
		return false
	}
	entry := fsm.GetCompletedSegment(segmentID)
	if entry == nil || !segmentExhaustedForPlanning(fsm, *entry) {
		return false
	}
	if !holdersWired {
		return true
	}
	if len(requiredHolders) == 0 {
		return false
	}
	return holdersCover(entry.Holders, requiredHolders)
}

// mayReleaseFromRegistry is like segmentReadyForRegistryRelease but refuses
// release when placement wiring is present yet unresolved (empty required
// slice). holdersCover treats empty required as "ready" for single-node tests;
// that must not drop segments on a multi-home vault when placement lookup fails.
func mayReleaseFromRegistry(fsm *vaultctlfsm.FSM, segmentID glid.GLID, requiredHolders []string, holdersWired bool) bool {
	if holdersWired && len(requiredHolders) == 0 {
		return false
	}
	return segmentReadyForRegistryRelease(fsm, segmentID, requiredHolders)
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
func partitionPendingRelease(fsm *vaultctlfsm.FSM, pending []glid.GLID, requiredHolders []string, holdersWired bool) (ready, stillPending []glid.GLID) {
	for _, id := range pending {
		if mayReleaseFromRegistry(fsm, id, requiredHolders, holdersWired) {
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

// enqueueRegistryReleaseCandidates queues every registry segment that is fully
// consumed and holder-ready. Catches segments missed by afterSealBuild and
// retries after the last required holder ack arrived before release ran.
func (v *vaultChunking) enqueueRegistryReleaseCandidates() {
	if !v.cfg.IsLeader() {
		return
	}
	fsm := v.fsm()
	required := v.requiredHolders()
	holdersWired := v.cfg.RequiredHolders != nil
	var candidates []glid.GLID
	for _, entry := range fsm.ListCompletedSegments() {
		if mayReleaseFromRegistry(fsm, entry.SegmentID, required, holdersWired) {
			candidates = append(candidates, entry.SegmentID)
		}
	}
	if len(candidates) == 0 {
		return
	}
	v.mu.Lock()
	v.pendingRelease = appendUniqueGLIDs(v.pendingRelease, candidates)
	v.mu.Unlock()
}
