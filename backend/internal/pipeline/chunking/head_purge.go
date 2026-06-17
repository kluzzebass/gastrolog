package chunking

import (
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

func manifestReferencesSegment(m *vaultctlfsm.OpenChunkManifest, segmentID glid.GLID) bool {
	if m == nil {
		return false
	}
	for _, ref := range m.Refs {
		if ref.SegmentID == segmentID {
			return true
		}
	}
	return false
}

func openChunkReferencesSegment(m *vaultctlfsm.OpenChunkManifest, segmentID glid.GLID) bool {
	return manifestReferencesSegment(m, segmentID)
}

// flushHeadPurgeForManifest removes head/ copies for manifest segment IDs once
// this home has built the sealed GLCB. Purging before build completes leaves
// "no such file" build failures on follower homes (gastrolog-3vlse).
func (v *vaultChunking) flushHeadPurgeForManifest(pending *vaultctlfsm.OpenChunkManifest, segmentIDs []glid.GLID) {
	if pending == nil || len(segmentIDs) == 0 {
		return
	}
	key := buildKey{chunkID: pending.ChunkID, sealedAt: pending.SealedAt}
	v.mu.Lock()
	built := v.doneBuild == key
	v.mu.Unlock()
	if !built {
		return
	}
	for _, id := range segmentIDs {
		_ = paths.PurgeHeadStaging(v.cfg.VaultRoot, id)
	}
}

// purgeStaleHeadCatchUp removes head/ files with no completed-segment registry
// entry (released or never published). Registry-backed segments are purged only
// after local GLCB build via flushHeadPurgeForManifest — not here.
func (v *vaultChunking) purgeStaleHeadCatchUp() {
	ids, err := paths.ListSegmentIDs(paths.HeadDir(v.cfg.VaultRoot))
	if err != nil || len(ids) == 0 {
		return
	}
	pending := v.sealedManifestForBuild()
	open := v.cfg.FSM.OpenChunk()
	for id := range ids {
		if v.cfg.FSM.GetCompletedSegment(id) != nil {
			continue
		}
		if manifestReferencesSegment(pending, id) || openChunkReferencesSegment(open, id) {
			continue
		}
		_ = paths.PurgeHeadStaging(v.cfg.VaultRoot, id)
	}
}
