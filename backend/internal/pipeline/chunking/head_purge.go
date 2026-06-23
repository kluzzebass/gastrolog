package chunking

import (
	"context"

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
// "no such file" build failures on follower homes (gastrolog-3vlse). Holder
// receipts are committed before purge so ReleaseSegments is not blocked on the
// vault-ctl leader missing its own ack (gastrolog-3vlse follow-up).
//
// On multi-home vaults, purge is also gated on every placement holder having
// committed a receipt. Origins promote completed→head (rename), so head/ is
// the only on-disk copy peers can pull; purging it early while collection is
// still catching up wedges remote homes with "segment file missing".
func (v *vaultChunking) flushHeadPurgeForManifest(ctx context.Context, pending *vaultctlfsm.OpenChunkManifest, segmentIDs []glid.GLID) {
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
	if v.cfg.Nudge != nil {
		_ = v.cfg.Nudge.CollectMissing(ctx)
	}
	required := v.requiredHolders()
	holdersWired := v.cfg.RequiredHolders != nil
	fsm := v.fsm()
	for _, id := range segmentIDs {
		if !mayPurgeHeadAfterBuild(fsm, id, required, holdersWired) {
			continue
		}
		_ = paths.PurgeHeadStaging(v.cfg.VaultRoot, id)
	}
}

// purgeStaleHeadCatchUp removes head/ files with no completed-segment registry
// entry (released or never published). Registry-backed segments are purged only
// after local GLCB build via flushHeadPurgeForManifest — not here.
//
// Skipped until the vault-ctl FSM has replayed: on process start the registry
// can be empty briefly while head/ still holds the prior process's files. Purging
// then makes the inspector show zero head counts until collection re-pulls every
// segment (gastrolog-3vlse follow-up).
func (v *vaultChunking) purgeStaleHeadCatchUp() {
	if v.fsm() != nil && !v.fsm().Ready() {
		return
	}
	ids, err := paths.ListSegmentIDs(paths.HeadDir(v.cfg.VaultRoot))
	if err != nil || len(ids) == 0 {
		return
	}
	pending := v.sealedManifestForBuild()
	open := v.fsm().OpenChunk()
	for id := range ids {
		if v.fsm().GetCompletedSegment(id) != nil {
			continue
		}
		if manifestReferencesSegment(pending, id) || openChunkReferencesSegment(open, id) {
			continue
		}
		_ = paths.PurgeHeadStaging(v.cfg.VaultRoot, id)
	}
}
