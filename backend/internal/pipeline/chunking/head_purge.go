package chunking

import (
	"context"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

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
	if !v.progress.alreadyBuilt(key) {
		return
	}
	if v.cfg.Collector != nil {
		_ = v.cfg.Collector.CollectOnce(ctx)
	}
	required := v.requiredHolders()
	holdersWired := v.cfg.RequiredHolders != nil
	fsm := v.fsm()
	for _, id := range segmentIDs {
		if !mayPurgeHeadAfterBuild(fsm, id, required, holdersWired) {
			continue
		}
		v.logger().Info("purging head segment after build",
			"segment", id, "chunk", pending.ChunkID)
		_ = paths.PurgeHeadStaging(v.cfg.VaultRoot, id)
	}
}

// drainReleasedPurge purges head/ copies for segment IDs queued by the
// wake-only ReleaseSegments FSM callback. Runs on the worker goroutine —
// never on the Raft apply goroutine (gastrolog-38snf4 teardown deadlock).
func (v *vaultChunking) drainReleasedPurge() {
	v.purgeMu.Lock()
	ids := v.pendingPurge
	v.pendingPurge = nil
	v.purgeMu.Unlock()
	if len(ids) > 0 {
		v.purgeReleasedHead(ids)
	}
}

// purgeReleasedHead drops head/pre-head copies for segments the registry just
// released. Idempotent; safe to call from both chunking and supervisor hooks.
func (v *vaultChunking) purgeReleasedHead(ids []glid.GLID) {
	root := v.cfg.VaultRoot
	if root == "" || len(ids) == 0 {
		return
	}
	for _, id := range ids {
		v.logger().Info("purging head segment after registry release", "segment", id)
		_ = paths.PurgeHeadStaging(root, id)
	}
}

// purgeStaleHeadCatchUp removes head/ files with no completed-segment registry
// entry (released or never published). Registry-backed segments are purged via
// flushHeadPurgeForManifest after local build, or purgeReleasedHead when the
// registry drops the entry.
//
// Skipped until the vault-ctl FSM has replayed: on process start the registry
// can be empty briefly while head/ still holds the prior process's files. Purging
// then makes the inspector show zero head counts until collection re-pulls every
// segment (gastrolog-3vlse follow-up).
func (v *vaultChunking) purgeStaleHeadCatchUp() {
	if v.fsm() != nil && !v.fsm().Ready() {
		return
	}
	root := v.cfg.VaultRoot
	if root == "" {
		return
	}
	ids, err := paths.ListSegmentIDs(paths.HeadDir(root))
	if err != nil || len(ids) == 0 {
		return
	}
	fsm := v.fsm()
	for id := range ids {
		if fsm.GetCompletedSegment(id) != nil {
			continue
		}
		// Full-queue reference scan, not just the head-of-queue manifest:
		// a segment can be referenced only by a later queued sealed manifest
		// (gastrolog-67c9b0).
		if fsm.SegmentReferencedInManifest(id) {
			continue
		}
		v.logger().Info("purging stale head segment with no registry entry", "segment", id)
		_ = paths.PurgeHeadStaging(root, id)
	}
}
