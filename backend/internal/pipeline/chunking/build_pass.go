package chunking

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// This file is the per-vault GLCB build/seal pass. Exactly-once transitions
// (built / seal proposed / post-seal / OnBuilt) live on sealProgress; the
// functions here sequence them against the vault-ctl FSM. Restart recovery
// (recover.go) shares proposeSealOnce and fireOnBuiltOnce so the two paths
// cannot diverge.

func (v *vaultChunking) buildDue() bool {
	pending := v.sealedManifestForBuild()
	if pending == nil {
		return false
	}
	key := buildKey{chunkID: pending.ChunkID, sealedAt: pending.SealedAt}
	if entry := v.fsm().Get(pending.ChunkID); entry != nil && entry.State == chunk.ChunkStateSealed {
		// Sealed cluster-wide but this home may still need to materialize GLCB.
		return !v.progress.alreadyBuilt(key)
	}
	if !v.progress.alreadyBuilt(key) {
		return true
	}
	if v.progress.shouldPropose(key) {
		return true
	}
	if v.cfg.IsLeader() && !v.chunkSealCommitted(pending.ChunkID) {
		v.progress.resetSealProposal()
		return true
	}
	return false
}

func (v *vaultChunking) sealedManifestForBuild() *vaultctlfsm.OpenChunkManifest {
	if pending := v.fsm().SealedManifest(); pending != nil {
		v.progress.setPending(pending)
		return pending
	}
	return v.progress.pendingManifest()
}

func (v *vaultChunking) afterSealBuild(ctx context.Context, pending *vaultctlfsm.OpenChunkManifest) {
	if pending == nil {
		return
	}
	key := buildKey{chunkID: pending.ChunkID, sealedAt: pending.SealedAt}
	// claimPostSeal refuses until the local build is marked done — see the
	// sealProgress method comment (gastrolog-3vlse).
	if !v.progress.claimPostSeal(key) {
		return
	}

	segmentIDs := releasableSegmentIDs(v.fsm(), pending)
	v.flushHeadPurgeForManifest(ctx, pending, segmentIDs)
	if v.cfg.IsLeader() {
		if len(segmentIDs) > 0 {
			v.mu.Lock()
			v.pendingRelease = appendUniqueGLIDs(v.pendingRelease, segmentIDs)
			v.mu.Unlock()
		}
		v.releaseWake.Notify()
	}
	v.progress.clearPendingAfterBuilt(pending.ChunkID, key)
}

func (v *vaultChunking) buildOnce(ctx context.Context) error {
	pending := v.sealedManifestForBuild()
	if pending == nil {
		return nil
	}
	if len(pending.Refs) == 0 && pending.TotalRecords == 0 {
		return v.discardEmptySealedManifest(pending)
	}

	key := buildKey{chunkID: pending.ChunkID, sealedAt: pending.SealedAt}
	if done, err := v.buildOnceIfSealedElsewhere(ctx, pending, key); done || err != nil {
		return err
	}

	result, builtNow, err := v.runBuildOncePass(ctx, pending, key)
	if errors.Is(err, ErrAwaitingLocalSegments) {
		return nil
	}
	if err != nil || (!builtNow && v.cfg.Applier == nil) {
		return err
	}
	if builtNow {
		// Stage throughput (gastrolog-10n6k8): this home just materialized
		// the sealed GLCB locally.
		v.sealedRecords.Add(uint64(result.RecordCount))
		v.sealedBytes.Add(uint64(result.Bytes)) //nolint:gosec // sizes are non-negative
	}

	if _, err := v.proposeSealOnce(ctx, pending, key, result); err != nil {
		return err
	}

	v.fireOnBuiltOnce(pending, key, builtNow)
	v.finishBuildOnce(ctx, pending, key)
	return nil
}

// buildOnceIfSealedElsewhere handles the case where CmdSealChunk already
// applied cluster-wide. Returns true when no further build work is needed.
func (v *vaultChunking) buildOnceIfSealedElsewhere(ctx context.Context, pending *vaultctlfsm.OpenChunkManifest, key buildKey) (bool, error) {
	entry := v.fsm().Get(pending.ChunkID)
	if entry == nil || entry.State != chunk.ChunkStateSealed {
		return false, nil
	}
	if !v.progress.alreadyBuilt(key) {
		return false, nil
	}
	v.progress.clearPendingAfterBuilt(pending.ChunkID, key)

	// Another home proposed CmdSealChunk first; this home still needs local
	// head purge and release-queue work (gastrolog-3vlse).
	v.afterSealBuild(ctx, pending)
	v.fireOnBuiltOnce(pending, key, true)
	return true, nil
}

func (v *vaultChunking) runBuildOncePass(ctx context.Context, pending *vaultctlfsm.OpenChunkManifest, key buildKey) (BuildResult, bool, error) {
	if v.progress.alreadyBuilt(key) {
		if v.cfg.Applier == nil {
			return BuildResult{}, false, nil
		}
		// Retry CmdSealChunk with the prior build output; do not re-read every
		// segment on each wake/tick while the sealed manifest is still pending.
		if result, ok := v.progress.cachedResult(key); ok {
			return result, false, nil
		}
	}
	if result, ok, err := v.adoptExistingGLCBIfPresent(pending, key); err != nil {
		return BuildResult{}, false, err
	} else if ok {
		return result, false, nil
	}
	result, err := v.build(ctx, pending)
	if err != nil {
		return BuildResult{}, false, err
	}
	v.progress.markBuilt(key, result)
	return result, true, nil
}

// adoptExistingGLCBIfPresent loads BuildResult when data.glcb is already on
// disk (BuildGLCBFile only renames into place after a complete build).
func (v *vaultChunking) adoptExistingGLCBIfPresent(pending *vaultctlfsm.OpenChunkManifest, key buildKey) (BuildResult, bool, error) {
	glcbPath := ChunkGLCBPath(v.cfg.ChunkRoot, pending.ChunkID)
	if _, err := os.Stat(glcbPath); err != nil {
		if os.IsNotExist(err) {
			return BuildResult{}, false, nil
		}
		return BuildResult{}, false, err
	}
	sealedAt := pending.SealedAt
	if sealedAt.IsZero() {
		if entry := v.fsm().Get(pending.ChunkID); entry != nil && !entry.WriteEnd.IsZero() {
			sealedAt = entry.WriteEnd
		}
	}
	result, readErr := BuildResultFromExistingGLCB(glcbPath, sealedAt)
	if readErr != nil {
		return BuildResult{}, false, nil //nolint:nilerr // corrupt GLCB; caller falls through to full rebuild
	}
	adoptKey := key
	if adoptKey.sealedAt.IsZero() {
		adoptKey.sealedAt = result.WriteEnd
	}
	v.progress.markBuilt(adoptKey, result)
	return result, true, nil
}

// clearSealProposedIfLeaderUncommitted drops a stale seal-proposed marker
// when this home is now vault-ctl leader but CmdSealChunk never committed
// (e.g. leadership transferred after a follower build pass).
func (v *vaultChunking) clearSealProposedIfLeaderUncommitted(pending *vaultctlfsm.OpenChunkManifest, key buildKey) bool {
	if pending == nil || !v.cfg.IsLeader() || v.chunkSealCommitted(pending.ChunkID) {
		return false
	}
	return v.progress.resetSealProposalIf(key)
}

// proposeSealOnce commits CmdSealChunk for a built GLCB exactly once per build
// cycle; shared by the live build path (buildOnce) and restart recovery
// (recoverBuiltGLCB). Returns true when this call committed the seal.
func (v *vaultChunking) proposeSealOnce(ctx context.Context, pending *vaultctlfsm.OpenChunkManifest, key buildKey, result BuildResult) (bool, error) {
	if v.cfg.Applier == nil {
		return false, nil
	}
	if !v.progress.shouldPropose(key) {
		if !v.clearSealProposedIfLeaderUncommitted(pending, key) {
			return false, nil
		}
	}
	// Only the vault-ctl leader commits CmdSealChunk. Follower homes
	// materialize the GLCB locally; proposing seal from a follower
	// forwards to the leader but local FSM verification races replication.
	// Mark proposed only while the seal is uncommitted: the guard is a single
	// slot, and marking a recovered already-sealed chunk would clobber the
	// suppression for the manifest actually pending seal.
	if !v.cfg.IsLeader() {
		if !v.chunkSealCommitted(pending.ChunkID) {
			v.progress.markProposed(key)
		}
		return false, nil
	}
	if err := v.cfg.Applier.Apply(vaultctlfsm.MarshalSealChunk(
		pending.ChunkID,
		result.WriteEnd,
		int64(result.RecordCount),
		result.Bytes,
		result.IngestStart,
		result.IngestEnd,
		result.SourceEnd,
		result.IngestTSMonotonic,
		v.now(),
	)); err != nil {
		return false, err
	}
	if !v.chunkSealCommitted(pending.ChunkID) {
		return false, fmt.Errorf("chunking: CmdSealChunk did not commit seal for %s", pending.ChunkID)
	}
	v.progress.markProposed(key)
	v.afterSealBuild(ctx, pending)
	return true, nil
}

// fireOnBuiltOnce fires OnBuilt at most once per build cycle. builtNow is
// false when the pass reused a cached or adopted result and OnBuilt should
// not fire from this call site.
func (v *vaultChunking) fireOnBuiltOnce(pending *vaultctlfsm.OpenChunkManifest, key buildKey, builtNow bool) {
	if !builtNow || v.cfg.OnBuilt == nil {
		return
	}
	if v.progress.claimOnBuilt(key) {
		v.cfg.OnBuilt(pending.ChunkID)
	}
}

func (v *vaultChunking) finishBuildOnce(ctx context.Context, pending *vaultctlfsm.OpenChunkManifest, key buildKey) {
	built := v.progress.alreadyBuilt(key)
	postSeal := v.progress.postSealDone(key)
	// Follower homes: CmdSealChunk often replicates before local build completes.
	// OnSealedManifestCleared is a no-op until built; finish the purge here.
	if built && !postSeal {
		if entry := v.fsm().Get(pending.ChunkID); entry != nil && entry.State == chunk.ChunkStateSealed {
			v.afterSealBuild(ctx, pending)
			postSeal = v.progress.postSealDone(key)
		}
	}
	if postSeal {
		v.flushHeadPurgeForManifest(ctx, pending, releasableSegmentIDs(v.fsm(), pending))
	}
	// Retain the pending manifest until CmdSealChunk commits so
	// OnSealedManifestCleared can run afterSealBuild on follower homes.
	if entry := v.fsm().Get(pending.ChunkID); entry != nil && entry.State == chunk.ChunkStateSealed {
		v.progress.clearPendingAfterBuilt(pending.ChunkID, key)
	}
}

func (v *vaultChunking) discardEmptySealedManifest(pending *vaultctlfsm.OpenChunkManifest) error {
	if pending == nil || v.cfg.Applier == nil || !v.cfg.IsLeader() {
		return nil
	}
	return v.cfg.Applier.Apply(vaultctlfsm.MarshalDiscardOpenChunkManifest(pending.ChunkID))
}

func (v *vaultChunking) build(ctx context.Context, pending *vaultctlfsm.OpenChunkManifest) (BuildResult, error) {
	manifest := sealedManifestFromFSM(pending)
	if err := v.materializeManifestSegments(ctx, manifest); err != nil {
		return BuildResult{}, err
	}
	input := BuildInput{
		Manifest:  manifest,
		VaultID:   v.cfg.VaultID,
		ChunkRoot: v.cfg.ChunkRoot,
		Locate:    v.cfg.Locate,
	}
	return BuildSealedChunk(input)
}

// materializeManifestSegments ensures every segment referenced by a sealed
// manifest awaiting GLCB build is present under this home's head/ or
// completed/. Collection is the normal multi-home replication path — each
// placement member builds the GLCB locally from the same manifest refs.
func (v *vaultChunking) materializeManifestSegments(ctx context.Context, manifest SealedManifest) error {
	if v.cfg.Locate == nil {
		return nil
	}
	missing := missingManifestSegmentIDs(manifest, v.cfg.Locate)
	if len(missing) == 0 {
		v.clearBuildBlocked()
		return nil
	}
	if v.cfg.Collector == nil {
		v.noteBuildBlocked(manifest.ChunkID, missing)
		if !v.cfg.IsLeader() {
			return ErrAwaitingLocalSegments
		}
		return &MissingSegmentsError{SegmentIDs: missing}
	}
	if err := v.cfg.Collector.CollectSegments(ctx, missing); err != nil {
		v.noteBuildBlocked(manifest.ChunkID, missing)
		if !v.cfg.IsLeader() {
			return ErrAwaitingLocalSegments
		}
		return err
	}
	stillMissing := missingManifestSegmentIDs(manifest, v.cfg.Locate)
	if len(stillMissing) == 0 {
		v.clearBuildBlocked()
		return nil
	}
	v.noteBuildBlocked(manifest.ChunkID, stillMissing)
	if !v.cfg.IsLeader() {
		return ErrAwaitingLocalSegments
	}
	return &MissingSegmentsError{SegmentIDs: stillMissing}
}

// buildBlockedAlertAfter is the grace period before a blocked build raises an
// operator alert. Collection normally materializes missing segments within
// seconds of a seal; anything blocked minutes is stuck, not catching up.
const buildBlockedAlertAfter = 2 * time.Minute

func (v *vaultChunking) buildBlockedAlertID() string {
	return "chunking-build-blocked-" + v.cfg.VaultID.String()
}

// noteBuildBlocked tracks how long the head-of-queue sealed manifest has been
// unbuildable because referenced segment files are missing on this node, and
// raises an operator alert once the condition outlives the grace period.
// Sealing is serial per vault: a blocked head manifest pins every later chunk
// in Sealing and the records inside are unqueryable until it clears
// (gastrolog-67c9b0). Called only from the build pass, under buildMu.
func (v *vaultChunking) noteBuildBlocked(chunkID chunk.ChunkID, missing []glid.GLID) {
	now := v.now()
	if v.blockedChunk != chunkID || v.blockedSince.IsZero() {
		v.blockedChunk = chunkID
		v.blockedSince = now
		return
	}
	blockedFor := now.Sub(v.blockedSince)
	if v.cfg.Alerts == nil || blockedFor < buildBlockedAlertAfter || len(missing) == 0 {
		return
	}
	v.cfg.Alerts.Set(v.buildBlockedAlertID(), alert.Error, "chunking",
		fmt.Sprintf("vault %s: chunk %s blocked in Sealing for %s — %d referenced segment(s) missing on this node (e.g. %s); later chunks cannot seal until this resolves",
			v.cfg.VaultID, chunkID, blockedFor.Round(time.Second), len(missing), missing[0]))
}

// clearBuildBlocked resets blocked-build tracking and drops the alert. Called
// when every referenced segment is present again (or the manifest advanced).
func (v *vaultChunking) clearBuildBlocked() {
	if v.blockedSince.IsZero() {
		return
	}
	v.blockedChunk = chunk.ChunkID{}
	v.blockedSince = time.Time{}
	if v.cfg.Alerts != nil {
		v.cfg.Alerts.Clear(v.buildBlockedAlertID())
	}
}

// chunkSealCommitted reports whether CmdSealChunk took effect cluster-wide.
// After a forwarded apply the local FSM may lag briefly; a cleared pending
// sealed manifest is treated as success even before the entry shows Sealed.
func (v *vaultChunking) chunkSealCommitted(chunkID chunk.ChunkID) bool {
	if entry := v.fsm().Get(chunkID); entry != nil && entry.State == chunk.ChunkStateSealed {
		return true
	}
	sm := v.fsm().SealedManifest()
	return sm == nil || sm.ChunkID != chunkID
}

func sealedManifestFromFSM(m *vaultctlfsm.OpenChunkManifest) SealedManifest {
	out := SealedManifest{
		ChunkID:  m.ChunkID,
		OpenedAt: m.OpenedAt,
		SealedAt: m.SealedAt,
		Refs:     make([]ManifestRefEntry, len(m.Refs)),
	}
	for i, ref := range m.Refs {
		out.Refs[i] = ManifestRefEntry{
			SegmentID:         ref.SegmentID,
			FirstRecordNumber: ref.FirstRecordNumber,
			LastRecordNumber:  ref.LastRecordNumber,
		}
	}
	return out
}
