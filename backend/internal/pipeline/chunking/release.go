package chunking

import (
	"fmt"
	"slices"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// retentionGiveUpAlarmType is the catalog type ID for a vault that keeps
// releasing segments unchunked at its retention give-up bound; the instance
// key is the vault ID. See docs/alarm-management-design.md.
const retentionGiveUpAlarmType = "chunking-retention-giveup"

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

// scanSegmentReady reports whether a segment may be dropped from the
// completed-segment registry. Two independent release signals (design-notes
// 28/39, R3): the segment is superseded — its records live in RF-replicated
// chunks, which un-pins the dead-holder case because the chunk reaches RF
// among the live homes — OR the fast path, every required home already holds
// the raw segment. A segment still referenced by an unbuilt manifest, or not
// fully consumed, is never released. Pure over the pass's ReleaseScan: a
// per-segment variant re-takes the FSM lock four times per segment, making a
// release pass O(N x refs) in lock round-trips.
func scanSegmentReady(scan *vaultctlfsm.ReleaseScan, entry *vaultctlfsm.CompletedSegmentEntry, requiredHolders []string) bool {
	if _, ok := scan.Referenced[entry.SegmentID]; ok {
		return false
	}
	if !scanSegmentExhausted(scan, entry) {
		return false
	}
	if _, ok := scan.Superseded[entry.SegmentID]; ok {
		return true
	}
	return holdersCover(entry.Holders, requiredHolders)
}

// scanSegmentExhausted mirrors segmentExhaustedForPlanning over the scan.
func scanSegmentExhausted(scan *vaultctlfsm.ReleaseScan, entry *vaultctlfsm.CompletedSegmentEntry) bool {
	n, ok := scan.Resume[entry.SegmentID]
	return ok && n >= entry.RecordCount
}

// mayPurgeHeadAfterBuild reports whether this home may drop its head/ copy of a
// segment after local GLCB build. Cluster vaults keep origin head/ and completed/
// until every placement holder has ack'd; an unresolved placement lookup
// (resolved=false) refuses the purge — fail closed — while the explicit
// NoRequiredHolders opt-out (resolved, empty requirement) purges right after
// build.
//
// A segment referenced by the open chunk or ANY queued sealed manifest must
// survive: "exhausted for planning" means fully assigned to manifests, not
// fully built. Purging on exhaustion alone deletes segments that later queued
// chunks still need, pinning those chunks in Sealing forever.
func mayPurgeHeadAfterBuild(fsm *vaultctlfsm.FSM, segmentID glid.GLID, requiredHolders []string, resolved bool, minChunkHolders int) bool {
	if fsm != nil && fsm.SegmentReferencedInManifest(segmentID) {
		return false
	}
	entry := fsm.GetCompletedSegment(segmentID)
	if entry == nil || !segmentExhaustedForPlanning(fsm, *entry) {
		return false
	}
	if !resolved {
		return false
	}
	// Superseded — records are durable in RF-replicated chunks; drop the local
	// head copy without waiting on a possibly-dead home (design-notes 39/R3).
	if fsm.SegmentSuperseded(segmentID, minChunkHolders) {
		return true
	}
	return holdersCover(entry.Holders, requiredHolders)
}

// scanMayRelease is the registry-release gate over a pass's ReleaseScan. It
// refuses release when the placement lookup is unresolved (resolved=false):
// holdersCover treats an empty requirement as "ready" — correct for the
// explicit NoRequiredHolders opt-out, fatal for a multi-home vault whose
// placement lookup failed.
func scanMayRelease(scan *vaultctlfsm.ReleaseScan, entry *vaultctlfsm.CompletedSegmentEntry, requiredHolders []string, resolved bool, giveUpTTL time.Duration, now time.Time) bool {
	// Give-up bound (design-notes 28): records that out-age the vault's
	// delete-disposition retention TTL are released even though they were
	// never chunked — retention would already have deleted them had chunking
	// succeeded. Bounds registry growth for island-origin segments no holder
	// can ever collect. Checked first: it must fire even when placement
	// lookup is unresolved, which is exactly the stuck case it exists for.
	if scanGiveUpExpired(scan, entry, giveUpTTL, now) {
		return true
	}
	if !resolved {
		return false
	}
	return scanSegmentReady(scan, entry, requiredHolders)
}

// scanGiveUpExpired reports whether a segment has sat un-chunked in THIS
// vault's registry longer than the give-up TTL — the counted give-up expiry
// (design-notes 28), the bound on how long an uncollectable/island-origin
// segment may leak registry space. A segment referenced by an unbuilt manifest
// never gives up: the build still needs its bytes, and its records ARE reaching
// a chunk.
//
// The anchor is PublishedAt (when the segment arrived in this vault's
// registry), NOT the records' IngestTS. Records ROUTED from another vault's
// retention-expired output arrive carrying their ORIGINAL (old) IngestTS —
// provenance preserves it through SubmitDrain → routing → digestion (never the
// fresh-ingest minter). Anchoring on record age releases every routed record
// unchunked at arrival, before collection can deliver a second holder and the planner can
// reference it: a cloud-backed destination could not chunk re-routed retention
// output at all, and that output never reached cloud. Records legitimately
// arrive older than a destination's retention window; the give-up
// bounds STUCK time, and the normal retention sweep deletes the records AFTER
// they are chunked.
func scanGiveUpExpired(scan *vaultctlfsm.ReleaseScan, entry *vaultctlfsm.CompletedSegmentEntry, ttl time.Duration, now time.Time) bool {
	if ttl <= 0 || now.IsZero() {
		return false
	}
	if _, ok := scan.Referenced[entry.SegmentID]; ok {
		return false
	}
	if entry.PublishedAt.IsZero() {
		return false
	}
	return now.Sub(entry.PublishedAt) > ttl
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
// ReleaseSegments now and those still awaiting holder receipts. Pure over the
// pass's ReleaseScan.
func partitionPendingRelease(scan *vaultctlfsm.ReleaseScan, pending []glid.GLID, requiredHolders []string, resolved bool, giveUpTTL time.Duration, now time.Time) (ready, stillPending []glid.GLID) {
	for _, id := range pending {
		entry := scan.Entry(id)
		if entry == nil {
			continue
		}
		if scanMayRelease(scan, entry, requiredHolders, resolved, giveUpTTL, now) {
			ready = append(ready, id)
			continue
		}
		stillPending = append(stillPending, id)
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
	required, resolved := v.requiredHolders()
	giveUpTTL, now := v.giveUpBound()
	scan := v.fsm().SnapshotReleaseScan(v.plannerMinHolders())
	var candidates []glid.GLID
	gaveUp := 0
	for i := range scan.Entries {
		entry := &scan.Entries[i]
		if !scanMayRelease(scan, entry, required, resolved, giveUpTTL, now) {
			continue
		}
		candidates = append(candidates, entry.SegmentID)
		if scanGiveUpExpired(scan, entry, giveUpTTL, now) &&
			!scanSegmentReady(scan, entry, required) {
			gaveUp++
		}
	}
	// The counted expiry (design-notes 28) must be deliberate and visible —
	// never a silent pipeline loss.
	v.noteRetentionGiveUp(gaveUp, giveUpTTL)
	if len(candidates) == 0 {
		return
	}
	v.mu.Lock()
	v.pendingRelease = appendUniqueGLIDs(v.pendingRelease, candidates)
	v.mu.Unlock()
}

// noteRetentionGiveUp raises the sustained-give-up operator alarm on the first
// give-up pass and logs the transition edge once. A vault releases a lone
// island-origin segment (no reachable holder) unchunked once, whereas a vault
// whose collection never delivers a second holder does it pass after pass —
// the catalog DelayOn keeps the lone case from annunciating while the STANDING
// flood does. Clearing is deliberately NOT tied to "a pass found nothing left
// to give up": the backlog drains in bursts, so that edge chatters between
// consecutive release passes. The condition that actually ended is "the vault
// started chunking again" — cleared at the seal site via clearRetentionGiveUp.
func (v *vaultChunking) noteRetentionGiveUp(gaveUp int, ttl time.Duration) {
	if gaveUp == 0 {
		return
	}
	v.mu.Lock()
	if v.giveUpAlerted {
		v.mu.Unlock()
		return
	}
	v.giveUpAlerted = true
	v.mu.Unlock()

	v.logger().Warn("releasing segments unchunked at the retention give-up bound — their records are discarded",
		"vault", v.cfg.VaultID, "segments", gaveUp, "ttl", ttl.String())
	if v.cfg.Alerts != nil {
		v.cfg.Alerts.Raise(retentionGiveUpAlarmType, v.cfg.VaultID.String(),
			fmt.Sprintf("Vault %s is losing records: chunking has not reached these segments within %s of publishing, so they are released and their records are discarded.",
				v.vaultLabel(), system.FormatDuration(ttl)))
	}
}

// clearRetentionGiveUp clears the give-up alarm once the vault seals a chunk
// again — the unambiguous signal that chunking recovered (collection delivered
// the holders the planner was waiting on). Called from the leader's seal path;
// a no-op when no give-up alarm stands.
func (v *vaultChunking) clearRetentionGiveUp() {
	v.mu.Lock()
	if !v.giveUpAlerted {
		v.mu.Unlock()
		return
	}
	v.giveUpAlerted = false
	v.mu.Unlock()

	v.logger().Info("retention give-up cleared — vault sealed a chunk again",
		"vault", v.cfg.VaultID)
	if v.cfg.Alerts != nil {
		v.cfg.Alerts.Clear(retentionGiveUpAlarmType, v.cfg.VaultID.String())
	}
}

// giveUpBound resolves the vault's retention give-up TTL and the evaluation
// clock; (0, zero time) when no bound is configured.
func (v *vaultChunking) giveUpBound() (time.Duration, time.Time) {
	if v.cfg.RetentionGiveUpTTL == nil {
		return 0, time.Time{}
	}
	ttl, ok := v.cfg.RetentionGiveUpTTL()
	if !ok || ttl <= 0 {
		return 0, time.Time{}
	}
	return ttl, v.now()
}
