package chunking

import (
	"context"
	"fmt"
	"maps"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/record"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

const (
	defaultRefApplyBatchSize = 64
	maxRefApplyBatchSize     = 256
)

// Planner chunk-fill heuristic: a completed segment holds roughly
// plannerRecordsPerRef records under typical close policies (8 MiB / 10 s),
// so MaxRecords/plannerRecordsPerRef approximates the segment refs needed to
// fill one chunk; plannerRefHeadroom pads that estimate for undersized
// trickle segments closed by age rather than size.
const (
	plannerRecordsPerRef = 5000
	plannerRefHeadroom   = 8
)

// estimatedRefsPerChunk is the shared sizing input for refApplyBatchSize and
// catchUpBudget: the rough number of segment refs one chunk absorbs under the
// rotation policy. A policy without MaxRecords falls back to the default
// apply batch size.
func estimatedRefsPerChunk(policy ManifestRotationPolicy) int64 {
	if policy.MaxRecords == 0 {
		return defaultRefApplyBatchSize
	}
	return int64(policy.MaxRecords)/plannerRecordsPerRef + plannerRefHeadroom //nolint:gosec // G115: MaxRecords bounded by rotation policy
}

// refApplyBatchSize is how many segment refs one planner apply may carry.
func refApplyBatchSize(policy ManifestRotationPolicy) int {
	perChunk := estimatedRefsPerChunk(policy)
	if perChunk < defaultRefApplyBatchSize {
		return defaultRefApplyBatchSize
	}
	if perChunk > maxRefApplyBatchSize {
		return maxRefApplyBatchSize
	}
	return int(perChunk)
}

type refBatchResult struct {
	refs   []AddRefDecision
	rotate bool
	noSeg  bool
}

// catchUpBudget returns how many planner steps to attempt in one wake/tick pass.
// Each step is at most one Raft proposal. Scale with eligible registry depth so
// a backlog drains in fewer wall-clock passes; cap so one pass cannot monopolize
// the vault-ctl leader indefinitely.
func catchUpBudget(eligible int, policy ManifestRotationPolicy) int {
	const minBudget = 32
	const maxBudget = 4096

	// Rough refs needed to fill one chunk (estimatedRefsPerChunk), plus
	// backlog proportional to eligible segments still holding records.
	backlog := int64(eligible)/4 + estimatedRefsPerChunk(policy)
	if backlog < minBudget {
		return minBudget
	}
	if backlog > maxBudget {
		return maxBudget
	}
	return int(backlog)
}

// emptyManifestStallFallback is the stall threshold for zero-ref open
// manifests when the rotation policy carries no MaxAge. 30s is several
// segment-close windows (default 10s MaxAge), so a manifest opened just
// before a lull is not churned while refs may still be in flight, while an
// idle vault still sheds its empty open manifest promptly. Do not change the
// value: chunking soak behavior was validated against it, and policies that
// care set MaxAge explicitly.
const emptyManifestStallFallback = 30 * time.Second

// discardStalledEmptyOpen drops an open manifest that has stayed at zero refs
// longer than the rotation MaxAge stall threshold. Empty manifests must not be
// sealed — GLCB build requires at least one record.
func discardStalledEmptyOpen(open *vaultctlfsm.OpenChunkManifest, manifest ManifestSnapshot, policy ManifestRotationPolicy, now time.Time, applier vaultctlfsm.Applier) error {
	if open == nil || len(manifest.Refs) != 0 || applier == nil {
		return nil
	}
	stall := policy.MaxAge
	if stall <= 0 {
		stall = emptyManifestStallFallback
	}
	if now.IsZero() {
		now = time.Now()
	}
	if now.Sub(open.OpenedAt) < stall {
		return nil
	}
	return applier.Apply(vaultctlfsm.MarshalDiscardOpenChunkManifest(open.ChunkID))
}

func (v *vaultChunking) now() time.Time {
	if v.cfg.Now != nil {
		return v.cfg.Now()
	}
	return time.Now()
}

// planOnce runs one leader planner step (at most one segment ref per apply).
// cronDue=true forces a cron rotation trigger for a non-empty open manifest;
// the planner no-ops for non-leaders regardless.
func (v *vaultChunking) planOnce(ctx context.Context, cronDue bool) error {
	return v.planLeaderStep(ctx, cronDue, 1, nil)
}

// plannerPass holds the registry-derived planner inputs for one plan pass,
// computed ONCE (one O(N) scan of the completed-segment registry) instead of
// once per step. The eligible set is fixed for the pass; resume advances from
// the planner's OWN committed decisions, so no step re-scans the FSM registry.
// Segments that become eligible mid-pass (replication catching up, fresh
// publishes) are picked up on the next pass — catch-up is iterative. This is
// what breaks the O(N^2) plan pass (budget ∝ N steps × O(N) scan each) that let
// completed/ accumulate under sustained load (gastrolog-36ba70 / gastrolog-423tpt;
// hot paths named in gastrolog-2m0f75).
type plannerPass struct {
	eligible   []vaultctlfsm.CompletedSegmentEntry
	resume     map[glid.GLID]uint32
	refAddedAt time.Time
}

// newPlannerPass snapshots the registry once and derives the eligible set,
// resume cursors (every registered segment, so a fresh manifest never re-chunks
// consumed records), and the ref-added-at stamp.
func (v *vaultChunking) newPlannerPass() *plannerPass {
	entries := v.fsm().ListCompletedSegments()
	eligible := v.eligibleFromEntries(entries)
	resume := make(map[glid.GLID]uint32, len(entries))
	for _, entry := range entries {
		if n, ok := v.fsm().ResumeRecordNumber(entry.SegmentID); ok {
			resume[entry.SegmentID] = n
		}
	}
	return &plannerPass{
		eligible:   eligible,
		resume:     resume,
		refAddedAt: plannerRefAddedAtForEligible(entries, eligible),
	}
}

// manifestFromOpen builds the planner manifest snapshot from the open chunk.
// Cheap (O(open refs)); read fresh each step so it reflects prior applies.
func manifestFromOpen(open *vaultctlfsm.OpenChunkManifest) ManifestSnapshot {
	if open == nil {
		return ManifestSnapshot{}
	}
	manifest := ManifestSnapshot{
		OpenedAt:     open.OpenedAt,
		TotalRecords: open.TotalRecords,
		TotalBytes:   open.TotalBytes,
		Bounds:       open.Bounds,
		Refs:         make([]ManifestRef, len(open.Refs)),
	}
	for i, ref := range open.Refs {
		manifest.Refs[i] = ManifestRef{
			SegmentID:         ref.SegmentID,
			FirstRecordNumber: ref.FirstRecordNumber,
			LastRecordNumber:  ref.LastRecordNumber,
		}
	}
	return manifest
}

// planLeaderStep proposes open/seal/ref vault-ctl commands. maxRefs caps how
// many segment refs one apply may carry (1 for planOnce, larger for catch-up).
func (v *vaultChunking) planLeaderStep(ctx context.Context, cronDue bool, maxRefs int, pass *plannerPass) error {
	if !v.cfg.IsLeader() || v.applier() == nil {
		return nil
	}

	v.planMu.Lock()

	// Standalone callers (planOnce, the sweep) pass nil and get a fresh pass;
	// the catch-up loop threads ONE pass across its steps to avoid re-scanning
	// the registry every step.
	if pass == nil {
		pass = v.newPlannerPass()
		v.pruneSegmentIndexCache(pass.eligible)
	}
	open := v.fsm().OpenChunk()

	if open == nil && len(pass.eligible) == 0 {
		v.planMu.Unlock()
		return nil
	}

	manifest := manifestFromOpen(open)
	resume := pass.resume
	refAddedAt := pass.refAddedAt
	evalNow := v.now()

	if open == nil {
		wire, ready := v.proposeOpenManifestWire(manifest, resume, pass.eligible, refAddedAt, evalNow, cronDue)
		v.planMu.Unlock()
		if !ready {
			// Non-blocking: collection's pass completion re-wakes this
			// worker (OnPassComplete). Blocking on a full pass here stalled
			// planning and sealing under backlog (gastrolog-1b51yf).
			if v.cfg.Collector != nil && len(pass.eligible) > 0 {
				v.cfg.Collector.Nudge()
			}
			return nil
		}
		return v.applier().Apply(wire)
	}

	if _, ok := v.cfg.Policy.rotateTrigger(manifest, cronDue, evalNow); ok {
		v.planMu.Unlock()
		return v.applySealOpenManifest(open.ChunkID, evalNow)
	}

	batch := v.collectRefBatch(manifest, resume, pass.eligible, refAddedAt, evalNow, cronDue, maxRefs)
	v.planMu.Unlock()

	if len(batch.refs) > 0 {
		refs := make([]vaultctlfsm.OpenChunkSegmentRef, len(batch.refs))
		for i, ref := range batch.refs {
			refs[i] = openChunkSegmentRefFromDecision(ref)
		}
		if err := v.applier().Apply(vaultctlfsm.MarshalAddOpenChunkSegmentRefs(open.ChunkID, refs)); err != nil {
			return err
		}
		// Advance the threaded resume cursor from our OWN committed decisions so
		// the next step needs no fresh registry scan (the O(N^2) breaker).
		for _, ref := range batch.refs {
			pass.resume[ref.SegmentID] = ref.LastRecordNumber + 1
		}
		return nil
	}
	if batch.rotate {
		return v.applySealOpenManifest(open.ChunkID, evalNow)
	}
	if batch.noSeg {
		if err := discardStalledEmptyOpen(open, manifest, v.cfg.Policy, evalNow, v.applier()); err != nil {
			return err
		}
		if v.cfg.Collector != nil && len(pass.eligible) > 0 {
			v.cfg.Collector.Nudge()
		}
	}
	return nil
}

func openChunkSegmentRefFromDecision(ref AddRefDecision) vaultctlfsm.OpenChunkSegmentRef {
	return vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         ref.SegmentID,
		FirstRecordNumber: ref.FirstRecordNumber,
		LastRecordNumber:  ref.LastRecordNumber,
		SliceBytes:        ref.SliceBytes,
		RefAddedAt:        ref.RefAddedAt,
		Bounds:            ref.Bounds,
	}
}

// collectRefBatch simulates planner steps under planMu and returns the refs
// that can be committed in one vault-ctl apply. Caller holds planMu.
func (v *vaultChunking) collectRefBatch(
	manifest ManifestSnapshot,
	resume map[glid.GLID]uint32,
	eligible []vaultctlfsm.CompletedSegmentEntry,
	refAddedAt, evalNow time.Time,
	cronDue bool,
	maxRefs int,
) refBatchResult {
	var out refBatchResult
	if maxRefs <= 0 {
		maxRefs = defaultRefApplyBatchSize
	}
	sim := manifest
	simResume := maps.Clone(resume)
	if simResume == nil {
		simResume = make(map[glid.GLID]uint32)
	}
	for range maxRefs {
		if _, ok := v.cfg.Policy.rotateTrigger(sim, cronDue, evalNow); ok && manifestHasContent(sim) {
			out.rotate = true
			return out
		}
		seg, ok := v.lazyPickSegment(sim, simResume, eligible)
		if !ok {
			out.noSeg = true
			return out
		}
		decision := Plan(PlannerInput{
			Manifest:   sim,
			Resume:     simResume,
			Segments:   []SegmentView{seg},
			Policy:     v.cfg.Policy,
			RefAddedAt: refAddedAt,
			EvalNow:    evalNow,
			CronDue:    cronDue,
		})
		switch decision.Action {
		case PlannerAddRef:
			out.refs = append(out.refs, decision.Ref)
			sim = manifestAfterAddRef(sim, decision.Ref)
			simResume[decision.Ref.SegmentID] = decision.Ref.LastRecordNumber + 1
		case PlannerRotate:
			out.rotate = true
			return out
		case PlannerIdle:
			return out
		}
	}
	return out
}

// applySealOpenManifest proposes SealOpenChunkManifest. Sealed manifests queue
// FIFO on the FSM so rotation is not blocked while earlier chunks build.
func (v *vaultChunking) applySealOpenManifest(chunkID chunk.ChunkID, sealedAt time.Time) error {
	return v.applier().Apply(vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, sealedAt))
}

// proposeOpenManifestWire picks a plannable segment and returns Raft log data
// for OpenChunkManifest when the planner would add the first ref. Caller holds
// planMu. The second return is false when no manifest should be opened yet.
func (v *vaultChunking) proposeOpenManifestWire(
	manifest ManifestSnapshot,
	resume map[glid.GLID]uint32,
	eligible []vaultctlfsm.CompletedSegmentEntry,
	refAddedAt, evalNow time.Time,
	cronDue bool,
) ([]byte, bool) {
	seg, ok := v.lazyPickSegment(manifest, resume, eligible)
	if !ok {
		return nil, false
	}
	decision := Plan(PlannerInput{
		Manifest:   manifest,
		Resume:     resume,
		Segments:   []SegmentView{seg},
		Policy:     v.cfg.Policy,
		RefAddedAt: refAddedAt,
		EvalNow:    evalNow,
		CronDue:    cronDue,
	})
	if decision.Action != PlannerAddRef {
		return nil, false
	}
	chunkID := v.cfg.newChunkID()
	// OpenedAt is the manifest's max-age rotation anchor and MUST be the
	// wall clock at open — never a segment-derived timestamp. The previous
	// stamp (newest eligible PublishedAt) trails open time by exactly the
	// planning lag, so under seal backlog every new manifest was born
	// already older than MaxAge and rotated at its first ref batch,
	// flooding the FIFO seal queue with tiny chunks (gastrolog-4olqp6).
	openedAt := evalNow
	if openedAt.IsZero() {
		// Replay-style callers without a clock: fall back to the segment
		// timestamps rather than stamping a zero OpenedAt.
		openedAt = refAddedAt
		if openedAt.IsZero() {
			openedAt = decisionRefAddedAt(eligible)
		}
	}
	return vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt), true
}

// planCatchUp runs planOnce until the manifest stops changing or a sealed
// manifest is pending. Callbacks from Apply may advance the chain; the loop
// covers catch-up when Run starts with work already in the FSM.
func (v *vaultChunking) planCatchUp(ctx context.Context) error {
	if !v.cfg.IsLeader() || v.applier() == nil {
		return nil
	}
	pass := v.newPlannerPass()
	v.planMu.Lock()
	v.pruneSegmentIndexCache(pass.eligible)
	v.planMu.Unlock()
	budget := catchUpBudget(len(pass.eligible), v.cfg.Policy)
	for range budget {
		open := v.fsm().OpenChunk()
		refs := 0
		var totalRecords uint64
		if open != nil {
			refs = len(open.Refs)
			totalRecords = open.TotalRecords
		}
		hadOpen := open != nil

		if err := v.planLeaderStep(ctx, false, refApplyBatchSize(v.cfg.Policy), pass); err != nil {
			return err
		}

		open = v.fsm().OpenChunk()
		newRefs := 0
		var newTotal uint64
		if open != nil {
			newRefs = len(open.Refs)
			newTotal = open.TotalRecords
		}
		if !hadOpen && open == nil {
			// The step could not open a manifest from the fixed eligible set —
			// nothing plannable remains this pass. Return rather than re-scan
			// and spin; the next worker wake starts a fresh pass and picks up
			// any segment that became eligible since.
			return nil
		}
		if !hadOpen && open != nil {
			continue
		}
		if newRefs > refs || newTotal > totalRecords {
			continue
		}
		if hadOpen && open != nil && newRefs == refs && newTotal == totalRecords {
			return nil
		}
	}
	return nil
}

// segmentExhaustedForPlanning reports whether the vault-ctl resume cursor has
// consumed all records the registry attributes to this segment. The planner can
// skip opening its on-disk index in that case — pickSegment would ignore it
// anyway (start >= Len). At high ingest rates the completed-segment registry
// grows quickly; re-indexing every segment on every planOnce step is O(n²)
// and stalls manifest fill after the first sealed chunk.
func segmentExhaustedForPlanning(fsm *vaultctlfsm.FSM, entry vaultctlfsm.CompletedSegmentEntry) bool {
	if fsm == nil {
		return false
	}
	n, ok := fsm.ResumeRecordNumber(entry.SegmentID)
	if !ok {
		return false
	}
	return n >= entry.RecordCount
}

func (v *vaultChunking) openOrderedIndex(path string) (*OrderedIndex, error) {
	if v.cfg.IndexOpener != nil {
		return v.cfg.IndexOpener(path)
	}
	return BuildOrderedIndex(path)
}

func (v *vaultChunking) closeSegmentIndexCache() {
	v.planMu.Lock()
	defer v.planMu.Unlock()
	for id, idx := range v.segmentIndexCache {
		if idx != nil {
			_ = idx.Close()
		}
		delete(v.segmentIndexCache, id)
	}
}

// eligibleRegistrySegments returns completed-segment registry entries that still
// have records to chunk. Does not open on-disk indexes.
func (v *vaultChunking) eligibleRegistrySegments() []vaultctlfsm.CompletedSegmentEntry {
	return v.eligibleFromEntries(v.fsm().ListCompletedSegments())
}

// eligibleFromEntries filters a registry snapshot to segments that still have
// records to chunk and are replicated widely enough to plan. Split from
// eligibleRegistrySegments so a pass can filter its single snapshot without a
// second ListCompletedSegments scan.
func (v *vaultChunking) eligibleFromEntries(entries []vaultctlfsm.CompletedSegmentEntry) []vaultctlfsm.CompletedSegmentEntry {
	minHolders := v.plannerMinHolders()
	now := v.now()
	out := make([]vaultctlfsm.CompletedSegmentEntry, 0, len(entries))
	var gated int
	var oldestGated time.Duration
	for _, entry := range entries {
		if segmentExhaustedForPlanning(v.fsm(), entry) {
			continue
		}
		// Replication-window gate (gastrolog-4bl9xx): never plan a segment
		// whose bytes exist on fewer than minHolders nodes. Registry publish
		// precedes replication, and a manifest referencing a single-copy
		// segment wedges the vault's entire serial seal queue if that copy's
		// node dies (builds require real bytes on every home; skipping would
		// be record loss). Release/purge already respect holder receipts;
		// this makes planning respect the same window. Cost: chunking lags
		// ingestion by replication lag — seconds in steady state.
		if minHolders > 0 && len(entry.Holders) < minHolders {
			gated++
			if age := now.Sub(entry.PublishedAt); age > oldestGated {
				oldestGated = age
			}
			continue
		}
		out = append(out, entry)
	}
	v.noteUnderReplicated(gated, oldestGated)
	return out
}

// plannerMinHolders returns the minimum holder count for a segment to be
// plannable: min(2, placement size), from the same live RequiredHolders
// source release/purge gating trusts. Zero (placement wiring absent —
// single-node tests, memory vaults) disables the gate.
func (v *vaultChunking) plannerMinHolders() int {
	if v.cfg.RequiredHolders == nil {
		return 0
	}
	n := len(v.cfg.RequiredHolders())
	if n <= 0 {
		return 0
	}
	return min(2, n)
}

// underReplicatedAlertAfter is the grace period before gated segments raise
// an operator alert. Fresh segments are under-replicated for the seconds
// between publish and the first holder receipts; anything gated minutes has
// lost its replication path (origin died holding the only copy) and the
// loss-vs-wait decision is explicit operator territory (gastrolog-4bl9xx).
const underReplicatedAlertAfter = 2 * time.Minute

func (v *vaultChunking) underReplicatedAlertID() string {
	return "chunking-underreplicated-" + v.cfg.VaultID.String()
}

// noteUnderReplicated raises/clears the under-replicated-segments alert and
// logs each transition once. Called from the planner pass with the count of
// gated segments and the age of the oldest one; the alert exists so a stuck
// replication window is a visible registry condition instead of a silent
// planning stall. Caller holds planMu.
func (v *vaultChunking) noteUnderReplicated(gated int, oldest time.Duration) {
	stuck := gated > 0 && oldest >= underReplicatedAlertAfter
	if stuck == v.underReplicatedAlerted {
		return
	}
	v.underReplicatedAlerted = stuck
	if stuck {
		v.logger().Warn("segments stuck inside their replication window — planning gated",
			"gated", gated, "oldest", oldest.Round(time.Second))
		if v.cfg.Alerts != nil {
			v.cfg.Alerts.Set(v.underReplicatedAlertID(), alert.Warning, "chunking",
				fmt.Sprintf("vault %s: %d segment(s) have been below the replication minimum for %s — chunking waits until a second node holds a copy. Check that all placement nodes are up and replication is progressing; if the origin node is permanently lost, the affected records exist only there",
					v.cfg.VaultID, gated, oldest.Round(time.Second)))
		}
		return
	}
	v.logger().Info("replication window cleared — planning resumed for gated segments")
	if v.cfg.Alerts != nil {
		v.cfg.Alerts.Clear(v.underReplicatedAlertID())
	}
}

func (v *vaultChunking) pruneSegmentIndexCache(eligible []vaultctlfsm.CompletedSegmentEntry) {
	active := make(map[glid.GLID]struct{}, len(eligible))
	for _, entry := range eligible {
		active[entry.SegmentID] = struct{}{}
	}
	for id, idx := range v.segmentIndexCache {
		if _, ok := active[id]; !ok {
			if idx != nil {
				_ = idx.Close()
			}
			delete(v.segmentIndexCache, id)
		}
	}
	v.prunePlanFailures(active)
}

// planFailure tracks one segment whose on-disk index cannot be opened or read.
// Log rate limiting is state-based (gastrolog-6wwdos): the Warn line emits when
// the failure message changes (first failure included), never once per retry.
type planFailure struct {
	count   int
	lastMsg string
}

// unplannableAlertFailures is how many failed plan attempts a segment
// accumulates before the operator alert fires. An index that fails twice is
// deterministic corruption, not a transient read; counting attempts keeps the
// trigger state-based instead of wall-clock-gated.
const unplannableAlertFailures = 2

func (v *vaultChunking) unplannableAlertID() string {
	return "chunking-unplannable-segment-" + v.cfg.VaultID.String()
}

// notePlanFailure records that a segment's on-disk index could not be opened
// or read. Such a segment is skipped by every planner pass: its records are
// never planned into a sealed manifest, never queryable via the chunk path,
// and its head copy is never purged — previously with zero diagnostics
// (gastrolog-6wwdos). Caller holds planMu.
func (v *vaultChunking) notePlanFailure(id glid.GLID, stage string, err error) {
	if v.planFailures == nil {
		v.planFailures = make(map[glid.GLID]*planFailure)
	}
	f := v.planFailures[id]
	if f == nil {
		f = &planFailure{}
		v.planFailures[id] = f
	}
	f.count++
	msg := stage + ": " + err.Error()
	if f.lastMsg != msg {
		f.lastMsg = msg
		v.logger().Warn("segment index unreadable — segment cannot be planned into a sealed manifest",
			"segment", id, "stage", stage, "error", err)
	}
	v.updateUnplannableAlert()
}

// clearPlanFailure drops a segment's plan-failure state once its index opens
// and reads again. Caller holds planMu.
func (v *vaultChunking) clearPlanFailure(id glid.GLID) {
	f := v.planFailures[id]
	if f == nil {
		return
	}
	delete(v.planFailures, id)
	v.logger().Info("segment index readable again — planning resumed",
		"segment", id, "failures", f.count)
	v.updateUnplannableAlert()
}

// prunePlanFailures drops failure state for segments that left the eligible
// set (released, retention give-up expiry, exhausted) so a segment that can
// never plan does not hold the alert after the registry lets it go. Caller
// holds planMu.
func (v *vaultChunking) prunePlanFailures(active map[glid.GLID]struct{}) {
	changed := false
	for id := range v.planFailures {
		if _, ok := active[id]; !ok {
			delete(v.planFailures, id)
			changed = true
		}
	}
	if changed {
		v.updateUnplannableAlert()
	}
}

// updateUnplannableAlert raises/clears the unplannable-segment operator alert
// on state transitions. A single failure may be a transient read; a segment at
// the repeat threshold is deterministically unplannable and needs an operator
// — left alone its records either sit unchunked forever or, when a retention
// give-up bound is configured, are eventually released unchunked as a counted
// expiry. Caller holds planMu.
func (v *vaultChunking) updateUnplannableAlert() {
	repeated := 0
	var example glid.GLID
	for id, f := range v.planFailures {
		if f.count >= unplannableAlertFailures {
			if repeated == 0 {
				example = id
			}
			repeated++
		}
	}
	stuck := repeated > 0
	if stuck == v.planFailureAlerted {
		return
	}
	v.planFailureAlerted = stuck
	if v.cfg.Alerts == nil {
		return
	}
	if stuck {
		v.cfg.Alerts.Set(v.unplannableAlertID(), alert.Error, "chunking",
			fmt.Sprintf("vault %s: %d segment(s) have unreadable on-disk indexes and cannot be planned into sealed manifests (e.g. %s) — their records stay unchunked and their head copies cannot be purged. Investigate segment file corruption on this node; if the vault has a delete-disposition retention TTL, the records will eventually be released unchunked as a counted expiry",
				v.cfg.VaultID, repeated, example))
		return
	}
	v.cfg.Alerts.Clear(v.unplannableAlertID())
}

// segmentViewForEntry opens the segment index at most once per cache generation.
// Caller must hold planMu.
func (v *vaultChunking) segmentViewForEntry(entry vaultctlfsm.CompletedSegmentEntry) (SegmentView, bool) {
	idx := v.segmentIndexCache[entry.SegmentID]
	if idx == nil {
		path, ok := v.cfg.Locate.SegmentPath(entry.SegmentID)
		if !ok {
			// Not a failure: the segment file is not on this node yet
			// (collection lag). The planner nudges the collector when
			// nothing is plannable.
			return SegmentView{}, false
		}
		var err error
		idx, err = v.openOrderedIndex(path)
		if err != nil {
			// A registry segment whose index cannot be built is never
			// planned into any sealed manifest: its records stay
			// unchunked and its head copy cannot be purged. Silent
			// skipping hid that condition entirely (gastrolog-6wwdos).
			v.notePlanFailure(entry.SegmentID, "open segment index", err)
			return SegmentView{}, false
		}
		v.segmentIndexCache[entry.SegmentID] = idx
		v.clearPlanFailure(entry.SegmentID)
	}
	return SegmentView{
		ID:            entry.SegmentID,
		FirstIngestTS: entry.FirstIngestTS,
		PublishedAt:   entry.PublishedAt,
		Index:         idx,
	}, true
}

// lazyPickSegment chooses the next segment to plan against, opening at most one
// index on the partial-manifest path (continuing the last ref) or one index per
// candidate considered on the k-way EventID path.
func (v *vaultChunking) lazyPickSegment(manifest ManifestSnapshot, resume map[glid.GLID]uint32, eligible []vaultctlfsm.CompletedSegmentEntry) (SegmentView, bool) {
	if len(eligible) == 0 {
		return SegmentView{}, false
	}

	byID := make(map[glid.GLID]vaultctlfsm.CompletedSegmentEntry, len(eligible))
	for _, entry := range eligible {
		byID[entry.SegmentID] = entry
	}

	if len(manifest.Refs) > 0 {
		last := manifest.Refs[len(manifest.Refs)-1]
		if entry, ok := byID[last.SegmentID]; ok && partialSegmentTarget(manifest, resume, entry.RecordCount, last.SegmentID) {
			return v.segmentViewForEntry(entry)
		}
	}

	var (
		bestView  SegmentView
		bestEvent record.EventID
		found     bool
	)
	for _, entry := range eligible {
		seg, ok := v.segmentViewForEntry(entry)
		if !ok {
			continue
		}
		start := resumeStart(resume, seg.ID)
		if start >= seg.Index.Len() {
			continue
		}
		entryAt, err := seg.Index.EntryAt(start)
		if err != nil {
			v.notePlanFailure(seg.ID, "read segment index entry", err)
			continue
		}
		v.clearPlanFailure(seg.ID)
		if !found || segmentPrecedes(seg, entryAt.EventID, bestView, bestEvent) {
			bestView = seg
			bestEvent = entryAt.EventID
			found = true
		}
	}
	return bestView, found
}

func plannerRefAddedAtForEligible(entries []vaultctlfsm.CompletedSegmentEntry, eligible []vaultctlfsm.CompletedSegmentEntry) time.Time {
	if len(eligible) == 0 {
		return time.Time{}
	}
	eligibleIDs := make(map[glid.GLID]struct{}, len(eligible))
	for _, e := range eligible {
		eligibleIDs[e.SegmentID] = struct{}{}
	}
	var best time.Time
	for _, entry := range entries {
		if _, ok := eligibleIDs[entry.SegmentID]; !ok {
			continue
		}
		if best.IsZero() || entry.PublishedAt.After(best) {
			best = entry.PublishedAt
		}
	}
	return best
}

func decisionRefAddedAt(eligible []vaultctlfsm.CompletedSegmentEntry) time.Time {
	var best time.Time
	for _, entry := range eligible {
		if best.IsZero() || entry.PublishedAt.After(best) {
			best = entry.PublishedAt
		}
	}
	return best
}
