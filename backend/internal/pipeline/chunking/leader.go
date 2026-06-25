package chunking

import (
	"context"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/record"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// CatchUpBudget returns how many planner steps to attempt in one wake/tick pass.
func CatchUpBudget(eligible int, policy ManifestRotationPolicy) int {
	return catchUpBudget(eligible, policy)
}

// catchUpBudget returns how many planner steps to attempt in one wake/tick pass.
// Each step is at most one Raft proposal. Scale with eligible registry depth so
// a backlog drains in fewer wall-clock passes; cap so one pass cannot monopolize
// the vault-ctl leader indefinitely.
func catchUpBudget(eligible int, policy ManifestRotationPolicy) int {
	const minBudget = 32
	const maxBudget = 4096

	// Rough refs needed to fill one chunk at typical ~5–10k records/segment/ref,
	// plus backlog proportional to eligible segments still holding records.
	perChunk := int64(64)
	if policy.MaxRecords > 0 {
		perChunk = int64(policy.MaxRecords)/5000 + 8 //nolint:gosec // G115: MaxRecords bounded by rotation policy
	}
	backlog := int64(eligible)/4 + perChunk
	if backlog < minBudget {
		return minBudget
	}
	if backlog > maxBudget {
		return maxBudget
	}
	return int(backlog)
}

// discardStalledEmptyOpen drops an open manifest that has stayed at zero refs
// longer than the rotation MaxAge stall threshold. Empty manifests must not be
// sealed — GLCB build requires at least one record.
func discardStalledEmptyOpen(open *vaultctlfsm.OpenChunkManifest, manifest ManifestSnapshot, policy ManifestRotationPolicy, now time.Time, applier vaultctlfsm.Applier) error {
	if open == nil || len(manifest.Refs) != 0 || applier == nil {
		return nil
	}
	stall := policy.MaxAge
	if stall <= 0 {
		stall = 30 * time.Second
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

// planOnce runs one leader planner step. cronDue=true forces a cron rotation
// trigger for a non-empty open manifest (scheduler-driven sealing); the planner
// no-ops for non-leaders regardless.
func (v *vaultChunking) planOnce(ctx context.Context, cronDue bool) error {
	if !v.cfg.IsLeader() || v.cfg.Applier == nil {
		return nil
	}

	v.planMu.Lock()

	open := v.fsm().OpenChunk()
	eligible := v.eligibleRegistrySegments()
	v.pruneSegmentIndexCache(eligible)

	if open == nil && len(eligible) == 0 {
		v.planMu.Unlock()
		return nil
	}

	manifest, resume := plannerStateFromFSM(v.fsm(), open)
	refAddedAt := plannerRefAddedAtForEligible(v.fsm().ListCompletedSegments(), eligible)
	evalNow := v.now()

	if open == nil {
		wire, ready := v.proposeOpenManifestWire(manifest, resume, eligible, refAddedAt, evalNow, cronDue)
		v.planMu.Unlock()
		if !ready {
			if v.cfg.Collector != nil && len(eligible) > 0 {
				_ = v.cfg.Collector.CollectOnce(ctx)
			}
			return nil
		}
		return v.cfg.Applier.Apply(wire)
	}

	if _, ok := v.cfg.Policy.rotateTrigger(manifest, cronDue, evalNow); ok {
		v.planMu.Unlock()
		return v.applySealOpenManifest(open.ChunkID, evalNow)
	}

	seg, ok := v.lazyPickSegment(manifest, resume, eligible)
	if !ok {
		if err := discardStalledEmptyOpen(open, manifest, v.cfg.Policy, evalNow, v.cfg.Applier); err != nil {
			v.planMu.Unlock()
			return err
		}
		if v.cfg.Collector != nil && len(eligible) > 0 {
			_ = v.cfg.Collector.CollectOnce(ctx)
		}
		v.planMu.Unlock()
		return nil
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
	v.planMu.Unlock()

	switch decision.Action {
	case PlannerIdle:
		return nil
	case PlannerRotate:
		return v.applySealOpenManifest(open.ChunkID, evalNow)
	case PlannerAddRef:
		ref := decision.Ref
		return v.cfg.Applier.Apply(vaultctlfsm.MarshalAddOpenChunkSegmentRef(open.ChunkID, vaultctlfsm.OpenChunkSegmentRef{
			SegmentID:         ref.SegmentID,
			FirstRecordNumber: ref.FirstRecordNumber,
			LastRecordNumber:  ref.LastRecordNumber,
			SliceBytes:        ref.SliceBytes,
			RefAddedAt:        ref.RefAddedAt,
			Bounds:            ref.Bounds,
		}))
	default:
		return nil
	}
}

// applySealOpenManifest proposes SealOpenChunkManifest. Sealed manifests queue
// FIFO on the FSM so rotation is not blocked while earlier chunks build.
func (v *vaultChunking) applySealOpenManifest(chunkID chunk.ChunkID, sealedAt time.Time) error {
	return v.cfg.Applier.Apply(vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, sealedAt))
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
	openedAt := refAddedAt
	if openedAt.IsZero() {
		openedAt = decisionRefAddedAt(eligible)
	}
	return vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt), true
}

// planCatchUp runs planOnce until the manifest stops changing or a sealed
// manifest is pending. Callbacks from Apply may advance the chain; the loop
// covers catch-up when Run starts with work already in the FSM.
func (v *vaultChunking) planCatchUp(ctx context.Context) error {
	if !v.cfg.IsLeader() || v.cfg.Applier == nil {
		return nil
	}
	budget := catchUpBudget(len(v.eligibleRegistrySegments()), v.cfg.Policy)
	for range budget {
		open := v.fsm().OpenChunk()
		refs := 0
		var totalRecords uint64
		if open != nil {
			refs = len(open.Refs)
			totalRecords = open.TotalRecords
		}
		hadOpen := open != nil

		if err := v.planOnce(ctx, false); err != nil {
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
			if len(v.eligibleRegistrySegments()) == 0 {
				return nil
			}
			continue
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
	entries := v.fsm().ListCompletedSegments()
	out := make([]vaultctlfsm.CompletedSegmentEntry, 0, len(entries))
	for _, entry := range entries {
		if segmentExhaustedForPlanning(v.fsm(), entry) {
			continue
		}
		out = append(out, entry)
	}
	return out
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
}

// segmentViewForEntry opens the segment index at most once per cache generation.
// Caller must hold planMu.
func (v *vaultChunking) segmentViewForEntry(entry vaultctlfsm.CompletedSegmentEntry) (SegmentView, bool) {
	idx := v.segmentIndexCache[entry.SegmentID]
	if idx == nil {
		path, ok := v.cfg.Locate.SegmentPath(entry.SegmentID)
		if !ok {
			return SegmentView{}, false
		}
		var err error
		idx, err = v.openOrderedIndex(path)
		if err != nil {
			return SegmentView{}, false
		}
		v.segmentIndexCache[entry.SegmentID] = idx
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
			continue
		}
		if !found || segmentPrecedes(seg, entryAt.EventID, bestView, bestEvent) {
			bestView = seg
			bestEvent = entryAt.EventID
			found = true
		}
	}
	return bestView, found
}

func plannerStateFromFSM(fsm *vaultctlfsm.FSM, open *vaultctlfsm.OpenChunkManifest) (ManifestSnapshot, map[glid.GLID]uint32) {
	// Resume positions must cover EVERY registered segment, not just refs in
	// the current open manifest: segmentResume persists across manifests, and
	// a freshly opened manifest (open == nil after a seal) would otherwise
	// re-chunk records that previous sealed chunks already consumed —
	// duplicating data in every subsequent chunk.
	resume := make(map[glid.GLID]uint32)
	for _, entry := range fsm.ListCompletedSegments() {
		if n, ok := fsm.ResumeRecordNumber(entry.SegmentID); ok {
			resume[entry.SegmentID] = n
		}
	}
	if open == nil {
		return ManifestSnapshot{}, resume
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
	return manifest, resume
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
