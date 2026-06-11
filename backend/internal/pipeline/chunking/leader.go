package chunking

import (
	"context"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// planOnce runs one leader planner step. cronDue=true forces a cron rotation
// trigger for a non-empty open manifest (scheduler-driven sealing); the planner
// no-ops for non-leaders regardless.
func (v *vaultChunking) planOnce(ctx context.Context, cronDue bool) error {
	if !v.cfg.IsLeader() || v.cfg.Applier == nil {
		return nil
	}
	if v.cfg.FSM.SealedManifest() != nil {
		return nil
	}

	v.planMu.Lock()

	open := v.cfg.FSM.OpenChunk()
	views, closeViews, err := v.loadSegmentViews()
	if err != nil {
		v.planMu.Unlock()
		return err
	}
	defer closeViews()

	if open == nil && len(views) == 0 {
		v.planMu.Unlock()
		return nil
	}

	manifest, resume := plannerStateFromFSM(v.cfg.FSM, open)
	refAddedAt := plannerRefAddedAt(v.cfg.FSM.ListCompletedSegments(), views)

	decision := Plan(PlannerInput{
		Manifest:   manifest,
		Resume:     resume,
		Segments:   views,
		Policy:     v.cfg.Policy,
		RefAddedAt: refAddedAt,
		CronDue:    cronDue,
	})
	v.planMu.Unlock()

	switch decision.Action {
	case PlannerIdle:
		return nil
	case PlannerRotate:
		if open == nil {
			return nil
		}
		sealedAt := refAddedAt
		if sealedAt.IsZero() {
			sealedAt = open.OpenedAt
		}
		return v.cfg.Applier.Apply(vaultctlfsm.MarshalSealOpenChunkManifest(open.ChunkID, sealedAt))
	case PlannerAddRef:
		if open == nil {
			chunkID := v.cfg.newChunkID()
			openedAt := refAddedAt
			if openedAt.IsZero() {
				openedAt = decision.Ref.RefAddedAt
			}
			return v.cfg.Applier.Apply(vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt))
		}
		ref := decision.Ref
		return v.cfg.Applier.Apply(vaultctlfsm.MarshalAddOpenChunkSegmentRef(open.ChunkID, vaultctlfsm.OpenChunkSegmentRef{
			SegmentID:         ref.SegmentID,
			FirstRecordNumber: ref.FirstRecordNumber,
			LastRecordNumber:  ref.LastRecordNumber,
			SliceBytes:        ref.SliceBytes,
			RefAddedAt:        ref.RefAddedAt,
		}))
	default:
		return nil
	}
}

// planCatchUp runs planOnce until the manifest stops changing or a sealed
// manifest is pending. Callbacks from Apply may advance the chain; the loop
// covers catch-up when Run starts with work already in the FSM.
func (v *vaultChunking) planCatchUp(ctx context.Context) error {
	for range 32 {
		if v.cfg.FSM.SealedManifest() != nil {
			return nil
		}
		open := v.cfg.FSM.OpenChunk()
		refs := 0
		if open != nil {
			refs = len(open.Refs)
		}
		hadOpen := open != nil

		if err := v.planOnce(ctx, false); err != nil {
			return err
		}

		if v.cfg.FSM.SealedManifest() != nil {
			return nil
		}
		open = v.cfg.FSM.OpenChunk()
		newRefs := 0
		if open != nil {
			newRefs = len(open.Refs)
		}
		if !hadOpen && open == nil {
			return nil
		}
		if !hadOpen && open != nil {
			continue
		}
		if newRefs > refs {
			continue
		}
		if hadOpen && open != nil && newRefs == refs {
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

func (v *vaultChunking) loadSegmentViews() ([]SegmentView, func(), error) {
	entries := v.cfg.FSM.ListCompletedSegments()
	views := make([]SegmentView, 0, len(entries))
	closers := make([]func(), 0, len(entries))
	for _, entry := range entries {
		if segmentExhaustedForPlanning(v.cfg.FSM, entry) {
			continue
		}
		path, ok := v.cfg.Locate.SegmentPath(entry.SegmentID)
		if !ok {
			continue
		}
		idx, err := BuildOrderedIndex(path)
		if err != nil {
			continue
		}
		closers = append(closers, func(i *OrderedIndex) func() {
			return func() { _ = i.Close() }
		}(idx))
		views = append(views, SegmentView{
			ID:            entry.SegmentID,
			FirstIngestTS: entry.FirstIngestTS,
			PublishedAt:   entry.PublishedAt,
			Index:         idx,
		})
	}
	return views, func() {
		for _, closeFn := range closers {
			closeFn()
		}
	}, nil
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

func plannerRefAddedAt(entries []vaultctlfsm.CompletedSegmentEntry, views []SegmentView) time.Time {
	if len(views) == 0 {
		return time.Time{}
	}
	eligible := make(map[glid.GLID]struct{}, len(views))
	for _, v := range views {
		eligible[v.ID] = struct{}{}
	}
	var best time.Time
	for _, entry := range entries {
		if _, ok := eligible[entry.SegmentID]; !ok {
			continue
		}
		if best.IsZero() || entry.PublishedAt.After(best) {
			best = entry.PublishedAt
		}
	}
	return best
}
