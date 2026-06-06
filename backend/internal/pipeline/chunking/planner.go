package chunking

import (
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/record"
)

// ManifestSnapshot is the open-chunk manifest state the planner reads.
type ManifestSnapshot struct {
	OpenedAt     time.Time
	TotalRecords uint64
	TotalBytes   uint64
	Refs         []ManifestRef
}

// ManifestRef names one segment slice in EventID order (inclusive last).
type ManifestRef struct {
	SegmentID         glid.GLID
	FirstRecordNumber uint32
	LastRecordNumber  uint32
}

// SegmentView is one eligible segment's EventID index and pick-order metadata.
type SegmentView struct {
	ID            glid.GLID
	FirstIngestTS time.Time
	PublishedAt   time.Time
	Index         *OrderedIndex
}

// ManifestRotationPolicy limits open-chunk growth before seal (V3 manifest model).
type ManifestRotationPolicy struct {
	MaxRecords uint64
	MaxBytes   uint64
	MaxAge     time.Duration
}

// PlannerInput pins every input for a deterministic Plan call.
type PlannerInput struct {
	Manifest   ManifestSnapshot
	Resume     map[glid.GLID]uint32
	Segments   []SegmentView
	Policy     ManifestRotationPolicy
	RefAddedAt time.Time
	CronDue    bool
}

// PlannerAction is the kind of decision Plan returns.
type PlannerAction int

const (
	PlannerIdle PlannerAction = iota
	PlannerRotate
	PlannerAddRef
)

// PlannerDecision is the next manifest step for the vault leader.
// Ref is valid only when Action == PlannerAddRef; Trigger when Action == PlannerRotate.
type PlannerDecision struct {
	Action  PlannerAction
	Trigger string
	Ref     AddRefDecision
}

// AddRefDecision appends one segment slice to the open manifest.
type AddRefDecision struct {
	SegmentID         glid.GLID
	FirstRecordNumber uint32
	LastRecordNumber  uint32
	SliceBytes        uint64
	RefAddedAt        time.Time
}

// Plan chooses the next AddSegmentRef or rotate-now decision.
func Plan(input PlannerInput) PlannerDecision {
	if trig, ok := input.Policy.rotateTrigger(input.Manifest, input.RefAddedAt, input.CronDue); ok {
		return PlannerDecision{Action: PlannerRotate, Trigger: trig}
	}

	segIdx, start, ok := pickSegment(input)
	if !ok {
		return PlannerDecision{Action: PlannerIdle}
	}
	seg := input.Segments[segIdx]

	first := start
	var sliceBytes uint64
	var count uint64
	pos := start
	for pos < seg.Index.Len() {
		frameBytes, err := seg.Index.FrameByteLenAt(pos)
		if err != nil {
			return PlannerDecision{Action: PlannerIdle}
		}
		if _, ok := input.Policy.wouldExceed(input.Manifest, count+1, sliceBytes+frameBytes); ok {
			break
		}
		count++
		sliceBytes += frameBytes
		pos++
	}

	if count == 0 {
		if trig, ok := input.Policy.rotateTrigger(input.Manifest, input.RefAddedAt, input.CronDue); ok {
			return PlannerDecision{Action: PlannerRotate, Trigger: trig}
		}
		return PlannerDecision{Action: PlannerIdle}
	}

	return PlannerDecision{
		Action: PlannerAddRef,
		Ref: AddRefDecision{
			SegmentID:         seg.ID,
			FirstRecordNumber: first,
			LastRecordNumber:  pos - 1,
			SliceBytes:        sliceBytes,
			RefAddedAt:        refAddedAtForSegment(seg, input.RefAddedAt),
		},
	}
}

func refAddedAtForSegment(seg SegmentView, fallback time.Time) time.Time {
	if !seg.PublishedAt.IsZero() {
		return seg.PublishedAt
	}
	return fallback
}

const (
	rotateTriggerCron    = "cron"
	rotateTriggerAge     = "age"
	rotateTriggerRecords = "records"
	rotateTriggerBytes   = "bytes"
)

func (p ManifestRotationPolicy) rotateTrigger(m ManifestSnapshot, refAddedAt time.Time, cronDue bool) (string, bool) {
	if cronDue && manifestHasContent(m) {
		return rotateTriggerCron, true
	}
	if p.MaxAge > 0 && !m.OpenedAt.IsZero() && refAddedAt.Sub(m.OpenedAt) > p.MaxAge && manifestHasContent(m) {
		return rotateTriggerAge, true
	}
	if p.MaxRecords > 0 && m.TotalRecords >= p.MaxRecords {
		return rotateTriggerRecords, true
	}
	if p.MaxBytes > 0 && m.TotalBytes >= p.MaxBytes {
		return rotateTriggerBytes, true
	}
	return "", false
}

func (p ManifestRotationPolicy) wouldExceed(m ManifestSnapshot, addRecords, addBytes uint64) (string, bool) {
	if p.MaxRecords > 0 && m.TotalRecords+addRecords > p.MaxRecords {
		return rotateTriggerRecords, true
	}
	if p.MaxBytes > 0 && m.TotalBytes+addBytes > p.MaxBytes {
		return rotateTriggerBytes, true
	}
	return "", false
}

func manifestHasContent(m ManifestSnapshot) bool {
	return m.TotalRecords > 0 || len(m.Refs) > 0
}

func pickSegment(input PlannerInput) (segIdx int, start uint32, ok bool) {
	if partialID, isPartial := partialSegmentID(input); isPartial {
		for i, seg := range input.Segments {
			if seg.ID != partialID {
				continue
			}
			start := resumeStart(input.Resume, seg.ID)
			if start < seg.Index.Len() {
				return i, start, true
			}
			return 0, 0, false
		}
		return 0, 0, false
	}

	bestIdx := -1
	var bestStart uint32
	var bestEvent record.EventID
	for i := range input.Segments {
		seg := &input.Segments[i]
		start := resumeStart(input.Resume, seg.ID)
		if start >= seg.Index.Len() {
			continue
		}
		entry, err := seg.Index.EntryAt(start)
		if err != nil {
			continue
		}
		if bestIdx < 0 || segmentPrecedes(*seg, entry.EventID, input.Segments[bestIdx], bestEvent) {
			bestIdx = i
			bestStart = start
			bestEvent = entry.EventID
		}
	}
	if bestIdx < 0 {
		return 0, 0, false
	}
	return bestIdx, bestStart, true
}

func resumeStart(resume map[glid.GLID]uint32, id glid.GLID) uint32 {
	if resume == nil {
		return 0
	}
	return resume[id]
}

func partialSegmentID(input PlannerInput) (glid.GLID, bool) {
	if len(input.Manifest.Refs) == 0 {
		return glid.Nil, false
	}
	last := input.Manifest.Refs[len(input.Manifest.Refs)-1]
	for _, seg := range input.Segments {
		if seg.ID != last.SegmentID {
			continue
		}
		start, hasResume := input.Resume[last.SegmentID]
		if !hasResume {
			start = last.LastRecordNumber + 1
		}
		if start < seg.Index.Len() {
			return last.SegmentID, true
		}
		return glid.Nil, false
	}
	return glid.Nil, false
}

func segmentPrecedes(a SegmentView, aEvent record.EventID, b SegmentView, bEvent record.EventID) bool {
	if cmp := aEvent.Compare(bEvent); cmp != 0 {
		return cmp < 0
	}
	if cmp := a.FirstIngestTS.Compare(b.FirstIngestTS); cmp != 0 {
		return cmp < 0
	}
	return a.ID.Compare(b.ID) < 0
}
