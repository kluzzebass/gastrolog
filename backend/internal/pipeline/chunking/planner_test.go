package chunking_test

import (
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/record"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

func segmentView(t *testing.T, path string, id glid.GLID, firstIngest time.Time) chunking.SegmentView {
	t.Helper()
	idx, err := chunking.BuildOrderedIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	return chunking.SegmentView{
		ID:            id,
		FirstIngestTS: firstIngest,
		Index:         idx,
	}
}

func applyRef(m *chunking.ManifestSnapshot, ref chunking.AddRefDecision) {
	m.TotalRecords += uint64(ref.LastRecordNumber - ref.FirstRecordNumber + 1)
	m.TotalBytes += ref.SliceBytes
	m.Refs = append(m.Refs, chunking.ManifestRef{
		SegmentID:         ref.SegmentID,
		FirstRecordNumber: ref.FirstRecordNumber,
		LastRecordNumber:  ref.LastRecordNumber,
	})
}

func TestPlannerPartialSegmentCut(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	path := writeSegment(t, segID, vaultID, []record.Record{
		makeRecord(0, base, "r0"),
		makeRecord(1, base.Add(time.Second), "r1"),
		makeRecord(2, base.Add(2*time.Second), "r2"),
		makeRecord(3, base.Add(3*time.Second), "r3"),
	})
	seg := segmentView(t, path, segID, base)
	now := base.Add(time.Minute)

	decision := chunking.Plan(chunking.PlannerInput{
		Manifest:   chunking.ManifestSnapshot{OpenedAt: base},
		Segments:   []chunking.SegmentView{seg},
		Policy:     chunking.ManifestRotationPolicy{MaxRecords: 2},
		RefAddedAt: now,
	})
	if decision.Action != chunking.PlannerAddRef {
		t.Fatalf("Plan = %+v, want AddRef", decision)
	}
	if decision.Ref.FirstRecordNumber != 0 || decision.Ref.LastRecordNumber != 1 {
		t.Fatalf("ref = [%d,%d], want [0,1]", decision.Ref.FirstRecordNumber, decision.Ref.LastRecordNumber)
	}
}

func TestPlannerResumeAfterPartial(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	path := writeSegment(t, segID, vaultID, []record.Record{
		makeRecord(0, base, "r0"),
		makeRecord(1, base.Add(time.Second), "r1"),
		makeRecord(2, base.Add(2*time.Second), "r2"),
		makeRecord(3, base.Add(3*time.Second), "r3"),
	})
	seg := segmentView(t, path, segID, base)
	now := base.Add(time.Minute)

	manifest := chunking.ManifestSnapshot{OpenedAt: base}
	first := chunking.Plan(chunking.PlannerInput{
		Manifest:   manifest,
		Segments:   []chunking.SegmentView{seg},
		Policy:     chunking.ManifestRotationPolicy{MaxRecords: 2},
		RefAddedAt: now,
	})
	applyRef(&manifest, first.Ref)

	second := chunking.Plan(chunking.PlannerInput{
		Manifest:   manifest,
		Resume:     map[glid.GLID]uint32{segID: 2},
		Segments:   []chunking.SegmentView{seg},
		Policy:     chunking.ManifestRotationPolicy{MaxRecords: 4},
		RefAddedAt: now,
	})
	if second.Action != chunking.PlannerAddRef {
		t.Fatalf("second Plan = %+v, want AddRef", second)
	}
	if second.Ref.SegmentID != segID || second.Ref.FirstRecordNumber != 2 {
		t.Fatalf("resume ref = %+v", second.Ref)
	}
}

func TestPlannerMultiSegmentInterleave(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	vaultID := glid.New()

	segA := glid.New()
	pathA := writeSegment(t, segA, vaultID, []record.Record{
		makeRecord(0, base, "a0"),
	})
	segB := glid.New()
	pathB := writeSegment(t, segB, vaultID, []record.Record{
		makeRecord(0, base.Add(time.Second), "b1"),
	})

	viewA := segmentView(t, pathA, segA, base)
	viewB := segmentView(t, pathB, segB, base.Add(time.Second))
	now := base.Add(time.Hour)

	manifest := chunking.ManifestSnapshot{OpenedAt: base}
	policy := chunking.ManifestRotationPolicy{MaxRecords: 2}

	first := chunking.Plan(chunking.PlannerInput{
		Manifest:   manifest,
		Segments:   []chunking.SegmentView{viewA, viewB},
		Policy:     policy,
		RefAddedAt: now,
	})
	if first.Action != chunking.PlannerAddRef || first.Ref.SegmentID != segA {
		t.Fatalf("first ref = %+v, want segA", first.Ref)
	}
	applyRef(&manifest, first.Ref)

	second := chunking.Plan(chunking.PlannerInput{
		Manifest:   manifest,
		Resume:     map[glid.GLID]uint32{segA: 1},
		Segments:   []chunking.SegmentView{viewA, viewB},
		Policy:     policy,
		RefAddedAt: now,
	})
	if second.Action != chunking.PlannerAddRef || second.Ref.SegmentID != segB {
		t.Fatalf("second ref = %+v, want segB", second.Ref)
	}
}

func TestPlannerRotateAtRecordThreshold(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	path := writeSegment(t, segID, vaultID, []record.Record{
		makeRecord(0, base, "a"),
		makeRecord(1, base.Add(time.Second), "b"),
	})
	seg := segmentView(t, path, segID, base)

	manifest := chunking.ManifestSnapshot{
		OpenedAt:     base,
		TotalRecords: 2,
		Refs: []chunking.ManifestRef{{
			SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 1,
		}},
	}
	decision := chunking.Plan(chunking.PlannerInput{
		Manifest:   manifest,
		Segments:   []chunking.SegmentView{seg},
		Policy:     chunking.ManifestRotationPolicy{MaxRecords: 2},
		RefAddedAt: base.Add(time.Minute),
	})
	if decision.Action != chunking.PlannerRotate || decision.Trigger != "records" {
		t.Fatalf("Plan = %+v, want rotate records", decision)
	}
}

func TestPlannerRotateAtByteThreshold(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	path := writeSegment(t, segID, vaultID, []record.Record{
		makeRecord(0, base, "payload"),
	})
	seg := segmentView(t, path, segID, base)

	first := chunking.Plan(chunking.PlannerInput{
		Manifest:   chunking.ManifestSnapshot{OpenedAt: base},
		Segments:   []chunking.SegmentView{seg},
		Policy:     chunking.ManifestRotationPolicy{MaxBytes: 1 << 20},
		RefAddedAt: base,
	})
	if first.Action != chunking.PlannerAddRef {
		t.Fatalf("first = %+v", first)
	}

	manifest := chunking.ManifestSnapshot{
		OpenedAt:     base,
		TotalBytes:   first.Ref.SliceBytes,
		TotalRecords: 1,
		Refs: []chunking.ManifestRef{{
			SegmentID:         segID,
			FirstRecordNumber: first.Ref.FirstRecordNumber,
			LastRecordNumber:  first.Ref.LastRecordNumber,
		}},
	}
	second := chunking.Plan(chunking.PlannerInput{
		Manifest:   manifest,
		Segments:   []chunking.SegmentView{seg},
		Policy:     chunking.ManifestRotationPolicy{MaxBytes: first.Ref.SliceBytes},
		RefAddedAt: base.Add(time.Second),
	})
	if second.Action != chunking.PlannerRotate || second.Trigger != "bytes" {
		t.Fatalf("second = %+v, want rotate bytes", second)
	}
}

func TestPlannerRotateAtMaxAge(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	firstWrite := base.Add(10 * time.Minute)
	evalNow := firstWrite.Add(2 * time.Hour)

	decision := chunking.Plan(chunking.PlannerInput{
		Manifest: chunking.ManifestSnapshot{
			OpenedAt:     base,
			TotalRecords: 1,
			Bounds: vaultctlfsm.ManifestTimeBounds{
				WriteStart: firstWrite,
				WriteEnd:   firstWrite.Add(time.Minute),
			},
			Refs: []chunking.ManifestRef{{
				SegmentID: glid.New(), FirstRecordNumber: 0, LastRecordNumber: 0,
			}},
		},
		Policy:  chunking.ManifestRotationPolicy{MaxAge: time.Hour},
		EvalNow: evalNow,
	})
	if decision.Action != chunking.PlannerRotate || decision.Trigger != "age" {
		t.Fatalf("Plan = %+v, want rotate age", decision)
	}
}

func TestPlannerMaxAgeUsesFirstChunkWriteNotLast(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	firstWrite := base.Add(time.Minute)
	lastWrite := base.Add(30 * time.Minute)
	// OpenedAt unset: age anchor is WriteStart, not WriteEnd on the latest ref.
	decision := chunking.Plan(chunking.PlannerInput{
		Manifest: chunking.ManifestSnapshot{
			TotalRecords: 2,
			Bounds: vaultctlfsm.ManifestTimeBounds{
				WriteStart: firstWrite,
				WriteEnd:   lastWrite,
			},
		},
		Policy:  chunking.ManifestRotationPolicy{MaxAge: time.Hour},
		EvalNow: lastWrite.Add(30 * time.Minute),
	})
	if decision.Action != chunking.PlannerIdle {
		t.Fatalf("Plan = %+v, want idle (age from WriteStart when OpenedAt unset)", decision)
	}
}

// TestPlannerMaxAgeBacklogDoesNotRotateOnStaleWriteStart: under backlog the
// planner ingests segments whose records carry old WriteTS. MaxAge must use
// manifest OpenedAt so catch-up can fill toward MaxRecords instead of sealing
// micro-chunks the moment the first ref lands.
func TestPlannerMaxAgeBacklogDoesNotRotateOnStaleWriteStart(t *testing.T) {
	t.Parallel()
	evalNow := time.Date(2024, 8, 1, 12, 5, 0, 0, time.UTC)
	staleWriteStart := evalNow.Add(-5 * time.Minute)
	decision := chunking.Plan(chunking.PlannerInput{
		Manifest: chunking.ManifestSnapshot{
			OpenedAt:     evalNow.Add(-30 * time.Second),
			TotalRecords: 25_000,
			Bounds: vaultctlfsm.ManifestTimeBounds{
				WriteStart: staleWriteStart,
				WriteEnd:   staleWriteStart.Add(time.Second),
			},
			Refs: []chunking.ManifestRef{{
				SegmentID: glid.New(), FirstRecordNumber: 0, LastRecordNumber: 24_999,
			}},
		},
		Policy:  chunking.ManifestRotationPolicy{MaxAge: time.Minute, MaxRecords: 1_000_000},
		EvalNow: evalNow,
	})
	if decision.Action == chunking.PlannerRotate && decision.Trigger == "age" {
		t.Fatalf("Plan = %+v, want no age rotate when OpenedAt is recent", decision)
	}
}

func TestPlannerMaxAgeRotatesAfterManifestOpenWallClock(t *testing.T) {
	t.Parallel()
	evalNow := time.Date(2024, 8, 1, 12, 5, 0, 0, time.UTC)
	staleWriteStart := evalNow.Add(-5 * time.Minute)
	decision := chunking.Plan(chunking.PlannerInput{
		Manifest: chunking.ManifestSnapshot{
			OpenedAt:     evalNow.Add(-2 * time.Minute),
			TotalRecords: 25_000,
			Bounds: vaultctlfsm.ManifestTimeBounds{
				WriteStart: staleWriteStart,
				WriteEnd:   staleWriteStart.Add(time.Second),
			},
		},
		Policy:  chunking.ManifestRotationPolicy{MaxAge: time.Minute},
		EvalNow: evalNow,
	})
	if decision.Action != chunking.PlannerRotate || decision.Trigger != "age" {
		t.Fatalf("Plan = %+v, want rotate age after manifest open exceeds MaxAge", decision)
	}
}

func TestPlannerPartialSegmentContinuesBeforeInterleave(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	vaultID := glid.New()
	segA := glid.New()
	pathA := writeSegment(t, segA, vaultID, []record.Record{
		makeRecord(0, base, "a0"),
		makeRecord(1, base.Add(3*time.Second), "a3"),
	})
	segB := glid.New()
	pathB := writeSegment(t, segB, vaultID, []record.Record{
		makeRecord(0, base.Add(time.Second), "b1"),
	})
	viewA := segmentView(t, pathA, segA, base)
	viewB := segmentView(t, pathB, segB, base.Add(time.Second))
	now := base.Add(time.Hour)

	manifest := chunking.ManifestSnapshot{OpenedAt: base}
	first := chunking.Plan(chunking.PlannerInput{
		Manifest:   manifest,
		Segments:   []chunking.SegmentView{viewA, viewB},
		Policy:     chunking.ManifestRotationPolicy{MaxRecords: 1},
		RefAddedAt: now,
	})
	applyRef(&manifest, first.Ref)

	second := chunking.Plan(chunking.PlannerInput{
		Manifest:   manifest,
		Resume:     map[glid.GLID]uint32{segA: first.Ref.LastRecordNumber + 1},
		Segments:   []chunking.SegmentView{viewA, viewB},
		Policy:     chunking.ManifestRotationPolicy{MaxRecords: 2},
		RefAddedAt: now,
	})
	if second.Action != chunking.PlannerAddRef || second.Ref.SegmentID != segA || second.Ref.FirstRecordNumber != 1 {
		t.Fatalf("second ref = %+v, want partial continuation on segA", second.Ref)
	}
}

func TestPlannerDeterministic(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	vaultID := glid.New()
	segA, segB := glid.New(), glid.New()
	pathA := writeSegment(t, segA, vaultID, []record.Record{makeRecord(0, base, "a")})
	pathB := writeSegment(t, segB, vaultID, []record.Record{makeRecord(0, base.Add(time.Second), "b")})

	input := chunking.PlannerInput{
		Manifest: chunking.ManifestSnapshot{OpenedAt: base},
		Segments: []chunking.SegmentView{
			segmentView(t, pathB, segB, base.Add(time.Second)),
			segmentView(t, pathA, segA, base),
		},
		Policy:     chunking.ManifestRotationPolicy{MaxRecords: 1},
		RefAddedAt: base.Add(time.Minute),
	}
	a := chunking.Plan(input)
	b := chunking.Plan(input)
	if a.Action != b.Action {
		t.Fatalf("actions differ: %+v vs %+v", a, b)
	}
	if a.Action == chunking.PlannerAddRef && b.Action == chunking.PlannerAddRef &&
		(a.Ref.SegmentID != b.Ref.SegmentID || a.Ref.FirstRecordNumber != b.Ref.FirstRecordNumber) {
		t.Fatalf("refs differ: %+v vs %+v", a.Ref, b.Ref)
	}
}

func TestPlannerIdleWhenNoEligibleSegments(t *testing.T) {
	t.Parallel()
	decision := chunking.Plan(chunking.PlannerInput{
		Manifest:   chunking.ManifestSnapshot{OpenedAt: time.Unix(0, 0).UTC()},
		RefAddedAt: time.Unix(0, 0).UTC(),
	})
	if decision.Action != chunking.PlannerIdle {
		t.Fatalf("Plan = %+v, want idle", decision)
	}
}

func TestPlannerCronDueRotates(t *testing.T) {
	t.Parallel()
	now := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	decision := chunking.Plan(chunking.PlannerInput{
		Manifest: chunking.ManifestSnapshot{
			OpenedAt:     now.Add(-time.Hour),
			TotalRecords: 1,
		},
		CronDue:    true,
		RefAddedAt: now,
	})
	if decision.Action != chunking.PlannerRotate || decision.Trigger != "cron" {
		t.Fatalf("Plan = %+v, want rotate cron", decision)
	}
}

func TestRecordSliceBytesMatchesFrameDelta(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	path := writeSegment(t, segID, vaultID, []record.Record{
		makeRecord(0, base, "aa"),
		makeRecord(1, base.Add(time.Second), "bbb"),
	})
	idx, err := chunking.BuildOrderedIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	b0, err := idx.RecordSliceBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	b1, err := idx.RecordSliceBytes(1)
	if err != nil {
		t.Fatal(err)
	}
	if b0 == 0 || b1 == 0 || b0 == b1 {
		t.Fatalf("slice bytes = %d %d", b0, b1)
	}
}
