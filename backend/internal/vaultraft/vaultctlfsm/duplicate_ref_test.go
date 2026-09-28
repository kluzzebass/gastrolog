package vaultctlfsm

// A segment ref must land in the open-chunk manifest exactly once however
// many times it is proposed and however the proposals interleave. Two
// proposers race in production — the event-driven planner and the reconcile
// pass — and a re-proposal separated from its original by another segment's
// ref must be as harmless as an adjacent one: a duplicate that lands counts
// the segment's records twice, inflates the rotation total so the tail
// displaces into the next chunk, and composes the same records into the
// built chunk twice.

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

func TestAddRefIgnoresDuplicateSeparatedByAnotherRef(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	f := New()
	s1 := glid.New()
	s2 := glid.New()
	pub(t, f, s1, 10, now)
	pub(t, f, s2, 10, now)

	chunkID := chunk.NewChunkID()
	applyCmd(t, f, MarshalOpenChunkManifest(chunkID, now))

	ref1 := OpenChunkSegmentRef{SegmentID: s1, FirstRecordNumber: 0, LastRecordNumber: 9, SliceBytes: 10, RefAddedAt: now}
	ref2 := OpenChunkSegmentRef{SegmentID: s2, FirstRecordNumber: 0, LastRecordNumber: 9, SliceBytes: 10, RefAddedAt: now}

	applyCmd(t, f, MarshalAddOpenChunkSegmentRef(chunkID, ref1))
	applyCmd(t, f, MarshalAddOpenChunkSegmentRef(chunkID, ref2))
	// The re-proposal: identical to ref1 but no longer adjacent to it.
	applyCmd(t, f, MarshalAddOpenChunkSegmentRef(chunkID, ref1))

	open := f.OpenChunk()
	if open == nil {
		t.Fatal("open chunk manifest absent")
	}
	if len(open.Refs) != 2 {
		t.Fatalf("open chunk holds %d refs, want 2 — a re-proposed segment ref landed twice", len(open.Refs))
	}
	if open.TotalRecords != 20 {
		t.Fatalf("open chunk counts %d records, want 20 — a duplicated ref inflates the rotation total, displacing the tail into the next chunk and composing the same records twice", open.TotalRecords)
	}
}

// The adjacent re-proposal is the case the guard has always caught; pinned so
// the wider fix cannot regress it.
func TestAddRefIgnoresAdjacentDuplicate(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	f := New()
	s1 := glid.New()
	pub(t, f, s1, 10, now)

	chunkID := chunk.NewChunkID()
	applyCmd(t, f, MarshalOpenChunkManifest(chunkID, now))

	ref1 := OpenChunkSegmentRef{SegmentID: s1, FirstRecordNumber: 0, LastRecordNumber: 9, SliceBytes: 10, RefAddedAt: now}
	applyCmd(t, f, MarshalAddOpenChunkSegmentRef(chunkID, ref1))
	applyCmd(t, f, MarshalAddOpenChunkSegmentRef(chunkID, ref1))

	open := f.OpenChunk()
	if open == nil || len(open.Refs) != 1 || open.TotalRecords != 10 {
		t.Fatalf("adjacent duplicate not absorbed: %+v", open)
	}
}

// Racing proposers stamp their own RefAddedAt, so a re-proposal of the same
// slice rarely matches the original field-for-field. The duplicate is the
// record range, not the ref struct.
func TestAddRefIgnoresDuplicateWithDifferentRefAddedAt(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	f := New()
	s1 := glid.New()
	pub(t, f, s1, 10, now)

	chunkID := chunk.NewChunkID()
	applyCmd(t, f, MarshalOpenChunkManifest(chunkID, now))

	ref := OpenChunkSegmentRef{SegmentID: s1, FirstRecordNumber: 0, LastRecordNumber: 9, SliceBytes: 10, RefAddedAt: now}
	applyCmd(t, f, MarshalAddOpenChunkSegmentRef(chunkID, ref))
	ref.RefAddedAt = now.Add(time.Second)
	applyCmd(t, f, MarshalAddOpenChunkSegmentRef(chunkID, ref))

	open := f.OpenChunk()
	if open == nil || len(open.Refs) != 1 || open.TotalRecords != 10 {
		t.Fatalf("re-proposal with a fresh RefAddedAt landed twice: %+v", open)
	}
}

// One segment attaches in several slices as it grows; deduplication must key
// on the record range, never on segment identity alone.
func TestAddRefAllowsFollowOnSlice(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	f := New()
	s1 := glid.New()
	pub(t, f, s1, 10, now)

	chunkID := chunk.NewChunkID()
	applyCmd(t, f, MarshalOpenChunkManifest(chunkID, now))

	applyCmd(t, f, MarshalAddOpenChunkSegmentRef(chunkID,
		OpenChunkSegmentRef{SegmentID: s1, FirstRecordNumber: 0, LastRecordNumber: 4, SliceBytes: 5, RefAddedAt: now}))
	applyCmd(t, f, MarshalAddOpenChunkSegmentRef(chunkID,
		OpenChunkSegmentRef{SegmentID: s1, FirstRecordNumber: 5, LastRecordNumber: 9, SliceBytes: 5, RefAddedAt: now}))

	open := f.OpenChunk()
	if open == nil || len(open.Refs) != 2 || open.TotalRecords != 10 {
		t.Fatalf("legitimate follow-on slice rejected: %+v", open)
	}
}

// A slice re-proposed after its chunk sealed must not land in the next open
// chunk: that composes the same records into two chunks. A genuinely new
// slice of the same segment still belongs there.
func TestAddRefIgnoresSliceAlreadyInSealedManifest(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	f := New()
	s1 := glid.New()
	pub(t, f, s1, 20, now)

	chunkA := chunk.NewChunkID()
	applyCmd(t, f, MarshalOpenChunkManifest(chunkA, now))
	applyCmd(t, f, MarshalAddOpenChunkSegmentRef(chunkA,
		OpenChunkSegmentRef{SegmentID: s1, FirstRecordNumber: 0, LastRecordNumber: 9, SliceBytes: 10, RefAddedAt: now}))
	applyCmd(t, f, MarshalSealOpenChunkManifest(chunkA, now))

	chunkB := chunk.NewChunkID()
	applyCmd(t, f, MarshalOpenChunkManifest(chunkB, now))
	// Stale re-proposal of the sealed slice.
	applyCmd(t, f, MarshalAddOpenChunkSegmentRef(chunkB,
		OpenChunkSegmentRef{SegmentID: s1, FirstRecordNumber: 0, LastRecordNumber: 9, SliceBytes: 10, RefAddedAt: now.Add(time.Second)}))
	open := f.OpenChunk()
	if open == nil || len(open.Refs) != 0 {
		t.Fatalf("slice already sealed into %s landed again in the next chunk: %+v", chunkA, open)
	}
	// The segment's fresh tail still belongs in the new chunk.
	applyCmd(t, f, MarshalAddOpenChunkSegmentRef(chunkB,
		OpenChunkSegmentRef{SegmentID: s1, FirstRecordNumber: 10, LastRecordNumber: 19, SliceBytes: 10, RefAddedAt: now.Add(time.Second)}))
	open = f.OpenChunk()
	if open == nil || len(open.Refs) != 1 || open.TotalRecords != 10 {
		t.Fatalf("fresh tail slice rejected after sealed-slice dedup: %+v", open)
	}
}
