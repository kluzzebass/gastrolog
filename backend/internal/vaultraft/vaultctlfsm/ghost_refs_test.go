package vaultctlfsm

import (
	"errors"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"

	hraft "github.com/hashicorp/raft"
)

func pub(t *testing.T, f *FSM, segID glid.GLID, count uint32, ts time.Time) {
	t.Helper()
	applyCmd(t, f, MarshalPublishCompletedSegment(CompletedSegmentEntry{
		SegmentID: segID, RecordCount: count, ByteSize: 1,
		FirstIngestTS: ts, LastIngestTS: ts, Checksum: 1, PublishedAt: ts,
	}))
}

// TestAddRefRejectsReleasedSegment pins the apply-time ghost-ref guard: a
// manifest ref naming a segment no longer in the registry (released between
// the planner's pass snapshot and the apply) must be refused — admitting it
// creates a sealed manifest no home can ever build and no release can clear,
// wedging the seal queue permanently.
func TestAddRefRejectsReleasedSegment(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	f := New()
	segID := glid.New()
	pub(t, f, segID, 10, now)

	chunkID := chunk.NewChunkID()
	applyCmd(t, f, MarshalOpenChunkManifest(chunkID, now))

	// Release the segment out from under the (stale) planner...
	// exhausted first: plan all records in a first manifest ref.
	applyCmd(t, f, MarshalAddOpenChunkSegmentRef(chunkID, OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 9, SliceBytes: 10, RefAddedAt: now,
	}))
	applyCmd(t, f, MarshalSealOpenChunkManifest(chunkID, now.Add(time.Minute)))
	applyCmd(t, f, MarshalSealChunk(chunkID, now.Add(time.Minute), 10, 10, now, now, now, true, now.Add(time.Minute)))
	applyCmd(t, f, MarshalReleaseSegments([]glid.GLID{segID}))

	// A stale ref to the released segment must be refused.
	next := chunk.NewChunkID()
	applyCmd(t, f, MarshalOpenChunkManifest(next, now.Add(2*time.Minute)))
	res := f.Apply(&hraft.Log{Data: MarshalAddOpenChunkSegmentRef(next, OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 9, SliceBytes: 10, RefAddedAt: now,
	})})
	err, ok := res.(error)
	if !ok || !errors.Is(err, ErrSegmentReleased) {
		t.Fatalf("stale ref to released segment: got %v, want ErrSegmentReleased", res)
	}
	if open := f.OpenChunk(); open == nil || len(open.Refs) != 0 {
		t.Fatalf("refused ref must not land in the manifest: %+v", open)
	}
}

// TestDiscardUnbuildableManifests pins the recovery command: with a ghost
// ref wedging the sealed head, applying discards the head, every queued
// manifest and the open manifest, rewinds surviving segments' resume cursors
// to their lowest discarded record number, and scrubs the discarded chunk
// IDs from the supersession mapping. Refuses non-wedged targets. The wedge
// state is constructed the only way it can now exist — restored from a
// pre-guard snapshot (the apply-time guard blocks new admissions).
func TestDiscardUnbuildableManifests(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	f := New()
	ghost := glid.New()
	survivor := glid.New()
	pub(t, f, ghost, 5, now)
	pub(t, f, survivor, 100, now)

	// Wedged head: refs ghost (0-4) and survivor (0-49).
	head := chunk.NewChunkID()
	applyCmd(t, f, MarshalOpenChunkManifest(head, now))
	applyCmd(t, f, MarshalAddOpenChunkSegmentRef(head, OpenChunkSegmentRef{
		SegmentID: ghost, FirstRecordNumber: 0, LastRecordNumber: 4, SliceBytes: 5, RefAddedAt: now,
	}))
	applyCmd(t, f, MarshalAddOpenChunkSegmentRef(head, OpenChunkSegmentRef{
		SegmentID: survivor, FirstRecordNumber: 0, LastRecordNumber: 49, SliceBytes: 50, RefAddedAt: now,
	}))
	applyCmd(t, f, MarshalSealOpenChunkManifest(head, now.Add(time.Minute)))

	// A queued open manifest planning survivor 50-79.
	tail := chunk.NewChunkID()
	applyCmd(t, f, MarshalOpenChunkManifest(tail, now.Add(2*time.Minute)))
	applyCmd(t, f, MarshalAddOpenChunkSegmentRef(tail, OpenChunkSegmentRef{
		SegmentID: survivor, FirstRecordNumber: 50, LastRecordNumber: 79, SliceBytes: 30, RefAddedAt: now,
	}))

	// Not wedged (ghost still registered): the command must refuse.
	res := f.Apply(&hraft.Log{Data: MarshalDiscardUnbuildableManifests(head)})
	if err, ok := res.(error); !ok || !errors.Is(err, ErrManifestNotWedged) {
		t.Fatalf("discard of buildable head: got %v, want ErrManifestNotWedged", res)
	}

	// Recreate the wedge as a pre-guard snapshot restore: same state minus
	// the ghost's registry entry (released by the old binary after a stale
	// ref had already been admitted).
	snap := f.SnapshotProto()
	kept := snap.GetCompletedSegments()[:0]
	for _, e := range snap.GetCompletedSegments() {
		if glid.FromBytes(e.GetSegmentId()) != ghost {
			kept = append(kept, e)
		}
	}
	snap.CompletedSegments = kept
	wedged := New()
	wedged.RestoreProto(snap)

	// Wrong target refuses.
	if err, ok := wedged.Apply(&hraft.Log{Data: MarshalDiscardUnbuildableManifests(tail)}).(error); !ok || !errors.Is(err, ErrManifestNotWedged) {
		t.Fatal("discard must refuse a non-head target")
	}

	// The recovery applies.
	if err, ok := wedged.Apply(&hraft.Log{Data: MarshalDiscardUnbuildableManifests(head)}).(error); ok && err != nil {
		t.Fatalf("discard of wedged head: %v", err)
	}
	if wedged.SealedManifest() != nil || wedged.OpenChunk() != nil {
		t.Fatal("all manifests must be discarded")
	}
	// Survivor rewound to its lowest discarded record number (0), so records
	// 0-79 re-plan; entry itself retained.
	if n, ok := wedged.ResumeRecordNumber(survivor); !ok || n != 0 {
		t.Fatalf("survivor resume = %d (ok=%v), want rewind to 0", n, ok)
	}
	if wedged.GetCompletedSegment(survivor) == nil {
		t.Fatal("survivor registry entry must remain")
	}
	// Discarded chunks must not count toward supersession.
	if wedged.SegmentSuperseded(survivor, 1) {
		t.Fatal("discarded chunks must be scrubbed from the supersession mapping")
	}
	// Chunk entries gone.
	if wedged.Get(head) != nil || wedged.Get(tail) != nil {
		t.Fatal("discarded manifests' chunk entries must be removed")
	}
}
