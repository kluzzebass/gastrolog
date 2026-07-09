package vaultctlfsm

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// buildChunkedSegment publishes one completed segment, references all its
// records into a single chunk, seals the manifest and the chunk, and returns
// the segment and chunk IDs. After this the manifest is popped (build done) and
// the segment→chunk mapping is what SegmentSuperseded consults.
func buildChunkedSegment(t *testing.T, f *FSM) (glid.GLID, chunk.ChunkID) {
	t.Helper()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	segID := glid.New()
	chunkID := chunk.NewChunkID()
	applyCmd(t, f, MarshalPublishCompletedSegment(CompletedSegmentEntry{
		SegmentID: segID, RecordCount: 1, ByteSize: 1,
		FirstIngestTS: now, LastIngestTS: now, Checksum: 1, PublishedAt: now,
	}))
	applyCmd(t, f, MarshalOpenChunkManifest(chunkID, now))
	applyCmd(t, f, MarshalAddOpenChunkSegmentRef(chunkID, OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 0, SliceBytes: 1, RefAddedAt: now,
	}))
	sealedAt := now.Add(time.Minute)
	applyCmd(t, f, MarshalSealOpenChunkManifest(chunkID, sealedAt))
	applyCmd(t, f, MarshalSealChunk(chunkID, sealedAt, 1, 1, now, now, now, true, sealedAt))
	return segID, chunkID
}

// TestSegmentSupersededByReplicatedChunk is R3's core invariant: a segment
// becomes superseded (releasable) once the chunk holding its records reaches the
// RF threshold among the LIVE homes — the dead home that never held the raw
// segment does not gate it (design-notes 39/R3, the completed/ leak fix).
func TestSegmentSupersededByReplicatedChunk(t *testing.T) {
	t.Parallel()
	f := New()
	segID, chunkID := buildChunkedSegment(t, f)

	// Chunked but not yet replicated: not superseded.
	if f.SegmentSuperseded(segID, 2) {
		t.Fatal("segment superseded before any chunk holder acked")
	}

	// One live home holds the chunk — still below RF=2.
	applyCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{chunkID}, "live-a")))
	if f.SegmentSuperseded(segID, 2) {
		t.Fatal("segment superseded at 1 chunk holder, want RF=2")
	}

	// A second LIVE home holds it — RF reached. "dead-home" never acked either
	// the segment or the chunk, yet the segment is now superseded.
	applyCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{chunkID}, "live-b")))
	if !f.SegmentSuperseded(segID, 2) {
		t.Fatal("segment not superseded after RF=2 chunk holders (dead home must not pin)")
	}

	// minChunkHolders<=0 (placement wiring absent) disables supersession.
	if f.SegmentSuperseded(segID, 0) {
		t.Fatal("supersession must be disabled when minChunkHolders<=0")
	}
}

// TestSegmentSupersededSurvivesSnapshot: the segment→chunk mapping must survive
// snapshot/restore, or a follower catching up via snapshot loses the release
// signal and the completed/ leak returns.
func TestSegmentSupersededSurvivesSnapshot(t *testing.T) {
	t.Parallel()
	f := New()
	segID, chunkID := buildChunkedSegment(t, f)
	applyCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{chunkID}, "live-a")))
	applyCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{chunkID}, "live-b")))
	if !f.SegmentSuperseded(segID, 2) {
		t.Fatal("precondition: segment should be superseded before snapshot")
	}

	restored := New()
	restored.RestoreProto(f.SnapshotProto())
	if !restored.SegmentSuperseded(segID, 2) {
		t.Fatal("supersession lost across snapshot round-trip")
	}

	// After release the mapping is dropped.
	applyCmd(t, restored, MarshalReleaseSegments([]glid.GLID{segID}))
	if restored.SegmentSuperseded(segID, 2) {
		t.Fatal("released segment still reports superseded (mapping not cleared)")
	}
}
