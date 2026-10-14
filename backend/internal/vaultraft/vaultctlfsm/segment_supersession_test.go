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
	applyCmd(t, f, MarshalSealChunk(chunkID, sealedAt, 1, 1, now, now, now, true, sealedAt, [32]byte{}))
	return segID, chunkID
}

// supersessionHomes is the placement the supersession tests evaluate against:
// two live homes plus one that never acks anything.
var supersessionHomes = []string{"live-a", "live-b", "dead-home"}

// TestSegmentSupersededByReplicatedChunk pins that a segment becomes
// superseded (releasable) once the chunk holding its records reaches the RF
// threshold among the LIVE homes — the dead home that never held the raw
// segment does not gate it, so its completed/ copies cannot leak.
func TestSegmentSupersededByReplicatedChunk(t *testing.T) {
	t.Parallel()
	f := New()
	segID, chunkID := buildChunkedSegment(t, f)

	// Chunked but not yet replicated: not superseded.
	if f.SegmentSuperseded(segID, supersessionHomes) {
		t.Fatal("segment superseded before any chunk holder acked")
	}

	// One live home holds the chunk — still below RF=2.
	applyCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{chunkID}, "live-a")))
	if f.SegmentSuperseded(segID, supersessionHomes) {
		t.Fatal("segment superseded at 1 chunk holder, want RF=2")
	}

	// A second LIVE home holds it — RF reached. "dead-home" never acked either
	// the segment or the chunk, yet the segment is now superseded.
	applyCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{chunkID}, "live-b")))
	if !f.SegmentSuperseded(segID, supersessionHomes) {
		t.Fatal("segment not superseded after RF=2 chunk holders (dead home must not pin)")
	}

	// An empty placement (wiring absent or unresolved) disables supersession.
	if f.SegmentSuperseded(segID, nil) {
		t.Fatal("supersession must be disabled for an empty placement")
	}
}

// TestSegmentSupersededByCloudBackedChunk: a cloud-backed chunk's durable copy
// is the uploaded object, so it supersedes its segments with no holder at all —
// the state every home reaches once cached-copy eviction has withdrawn its
// receipt. Before the upload commits, the same zero-holder chunk supersedes
// nothing.
func TestSegmentSupersededByCloudBackedChunk(t *testing.T) {
	t.Parallel()
	f := New()
	segID, chunkID := buildChunkedSegment(t, f)

	if f.SegmentSuperseded(segID, supersessionHomes) {
		t.Fatal("segment superseded by a sealed chunk with no holders and no upload")
	}

	applyCmd(t, f, MarshalUploadChunk(chunkID, 64, 0, 0, 0, 0, [32]byte{1}, glid.New(), 0))
	if !f.SegmentSuperseded(segID, supersessionHomes) {
		t.Fatal("segment not superseded by its cloud-backed chunk (zero holders after eviction must not re-pin it)")
	}

	// Supersession by upload survives a holder that acked and then revoked.
	applyCmd(t, f, mustMarshalCommand(NewAckChunkHolders([]chunk.ChunkID{chunkID}, "live-a")))
	applyCmd(t, f, mustMarshalCommand(NewRevokeChunkHolders([]chunk.ChunkID{chunkID}, "live-a")))
	if !f.SegmentSuperseded(segID, supersessionHomes) {
		t.Fatal("holder revoke after eviction re-pinned a segment of a cloud-backed chunk")
	}

	// An unresolved placement still fails closed, upload or not.
	if f.SegmentSuperseded(segID, nil) {
		t.Fatal("supersession must stay disabled when placement is unresolved")
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
	if !f.SegmentSuperseded(segID, supersessionHomes) {
		t.Fatal("precondition: segment should be superseded before snapshot")
	}

	restored := New()
	restored.RestoreProto(f.SnapshotProto())
	if !restored.SegmentSuperseded(segID, supersessionHomes) {
		t.Fatal("supersession lost across snapshot round-trip")
	}

	// After release the mapping is dropped.
	applyCmd(t, restored, MarshalReleaseSegments([]glid.GLID{segID}))
	if restored.SegmentSuperseded(segID, supersessionHomes) {
		t.Fatal("released segment still reports superseded (mapping not cleared)")
	}
}
