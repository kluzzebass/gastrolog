package vaultctlfsm

import (
	"bytes"
	"io"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"

	"google.golang.org/protobuf/proto"
)

// sealManifestForTest opens, fills (one ref over segID) and seals a manifest
// for chunkID, leaving it in the seal queue.
func sealManifestForTest(t *testing.T, fsm *FSM, chunkID chunk.ChunkID, segID glid.GLID, first, last uint32, at time.Time) {
	t.Helper()
	applyCmd(t, fsm, MarshalOpenChunkManifest(chunkID, at))
	applyCmd(t, fsm, MarshalAddOpenChunkSegmentRef(chunkID, OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: first, LastRecordNumber: last, SliceBytes: 100, RefAddedAt: at,
	}))
	applyCmd(t, fsm, MarshalSealOpenChunkManifest(chunkID, at.Add(time.Second)))
}

func commitSealForTest(t *testing.T, fsm *FSM, chunkID chunk.ChunkID, at time.Time) {
	t.Helper()
	applyCmd(t, fsm, MarshalSealChunk(chunkID, at, 1, 100, at, at, at, true, at))
}

func publishForTest(t *testing.T, fsm *FSM, segID glid.GLID, records uint32) {
	t.Helper()
	applyCmd(t, fsm, MarshalPublishCompletedSegment(CompletedSegmentEntry{SegmentID: segID, RecordCount: records, ByteSize: 1, Checksum: 1}))
}

func TestSealChunkKeepsTheManifestForHomesStillBuilding(t *testing.T) {
	t.Parallel()
	fsm := New()
	at := time.Unix(0, 1_700_000_000_000).UTC()
	segID := glid.New()
	publishForTest(t, fsm, segID, 10)
	first, second := testChunkID(0x61), testChunkID(0x62)
	sealManifestForTest(t, fsm, first, segID, 0, 4, at)
	sealManifestForTest(t, fsm, second, segID, 5, 9, at.Add(time.Minute))

	if ids := fsm.SealedChunkManifestIDs(); len(ids) != 0 {
		t.Fatalf("sealed-chunk manifests before any seal committed = %v, want none", ids)
	}
	commitSealForTest(t, fsm, first, at.Add(2*time.Minute))

	if head, ok := fsm.SealedManifestHeadChunkID(); !ok || head != second {
		t.Fatalf("seal-queue head = %s ok=%v, want %s", head, ok, second)
	}
	if ids := fsm.SealedChunkManifestIDs(); !slices.Equal(ids, []chunk.ChunkID{first}) {
		t.Fatalf("sealed-chunk manifests = %v, want [%s]", ids, first)
	}
	kept := fsm.SealedChunkManifest(first)
	if kept == nil || len(kept.Refs) != 1 || kept.Refs[0].SegmentID != segID ||
		kept.Refs[0].FirstRecordNumber != 0 || kept.Refs[0].LastRecordNumber != 4 {
		t.Fatalf("kept manifest = %+v, want the sealed refs", kept)
	}
	kept.Refs[0].LastRecordNumber = 99
	if again := fsm.SealedChunkManifest(first); again.Refs[0].LastRecordNumber != 4 {
		t.Fatal("SealedChunkManifest returned FSM-owned refs")
	}
	if fsm.AwaitingBuild(first) {
		t.Fatal("a chunk whose seal committed must not read as awaiting its seal")
	}

	commitSealForTest(t, fsm, second, at.Add(3*time.Minute))
	if ids := fsm.SealedChunkManifestIDs(); !slices.Equal(ids, []chunk.ChunkID{first, second}) {
		t.Fatalf("sealed-chunk manifests = %v, want seal order [%s %s]", ids, first, second)
	}

	// A replayed CmdSealChunk must not retain the manifest twice.
	commitSealForTest(t, fsm, first, at.Add(2*time.Minute))
	if ids := fsm.SealedChunkManifestIDs(); len(ids) != 2 {
		t.Fatalf("sealed-chunk manifests after replay = %v, want 2", ids)
	}
}

func TestReleasingASegmentDropsEverySealedChunkManifestThatNeedsIt(t *testing.T) {
	t.Parallel()
	fsm := New()
	at := time.Unix(0, 1_700_000_000_000).UTC()
	shared, own := glid.New(), glid.New()
	publishForTest(t, fsm, shared, 10)
	publishForTest(t, fsm, own, 10)
	a, b, c := testChunkID(0x71), testChunkID(0x72), testChunkID(0x73)
	sealManifestForTest(t, fsm, a, shared, 0, 4, at)
	sealManifestForTest(t, fsm, b, shared, 5, 9, at.Add(time.Minute))
	sealManifestForTest(t, fsm, c, own, 0, 9, at.Add(2*time.Minute))
	for _, id := range []chunk.ChunkID{a, b, c} {
		commitSealForTest(t, fsm, id, at.Add(3*time.Minute))
	}

	applyCmd(t, fsm, MarshalReleaseSegments([]glid.GLID{shared}))
	if ids := fsm.SealedChunkManifestIDs(); !slices.Equal(ids, []chunk.ChunkID{c}) {
		t.Fatalf("sealed-chunk manifests after releasing the shared segment = %v, want [%s]", ids, c)
	}
	if fsm.SealedChunkManifest(a) != nil || fsm.SealedChunkManifest(b) != nil {
		t.Fatal("a manifest whose segment was released is still offered for a build")
	}

	applyCmd(t, fsm, MarshalReleaseSegments([]glid.GLID{own}))
	if ids := fsm.SealedChunkManifestIDs(); len(ids) != 0 {
		t.Fatalf("sealed-chunk manifests after releasing every segment = %v, want none", ids)
	}
}

func TestDeletingAChunkDropsItsSealedChunkManifest(t *testing.T) {
	t.Parallel()
	fsm := New()
	at := time.Unix(0, 1_700_000_000_000).UTC()
	segID := glid.New()
	publishForTest(t, fsm, segID, 10)
	kept, deleted := testChunkID(0x81), testChunkID(0x82)
	sealManifestForTest(t, fsm, deleted, segID, 0, 4, at)
	sealManifestForTest(t, fsm, kept, segID, 5, 9, at.Add(time.Minute))
	commitSealForTest(t, fsm, deleted, at.Add(2*time.Minute))
	commitSealForTest(t, fsm, kept, at.Add(2*time.Minute))

	applyCmd(t, fsm, MarshalFinalizeDelete(deleted))
	if ids := fsm.SealedChunkManifestIDs(); !slices.Equal(ids, []chunk.ChunkID{kept}) {
		t.Fatalf("sealed-chunk manifests after delete = %v, want [%s]", ids, kept)
	}
}

func TestSealedChunkManifestsSurviveSnapshotRestore(t *testing.T) {
	t.Parallel()
	src := New()
	at := time.Unix(0, 1_700_000_000_000).UTC()
	segID := glid.New()
	publishForTest(t, src, segID, 30)
	ids := []chunk.ChunkID{testChunkID(0x91), testChunkID(0x92), testChunkID(0x93)}
	for i, id := range ids {
		sealManifestForTest(t, src, id, segID, uint32(i*10), uint32(i*10+9), at.Add(time.Duration(i)*time.Minute)) //nolint:gosec // small test indices
	}
	commitSealForTest(t, src, ids[0], at.Add(time.Hour))
	commitSealForTest(t, src, ids[1], at.Add(time.Hour))

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&bufSink{Writer: &buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	dst := New()
	if err := dst.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if !proto.Equal(src.SnapshotProto(), dst.SnapshotProto()) {
		t.Fatal("snapshot state differs after round trip")
	}
	if got := dst.SealedChunkManifestIDs(); !slices.Equal(got, ids[:2]) {
		t.Fatalf("restored sealed-chunk manifests = %v, want %v", got, ids[:2])
	}
	if m := dst.SealedChunkManifest(ids[1]); m == nil || len(m.Refs) != 1 || m.Refs[0].FirstRecordNumber != 10 {
		t.Fatalf("restored manifest = %+v", m)
	}
	if head, ok := dst.SealedManifestHeadChunkID(); !ok || head != ids[2] {
		t.Fatalf("restored seal-queue head = %s ok=%v, want %s", head, ok, ids[2])
	}
}
