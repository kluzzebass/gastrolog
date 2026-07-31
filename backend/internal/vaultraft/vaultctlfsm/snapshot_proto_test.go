package vaultctlfsm

import (
	"bytes"
	"io"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

// buildRichFSM populates an FSM with state in all snapshot sections plus
// tombstones, including a chunk carrying the Hash / CloudServiceID /
// KeyScheme fields.
func buildRichFSM(t *testing.T) *FSM {
	t.Helper()
	f := New()
	now := time.Unix(0, 1_700_000_000_000_000_000)

	// entries: an active chunk and a fully uploaded chunk with extra fields.
	active := testChunkID(10)
	applyCmd(t, f, MarshalCreateChunk(active, now, now, now))

	uploaded := testChunkID(11)
	var hash [32]byte
	hash[0], hash[31] = 0x11, 0xFF
	cloud := glidFromByte(0x42)
	applyCmd(t, f, MarshalCreateChunk(uploaded, now, now, now))
	applyCmd(t, f, MarshalSealChunk(uploaded, now.Add(time.Second), 7, 700, now, now, now, true, now.Add(time.Second)))
	applyCmd(t, f, MarshalUploadChunk(uploaded, 300, 1, 2, 3, 4, hash, cloud, 2))

	// tombstones: deleted chunk + a ghost tombstone for a never-seen chunk.
	dead := testChunkID(12)
	applyCmd(t, f, MarshalCreateChunk(dead, now, now, now))
	applyCmd(t, f, MarshalFinalizeDelete(dead))
	applyCmd(t, f, MarshalFinalizeDelete(testChunkID(13)))

	// pending deletes: an in-flight delete with several expected-from nodes.
	applyCmd(t, f, MarshalRequestDelete(testChunkID(14), now, "retention-ttl", []string{"n3", "n1", "n2"}))

	// completed segments: pipeline registry.
	applyCmd(t, f, MarshalPublishCompletedSegment(CompletedSegmentEntry{
		SegmentID:     glidFromByte(0x55),
		RecordCount:   9,
		ByteSize:      900,
		FirstIngestTS: now.Add(-time.Minute),
		LastIngestTS:  now,
		Checksum:      0xBEEF,
		OriginNodeID:  "node-seg-origin",
		PublishedAt:   now,
	}))

	// open-chunk manifest: sealed pending materialization. The ref'd segment
	// must exist in the registry (apply-time ghost-ref guard).
	applyCmd(t, f, MarshalPublishCompletedSegment(CompletedSegmentEntry{
		SegmentID: glidFromByte(0x66), RecordCount: 1000, ByteSize: 1, Checksum: 1, PublishedAt: now,
	}))
	openChunk := testChunkID(0x53)
	applyCmd(t, f, MarshalOpenChunkManifest(openChunk, now))
	applyCmd(t, f, MarshalAddOpenChunkSegmentRef(openChunk, OpenChunkSegmentRef{
		SegmentID:         glidFromByte(0x66),
		FirstRecordNumber: 0,
		LastRecordNumber:  4,
		SliceBytes:        500,
		RefAddedAt:        now.Add(time.Second),
	}))
	applyCmd(t, f, MarshalSealOpenChunkManifest(openChunk, now.Add(2*time.Second)))

	return f
}

// TestSnapshotProtoRoundTripAllSections verifies every snapshot section
// round-trips through SnapshotProto/RestoreProto with identical proto state,
// and that the manifest extra fields survive.
func TestSnapshotProtoRoundTripAllSections(t *testing.T) {
	t.Parallel()
	src := buildRichFSM(t)

	dst := New()
	dst.RestoreProto(src.SnapshotProto())

	if !proto.Equal(src.SnapshotProto(), dst.SnapshotProto()) {
		t.Fatalf("snapshot proto state differs after restore")
	}

	// Spot-check the extra fields the legacy codec dropped.
	got := dst.Get(testChunkID(11))
	if got == nil {
		t.Fatal("uploaded chunk missing after restore")
	}
	if got.Hash[0] != 0x11 || got.Hash[31] != 0xFF {
		t.Errorf("hash not preserved: %x", got.Hash)
	}
	if got.KeyScheme != 2 {
		t.Errorf("key scheme: got %d want 2", got.KeyScheme)
	}
	if got.CloudServiceID != glidFromByte(0x42) {
		t.Errorf("cloud service id not preserved: %v", got.CloudServiceID)
	}

	// Pending delete obligations survive the boundary.
	pd := dst.PendingDelete(testChunkID(14))
	if pd == nil || len(pd.ExpectedFrom) != 3 {
		t.Errorf("pending delete not preserved: %+v", pd)
	}
}

// TestSnapshotProtoDeterministic verifies that two snapshots of equal FSM
// state marshal to byte-identical payloads, despite the map-backed sections
// (tombstones, pending deletes) having nondeterministic Go iteration order.
// InstallSnapshot does not require this, but determinism keeps snapshot
// diffing and debugging sane.
func TestSnapshotProtoDeterministic(t *testing.T) {
	t.Parallel()
	f := buildRichFSM(t)

	first, err := proto.Marshal(f.SnapshotProto())
	if err != nil {
		t.Fatal(err)
	}
	for i := range 16 {
		next, err := proto.Marshal(f.SnapshotProto())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(first, next) {
			t.Fatalf("snapshot %d not byte-stable", i)
		}
	}
}

// TestSnapshotPersistRestoreFullRoundTrip exercises the hraft FSMSnapshot
// Persist path and the marshaled Restore path end-to-end.
func TestSnapshotPersistRestoreFullRoundTrip(t *testing.T) {
	t.Parallel()
	src := buildRichFSM(t)

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&bufSink{Writer: &buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	dst := New()
	if err := dst.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if !proto.Equal(src.SnapshotProto(), dst.SnapshotProto()) {
		t.Fatalf("state differs after Persist/Restore round trip")
	}
}

// TestRestoreRejectsMalformedSnapshot verifies Restore returns an error (not a
// panic) for undecodable snapshot bytes (unhappy path).
func TestRestoreRejectsMalformedSnapshot(t *testing.T) {
	t.Parallel()
	f := New()
	// Field 1 declared as varint but truncated — invalid VaultCtlSnapshot.
	garbage := []byte{0x08}
	if err := f.Restore(io.NopCloser(bytes.NewReader(garbage))); err == nil {
		t.Error("expected error restoring malformed snapshot")
	}
}

// TestRestoreEmptySnapshotIsZeroState verifies an empty (zero-entry) snapshot
// restores to a clean, ready FSM (edge case).
func TestRestoreEmptySnapshotIsZeroState(t *testing.T) {
	t.Parallel()
	src := New()
	dst := New()
	raw, err := proto.Marshal(src.SnapshotProto())
	if err != nil {
		t.Fatal(err)
	}
	if err := dst.Restore(io.NopCloser(bytes.NewReader(raw))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if dst.Count() != 0 {
		t.Errorf("expected empty FSM, got %d chunks", dst.Count())
	}
}
