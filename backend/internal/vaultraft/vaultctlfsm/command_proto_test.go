package vaultctlfsm

import (
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

// decodeCommand unmarshals marshaled command bytes into a VaultCtlCommand.
func decodeCommand(t *testing.T, data []byte) *gastrologv1.VaultCtlCommand {
	t.Helper()
	var cmd gastrologv1.VaultCtlCommand
	if err := proto.Unmarshal(data, &cmd); err != nil {
		t.Fatalf("unmarshal command: %v", err)
	}
	return &cmd
}

// TestCommandRoundTrip verifies every Marshal* produces a VaultCtlCommand that
// decodes back to the expected oneof case with its key fields intact
// (gastrolog-5lrg7).
func TestCommandRoundTrip(t *testing.T) {
	t.Parallel()
	id := testChunkID(7)
	now := time.Unix(0, 1_700_000_000_123_456_789)

	t.Run("create", func(t *testing.T) {
		c := decodeCommand(t, MarshalCreateChunk(id, now, now.Add(1), now.Add(2)))
		got := c.GetCreateChunk()
		if got == nil {
			t.Fatalf("wrong case: %T", c.GetCommand())
		}
		if string(got.GetId()) != string(id[:]) {
			t.Errorf("id mismatch")
		}
		if got.GetWriteStartNanos() != now.UnixNano() {
			t.Errorf("write start: %d", got.GetWriteStartNanos())
		}
		if got.GetSourceStartNanos() != now.Add(2).UnixNano() {
			t.Errorf("source start: %d", got.GetSourceStartNanos())
		}
	})

	t.Run("seal", func(t *testing.T) {
		c := decodeCommand(t, MarshalSealChunk(id, now, 42, 99, now, now, now, true, now))
		got := c.GetSealChunk()
		if got == nil {
			t.Fatalf("wrong case: %T", c.GetCommand())
		}
		if got.GetRecordCount() != 42 || got.GetBytes() != 99 {
			t.Errorf("counts: %d %d", got.GetRecordCount(), got.GetBytes())
		}
		if !got.GetIngestTsMonotonic() {
			t.Errorf("monotonic flag lost")
		}
	})

	t.Run("compress", func(t *testing.T) {
		c := decodeCommand(t, MarshalCompressChunk(id, 555))
		if c.GetCompressChunk().GetDiskBytes() != 555 {
			t.Errorf("disk bytes: %d", c.GetCompressChunk().GetDiskBytes())
		}
	})

	t.Run("upload", func(t *testing.T) {
		var hash [32]byte
		hash[0], hash[31] = 0xAB, 0xCD
		cloud := glidFromByte(9)
		c := decodeCommand(t, MarshalUploadChunk(id, 1, 2, 3, 4, 5, hash, cloud, 1))
		got := c.GetUploadChunk()
		if got == nil {
			t.Fatalf("wrong case: %T", c.GetCommand())
		}
		if len(got.GetHash()) != 32 || got.GetHash()[31] != 0xCD {
			t.Errorf("hash lost: %x", got.GetHash())
		}
		if got.GetKeyScheme() != 1 {
			t.Errorf("key scheme: %d", got.GetKeyScheme())
		}
		if string(got.GetCloudServiceId()) != string(cloud[:]) {
			t.Errorf("cloud service id lost")
		}
	})

	t.Run("attach_offsets", func(t *testing.T) {
		c := decodeCommand(t, MarshalAttachOffsets(id, 10, 20, 30, 40))
		got := c.GetAttachOffsets()
		if got.GetIngestIdxOffset() != 10 || got.GetSourceIdxSize() != 40 {
			t.Errorf("offsets: %+v", got)
		}
	})

	t.Run("begin_seal", func(t *testing.T) {
		if decodeCommand(t, MarshalBeginSeal(id)).GetBeginSeal() == nil {
			t.Errorf("wrong case")
		}
	})

	t.Run("delete", func(t *testing.T) {
		if decodeCommand(t, MarshalDeleteChunk(id)).GetDeleteChunk() == nil {
			t.Errorf("wrong case")
		}
	})

	t.Run("retention_pending", func(t *testing.T) {
		if decodeCommand(t, MarshalRetentionPending(id)).GetRetentionPending() == nil {
			t.Errorf("wrong case")
		}
	})

	t.Run("repatriate", func(t *testing.T) {
		entry := ManifestEntry{ID: id, RecordCount: 17, KeyScheme: 3}
		entry.Hash[0] = 0x55
		data, err := MarshalRepatriateChunk(entry)
		if err != nil {
			t.Fatal(err)
		}
		got := decodeCommand(t, data).GetRepatriateChunk().GetEntry()
		if got.GetRecordCount() != 17 || got.GetKeyScheme() != 3 {
			t.Errorf("entry fields lost: %+v", got)
		}
		if got.GetHash()[0] != 0x55 {
			t.Errorf("entry hash lost")
		}
	})

	t.Run("request_delete", func(t *testing.T) {
		c := decodeCommand(t, MarshalRequestDelete(id, now, "retention-ttl", []string{"n1", "n2"}))
		got := c.GetRequestDelete()
		if got.GetReason() != "retention-ttl" {
			t.Errorf("reason: %q", got.GetReason())
		}
		if len(got.GetExpectedFrom()) != 2 {
			t.Errorf("expected from: %v", got.GetExpectedFrom())
		}
	})

	t.Run("ack_delete", func(t *testing.T) {
		c := decodeCommand(t, MarshalAckDelete(id, "node-9"))
		if c.GetAckDelete().GetNodeId() != "node-9" {
			t.Errorf("node id: %q", c.GetAckDelete().GetNodeId())
		}
	})

	t.Run("finalize_delete", func(t *testing.T) {
		if decodeCommand(t, MarshalFinalizeDelete(id)).GetFinalizeDelete() == nil {
			t.Errorf("wrong case")
		}
	})

	t.Run("prune_node", func(t *testing.T) {
		if decodeCommand(t, MarshalPruneNode("node-x")).GetPruneNode().GetNodeId() != "node-x" {
			t.Errorf("node id lost")
		}
	})

	t.Run("publish_completed_segment", func(t *testing.T) {
		segID := glidFromByte(0xAB)
		c := decodeCommand(t, MarshalPublishCompletedSegment(CompletedSegmentEntry{
			SegmentID:     segID,
			RecordCount:   12,
			ByteSize:      345,
			FirstIngestTS: now,
			LastIngestTS:  now.Add(time.Second),
			Checksum:      0x1234,
			OriginNodeID:  "node-origin",
			PublishedAt:   now,
		}))
		got := c.GetPublishCompletedSegment()
		if got == nil {
			t.Fatalf("wrong case: %T", c.GetCommand())
		}
		if string(got.GetSegmentId()) != string(segID[:]) {
			t.Errorf("segment id mismatch")
		}
		if got.GetRecordCount() != 12 || got.GetOriginNodeId() != "node-origin" {
			t.Errorf("fields = %+v", got)
		}
	})

	t.Run("open_chunk_manifest", func(t *testing.T) {
		c := decodeCommand(t, MarshalOpenChunkManifest(id, now))
		got := c.GetOpenChunkManifest()
		if got == nil || string(got.GetChunkId()) != string(id[:]) {
			t.Errorf("open chunk manifest: %+v", got)
		}
	})

	t.Run("add_open_chunk_segment_ref", func(t *testing.T) {
		segID := glidFromByte(0xCC)
		ref := OpenChunkSegmentRef{
			SegmentID:         segID,
			FirstRecordNumber: 1,
			LastRecordNumber:  10,
			SliceBytes:        2048,
			RefAddedAt:        now,
		}
		c := decodeCommand(t, MarshalAddOpenChunkSegmentRef(id, ref))
		got := c.GetAddOpenChunkSegmentRef()
		if got == nil {
			t.Fatalf("wrong case: %T", c.GetCommand())
		}
		if got.GetFirstRecordNumber() != 1 || got.GetLastRecordNumber() != 10 || got.GetSliceBytes() != 2048 {
			t.Errorf("ref fields = %+v", got)
		}
	})

	t.Run("add_open_chunk_segment_refs", func(t *testing.T) {
		segA := glidFromByte(0xCE)
		segB := glidFromByte(0xCF)
		c := decodeCommand(t, MarshalAddOpenChunkSegmentRefs(id, []OpenChunkSegmentRef{
			{SegmentID: segA, FirstRecordNumber: 0, LastRecordNumber: 1, SliceBytes: 10, RefAddedAt: now},
			{SegmentID: segB, FirstRecordNumber: 0, LastRecordNumber: 2, SliceBytes: 20, RefAddedAt: now},
		}))
		got := c.GetAddOpenChunkSegmentRefs()
		if got == nil {
			t.Fatalf("wrong case: %T", c.GetCommand())
		}
		if string(got.GetChunkId()) != string(id[:]) {
			t.Errorf("chunk id mismatch")
		}
		if len(got.GetRefs()) != 2 {
			t.Fatalf("refs = %d, want 2", len(got.GetRefs()))
		}
		if got.GetRefs()[1].GetSliceBytes() != 20 {
			t.Errorf("second ref bytes = %d", got.GetRefs()[1].GetSliceBytes())
		}
	})

	t.Run("seal_open_chunk_manifest", func(t *testing.T) {
		c := decodeCommand(t, MarshalSealOpenChunkManifest(id, now))
		if c.GetSealOpenChunkManifest().GetSealedAtNanos() != now.UnixNano() {
			t.Errorf("sealed at lost")
		}
	})

	t.Run("release_segments", func(t *testing.T) {
		seg := glidFromByte(0xDD)
		c := decodeCommand(t, MarshalReleaseSegments([]glid.GLID{seg}))
		got := c.GetReleaseSegments()
		if len(got.GetSegmentIds()) != 1 || string(got.GetSegmentIds()[0]) != string(seg[:]) {
			t.Errorf("release segments = %+v", got)
		}
	})
}

// TestApplyRejectsMalformedBytes verifies Apply returns an error (not a panic)
// for empty and undecodable command bytes (gastrolog-5lrg7 unhappy path).
func TestApplyRejectsMalformedBytes(t *testing.T) {
	t.Parallel()
	f := New()

	if err, ok := f.Apply(&hraft.Log{Data: nil}).(error); !ok || err == nil {
		t.Errorf("empty data: expected error, got %v", f.Apply(&hraft.Log{Data: nil}))
	}

	// A wire-type/field combination that cannot decode as VaultCtlCommand:
	// field 1 declared as varint but truncated.
	garbage := []byte{0x08}
	if err, ok := f.Apply(&hraft.Log{Data: garbage}).(error); !ok || err == nil {
		t.Errorf("garbage data: expected error, got %v", f.Apply(&hraft.Log{Data: garbage}))
	}
}

// TestApplyRejectsEmptyOneof verifies a VaultCtlCommand with no command set is
// rejected by the dispatch (gastrolog-5lrg7 unhappy path).
func TestApplyRejectsEmptyOneof(t *testing.T) {
	t.Parallel()
	f := New()
	data, err := proto.Marshal(&gastrologv1.VaultCtlCommand{})
	if err != nil {
		t.Fatal(err)
	}
	result := f.Apply(&hraft.Log{Data: data})
	if err, ok := result.(error); !ok || err == nil {
		t.Errorf("empty oneof: expected error, got %v", result)
	}
}

func glidFromByte(b byte) (g glid.GLID) {
	g[0] = b
	return g
}
