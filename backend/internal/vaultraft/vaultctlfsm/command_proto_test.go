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
		c := decodeCommand(t, MarshalSealChunk(id, now, 42, 99, now, now, now, true))
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

	t.Run("reserve_seq_range", func(t *testing.T) {
		data, err := MarshalReserveSeqRange("holder-1", 3, 100)
		if err != nil {
			t.Fatal(err)
		}
		got := decodeCommand(t, data).GetReserveSeqRange()
		if got.GetHolderId() != "holder-1" || got.GetEpoch() != 3 || got.GetCount() != 100 {
			t.Errorf("reserve fields: %+v", got)
		}
	})

	t.Run("burn_seq_lease_tail", func(t *testing.T) {
		data, err := MarshalBurnSeqLeaseTail("holder-1", 3, 50)
		if err != nil {
			t.Fatal(err)
		}
		got := decodeCommand(t, data).GetBurnSeqLeaseTail()
		if got.GetConsumedEnd() != 50 {
			t.Errorf("consumed end: %d", got.GetConsumedEnd())
		}
	})

	t.Run("bump_seq_allocator_epoch", func(t *testing.T) {
		if decodeCommand(t, MarshalBumpSeqAllocatorEpoch()).GetBumpSeqAllocatorEpoch() == nil {
			t.Errorf("wrong case")
		}
	})

	t.Run("publish_fence", func(t *testing.T) {
		c := decodeCommand(t, MarshalPublishFence(123, now))
		if c.GetPublishFence().GetUpperBoundSeq() != 123 {
			t.Errorf("upper bound: %d", c.GetPublishFence().GetUpperBoundSeq())
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

// TestSeqCommandRejectsInvalidHolder verifies holder-ID validation survives the
// proto migration on both the marshal and apply paths (gastrolog-5lrg7 edge).
func TestSeqCommandRejectsInvalidHolder(t *testing.T) {
	t.Parallel()

	if _, err := MarshalReserveSeqRange("", 1, 1); err == nil {
		t.Error("empty holder: expected marshal error")
	}
	oversize := make([]byte, maxSeqHolderIDLen+1)
	if _, err := MarshalReserveSeqRange(string(oversize), 1, 1); err == nil {
		t.Error("oversize holder: expected marshal error")
	}

	// Apply path: a command that bypasses the marshal validation must still
	// be rejected.
	f := New()
	data, err := proto.Marshal(&gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_ReserveSeqRange{
		ReserveSeqRange: &gastrologv1.ReserveSeqRangeCommand{HolderId: "", Epoch: InitialSeqEpoch, Count: 1},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if result := f.Apply(&hraft.Log{Data: data}); result != ErrSeqAllocatorInvalidHolder {
		t.Errorf("apply empty holder: got %v, want %v", result, ErrSeqAllocatorInvalidHolder)
	}
}

func glidFromByte(b byte) (g glid.GLID) {
	g[0] = b
	return g
}
