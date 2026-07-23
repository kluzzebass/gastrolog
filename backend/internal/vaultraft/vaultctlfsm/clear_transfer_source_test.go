package vaultctlfsm

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

// TestApplyClearTransferSource_HappyPath verifies CmdClearTransferSource
// zeroes TransferSourceVaultID on an existing entry — the fix for
// gastrolog-2l918 review finding 1: without this, an entry introduced by
// retention transfer keeps pointing replica-repair pulls at the source
// vault forever, even after the source has expired its own copies.
func TestApplyClearTransferSource_HappyPath(t *testing.T) {
	t.Parallel()
	f := New()
	id := testChunkID(1)
	src := glidFromByte(0x77)
	applyCmd(t, f, mustMarshalRepatriate(t, ManifestEntry{
		ID:                    id,
		RecordCount:           3,
		TransferSourceVaultID: src,
	}))

	got := f.Get(id)
	if got == nil || got.TransferSourceVaultID != src {
		t.Fatalf("seed entry TransferSourceVaultID = %v, want %v", got, src)
	}

	applyCmd(t, f, MarshalClearTransferSource(id))

	got = f.Get(id)
	if got == nil {
		t.Fatal("entry disappeared after ClearTransferSource")
	}
	if !got.TransferSourceVaultID.IsZero() {
		t.Errorf("TransferSourceVaultID = %v, want zero after clear", got.TransferSourceVaultID)
	}
	// Every other field is untouched.
	if got.RecordCount != 3 {
		t.Errorf("RecordCount = %d, want 3 (clear must not touch unrelated fields)", got.RecordCount)
	}
}

// TestApplyClearTransferSource_NoOpWhenMissing verifies applying the clear
// for an unknown chunk ID is a harmless no-op (never an error) — a
// replayed clear racing an already-deleted entry must not fail Apply.
func TestApplyClearTransferSource_NoOpWhenMissing(t *testing.T) {
	t.Parallel()
	f := New()
	if got := applyLog(f, MarshalClearTransferSource(testChunkID(9))); got != nil {
		t.Fatalf("Apply on missing entry: %v, want nil (no-op)", got)
	}
}

// TestApplyClearTransferSource_IdempotentWhenAlreadyClear verifies a
// second clear (replay, retry) against an entry with no
// TransferSourceVaultID (including a normal, non-transfer chunk) is a
// no-op.
func TestApplyClearTransferSource_IdempotentWhenAlreadyClear(t *testing.T) {
	t.Parallel()
	f := New()
	id := testChunkID(2)
	now := time.Unix(0, 1_700_000_000_000_000_000)
	applyCmd(t, f, MarshalCreateChunk(id, now, now, now))

	if got := applyLog(f, MarshalClearTransferSource(id)); got != nil {
		t.Fatalf("Apply on already-clear entry: %v, want nil (no-op)", got)
	}
	entry := f.Get(id)
	if entry == nil || !entry.TransferSourceVaultID.IsZero() {
		t.Fatalf("entry after no-op clear = %+v, want unchanged zero TransferSourceVaultID", entry)
	}
}

// TestClearTransferSource_SnapshotRoundTrip verifies the cleared field
// survives a snapshot Persist/Restore round trip — a transfer-introduced
// entry cleared just before a snapshot must not resurrect
// TransferSourceVaultID on the restoring side.
func TestClearTransferSource_SnapshotRoundTrip(t *testing.T) {
	t.Parallel()
	src := New()
	id := testChunkID(3)
	applyCmd(t, src, mustMarshalRepatriate(t, ManifestEntry{
		ID:                    id,
		RecordCount:           5,
		TransferSourceVaultID: glidFromByte(0x88),
	}))
	applyCmd(t, src, MarshalClearTransferSource(id))

	dst := New()
	dst.RestoreProto(src.SnapshotProto())

	if !proto.Equal(src.SnapshotProto(), dst.SnapshotProto()) {
		t.Fatalf("snapshot proto state differs after restore")
	}
	got := dst.Get(id)
	if got == nil {
		t.Fatal("entry missing after restore")
	}
	if !got.TransferSourceVaultID.IsZero() {
		t.Errorf("TransferSourceVaultID = %v after restore, want zero (clear must survive the snapshot boundary)", got.TransferSourceVaultID)
	}
}

func mustMarshalRepatriate(t *testing.T, e ManifestEntry) []byte {
	t.Helper()
	data, err := MarshalRepatriateChunk(e)
	if err != nil {
		t.Fatalf("MarshalRepatriateChunk: %v", err)
	}
	return data
}
