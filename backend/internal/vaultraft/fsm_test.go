package vaultraft

import (
	"bytes"
	"io"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

func testChunkID(b byte) chunk.ChunkID {
	var id chunk.ChunkID
	id[0] = b
	return id
}

func TestFSM_ApplyNoopAndUnknown(t *testing.T) {
	t.Parallel()
	f := NewFSM()
	if got := f.Apply(&hraft.Log{Data: MarshalNoop()}); got != nil {
		t.Fatalf("noop: %v", got)
	}
	if got := f.Apply(&hraft.Log{Data: []byte{0xFF}}); got == nil {
		t.Fatal("expected error for unknown opcode")
	}
}

func TestFSM_OpVaultChunkFSM_delegate(t *testing.T) {
	t.Parallel()
	f := NewFSM()
	instID := glid.New()
	cid := testChunkID(7)
	now := time.Now().Truncate(time.Nanosecond)
	wire := vaultctlfsm.MarshalCreateChunk(cid, now, now, now)
	cmd := MarshalVaultChunkCommand(instID, wire)
	if got := f.Apply(&hraft.Log{Data: cmd}); got != nil {
		t.Fatalf("apply: %v", got)
	}
	sub := f.InstanceFSM(instID)
	if sub == nil {
		t.Fatal("expected instance sub-FSM")
	}
	e := sub.Get(cid)
	if e == nil {
		t.Fatal("expected chunk in instance FSM")
	}
	if e.ID != cid {
		t.Fatalf("chunk id: got %v want %v", e.ID, cid)
	}
}

func TestFSM_SnapshotRestore_empty(t *testing.T) {
	t.Parallel()
	f := NewFSM()
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&bufSink{Writer: &buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if err := f.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
}

func TestFSM_SnapshotRestore_twoInstances(t *testing.T) {
	t.Parallel()
	f := NewFSM()
	instA, instB := glid.New(), glid.New()
	if bytes.Compare(instA[:], instB[:]) > 0 {
		instA, instB = instB, instA
	}
	now := time.Now().Truncate(time.Nanosecond)
	a := testChunkID(1)
	b := testChunkID(2)
	if r := f.Apply(&hraft.Log{Data: MarshalVaultChunkCommand(instA, vaultctlfsm.MarshalCreateChunk(a, now, now, now))}); r != nil {
		t.Fatalf("instance A: %v", r)
	}
	if r := f.Apply(&hraft.Log{Data: MarshalVaultChunkCommand(instB, vaultctlfsm.MarshalCreateChunk(b, now, now, now))}); r != nil {
		t.Fatalf("instance B: %v", r)
	}
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&bufSink{Writer: &buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	f2 := NewFSM()
	if err := f2.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if f2.InstanceFSM(instA).Get(a) == nil {
		t.Fatal("instance A chunk missing after restore")
	}
	if f2.InstanceFSM(instB).Get(b) == nil {
		t.Fatal("instance B chunk missing after restore")
	}
}

// TestFSM_OnAfterRestoreFires pins the gastrolog-51gme catchup hook:
// snapshot install must fire SetOnAfterRestore so the orchestrator can
// run ReconcileFromSnapshot on every instance. Without this, the receipt
// protocol's pendingDeletes silently leak across snapshot boundaries.
func TestFSM_OnAfterRestoreFires(t *testing.T) {
	t.Parallel()

	src := NewFSM()
	now := time.Now().Truncate(time.Nanosecond)
	instA, instB := glid.New(), glid.New()
	_ = src.Apply(&hraft.Log{Data: MarshalVaultChunkCommand(instA, vaultctlfsm.MarshalCreateChunk(testChunkID(1), now, now, now))})
	_ = src.Apply(&hraft.Log{Data: MarshalVaultChunkCommand(instB, vaultctlfsm.MarshalCreateChunk(testChunkID(2), now, now, now))})

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&bufSink{Writer: &buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	dst := NewFSM()
	var fires int32
	dst.SetOnAfterRestore(func() { fires++ })

	if err := dst.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if fires != 1 {
		t.Errorf("OnAfterRestore fires = %d, want 1", fires)
	}
	// Sanity: the hook fires AFTER tiers were swapped in, so the
	// orchestrator's handler can already iterate Tiers() to find work.
	if got := dst.Instances(); len(got) != 2 {
		t.Errorf("post-restore Tiers() = %d, want 2", len(got))
	}
}

// TestFSM_OnAfterRestoreFires_legacyEmpty pins that the legacy
// single-byte empty-snapshot code path also fires the hook. A node
// rejoining a freshly-bootstrapped cluster takes this path; the
// receipt protocol's catchup needs to run there too.
func TestFSM_OnAfterRestoreFires_legacyEmpty(t *testing.T) {
	t.Parallel()
	f := NewFSM()
	var fires int32
	f.SetOnAfterRestore(func() { fires++ })
	if err := f.Restore(io.NopCloser(bytes.NewReader([]byte{1}))); err != nil {
		t.Fatalf("legacy restore: %v", err)
	}
	if fires != 1 {
		t.Errorf("OnAfterRestore fires = %d on legacy empty snapshot, want 1", fires)
	}
}

func TestFSM_Restore_legacyEmptyByte(t *testing.T) {
	t.Parallel()
	f := NewFSM()
	instID := glid.New()
	now := time.Now().Truncate(time.Nanosecond)
	if r := f.Apply(&hraft.Log{Data: MarshalVaultChunkCommand(instID, vaultctlfsm.MarshalCreateChunk(testChunkID(9), now, now, now))}); r != nil {
		t.Fatalf("apply: %v", r)
	}
	if f.InstanceFSM(instID) == nil {
		t.Fatal("expected instance before legacy restore")
	}
	if err := f.Restore(io.NopCloser(bytes.NewReader([]byte{1}))); err != nil {
		t.Fatalf("legacy restore: %v", err)
	}
	if f.InstanceFSM(instID) != nil {
		t.Fatal("legacy restore should reset instance state")
	}
}
