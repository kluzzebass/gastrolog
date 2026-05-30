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

// bufSink adapts an io.Writer to hraft.SnapshotSink for snapshot round-trip
// tests (the production snapshot path writes straight to the Raft sink).
type bufSink struct{ io.Writer }

func (s *bufSink) Close() error  { return nil }
func (s *bufSink) ID() string    { return "test" }
func (s *bufSink) Cancel() error { return nil }

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
	vaultID := glid.New()
	cid := testChunkID(7)
	now := time.Now().Truncate(time.Nanosecond)
	wire := vaultctlfsm.MarshalCreateChunk(cid, now, now, now)
	cmd := MarshalVaultChunkCommand(vaultID, wire)
	if got := f.Apply(&hraft.Log{Data: cmd}); got != nil {
		t.Fatalf("apply: %v", got)
	}
	sub := f.VaultFSM(vaultID)
	if sub == nil {
		t.Fatal("expected vault sub-FSM")
	}
	e := sub.Get(cid)
	if e == nil {
		t.Fatal("expected chunk in vault FSM")
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
	vaultA, vaultB := glid.New(), glid.New()
	if bytes.Compare(vaultA[:], vaultB[:]) > 0 {
		vaultA, vaultB = vaultB, vaultA
	}
	now := time.Now().Truncate(time.Nanosecond)
	a := testChunkID(1)
	b := testChunkID(2)
	if r := f.Apply(&hraft.Log{Data: MarshalVaultChunkCommand(vaultA, vaultctlfsm.MarshalCreateChunk(a, now, now, now))}); r != nil {
		t.Fatalf("vault A: %v", r)
	}
	if r := f.Apply(&hraft.Log{Data: MarshalVaultChunkCommand(vaultB, vaultctlfsm.MarshalCreateChunk(b, now, now, now))}); r != nil {
		t.Fatalf("vault B: %v", r)
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
	if f2.VaultFSM(vaultA).Get(a) == nil {
		t.Fatal("vault A chunk missing after restore")
	}
	if f2.VaultFSM(vaultB).Get(b) == nil {
		t.Fatal("vault B chunk missing after restore")
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
	vaultA, vaultB := glid.New(), glid.New()
	_ = src.Apply(&hraft.Log{Data: MarshalVaultChunkCommand(vaultA, vaultctlfsm.MarshalCreateChunk(testChunkID(1), now, now, now))})
	_ = src.Apply(&hraft.Log{Data: MarshalVaultChunkCommand(vaultB, vaultctlfsm.MarshalCreateChunk(testChunkID(2), now, now, now))})

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
	// Sanity: the hook fires AFTER instances were swapped in, so the
	// orchestrator's handler can already iterate Instances() to find work.
	if got := dst.Vaults(); len(got) != 2 {
		t.Errorf("post-restore Instances() = %d, want 2", len(got))
	}
}

