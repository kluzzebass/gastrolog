package vaultraft

import (
	"bytes"
	"io"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	tierfsm "gastrolog/internal/tier/raftfsm"

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

func TestFSM_OpTierFSM_delegate(t *testing.T) {
	t.Parallel()
	f := NewFSM()
	tierID := glid.New()
	cid := testChunkID(7)
	now := time.Now().Truncate(time.Nanosecond)
	wire := tierfsm.MarshalCreateChunk(cid, now, now, now)
	cmd := MarshalTierCommand(tierID, wire)
	if got := f.Apply(&hraft.Log{Data: cmd}); got != nil {
		t.Fatalf("apply: %v", got)
	}
	sub := f.TierFSM(tierID)
	if sub == nil {
		t.Fatal("expected tier sub-FSM")
	}
	e := sub.Get(cid)
	if e == nil {
		t.Fatal("expected chunk in tier FSM")
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

func TestFSM_SnapshotRestore_twoTiers(t *testing.T) {
	t.Parallel()
	f := NewFSM()
	tierA, tierB := glid.New(), glid.New()
	if bytes.Compare(tierA[:], tierB[:]) > 0 {
		tierA, tierB = tierB, tierA
	}
	now := time.Now().Truncate(time.Nanosecond)
	a := testChunkID(1)
	b := testChunkID(2)
	if r := f.Apply(&hraft.Log{Data: MarshalTierCommand(tierA, tierfsm.MarshalCreateChunk(a, now, now, now))}); r != nil {
		t.Fatalf("tier A: %v", r)
	}
	if r := f.Apply(&hraft.Log{Data: MarshalTierCommand(tierB, tierfsm.MarshalCreateChunk(b, now, now, now))}); r != nil {
		t.Fatalf("tier B: %v", r)
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
	if f2.TierFSM(tierA).Get(a) == nil {
		t.Fatal("tier A chunk missing after restore")
	}
	if f2.TierFSM(tierB).Get(b) == nil {
		t.Fatal("tier B chunk missing after restore")
	}
}

func TestFSM_Restore_legacyEmptyByte(t *testing.T) {
	t.Parallel()
	f := NewFSM()
	tierID := glid.New()
	now := time.Now().Truncate(time.Nanosecond)
	if r := f.Apply(&hraft.Log{Data: MarshalTierCommand(tierID, tierfsm.MarshalCreateChunk(testChunkID(9), now, now, now))}); r != nil {
		t.Fatalf("apply: %v", r)
	}
	if f.TierFSM(tierID) == nil {
		t.Fatal("expected tier before legacy restore")
	}
	if err := f.Restore(io.NopCloser(bytes.NewReader([]byte{1}))); err != nil {
		t.Fatalf("legacy restore: %v", err)
	}
	if f.TierFSM(tierID) != nil {
		t.Fatal("legacy restore should reset tier state")
	}
}
