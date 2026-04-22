package vaultraft

import (
	"bytes"
	"io"
	"testing"

	hraft "github.com/hashicorp/raft"
)

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

func TestFSM_SnapshotRestore(t *testing.T) {
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
