package raftfsm

import (
	"io"
	"testing"
	"time"

	"gastrolog/internal/chunk"

	hraft "github.com/hashicorp/raft"
)

func TestFSMNotReadyBeforeAnyOperation(t *testing.T) {
	t.Parallel()
	fsm := New()
	if fsm.Ready() {
		t.Error("expected Ready()=false on fresh FSM")
	}
}

func TestFSMReadyAfterApply(t *testing.T) {
	t.Parallel()
	fsm := New()
	id := chunk.NewChunkID()
	fsm.Apply(&hraft.Log{Data: MarshalCreateChunk(id, time.Now(), time.Now(), time.Now())})
	if !fsm.Ready() {
		t.Error("expected Ready()=true after Apply")
	}
}

func TestFSMReadyAfterRestore(t *testing.T) {
	t.Parallel()

	// Real restore path: produce a snapshot via Persist (which writes the
	// versioned header) and restore it into a fresh FSM. An FSM whose
	// chunk map happens to be empty still goes through the header + section
	// framing — that's what Raft actually feeds us at runtime.
	src := New()
	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	pr, pw := io.Pipe()
	go func() {
		_ = snap.Persist(&pipeSink{pw})
		_ = pw.Close()
	}()

	fsm := New()
	if err := fsm.Restore(io.NopCloser(pr)); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if !fsm.Ready() {
		t.Error("expected Ready()=true after Restore, even with empty snapshot")
	}
}

func TestFSMReadyAfterRestoreWithEntries(t *testing.T) {
	t.Parallel()

	// Build a snapshot with one entry.
	src := New()
	id := chunk.NewChunkID()
	src.Apply(&hraft.Log{Data: MarshalCreateChunk(id, time.Now(), time.Now(), time.Now())})
	snap, _ := src.Snapshot()
	pr, pw := io.Pipe()
	go func() {
		snap.Persist(&pipeSink{pw})
		pw.Close()
	}()

	// Restore into a fresh FSM.
	dst := New()
	if dst.Ready() {
		t.Fatal("fresh FSM should not be ready")
	}
	if err := dst.Restore(pr); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if !dst.Ready() {
		t.Error("expected Ready()=true after Restore with entries")
	}
	if dst.Get(id) == nil {
		t.Error("expected entry to be restored")
	}
}

// emptyReader always returns EOF.
type emptyReader struct{}

func (emptyReader) Read([]byte) (int, error) { return 0, io.EOF }

// pipeSink adapts an io.WriteCloser to hraft.SnapshotSink.
type pipeSink struct{ w io.WriteCloser }

func (s *pipeSink) Write(p []byte) (int, error) { return s.w.Write(p) }
func (s *pipeSink) Close() error                { return s.w.Close() }
func (s *pipeSink) ID() string                  { return "test" }
func (s *pipeSink) Cancel() error               { return s.w.Close() }
