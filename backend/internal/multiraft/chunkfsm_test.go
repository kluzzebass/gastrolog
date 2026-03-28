package multiraft

import (
	"bytes"
	"io"
	"testing"
	"time"

	"gastrolog/internal/chunk"

	hraft "github.com/hashicorp/raft"
)

func testChunkID(b byte) chunk.ChunkID {
	var id chunk.ChunkID
	id[0] = b
	return id
}

func applyCmd(t *testing.T, fsm *ChunkFSM, data []byte) {
	t.Helper()
	result := fsm.Apply(&hraft.Log{Data: data})
	if err, ok := result.(error); ok && err != nil {
		t.Fatalf("apply: %v", err)
	}
}

func TestChunkFSMCreateAndGet(t *testing.T) {
	t.Parallel()
	fsm := NewChunkFSM()

	id := testChunkID(1)
	now := time.Now().Truncate(time.Nanosecond)
	applyCmd(t, fsm, MarshalCreateChunk(id, now, now.Add(time.Millisecond), now.Add(2*time.Millisecond)))

	e := fsm.Get(id)
	if e == nil {
		t.Fatal("expected chunk entry")
	}
	if e.ID != id {
		t.Errorf("ID: got %s, want %s", e.ID, id)
	}
	if !e.WriteStart.Equal(now) {
		t.Errorf("WriteStart: got %v, want %v", e.WriteStart, now)
	}
	if !e.IngestStart.Equal(now.Add(time.Millisecond)) {
		t.Errorf("IngestStart mismatch")
	}
	if e.Sealed {
		t.Error("should not be sealed")
	}
}

func TestChunkFSMSeal(t *testing.T) {
	t.Parallel()
	fsm := NewChunkFSM()

	id := testChunkID(2)
	now := time.Now().Truncate(time.Nanosecond)
	end := now.Add(5 * time.Second)

	applyCmd(t, fsm, MarshalCreateChunk(id, now, now, now))
	applyCmd(t, fsm, MarshalSealChunk(id, end, 500, 1024*1024, end, end))

	e := fsm.Get(id)
	if !e.Sealed {
		t.Error("should be sealed")
	}
	if e.RecordCount != 500 {
		t.Errorf("RecordCount: got %d, want 500", e.RecordCount)
	}
	if e.Bytes != 1024*1024 {
		t.Errorf("Bytes: got %d, want %d", e.Bytes, 1024*1024)
	}
	if !e.WriteEnd.Equal(end) {
		t.Errorf("WriteEnd mismatch")
	}
}

func TestChunkFSMCompress(t *testing.T) {
	t.Parallel()
	fsm := NewChunkFSM()

	id := testChunkID(3)
	now := time.Now().Truncate(time.Nanosecond)

	applyCmd(t, fsm, MarshalCreateChunk(id, now, now, now))
	applyCmd(t, fsm, MarshalSealChunk(id, now, 100, 50000, now, now))
	applyCmd(t, fsm, MarshalCompressChunk(id, 12000))

	e := fsm.Get(id)
	if !e.Compressed {
		t.Error("should be compressed")
	}
	if e.DiskBytes != 12000 {
		t.Errorf("DiskBytes: got %d, want 12000", e.DiskBytes)
	}
}

func TestChunkFSMUpload(t *testing.T) {
	t.Parallel()
	fsm := NewChunkFSM()

	id := testChunkID(4)
	now := time.Now().Truncate(time.Nanosecond)

	applyCmd(t, fsm, MarshalCreateChunk(id, now, now, now))
	applyCmd(t, fsm, MarshalSealChunk(id, now, 200, 80000, now, now))
	applyCmd(t, fsm, MarshalCompressChunk(id, 30000))
	applyCmd(t, fsm, MarshalUploadChunk(id, 25000, 1000, 2000, 3000, 4000, 16))

	e := fsm.Get(id)
	if !e.CloudBacked {
		t.Error("should be cloud-backed")
	}
	if e.DiskBytes != 25000 {
		t.Errorf("DiskBytes: got %d, want 25000", e.DiskBytes)
	}
	if e.IngestIdxOffset != 1000 {
		t.Errorf("IngestIdxOffset: got %d, want 1000", e.IngestIdxOffset)
	}
	if e.IngestIdxSize != 2000 {
		t.Errorf("IngestIdxSize: got %d, want 2000", e.IngestIdxSize)
	}
	if e.SourceIdxOffset != 3000 {
		t.Errorf("SourceIdxOffset: got %d, want 3000", e.SourceIdxOffset)
	}
	if e.SourceIdxSize != 4000 {
		t.Errorf("SourceIdxSize: got %d, want 4000", e.SourceIdxSize)
	}
	if e.NumFrames != 16 {
		t.Errorf("NumFrames: got %d, want 16", e.NumFrames)
	}
}

func TestChunkFSMDelete(t *testing.T) {
	t.Parallel()
	fsm := NewChunkFSM()

	id := testChunkID(5)
	now := time.Now().Truncate(time.Nanosecond)

	applyCmd(t, fsm, MarshalCreateChunk(id, now, now, now))
	if fsm.Count() != 1 {
		t.Fatalf("count: got %d, want 1", fsm.Count())
	}

	applyCmd(t, fsm, MarshalDeleteChunk(id))
	if fsm.Count() != 0 {
		t.Errorf("count after delete: got %d, want 0", fsm.Count())
	}
	if fsm.Get(id) != nil {
		t.Error("should be nil after delete")
	}
}

func TestChunkFSMSnapshotRestore(t *testing.T) {
	t.Parallel()
	fsm := NewChunkFSM()

	now := time.Now().Truncate(time.Nanosecond)

	// Create a mix of chunk states.
	id1 := testChunkID(10)
	applyCmd(t, fsm, MarshalCreateChunk(id1, now, now, now))
	applyCmd(t, fsm, MarshalSealChunk(id1, now.Add(time.Second), 100, 50000, now.Add(time.Second), now.Add(time.Second)))

	id2 := testChunkID(20)
	applyCmd(t, fsm, MarshalCreateChunk(id2, now, now, now))
	applyCmd(t, fsm, MarshalSealChunk(id2, now.Add(2*time.Second), 200, 80000, now.Add(2*time.Second), now.Add(2*time.Second)))
	applyCmd(t, fsm, MarshalCompressChunk(id2, 30000))
	applyCmd(t, fsm, MarshalUploadChunk(id2, 25000, 100, 200, 300, 400, 8))

	id3 := testChunkID(30)
	applyCmd(t, fsm, MarshalCreateChunk(id3, now, now, now))

	// Snapshot.
	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&bufSink{Writer: &buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	// Restore into a fresh FSM.
	fsm2 := NewChunkFSM()
	if err := fsm2.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if fsm2.Count() != 3 {
		t.Fatalf("count after restore: got %d, want 3", fsm2.Count())
	}

	// Verify sealed chunk.
	e1 := fsm2.Get(id1)
	if e1 == nil || !e1.Sealed || e1.RecordCount != 100 {
		t.Errorf("chunk 1: sealed=%v, records=%d", e1 != nil && e1.Sealed, e1.RecordCount)
	}

	// Verify cloud-backed chunk.
	e2 := fsm2.Get(id2)
	if e2 == nil || !e2.CloudBacked || e2.NumFrames != 8 || e2.DiskBytes != 25000 {
		t.Errorf("chunk 2: cloud=%v, frames=%d, diskBytes=%d",
			e2 != nil && e2.CloudBacked, e2.NumFrames, e2.DiskBytes)
	}
	if e2.IngestIdxOffset != 100 || e2.SourceIdxSize != 400 {
		t.Errorf("chunk 2: TOC offsets wrong")
	}

	// Verify unsealed chunk.
	e3 := fsm2.Get(id3)
	if e3 == nil || e3.Sealed {
		t.Error("chunk 3: should exist and be unsealed")
	}
}

func TestChunkFSMToChunkMeta(t *testing.T) {
	t.Parallel()
	fsm := NewChunkFSM()

	id := testChunkID(99)
	now := time.Now().Truncate(time.Nanosecond)

	applyCmd(t, fsm, MarshalCreateChunk(id, now, now, now))
	applyCmd(t, fsm, MarshalSealChunk(id, now.Add(time.Second), 42, 1234, now.Add(time.Second), now.Add(time.Second)))

	e := fsm.Get(id)
	meta := e.ToChunkMeta()

	if meta.ID != id {
		t.Error("ID mismatch")
	}
	if meta.RecordCount != 42 {
		t.Errorf("RecordCount: got %d, want 42", meta.RecordCount)
	}
	if !meta.Sealed {
		t.Error("should be sealed")
	}
}

func TestChunkFSMListReturnsAll(t *testing.T) {
	t.Parallel()
	fsm := NewChunkFSM()

	now := time.Now().Truncate(time.Nanosecond)
	for i := range byte(5) {
		applyCmd(t, fsm, MarshalCreateChunk(testChunkID(i), now, now, now))
	}

	list := fsm.List()
	if len(list) != 5 {
		t.Errorf("List: got %d entries, want 5", len(list))
	}
}

func TestChunkFSMSealNonexistentReturnsError(t *testing.T) {
	t.Parallel()
	fsm := NewChunkFSM()

	now := time.Now()
	result := fsm.Apply(&hraft.Log{Data: MarshalSealChunk(testChunkID(0xFF), now, 0, 0, now, now)})
	if result == nil {
		t.Fatal("expected error for sealing nonexistent chunk")
	}
	if _, ok := result.(error); !ok {
		t.Fatal("expected error type")
	}
}

// bufSink adapts an io.Writer to hraft.SnapshotSink for testing.
type bufSink struct {
	io.Writer
}

func (s *bufSink) Close() error  { return nil }
func (s *bufSink) ID() string    { return "test" }
func (s *bufSink) Cancel() error { return nil }
