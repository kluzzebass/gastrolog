package tierfsm

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

func applyCmd(t *testing.T, fsm *FSM, data []byte) {
	t.Helper()
	result := fsm.Apply(&hraft.Log{Data: data})
	if err, ok := result.(error); ok && err != nil {
		t.Fatalf("apply: %v", err)
	}
}

func TestFSMCreateAndGet(t *testing.T) {
	// Not parallel — consistent with other multiraft tests.
	fsm := New()

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

func TestFSMSeal(t *testing.T) {
	// Not parallel — consistent with other multiraft tests.
	fsm := New()

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

func TestFSMCompress(t *testing.T) {
	// Not parallel — consistent with other multiraft tests.
	fsm := New()

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

func TestFSMUpload(t *testing.T) {
	// Not parallel — consistent with other multiraft tests.
	fsm := New()

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

func TestFSMDelete(t *testing.T) {
	// Not parallel — consistent with other multiraft tests.
	fsm := New()

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

func TestFSMSnapshotRestore(t *testing.T) {
	// Not parallel — consistent with other multiraft tests.
	fsm := New()

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
	fsm2 := New()
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

// TestFSMSnapshotRestoreTombstones verifies that tombstones round-trip
// through Snapshot/Restore so receivers keep rejecting stale replication
// commands across restarts and snapshot installs. See gastrolog-11rzz.
func TestFSMSnapshotRestoreTombstones(t *testing.T) {
	fsm := New()
	now := time.Now().Truncate(time.Nanosecond)

	// One live chunk, two deleted chunks (tombstoned).
	live := testChunkID(1)
	applyCmd(t, fsm, MarshalCreateChunk(live, now, now, now))
	applyCmd(t, fsm, MarshalSealChunk(live, now.Add(time.Second), 10, 100, now.Add(time.Second), now.Add(time.Second)))

	dead1 := testChunkID(2)
	applyCmd(t, fsm, MarshalCreateChunk(dead1, now, now, now))
	applyCmd(t, fsm, MarshalDeleteChunk(dead1))

	dead2 := testChunkID(3)
	applyCmd(t, fsm, MarshalCreateChunk(dead2, now, now, now))
	applyCmd(t, fsm, MarshalDeleteChunk(dead2))

	// Also tombstone a chunk that never existed in this FSM (e.g. out-of-order
	// log replay). Tombstone should still be recorded so a subsequent stale
	// create would be rejected.
	ghost := testChunkID(4)
	applyCmd(t, fsm, MarshalDeleteChunk(ghost))

	if !fsm.IsTombstoned(dead1) || !fsm.IsTombstoned(dead2) || !fsm.IsTombstoned(ghost) {
		t.Fatal("expected all three deleted chunks to be tombstoned before snapshot")
	}
	if fsm.IsTombstoned(live) {
		t.Fatal("live chunk must not be tombstoned")
	}

	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&bufSink{Writer: &buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	fsm2 := New()
	if err := fsm2.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if !fsm2.IsTombstoned(dead1) || !fsm2.IsTombstoned(dead2) || !fsm2.IsTombstoned(ghost) {
		t.Error("tombstones not preserved across Snapshot/Restore")
	}
	if fsm2.IsTombstoned(live) {
		t.Error("live chunk wrongly tombstoned after restore")
	}
	if fsm2.Count() != 1 {
		t.Errorf("expected 1 live chunk after restore, got %d", fsm2.Count())
	}
}

func TestFSMPruneTombstones(t *testing.T) {
	fsm := New()

	// Three tombstones; we'll prune the two old ones.
	for i := 1; i <= 3; i++ {
		applyCmd(t, fsm, MarshalDeleteChunk(testChunkID(byte(i))))
	}
	if len(fsm.tombstones) != 3 {
		t.Fatalf("expected 3 tombstones, got %d", len(fsm.tombstones))
	}

	// Age the first two so the prune sweep finds them.
	old := time.Now().Add(-2 * time.Hour)
	fsm.mu.Lock()
	fsm.tombstones[testChunkID(1)] = old
	fsm.tombstones[testChunkID(2)] = old
	fsm.mu.Unlock()

	pruned := fsm.PruneTombstones(time.Now().Add(-1 * time.Hour))
	if pruned != 2 {
		t.Errorf("expected 2 pruned, got %d", pruned)
	}
	if fsm.IsTombstoned(testChunkID(1)) || fsm.IsTombstoned(testChunkID(2)) {
		t.Error("aged tombstones should have been pruned")
	}
	if !fsm.IsTombstoned(testChunkID(3)) {
		t.Error("recent tombstone should still be present")
	}
}

func TestFSMToChunkMeta(t *testing.T) {
	// Not parallel — consistent with other multiraft tests.
	fsm := New()

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

func TestFSMListReturnsAll(t *testing.T) {
	// Not parallel — consistent with other multiraft tests.
	fsm := New()

	now := time.Now().Truncate(time.Nanosecond)
	for i := range byte(5) {
		applyCmd(t, fsm, MarshalCreateChunk(testChunkID(i), now, now, now))
	}

	list := fsm.List()
	if len(list) != 5 {
		t.Errorf("List: got %d entries, want 5", len(list))
	}
}

func TestFSMSealNonexistentReturnsError(t *testing.T) {
	// Not parallel — consistent with other multiraft tests.
	fsm := New()

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
