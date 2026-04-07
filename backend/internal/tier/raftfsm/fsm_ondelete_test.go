package raftfsm

import (
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chunk"

	hraft "github.com/hashicorp/raft"
)

// applyEntry applies a single command to the FSM via the public Apply API.
// Returns the result so tests can assert error or nil-result.
func applyEntry(f *FSM, data []byte) any {
	return f.Apply(&hraft.Log{Index: 1, Term: 1, Data: data})
}

func TestFSMOnDelete_FiresAfterApply(t *testing.T) {
	t.Parallel()

	f := New()

	var fired atomic.Int32
	var firedID atomic.Pointer[chunk.ChunkID]
	f.SetOnDelete(func(id chunk.ChunkID) {
		fired.Add(1)
		firedID.Store(&id)
	})

	id := chunk.ChunkID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	// Create the chunk first.
	if err := applyEntry(f, MarshalCreateChunk(id, time.Now(), time.Now(), time.Now())); err != nil {
		t.Fatalf("CreateChunk: %v", err)
	}
	if fired.Load() != 0 {
		t.Errorf("OnDelete fired on Create; want 0, got %d", fired.Load())
	}

	// Delete it.
	if err := applyEntry(f, MarshalDeleteChunk(id)); err != nil {
		t.Fatalf("DeleteChunk: %v", err)
	}
	if got := fired.Load(); got != 1 {
		t.Errorf("OnDelete fire count after delete: got %d, want 1", got)
	}
	if got := firedID.Load(); got == nil || *got != id {
		t.Errorf("OnDelete received wrong ID: got %v, want %v", got, id)
	}
}

func TestFSMOnDelete_NoFireOnReplay(t *testing.T) {
	t.Parallel()

	f := New()
	var fired atomic.Int32
	f.SetOnDelete(func(id chunk.ChunkID) { fired.Add(1) })

	id := chunk.ChunkID{0xaa}
	_ = applyEntry(f, MarshalCreateChunk(id, time.Now(), time.Now(), time.Now()))

	// Apply the delete twice. The second apply should be a no-op (chunk
	// already gone) and OnDelete should not fire a second time.
	_ = applyEntry(f, MarshalDeleteChunk(id))
	_ = applyEntry(f, MarshalDeleteChunk(id))

	if got := fired.Load(); got != 1 {
		t.Errorf("OnDelete fire count after replay: got %d, want 1", got)
	}
}

func TestFSMOnDelete_NoFireOnDeleteOfMissing(t *testing.T) {
	t.Parallel()

	f := New()
	var fired atomic.Int32
	f.SetOnDelete(func(id chunk.ChunkID) { fired.Add(1) })

	id := chunk.ChunkID{0xff}
	// No CreateChunk first — delete a chunk that was never present.
	_ = applyEntry(f, MarshalDeleteChunk(id))

	if got := fired.Load(); got != 0 {
		t.Errorf("OnDelete fired for missing chunk: got %d, want 0", got)
	}
}

func TestFSMOnDelete_NotInvokedWhenUnset(t *testing.T) {
	t.Parallel()

	// Constructing a fresh FSM with no SetOnDelete should not panic on
	// delete — the nil callback is just skipped.
	f := New()
	id := chunk.ChunkID{0x42}
	_ = applyEntry(f, MarshalCreateChunk(id, time.Now(), time.Now(), time.Now()))
	_ = applyEntry(f, MarshalDeleteChunk(id))
	// No assertion — the absence of a panic is the test.
}

// TestFSMOnDelete_FiresOutsideMutex verifies that the callback is invoked
// after the FSM mutex has been released. We do this by acquiring a read
// lock from inside the callback — if the callback ran while the FSM still
// held its write lock, this would deadlock.
func TestFSMOnDelete_FiresOutsideMutex(t *testing.T) {
	t.Parallel()

	f := New()

	done := make(chan struct{})
	f.SetOnDelete(func(id chunk.ChunkID) {
		// If Apply still holds f.mu, this RLock would deadlock.
		_ = f.Get(id)
		close(done)
	})

	id := chunk.ChunkID{0x77}
	_ = applyEntry(f, MarshalCreateChunk(id, time.Now(), time.Now(), time.Now()))
	_ = applyEntry(f, MarshalDeleteChunk(id))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("OnDelete callback did not return within 2s — likely deadlocked on FSM mutex")
	}
}
