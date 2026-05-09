package vaultctlfsm

import (
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"

	hraft "github.com/hashicorp/raft"
)

// gastrolog-51gme step 1: tests pinning the four new FSM apply
// callbacks (onSeal, onRetentionPending, onTransitionStreamed,
// onTransitionReceived) so the reconciler can rely on them.

func TestOnSealCallbackFires(t *testing.T) {
	t.Parallel()

	fsm := New()
	id := chunk.NewChunkID()
	now := time.Now()

	var mu sync.Mutex
	var captured *ManifestEntry
	fsm.SetOnSeal(func(e ManifestEntry) {
		mu.Lock()
		captured = &e
		mu.Unlock()
	})

	fsm.Apply(&hraft.Log{Data: MarshalCreateChunk(id, now, now, now)})
	fsm.Apply(&hraft.Log{Data: MarshalSealChunk(id, now, 100, 12345, now, now, now, false)})

	mu.Lock()
	defer mu.Unlock()
	if captured == nil {
		t.Fatal("OnSeal callback was not called")
	}
	if captured.ID != id {
		t.Errorf("ID = %s, want %s", captured.ID, id)
	}
	if !captured.IsSealed() {
		t.Error("Sealed should be true")
	}
	if captured.RecordCount != 100 {
		t.Errorf("RecordCount = %d, want 100", captured.RecordCount)
	}
	if captured.Bytes != 12345 {
		t.Errorf("Bytes = %d, want 12345", captured.Bytes)
	}
}

func TestOnSealCallbackNotCalledWhenChunkUnknown(t *testing.T) {
	t.Parallel()

	fsm := New()
	id := chunk.NewChunkID()
	now := time.Now()

	var fired sync.WaitGroup
	fired.Add(1)
	called := false
	fsm.SetOnSeal(func(ManifestEntry) {
		called = true
		fired.Done()
	})

	// Seal a chunk that was never created — applySeal returns an error,
	// so the callback must not fire.
	res := fsm.Apply(&hraft.Log{Data: MarshalSealChunk(id, now, 1, 1, now, now, now, false)})
	if res == nil {
		t.Fatal("expected error sealing unknown chunk, got nil")
	}
	if called {
		t.Error("OnSeal callback should not fire when applySeal returns an error")
	}
}

func TestOnRetentionPendingCallbackFires(t *testing.T) {
	t.Parallel()

	fsm := New()
	id := chunk.NewChunkID()
	now := time.Now()

	var mu sync.Mutex
	var captured *chunk.ChunkID
	fsm.SetOnRetentionPending(func(cid chunk.ChunkID) {
		mu.Lock()
		captured = &cid
		mu.Unlock()
	})

	fsm.Apply(&hraft.Log{Data: MarshalCreateChunk(id, now, now, now)})
	fsm.Apply(&hraft.Log{Data: MarshalRetentionPending(id)})

	mu.Lock()
	defer mu.Unlock()
	if captured == nil {
		t.Fatal("OnRetentionPending callback was not called")
	}
	if *captured != id {
		t.Errorf("ID = %s, want %s", *captured, id)
	}
}

// gastrolog-5sywa: TestOnTransitionStreamedCallbackFires and
// TestOnTransitionReceivedCallbackFires were deleted along with the
// receipt protocol they pinned. The earlier "no-panic when callbacks
// unregistered" test stays useful for surviving callbacks.
func TestNewCallbacksNoPanicWhenUnregistered(t *testing.T) {
	t.Parallel()

	fsm := New()
	id := chunk.NewChunkID()
	now := time.Now()

	// None of the new callbacks are set. Applying each command must
	// not panic and must not regress existing apply behavior.
	fsm.Apply(&hraft.Log{Data: MarshalCreateChunk(id, now, now, now)})
	if err := fsm.Apply(&hraft.Log{Data: MarshalSealChunk(id, now, 1, 1, now, now, now, false)}); err != nil {
		t.Errorf("seal apply unexpected error: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: MarshalRetentionPending(id)}); err != nil {
		t.Errorf("retention-pending apply unexpected error: %v", err)
	}
}
