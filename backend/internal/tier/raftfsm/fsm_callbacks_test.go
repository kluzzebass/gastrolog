package raftfsm

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
	var captured *Entry
	fsm.SetOnSeal(func(e Entry) {
		mu.Lock()
		captured = &e
		mu.Unlock()
	})

	fsm.Apply(&hraft.Log{Data: MarshalCreateChunk(id, now, now, now)})
	fsm.Apply(&hraft.Log{Data: MarshalSealChunk(id, now, 100, 12345, now, now)})

	mu.Lock()
	defer mu.Unlock()
	if captured == nil {
		t.Fatal("OnSeal callback was not called")
	}
	if captured.ID != id {
		t.Errorf("ID = %s, want %s", captured.ID, id)
	}
	if !captured.Sealed {
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
	fsm.SetOnSeal(func(Entry) {
		called = true
		fired.Done()
	})

	// Seal a chunk that was never created — applySeal returns an error,
	// so the callback must not fire.
	res := fsm.Apply(&hraft.Log{Data: MarshalSealChunk(id, now, 1, 1, now, now)})
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

func TestOnTransitionStreamedCallbackFires(t *testing.T) {
	t.Parallel()

	fsm := New()
	id := chunk.NewChunkID()
	now := time.Now()

	var mu sync.Mutex
	var captured *chunk.ChunkID
	fsm.SetOnTransitionStreamed(func(cid chunk.ChunkID) {
		mu.Lock()
		captured = &cid
		mu.Unlock()
	})

	fsm.Apply(&hraft.Log{Data: MarshalCreateChunk(id, now, now, now)})
	fsm.Apply(&hraft.Log{Data: marshalTransitionStreamed(id)})

	mu.Lock()
	defer mu.Unlock()
	if captured == nil {
		t.Fatal("OnTransitionStreamed callback was not called")
	}
	if *captured != id {
		t.Errorf("ID = %s, want %s", *captured, id)
	}
}

func TestOnTransitionReceivedCallbackFires(t *testing.T) {
	t.Parallel()

	fsm := New()
	sourceID := chunk.NewChunkID()

	var mu sync.Mutex
	var captured *chunk.ChunkID
	fsm.SetOnTransitionReceived(func(cid chunk.ChunkID) {
		mu.Lock()
		captured = &cid
		mu.Unlock()
	})

	// TransitionReceived doesn't require the source chunk to exist on
	// THIS tier's FSM — it's a destination-side receipt for a source
	// that lives on the previous tier. Apply directly.
	fsm.Apply(&hraft.Log{Data: marshalTransitionReceived(sourceID)})

	mu.Lock()
	defer mu.Unlock()
	if captured == nil {
		t.Fatal("OnTransitionReceived callback was not called")
	}
	if *captured != sourceID {
		t.Errorf("ID = %s, want %s", *captured, sourceID)
	}
}

func TestNewCallbacksNoPanicWhenUnregistered(t *testing.T) {
	t.Parallel()

	fsm := New()
	id := chunk.NewChunkID()
	now := time.Now()

	// None of the new callbacks are set. Applying each command must
	// not panic and must not regress existing apply behavior.
	fsm.Apply(&hraft.Log{Data: MarshalCreateChunk(id, now, now, now)})
	if err := fsm.Apply(&hraft.Log{Data: MarshalSealChunk(id, now, 1, 1, now, now)}); err != nil {
		t.Errorf("seal apply unexpected error: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: MarshalRetentionPending(id)}); err != nil {
		t.Errorf("retention-pending apply unexpected error: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: marshalTransitionStreamed(id)}); err != nil {
		t.Errorf("transition-streamed apply unexpected error: %v", err)
	}
	src := chunk.NewChunkID()
	if err := fsm.Apply(&hraft.Log{Data: marshalTransitionReceived(src)}); err != nil {
		t.Errorf("transition-received apply unexpected error: %v", err)
	}
}

// marshalTransitionStreamed and marshalTransitionReceived build the
// raft log data for the corresponding commands. The package only
// exposes Marshal* helpers for the commands that have external
// callers; these test-local helpers exist so the callback tests can
// stay self-contained without leaking new exported APIs that the
// reconciler hasn't yet locked down.
func marshalTransitionStreamed(id chunk.ChunkID) []byte {
	buf := make([]byte, 1+16)
	buf[0] = byte(CmdTransitionStreamed)
	copy(buf[1:17], id[:])
	return buf
}

func marshalTransitionReceived(id chunk.ChunkID) []byte {
	buf := make([]byte, 1+16)
	buf[0] = byte(CmdTransitionReceived)
	copy(buf[1:17], id[:])
	return buf
}
