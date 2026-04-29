package raftfsm

import (
	"bytes"
	"reflect"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"

	hraft "github.com/hashicorp/raft"
)

// gastrolog-51gme step 2 — receipt-based deletion protocol tests.

func TestRequestDeleteAddsPending(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()
	now := time.Now()

	var mu sync.Mutex
	var captured *PendingDelete
	f.SetOnRequestDelete(func(p PendingDelete) {
		mu.Lock()
		c := p
		captured = &c
		mu.Unlock()
	})

	if err := f.Apply(&hraft.Log{Data: MarshalRequestDelete(id, now, "retention-ttl", []string{"node-A", "node-B", "node-C"})}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	got := f.PendingDelete(id)
	if got == nil {
		t.Fatalf("PendingDelete(%s): expected entry, got nil", id)
	}
	if got.ChunkID != id {
		t.Errorf("ChunkID = %s, want %s", got.ChunkID, id)
	}
	if got.Reason != "retention-ttl" {
		t.Errorf("Reason = %q, want %q", got.Reason, "retention-ttl")
	}
	if !got.ProposedAt.Equal(now.Truncate(time.Nanosecond)) {
		t.Errorf("ProposedAt = %v, want %v", got.ProposedAt, now)
	}
	if !reflect.DeepEqual(got.ExpectedFrom, map[string]bool{"node-A": true, "node-B": true, "node-C": true}) {
		t.Errorf("ExpectedFrom = %v, want {node-A, node-B, node-C}", got.ExpectedFrom)
	}

	mu.Lock()
	defer mu.Unlock()
	if captured == nil {
		t.Fatal("OnRequestDelete callback did not fire")
	}
	if captured.ChunkID != id {
		t.Errorf("callback ChunkID = %s, want %s", captured.ChunkID, id)
	}
}

func TestRequestDeleteIdempotent(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()
	now := time.Now()

	var fires int32
	f.SetOnRequestDelete(func(PendingDelete) { fires++ })

	// First request adds the entry and fires the callback.
	f.Apply(&hraft.Log{Data: MarshalRequestDelete(id, now, "first", []string{"node-A", "node-B"})})

	// Simulate one ack so the second request, if it weren't idempotent,
	// would erase the partial progress.
	f.Apply(&hraft.Log{Data: MarshalAckDelete(id, "node-A")})

	// Second request — different reason, different expected set.
	// MUST be a no-op: same chunk already pending, callback must not
	// fire, expectedFrom must NOT reset.
	f.Apply(&hraft.Log{Data: MarshalRequestDelete(id, now, "second", []string{"node-X"})})

	got := f.PendingDelete(id)
	if got == nil {
		t.Fatal("expected pending entry to survive second request")
	}
	if got.Reason != "first" {
		t.Errorf("Reason = %q, want %q (re-request must not overwrite)", got.Reason, "first")
	}
	if got.ExpectedFrom["node-A"] {
		t.Error("node-A should still be acked despite the second request")
	}
	if !got.ExpectedFrom["node-B"] {
		t.Error("node-B should still owe an ack")
	}
	if got.ExpectedFrom["node-X"] {
		t.Error("node-X should NOT be in expectedFrom — second request was a no-op")
	}
	if fires != 1 {
		t.Errorf("OnRequestDelete fires = %d, want 1 (second request must not re-fire)", fires)
	}
}

func TestAckDeleteRemovesNodeFromExpected(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()
	now := time.Now()

	var (
		mu             sync.Mutex
		ackedID        chunk.ChunkID
		ackedNodeID    string
		callbackFires  int
	)
	f.SetOnAckDelete(func(cid chunk.ChunkID, node string) {
		mu.Lock()
		ackedID = cid
		ackedNodeID = node
		callbackFires++
		mu.Unlock()
	})

	f.Apply(&hraft.Log{Data: MarshalRequestDelete(id, now, "test", []string{"node-A", "node-B"})})
	f.Apply(&hraft.Log{Data: MarshalAckDelete(id, "node-A")})

	got := f.PendingDelete(id)
	if got == nil {
		t.Fatal("expected pending entry")
	}
	if got.ExpectedFrom["node-A"] {
		t.Error("node-A should be removed from expectedFrom after acking")
	}
	if !got.ExpectedFrom["node-B"] {
		t.Error("node-B should still owe an ack")
	}

	mu.Lock()
	defer mu.Unlock()
	if callbackFires != 1 {
		t.Errorf("OnAckDelete fires = %d, want 1", callbackFires)
	}
	if ackedID != id || ackedNodeID != "node-A" {
		t.Errorf("callback got (%s, %s), want (%s, node-A)", ackedID, ackedNodeID, id)
	}
}

func TestAckDeleteIdempotent(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()
	now := time.Now()

	var fires int32
	f.SetOnAckDelete(func(chunk.ChunkID, string) { fires++ })

	f.Apply(&hraft.Log{Data: MarshalRequestDelete(id, now, "test", []string{"node-A"})})

	f.Apply(&hraft.Log{Data: MarshalAckDelete(id, "node-A")})
	f.Apply(&hraft.Log{Data: MarshalAckDelete(id, "node-A")}) // duplicate
	f.Apply(&hraft.Log{Data: MarshalAckDelete(id, "node-Z")}) // never expected

	if fires != 1 {
		t.Errorf("OnAckDelete fires = %d, want 1 (duplicate and unknown-node acks must not fire)", fires)
	}
}

func TestFinalizeDeleteRemovesEntry(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()
	now := time.Now()

	var finalized chunk.ChunkID
	f.SetOnFinalizeDelete(func(cid chunk.ChunkID) { finalized = cid })

	f.Apply(&hraft.Log{Data: MarshalRequestDelete(id, now, "test", []string{"node-A"})})
	f.Apply(&hraft.Log{Data: MarshalAckDelete(id, "node-A")})
	f.Apply(&hraft.Log{Data: MarshalFinalizeDelete(id)})

	if got := f.PendingDelete(id); got != nil {
		t.Errorf("PendingDelete(%s) after finalize: want nil, got %+v", id, got)
	}
	if finalized != id {
		t.Errorf("OnFinalizeDelete got %s, want %s", finalized, id)
	}
}

func TestFinalizeDeleteIdempotent(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()

	var fires int32
	f.SetOnFinalizeDelete(func(chunk.ChunkID) { fires++ })

	// Finalize a chunk that was never requested. Apply succeeds (raft
	// entry is consistent) but no callback should fire.
	if err := f.Apply(&hraft.Log{Data: MarshalFinalizeDelete(id)}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if fires != 0 {
		t.Errorf("OnFinalizeDelete fires = %d, want 0 for unknown chunk", fires)
	}
}

func TestIsExpectedToAck(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()
	now := time.Now()

	if f.IsExpectedToAck(id, "node-A") {
		t.Error("IsExpectedToAck on unknown chunk should be false")
	}

	f.Apply(&hraft.Log{Data: MarshalRequestDelete(id, now, "test", []string{"node-A", "node-B"})})

	if !f.IsExpectedToAck(id, "node-A") {
		t.Error("node-A should owe an ack")
	}
	if !f.IsExpectedToAck(id, "node-B") {
		t.Error("node-B should owe an ack")
	}
	if f.IsExpectedToAck(id, "node-Z") {
		t.Error("node-Z should not owe an ack — never expected")
	}

	f.Apply(&hraft.Log{Data: MarshalAckDelete(id, "node-A")})

	if f.IsExpectedToAck(id, "node-A") {
		t.Error("node-A should not owe an ack after acking")
	}
}

func TestPendingDeletesSurviveSnapshotRoundtrip(t *testing.T) {
	t.Parallel()

	src := New()
	now := time.Now()

	// Three pending deletes in different stages of progress.
	for i, cfg := range []struct {
		reason string
		expect []string
	}{
		{"retention-ttl", []string{"node-A", "node-B", "node-C"}},
		{"transition-source-expire", []string{"node-A"}},
		{"manual-delete-rpc", []string{"node-A", "node-B"}},
	} {
		id := chunk.NewChunkID()
		src.Apply(&hraft.Log{Data: MarshalRequestDelete(id, now.Add(time.Duration(i)*time.Second), cfg.reason, cfg.expect)})
		// Ack the first node on entries 1 and 2 to test that mid-flight progress survives.
		if i >= 1 {
			src.Apply(&hraft.Log{Data: MarshalAckDelete(id, "node-A")})
		}
	}

	beforeSnap := src.PendingDeletes()
	if len(beforeSnap) != 3 {
		t.Fatalf("expected 3 pending deletes, got %d", len(beforeSnap))
	}

	// Persist + Restore round trip.
	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	sink := &fakeSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("persist: %v", err)
	}

	dst := New()
	if err := dst.Restore(&fakeReadCloser{Reader: bytes.NewReader(sink.Bytes())}); err != nil {
		t.Fatalf("restore: %v", err)
	}

	afterSnap := dst.PendingDeletes()
	if len(afterSnap) != 3 {
		t.Fatalf("after restore: expected 3 pending deletes, got %d", len(afterSnap))
	}

	// Build a comparable map keyed by chunk ID.
	bm := map[chunk.ChunkID]PendingDelete{}
	for _, p := range beforeSnap {
		bm[p.ChunkID] = p
	}
	am := map[chunk.ChunkID]PendingDelete{}
	for _, p := range afterSnap {
		am[p.ChunkID] = p
	}
	if !reflect.DeepEqual(bm, am) {
		t.Errorf("pending deletes did not round-trip cleanly:\nbefore: %+v\nafter:  %+v", bm, am)
	}
}

// fakeSink + fakeReadCloser are minimal hraft.SnapshotSink / io.ReadCloser
// implementations so the snapshot serialization test doesn't need an
// hraft.Raft instance.

type fakeSink struct{ buf bytes.Buffer }

func (s *fakeSink) Write(p []byte) (int, error) { return s.buf.Write(p) }
func (s *fakeSink) Close() error                { return nil }
func (s *fakeSink) Cancel() error               { return nil }
func (s *fakeSink) ID() string                  { return "test" }
func (s *fakeSink) Bytes() []byte               { return s.buf.Bytes() }

type fakeReadCloser struct{ *bytes.Reader }

func (r *fakeReadCloser) Close() error { return nil }
