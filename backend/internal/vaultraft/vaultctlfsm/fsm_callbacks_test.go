package vaultctlfsm

import (
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"

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
	fsm.Apply(&hraft.Log{Data: MarshalSealChunk(id, now, 100, 12345, now, now, now, false, now)})

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

func TestOnSealedManifestCallbackFires(t *testing.T) {
	t.Parallel()

	fsm := New()
	chunkID := chunk.NewChunkID()
	now := time.Now()

	var mu sync.Mutex
	var captured *OpenChunkManifest
	fsm.SetOnSealedManifest(func(m *OpenChunkManifest) {
		mu.Lock()
		captured = m
		mu.Unlock()
	})

	fsm.Apply(&hraft.Log{Data: MarshalOpenChunkManifest(chunkID, now)})
	fsm.Apply(&hraft.Log{Data: MarshalSealOpenChunkManifest(chunkID, now.Add(time.Minute))})

	mu.Lock()
	defer mu.Unlock()
	if captured == nil {
		t.Fatal("OnSealedManifest callback was not called")
	}
	if captured.ChunkID != chunkID {
		t.Errorf("ChunkID = %s, want %s", captured.ChunkID, chunkID)
	}
}

func TestOnSealedManifestCallbackNotOnIdempotentReplay(t *testing.T) {
	t.Parallel()

	fsm := New()
	chunkID := chunk.NewChunkID()
	now := time.Now()
	sealedAt := now.Add(time.Minute)

	var calls int
	fsm.SetOnSealedManifest(func(*OpenChunkManifest) {
		calls++
	})

	fsm.Apply(&hraft.Log{Data: MarshalOpenChunkManifest(chunkID, now)})
	fsm.Apply(&hraft.Log{Data: MarshalSealOpenChunkManifest(chunkID, sealedAt)})
	if calls != 1 {
		t.Fatalf("callback calls = %d, want 1", calls)
	}

	if err := fsm.Apply(&hraft.Log{Data: MarshalSealOpenChunkManifest(chunkID, sealedAt)}); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if calls != 1 {
		t.Fatalf("callback calls after replay = %d, want 1", calls)
	}
}

func TestOnSealedManifestClearedCallbackFires(t *testing.T) {
	t.Parallel()

	fsm := New()
	chunkID := chunk.NewChunkID()
	now := time.Now()

	var calls int
	var captured chunk.ChunkID
	fsm.SetOnSealedManifestCleared(func(id chunk.ChunkID) {
		calls++
		captured = id
	})

	// Open → seal manifest: pending sealed manifest exists, no clear yet.
	fsm.Apply(&hraft.Log{Data: MarshalOpenChunkManifest(chunkID, now)})
	fsm.Apply(&hraft.Log{Data: MarshalSealOpenChunkManifest(chunkID, now.Add(time.Minute))})
	if calls != 0 {
		t.Fatalf("callback fired before CmdSealChunk (calls=%d)", calls)
	}

	// CmdSealChunk clears the pending manifest → callback fires once.
	sealWire := MarshalSealChunk(chunkID, now, 10, 1024, now, now, now, true, now)
	if err := fsm.Apply(&hraft.Log{Data: sealWire}); err != nil {
		t.Fatalf("SealChunk: %v", err)
	}
	if calls != 1 || captured != chunkID {
		t.Fatalf("calls=%d captured=%s, want 1 call with %s", calls, captured, chunkID)
	}

	// Replay: the manifest is already cleared, so no second fire.
	if err := fsm.Apply(&hraft.Log{Data: sealWire}); err != nil {
		t.Fatalf("SealChunk replay: %v", err)
	}
	if calls != 1 {
		t.Fatalf("callback calls after replay = %d, want 1", calls)
	}
}

func TestOnSealedManifestClearedNotForUnrelatedSeal(t *testing.T) {
	t.Parallel()

	fsm := New()
	manifestChunk := chunk.NewChunkID()
	otherChunk := chunk.NewChunkID()
	now := time.Now()

	var calls int
	fsm.SetOnSealedManifestCleared(func(chunk.ChunkID) { calls++ })

	fsm.Apply(&hraft.Log{Data: MarshalOpenChunkManifest(manifestChunk, now)})
	fsm.Apply(&hraft.Log{Data: MarshalSealOpenChunkManifest(manifestChunk, now.Add(time.Minute))})

	// Sealing a different chunk leaves the pending manifest in place.
	fsm.Apply(&hraft.Log{Data: MarshalCreateChunk(otherChunk, now, now, now)})
	if err := fsm.Apply(&hraft.Log{Data: MarshalSealChunk(otherChunk, now, 1, 1, now, now, now, false, now)}); err != nil {
		t.Fatalf("SealChunk other: %v", err)
	}
	if calls != 0 {
		t.Fatalf("callback fired for unrelated seal (calls=%d)", calls)
	}
	if fsm.SealedManifest() == nil {
		t.Fatal("pending sealed manifest must survive unrelated seal")
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
	res := fsm.Apply(&hraft.Log{Data: MarshalSealChunk(id, now, 1, 1, now, now, now, false, now)})
	if res == nil {
		t.Fatal("expected error sealing unknown chunk, got nil")
	}
	if called {
		t.Error("OnSeal callback should not fire when applySeal returns an error")
	}
}

func TestOnPublishCompletedSegmentCallbackFires(t *testing.T) {
	t.Parallel()

	fsm := New()
	segID := glid.New()
	now := time.Now()

	var mu sync.Mutex
	var captured *CompletedSegmentEntry
	fsm.AddOnPublishCompletedSegment(func(e CompletedSegmentEntry) {
		mu.Lock()
		captured = &e
		mu.Unlock()
	})

	entry := CompletedSegmentEntry{
		SegmentID:     segID,
		RecordCount:   2,
		ByteSize:      100,
		FirstIngestTS: now,
		LastIngestTS:  now,
		Checksum:      42,
		OriginNodeID:  "node-a",
		PublishedAt:   now,
	}
	fsm.Apply(&hraft.Log{Data: MarshalPublishCompletedSegment(entry)})

	mu.Lock()
	defer mu.Unlock()
	if captured == nil {
		t.Fatal("OnPublishCompletedSegment callback was not called")
	}
	if captured.SegmentID != segID {
		t.Errorf("SegmentID = %s, want %s", captured.SegmentID, segID)
	}
}

func TestOnPublishCompletedSegmentCallbackNotOnIdempotentReplay(t *testing.T) {
	t.Parallel()

	fsm := New()
	segID := glid.New()
	now := time.Now()
	entry := CompletedSegmentEntry{
		SegmentID:     segID,
		RecordCount:   1,
		ByteSize:      50,
		FirstIngestTS: now,
		LastIngestTS:  now,
		Checksum:      7,
		OriginNodeID:  "node-a",
		PublishedAt:   now,
	}

	var calls int
	fsm.AddOnPublishCompletedSegment(func(CompletedSegmentEntry) {
		calls++
	})

	fsm.Apply(&hraft.Log{Data: MarshalPublishCompletedSegment(entry)})
	if calls != 1 {
		t.Fatalf("callback calls = %d, want 1", calls)
	}
	if err := fsm.Apply(&hraft.Log{Data: MarshalPublishCompletedSegment(entry)}); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if calls != 1 {
		t.Fatalf("callback calls after replay = %d, want 1", calls)
	}
}

func TestOnOpenChunkManifestCallbackFires(t *testing.T) {
	t.Parallel()

	fsm := New()
	chunkID := chunk.NewChunkID()
	now := time.Now()

	var mu sync.Mutex
	var captured *OpenChunkManifest
	fsm.SetOnOpenChunkManifest(func(m *OpenChunkManifest) {
		mu.Lock()
		captured = m
		mu.Unlock()
	})

	fsm.Apply(&hraft.Log{Data: MarshalOpenChunkManifest(chunkID, now)})

	mu.Lock()
	defer mu.Unlock()
	if captured == nil {
		t.Fatal("OnOpenChunkManifest callback was not called")
	}
	if captured.ChunkID != chunkID {
		t.Errorf("ChunkID = %s, want %s", captured.ChunkID, chunkID)
	}
}

func TestOnOpenChunkRefAddedCallbackFires(t *testing.T) {
	t.Parallel()

	fsm := New()
	chunkID := chunk.NewChunkID()
	segID := glid.New()
	now := time.Now()

	var calls int
	fsm.SetOnOpenChunkRefAdded(func(*OpenChunkManifest) {
		calls++
	})

	_ = fsm.Apply(&hraft.Log{Data: MarshalPublishCompletedSegment(CompletedSegmentEntry{SegmentID: segID, RecordCount: 1000, ByteSize: 1, Checksum: 1})})
	fsm.Apply(&hraft.Log{Data: MarshalOpenChunkManifest(chunkID, now)})
	fsm.Apply(&hraft.Log{Data: MarshalAddOpenChunkSegmentRef(chunkID, OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 0,
		LastRecordNumber:  1,
		SliceBytes:        100,
		RefAddedAt:        now,
	})})
	if calls != 1 {
		t.Fatalf("callback calls = %d, want 1", calls)
	}
	if err := fsm.Apply(&hraft.Log{Data: MarshalAddOpenChunkSegmentRef(chunkID, OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 0,
		LastRecordNumber:  1,
		SliceBytes:        100,
		RefAddedAt:        now,
	})}); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if calls != 1 {
		t.Fatalf("callback calls after replay = %d, want 1", calls)
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
	if err := fsm.Apply(&hraft.Log{Data: MarshalSealChunk(id, now, 1, 1, now, now, now, false, now)}); err != nil {
		t.Errorf("seal apply unexpected error: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: MarshalRetentionPending(id)}); err != nil {
		t.Errorf("retention-pending apply unexpected error: %v", err)
	}
}

func TestOnAckSegmentHolderCallbackFires(t *testing.T) {
	t.Parallel()

	fsm := New()
	segID := glid.New()
	now := time.Now()
	fsm.Apply(&hraft.Log{Data: MarshalPublishCompletedSegment(CompletedSegmentEntry{
		SegmentID: segID, RecordCount: 1, ByteSize: 1,
		FirstIngestTS: now, LastIngestTS: now, Checksum: 1, PublishedAt: now,
	})})

	var mu sync.Mutex
	var acked glid.GLID
	fsm.AddOnAckSegmentHolder(func(id glid.GLID) {
		mu.Lock()
		acked = id
		mu.Unlock()
	})

	fsm.Apply(&hraft.Log{Data: MarshalAckSegmentHolder(segID, "node-b")})

	mu.Lock()
	defer mu.Unlock()
	if acked != segID {
		t.Fatalf("acked = %s, want %s", acked, segID)
	}
}

func TestOnReleaseSegmentsCallbackFires(t *testing.T) {
	t.Parallel()

	fsm := New()
	segA := glid.New()
	segB := glid.New()
	now := time.Now()
	fsm.Apply(&hraft.Log{Data: MarshalPublishCompletedSegment(CompletedSegmentEntry{
		SegmentID: segA, RecordCount: 1, ByteSize: 1,
		FirstIngestTS: now, LastIngestTS: now, Checksum: 1, PublishedAt: now,
	})})
	fsm.Apply(&hraft.Log{Data: MarshalPublishCompletedSegment(CompletedSegmentEntry{
		SegmentID: segB, RecordCount: 1, ByteSize: 1,
		FirstIngestTS: now, LastIngestTS: now, Checksum: 1, PublishedAt: now,
	})})

	var mu sync.Mutex
	var released []glid.GLID
	fsm.AddOnReleaseSegments(func(ids []glid.GLID) {
		mu.Lock()
		released = append([]glid.GLID(nil), ids...)
		mu.Unlock()
	})

	fsm.Apply(&hraft.Log{Data: MarshalReleaseSegments([]glid.GLID{segA})})

	mu.Lock()
	defer mu.Unlock()
	if len(released) != 1 || released[0] != segA {
		t.Fatalf("released = %v, want [%s]", released, segA)
	}
}

func TestAddOnOpenChunkManifestCoexistsWithSetOn(t *testing.T) {
	t.Parallel()

	fsm := New()
	chunkID := chunk.NewChunkID()
	openedAt := time.Now().UTC()

	var mu sync.Mutex
	var primary, secondary int
	fsm.SetOnOpenChunkManifest(func(*OpenChunkManifest) {
		mu.Lock()
		primary++
		mu.Unlock()
	})
	remove := fsm.AddOnOpenChunkManifest(func(*OpenChunkManifest) {
		mu.Lock()
		secondary++
		mu.Unlock()
	})
	defer remove()

	fsm.Apply(&hraft.Log{Data: MarshalOpenChunkManifest(chunkID, openedAt)})

	mu.Lock()
	defer mu.Unlock()
	if primary != 1 || secondary != 1 {
		t.Fatalf("primary=%d secondary=%d, want 1/1", primary, secondary)
	}
}

func TestAddOnSealedManifestCoexistsWithSetOn(t *testing.T) {
	t.Parallel()

	fsm := New()
	chunkID := chunk.NewChunkID()
	openedAt := time.Now().UTC()
	sealedAt := openedAt.Add(time.Minute)

	fsm.Apply(&hraft.Log{Data: MarshalOpenChunkManifest(chunkID, openedAt)})

	var mu sync.Mutex
	var primary, secondary int
	fsm.SetOnSealedManifest(func(*OpenChunkManifest) {
		mu.Lock()
		primary++
		mu.Unlock()
	})
	remove := fsm.AddOnSealedManifest(func(*OpenChunkManifest) {
		mu.Lock()
		secondary++
		mu.Unlock()
	})
	defer remove()

	fsm.Apply(&hraft.Log{Data: MarshalSealOpenChunkManifest(chunkID, sealedAt)})

	mu.Lock()
	defer mu.Unlock()
	if primary != 1 || secondary != 1 {
		t.Fatalf("primary=%d secondary=%d, want 1/1", primary, secondary)
	}
}

func TestAddOnSealCoexistsWithSetOn(t *testing.T) {
	t.Parallel()

	fsm := New()
	id := chunk.NewChunkID()
	now := time.Now()

	var mu sync.Mutex
	var primary, secondary int
	fsm.SetOnSeal(func(ManifestEntry) {
		mu.Lock()
		primary++
		mu.Unlock()
	})
	remove := fsm.AddOnSeal(func(ManifestEntry) {
		mu.Lock()
		secondary++
		mu.Unlock()
	})
	defer remove()

	fsm.Apply(&hraft.Log{Data: MarshalCreateChunk(id, now, now, now)})
	fsm.Apply(&hraft.Log{Data: MarshalSealChunk(id, now, 1, 1, now, now, now, false, now)})

	mu.Lock()
	defer mu.Unlock()
	if primary != 1 || secondary != 1 {
		t.Fatalf("primary=%d secondary=%d, want 1/1", primary, secondary)
	}
}
