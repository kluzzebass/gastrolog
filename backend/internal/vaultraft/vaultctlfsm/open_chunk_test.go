package vaultctlfsm

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

func TestOpenChunkManifestLifecycle(t *testing.T) {
	t.Parallel()
	fsm := New()
	chunkID := testChunkID(0x53)
	openedAt := time.Unix(0, 1_700_000_000_000).UTC()
	segID := glid.New()
	refAddedAt := openedAt.Add(time.Second)

	applyCmd(t, fsm, MarshalPublishCompletedSegment(CompletedSegmentEntry{SegmentID: segID, RecordCount: 1000, ByteSize: 1, Checksum: 1}))
	applyCmd(t, fsm, MarshalOpenChunkManifest(chunkID, openedAt))
	open := fsm.OpenChunk()
	if open == nil || open.ChunkID != chunkID || !open.OpenedAt.Equal(openedAt) {
		t.Fatalf("OpenChunk = %+v", open)
	}
	if fsm.Get(chunkID) != nil {
		t.Fatal("chunk entry must not exist until the first segment ref is planned")
	}

	ref := OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 0,
		LastRecordNumber:  49,
		SliceBytes:        8192,
		RefAddedAt:        refAddedAt,
	}
	applyCmd(t, fsm, MarshalAddOpenChunkSegmentRef(chunkID, ref))
	entry := fsm.Get(chunkID)
	if entry == nil || entry.State != chunk.ChunkStateActive {
		t.Fatalf("chunk entry after first ref = %+v", entry)
	}
	open = fsm.OpenChunk()
	if open.TotalRecords != 50 || open.TotalBytes != 8192 {
		t.Fatalf("totals = records:%d bytes:%d", open.TotalRecords, open.TotalBytes)
	}
	if len(open.Refs) != 1 || open.Refs[0].RecordCount() != 50 {
		t.Fatalf("refs = %+v", open.Refs)
	}
	summary, ok := fsm.OpenChunkSummary()
	if !ok || summary.TotalRecords != 50 || summary.RefCount != 1 || summary.ChunkID != chunkID {
		t.Fatalf("OpenChunkSummary = %+v ok=%v", summary, ok)
	}
	next, ok := fsm.ResumeRecordNumber(segID)
	if !ok || next != 50 {
		t.Fatalf("resume = %d ok=%v", next, ok)
	}

	sealedAt := refAddedAt.Add(time.Minute)
	applyCmd(t, fsm, MarshalSealOpenChunkManifest(chunkID, sealedAt))
	if fsm.OpenChunk() != nil {
		t.Fatal("open manifest must be cleared after seal")
	}
	pending := fsm.SealedManifest()
	if pending == nil || pending.ChunkID != chunkID || !pending.SealedAt.Equal(sealedAt) {
		t.Fatalf("SealedManifest = %+v", pending)
	}
	if pending.TotalRecords != 50 {
		t.Fatalf("pending totals = %d", pending.TotalRecords)
	}
	entry = fsm.Get(chunkID)
	if entry == nil || entry.State != chunk.ChunkStateSealing {
		t.Fatalf("chunk state after seal = %+v", entry)
	}

	summary, hasOpen := fsm.OpenChunkSummary()
	if hasOpen {
		t.Fatal("OpenChunkSummary should be false after seal")
	}
	_ = summary
}

func TestAddOpenChunkSegmentRefPartialResume(t *testing.T) {
	t.Parallel()
	fsm := New()
	chunkID := testChunkID(0x54)
	now := time.Unix(0, 1_700_000_000_000).UTC()
	segID := glid.New()
	applyCmd(t, fsm, MarshalPublishCompletedSegment(CompletedSegmentEntry{SegmentID: segID, RecordCount: 1000, ByteSize: 1, Checksum: 1}))
	applyCmd(t, fsm, MarshalOpenChunkManifest(chunkID, now))

	first := OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 0,
		LastRecordNumber:  24,
		SliceBytes:        4096,
		RefAddedAt:        now,
	}
	second := OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 25,
		LastRecordNumber:  99,
		SliceBytes:        12_288,
		RefAddedAt:        now.Add(time.Second),
	}
	applyCmd(t, fsm, MarshalAddOpenChunkSegmentRef(chunkID, first))
	applyCmd(t, fsm, MarshalAddOpenChunkSegmentRef(chunkID, second))

	open := fsm.OpenChunk()
	if open.TotalRecords != 100 {
		t.Fatalf("total records = %d", open.TotalRecords)
	}
	next, ok := fsm.ResumeRecordNumber(segID)
	if !ok || next != 100 {
		t.Fatalf("resume = %d ok=%v", next, ok)
	}
}

func TestAddOpenChunkSegmentRefsBatchApply(t *testing.T) {
	t.Parallel()
	fsm := New()
	chunkID := testChunkID(0x56)
	now := time.Unix(0, 1_700_000_000_000).UTC()
	segA, segB := glid.New(), glid.New()
	applyCmd(t, fsm, MarshalPublishCompletedSegment(CompletedSegmentEntry{SegmentID: segA, RecordCount: 1000, ByteSize: 1, Checksum: 1}))
	applyCmd(t, fsm, MarshalPublishCompletedSegment(CompletedSegmentEntry{SegmentID: segB, RecordCount: 1000, ByteSize: 1, Checksum: 1}))
	applyCmd(t, fsm, MarshalOpenChunkManifest(chunkID, now))
	applyCmd(t, fsm, MarshalAddOpenChunkSegmentRefs(chunkID, []OpenChunkSegmentRef{
		{
			SegmentID:         segA,
			FirstRecordNumber: 0,
			LastRecordNumber:  4,
			SliceBytes:        512,
			RefAddedAt:        now,
		},
		{
			SegmentID:         segB,
			FirstRecordNumber: 0,
			LastRecordNumber:  9,
			SliceBytes:        1024,
			RefAddedAt:        now.Add(time.Millisecond),
		},
	}))

	open := fsm.OpenChunk()
	if open == nil || len(open.Refs) != 2 {
		t.Fatalf("refs = %d, want 2", len(open.Refs))
	}
	if open.TotalRecords != 15 {
		t.Fatalf("total records = %d, want 15", open.TotalRecords)
	}
	if open.TotalBytes != 1536 {
		t.Fatalf("total bytes = %d, want 1536", open.TotalBytes)
	}
	nextA, ok := fsm.ResumeRecordNumber(segA)
	if !ok || nextA != 5 {
		t.Fatalf("segA resume = %d ok=%v", nextA, ok)
	}
	nextB, ok := fsm.ResumeRecordNumber(segB)
	if !ok || nextB != 10 {
		t.Fatalf("segB resume = %d ok=%v", nextB, ok)
	}
}

func TestOpenChunkIdempotentReplay(t *testing.T) {
	t.Parallel()
	fsm := New()
	chunkID := testChunkID(0x55)
	now := time.Unix(0, 1_700_000_000_000).UTC()
	openWire := MarshalOpenChunkManifest(chunkID, now)
	applyCmd(t, fsm, openWire)
	applyCmd(t, fsm, openWire)

	refSeg := glid.New()
	applyCmd(t, fsm, MarshalPublishCompletedSegment(CompletedSegmentEntry{
		SegmentID: refSeg, RecordCount: 1000, ByteSize: 1, Checksum: 1,
	}))
	ref := OpenChunkSegmentRef{
		SegmentID:         refSeg,
		FirstRecordNumber: 10,
		LastRecordNumber:  19,
		SliceBytes:        512,
		RefAddedAt:        now,
	}
	refWire := MarshalAddOpenChunkSegmentRef(chunkID, ref)
	applyCmd(t, fsm, refWire)
	applyCmd(t, fsm, refWire)

	sealedAt := now.Add(time.Minute)
	sealWire := MarshalSealOpenChunkManifest(chunkID, sealedAt)
	applyCmd(t, fsm, sealWire)
	applyCmd(t, fsm, sealWire)

	open := fsm.OpenChunk()
	pending := fsm.SealedManifest()
	if open != nil || pending == nil || pending.TotalRecords != 10 {
		t.Fatalf("open=%+v pending=%+v", open, pending)
	}
}

func TestOpenChunkErrors(t *testing.T) {
	t.Parallel()
	fsm := New()
	chunkA := testChunkID(0x56)
	chunkB := testChunkID(0x57)
	now := time.Unix(0, 1_700_000_000_000).UTC()
	applyCmd(t, fsm, MarshalOpenChunkManifest(chunkA, now))

	result := fsm.Apply(&hraft.Log{Data: MarshalOpenChunkManifest(chunkB, now)})
	if err, ok := result.(error); !ok || !errors.Is(err, ErrOpenChunkExists) {
		t.Fatalf("second open = %v", result)
	}

	result = fsm.Apply(&hraft.Log{Data: MarshalAddOpenChunkSegmentRef(chunkB, OpenChunkSegmentRef{
		SegmentID:         glid.New(),
		FirstRecordNumber: 0,
		LastRecordNumber:  0,
	})})
	if err, ok := result.(error); !ok || !errors.Is(err, ErrOpenChunkChunkIDMismatch) {
		t.Fatalf("wrong chunk ref = %v", result)
	}

	result = fsm.Apply(&hraft.Log{Data: MarshalAddOpenChunkSegmentRef(chunkA, OpenChunkSegmentRef{
		SegmentID:         glid.New(),
		FirstRecordNumber: 5,
		LastRecordNumber:  2,
	})})
	if err, ok := result.(error); !ok || !errors.Is(err, ErrInvalidSegmentRef) {
		t.Fatalf("invalid ref = %v", result)
	}

	result = fsm.Apply(&hraft.Log{Data: MarshalSealOpenChunkManifest(chunkB, now)})
	if err, ok := result.(error); !ok || !errors.Is(err, ErrOpenChunkChunkIDMismatch) {
		t.Fatalf("seal wrong chunk = %v", result)
	}
}

func TestReleaseSegments(t *testing.T) {
	t.Parallel()
	fsm := New()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	segA := glid.New()
	segB := glid.New()
	applyCmd(t, fsm, MarshalPublishCompletedSegment(CompletedSegmentEntry{
		SegmentID:     segA,
		RecordCount:   1,
		ByteSize:      1,
		FirstIngestTS: now,
		LastIngestTS:  now,
		Checksum:      1,
		PublishedAt:   now,
	}))
	applyCmd(t, fsm, MarshalPublishCompletedSegment(CompletedSegmentEntry{
		SegmentID:     segB,
		RecordCount:   1,
		ByteSize:      1,
		FirstIngestTS: now,
		LastIngestTS:  now,
		Checksum:      1,
		PublishedAt:   now,
	}))

	chunkID := testChunkID(0x58)
	applyCmd(t, fsm, MarshalOpenChunkManifest(chunkID, now))
	applyCmd(t, fsm, MarshalAddOpenChunkSegmentRef(chunkID, OpenChunkSegmentRef{
		SegmentID:         segA,
		FirstRecordNumber: 0,
		LastRecordNumber:  0,
		SliceBytes:        1,
		RefAddedAt:        now,
	}))

	// segA is referenced by the open chunk manifest: the apply-time guard must
	// refuse the release even though a (buggy or racing) leader proposed it.
	// Releasing a referenced segment purges the head/ copy homes still need to
	// build the chunk (gastrolog-67c9b0).
	applyCmd(t, fsm, MarshalReleaseSegments([]glid.GLID{segA}))
	if fsm.GetCompletedSegment(segA) == nil {
		t.Fatal("segA is referenced by the open chunk; release must be refused")
	}

	// Unreferenced segB releases normally.
	applyCmd(t, fsm, MarshalReleaseSegments([]glid.GLID{segB}))
	if fsm.GetCompletedSegment(segB) != nil {
		t.Fatal("segB should be released from completed registry")
	}
	if _, ok := fsm.ResumeRecordNumber(segB); ok {
		t.Fatal("segB resume should be cleared")
	}

	// Run the manifest through its full lifecycle (seal manifest → seal chunk
	// pops it from the queue). With no manifest referencing segA, release
	// succeeds.
	applyCmd(t, fsm, MarshalSealOpenChunkManifest(chunkID, now))
	applyCmd(t, fsm, MarshalSealChunk(chunkID, now, 1, 1, now, now, now, false, now))
	applyCmd(t, fsm, MarshalReleaseSegments([]glid.GLID{segA}))
	if fsm.GetCompletedSegment(segA) != nil {
		t.Fatal("segA should be released once no manifest references it")
	}
	if _, ok := fsm.ResumeRecordNumber(segA); ok {
		t.Fatal("segA resume should be cleared")
	}
}

func TestOpenChunkSnapshotRoundTrip(t *testing.T) {
	t.Parallel()
	src := New()
	chunkID := testChunkID(0x59)
	openedAt := time.Unix(0, 1_700_000_000_000).UTC()
	sealedAt := openedAt.Add(2 * time.Minute)
	segID := glid.New()
	applyCmd(t, src, MarshalPublishCompletedSegment(CompletedSegmentEntry{SegmentID: segID, RecordCount: 1000, ByteSize: 1, Checksum: 1}))
	applyCmd(t, src, MarshalOpenChunkManifest(chunkID, openedAt))
	applyCmd(t, src, MarshalAddOpenChunkSegmentRef(chunkID, OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 3,
		LastRecordNumber:  7,
		SliceBytes:        999,
		RefAddedAt:        openedAt.Add(time.Second),
	}))
	applyCmd(t, src, MarshalSealOpenChunkManifest(chunkID, sealedAt))

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&bufSink{Writer: &buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	dst := New()
	if err := dst.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if !proto.Equal(src.SnapshotProto(), dst.SnapshotProto()) {
		t.Fatal("snapshot state differs after round trip")
	}
	pending := dst.SealedManifest()
	if pending == nil || pending.TotalRecords != 5 || pending.TotalBytes != 999 {
		t.Fatalf("restored pending = %+v", pending)
	}
	next, ok := dst.ResumeRecordNumber(segID)
	if !ok || next != 8 {
		t.Fatalf("restored resume = %d ok=%v", next, ok)
	}
}

func TestOpenChunkManifestWhileSealedManifestPending(t *testing.T) {
	t.Parallel()
	fsm := New()
	chunkA := testChunkID(0x5A)
	chunkB := testChunkID(0x5B)
	now := time.Unix(0, 1_700_000_000_000).UTC()
	applyCmd(t, fsm, MarshalOpenChunkManifest(chunkA, now))
	applyCmd(t, fsm, MarshalSealOpenChunkManifest(chunkA, now.Add(time.Minute)))

	result := fsm.Apply(&hraft.Log{Data: MarshalOpenChunkManifest(chunkB, now)})
	if err, ok := result.(error); ok && err != nil {
		t.Fatalf("open while pending sealed build = %v", result)
	}
	open := fsm.OpenChunk()
	if open == nil || open.ChunkID != chunkB {
		t.Fatalf("open = %+v, want chunk %s", open, chunkB)
	}
	if pending := fsm.SealedManifest(); pending == nil || pending.ChunkID != chunkA {
		t.Fatalf("sealed = %+v, want chunk %s pending", pending, chunkA)
	}
}

func TestSealOpenChunkWhileOtherSealedPending(t *testing.T) {
	t.Parallel()
	fsm := New()
	chunkA := testChunkID(0x5A)
	chunkB := testChunkID(0x5B)
	now := time.Unix(0, 1_700_000_000_000).UTC()
	applyCmd(t, fsm, MarshalOpenChunkManifest(chunkA, now))
	applyCmd(t, fsm, MarshalSealOpenChunkManifest(chunkA, now.Add(time.Minute)))
	applyCmd(t, fsm, MarshalOpenChunkManifest(chunkB, now))

	result := fsm.Apply(&hraft.Log{Data: MarshalSealOpenChunkManifest(chunkB, now.Add(2*time.Minute))})
	if err, ok := result.(error); ok && err != nil {
		t.Fatalf("seal open B while A pending = %v", result)
	}
	if open := fsm.OpenChunk(); open != nil {
		t.Fatalf("open = %+v, want nil after B sealed", open)
	}
	if fsm.SealedManifestCount() != 2 {
		t.Fatalf("sealed queue depth = %d, want 2", fsm.SealedManifestCount())
	}
	if head := fsm.SealedManifest(); head == nil || head.ChunkID != chunkA {
		t.Fatalf("queue head = %+v, want chunk %s", head, chunkA)
	}
}

func TestSealChunkRepairsMissingManifestEntry(t *testing.T) {
	t.Parallel()
	fsm := New()
	chunkID := testChunkID(0x99)
	segID := glid.New()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	applyCmd(t, fsm, MarshalPublishCompletedSegment(CompletedSegmentEntry{SegmentID: segID, RecordCount: 1000, ByteSize: 1, Checksum: 1}))
	applyCmd(t, fsm, MarshalOpenChunkManifest(chunkID, now))
	applyCmd(t, fsm, MarshalAddOpenChunkSegmentRef(chunkID, OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 9,
		SliceBytes: 100, RefAddedAt: now,
	}))
	applyCmd(t, fsm, MarshalSealOpenChunkManifest(chunkID, now.Add(time.Minute)))

	fsm.mu.Lock()
	delete(fsm.chunks, chunkID)
	fsm.mu.Unlock()
	if fsm.Get(chunkID) != nil {
		t.Fatal("expected missing manifest entry before repair")
	}

	applyCmd(t, fsm, MarshalSealChunk(chunkID, now.Add(2*time.Minute), 10, 500, now, now, now, true, now.Add(2*time.Minute)))
	entry := fsm.Get(chunkID)
	if entry == nil || entry.State != chunk.ChunkStateSealed {
		t.Fatalf("entry after repair = %+v", entry)
	}
	if fsm.SealedManifest() != nil {
		t.Fatal("sealed manifest must clear after SealChunk")
	}
}

func TestSealChunkClearsStaleTombstoneForPendingManifest(t *testing.T) {
	t.Parallel()
	fsm := New()
	chunkID := testChunkID(0xCC)
	segID := glid.New()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	applyCmd(t, fsm, MarshalPublishCompletedSegment(CompletedSegmentEntry{SegmentID: segID, RecordCount: 1000, ByteSize: 1, Checksum: 1}))
	applyCmd(t, fsm, MarshalOpenChunkManifest(chunkID, now))
	applyCmd(t, fsm, MarshalAddOpenChunkSegmentRef(chunkID, OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 9,
		SliceBytes: 100, RefAddedAt: now,
	}))
	applyCmd(t, fsm, MarshalSealOpenChunkManifest(chunkID, now.Add(time.Minute)))

	fsm.mu.Lock()
	fsm.tombstones[chunkID] = time.Now()
	delete(fsm.chunks, chunkID)
	fsm.mu.Unlock()
	if !fsm.IsTombstoned(chunkID) {
		t.Fatal("expected tombstone before repair")
	}

	applyCmd(t, fsm, MarshalSealChunk(chunkID, now.Add(2*time.Minute), 10, 500, now, now, now, true, now.Add(2*time.Minute)))
	if fsm.IsTombstoned(chunkID) {
		t.Fatal("stale tombstone must clear when pending sealed manifest seals")
	}
	entry := fsm.Get(chunkID)
	if entry == nil || entry.State != chunk.ChunkStateSealed {
		t.Fatalf("entry after tombstone repair = %+v", entry)
	}
}

func TestDiscardEmptyOpenChunkManifest(t *testing.T) {
	t.Parallel()
	fsm := New()
	chunkID := testChunkID(0x60)
	now := time.Unix(0, 1_700_000_000_000).UTC()
	applyCmd(t, fsm, MarshalOpenChunkManifest(chunkID, now))
	applyCmd(t, fsm, MarshalDiscardOpenChunkManifest(chunkID))
	if fsm.OpenChunk() != nil {
		t.Fatal("open manifest must clear on discard")
	}
	if fsm.Get(chunkID) != nil {
		t.Fatal("phantom active entry must be removed")
	}
}

func TestDiscardEmptySealedManifest(t *testing.T) {
	t.Parallel()
	fsm := New()
	chunkID := testChunkID(0x61)
	now := time.Unix(0, 1_700_000_000_000).UTC()
	applyCmd(t, fsm, MarshalOpenChunkManifest(chunkID, now))
	applyCmd(t, fsm, MarshalSealOpenChunkManifest(chunkID, now.Add(time.Minute)))
	if fsm.SealedManifest() == nil {
		t.Fatal("expected sealed pending manifest")
	}
	applyCmd(t, fsm, MarshalDiscardOpenChunkManifest(chunkID))
	if fsm.SealedManifest() != nil {
		t.Fatal("sealed manifest must clear on discard")
	}
	if fsm.Get(chunkID) != nil {
		t.Fatal("phantom sealing entry must be removed")
	}
}

func TestDiscardRejectsNonemptyManifest(t *testing.T) {
	t.Parallel()
	fsm := New()
	chunkID := testChunkID(0x62)
	now := time.Unix(0, 1_700_000_000_000).UTC()
	segID := glid.New()
	applyCmd(t, fsm, MarshalPublishCompletedSegment(CompletedSegmentEntry{SegmentID: segID, RecordCount: 1000, ByteSize: 1, Checksum: 1}))
	applyCmd(t, fsm, MarshalOpenChunkManifest(chunkID, now))
	applyCmd(t, fsm, MarshalAddOpenChunkSegmentRef(chunkID, OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 0,
		SliceBytes: 100, RefAddedAt: now,
	}))
	if err := fsm.Apply(&hraft.Log{Data: MarshalDiscardOpenChunkManifest(chunkID)}); err == nil {
		t.Fatal("expected error discarding non-empty manifest")
	}
}

func TestListIncludingPipelineManifestIngestBounds(t *testing.T) {
	t.Parallel()
	fsm := New()
	chunkID := testChunkID(0x63)
	openedAt := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	ingestStart := openedAt.Add(-time.Minute)
	ingestEnd := openedAt.Add(time.Minute)
	segID := glid.New()

	applyCmd(t, fsm, MarshalPublishCompletedSegment(CompletedSegmentEntry{SegmentID: segID, RecordCount: 1000, ByteSize: 1, Checksum: 1}))
	applyCmd(t, fsm, MarshalOpenChunkManifest(chunkID, openedAt))
	applyCmd(t, fsm, MarshalAddOpenChunkSegmentRef(chunkID, OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 0,
		LastRecordNumber:  9,
		SliceBytes:        512,
		RefAddedAt:        openedAt,
		Bounds: ManifestTimeBounds{
			WriteStart:  openedAt,
			WriteEnd:    openedAt.Add(time.Second),
			IngestStart: ingestStart,
			IngestEnd:   ingestEnd,
		},
	}))
	applyCmd(t, fsm, MarshalSealOpenChunkManifest(chunkID, openedAt.Add(2*time.Minute)))

	for _, e := range fsm.ListIncludingPipelineManifest() {
		if e.ID != chunkID {
			continue
		}
		if e.State != chunk.ChunkStateSealing {
			t.Fatalf("state = %v, want Sealing", e.State)
		}
		if !e.IngestStart.Equal(ingestStart) || !e.IngestEnd.Equal(ingestEnd) {
			t.Fatalf("ingest bounds = %v..%v, want %v..%v", e.IngestStart, e.IngestEnd, ingestStart, ingestEnd)
		}
		return
	}
	t.Fatal("sealing chunk missing from ListIncludingPipelineManifest")
}
