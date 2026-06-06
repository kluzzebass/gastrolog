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

	applyCmd(t, fsm, MarshalOpenChunkManifest(chunkID, openedAt))
	open := fsm.OpenChunk()
	if open == nil || open.ChunkID != chunkID || !open.OpenedAt.Equal(openedAt) {
		t.Fatalf("OpenChunk = %+v", open)
	}
	entry := fsm.Get(chunkID)
	if entry == nil || entry.State != chunk.ChunkStateActive {
		t.Fatalf("chunk entry = %+v", entry)
	}

	ref := OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 0,
		LastRecordNumber:  49,
		SliceBytes:        8192,
		RefAddedAt:        refAddedAt,
	}
	applyCmd(t, fsm, MarshalAddOpenChunkSegmentRef(chunkID, ref))
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
	pending := fsm.MaterializePending()
	if pending == nil || pending.ChunkID != chunkID || !pending.SealedAt.Equal(sealedAt) {
		t.Fatalf("MaterializePending = %+v", pending)
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

func TestOpenChunkIdempotentReplay(t *testing.T) {
	t.Parallel()
	fsm := New()
	chunkID := testChunkID(0x55)
	now := time.Unix(0, 1_700_000_000_000).UTC()
	openWire := MarshalOpenChunkManifest(chunkID, now)
	applyCmd(t, fsm, openWire)
	applyCmd(t, fsm, openWire)

	ref := OpenChunkSegmentRef{
		SegmentID:         glid.New(),
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
	pending := fsm.MaterializePending()
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

	applyCmd(t, fsm, MarshalReleaseSegments([]glid.GLID{segA}))
	if fsm.GetCompletedSegment(segA) != nil {
		t.Fatal("segA should be released from completed registry")
	}
	if fsm.GetCompletedSegment(segB) == nil {
		t.Fatal("segB should remain")
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
	pending := dst.MaterializePending()
	if pending == nil || pending.TotalRecords != 5 || pending.TotalBytes != 999 {
		t.Fatalf("restored pending = %+v", pending)
	}
	next, ok := dst.ResumeRecordNumber(segID)
	if !ok || next != 8 {
		t.Fatalf("restored resume = %d ok=%v", next, ok)
	}
}

func TestOpenChunkManifestWhileMaterializePending(t *testing.T) {
	t.Parallel()
	fsm := New()
	chunkID := testChunkID(0x5A)
	now := time.Unix(0, 1_700_000_000_000).UTC()
	applyCmd(t, fsm, MarshalOpenChunkManifest(chunkID, now))
	applyCmd(t, fsm, MarshalSealOpenChunkManifest(chunkID, now.Add(time.Minute)))

	result := fsm.Apply(&hraft.Log{Data: MarshalOpenChunkManifest(testChunkID(0x5B), now)})
	if err, ok := result.(error); !ok || !errors.Is(err, ErrMaterializePending) {
		t.Fatalf("open while pending = %v", result)
	}
}
