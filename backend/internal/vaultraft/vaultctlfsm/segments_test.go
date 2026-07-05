package vaultctlfsm

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"gastrolog/internal/glid"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

func TestPublishCompletedSegmentApplyAndList(t *testing.T) {
	t.Parallel()
	fsm := New()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	segID := glid.New()
	entry := CompletedSegmentEntry{
		SegmentID:     segID,
		RecordCount:   100,
		ByteSize:      4096,
		FirstIngestTS: now.Add(-time.Minute),
		LastIngestTS:  now,
		Checksum:      0xCAFEBABE,
		OriginNodeID:  "node-1",
		PublishedAt:   now,
	}
	applyCmd(t, fsm, MarshalPublishCompletedSegment(entry))

	list := fsm.ListCompletedSegments()
	if len(list) != 1 {
		t.Fatalf("len = %d, want 1", len(list))
	}
	if list[0].SegmentID != segID {
		t.Fatalf("SegmentID = %s", list[0].SegmentID)
	}
	got := fsm.GetCompletedSegment(segID)
	if got == nil || got.RecordCount != 100 {
		t.Fatalf("GetCompletedSegment = %+v", got)
	}
}

func TestPublishCompletedSegmentIdempotentReplay(t *testing.T) {
	t.Parallel()
	fsm := New()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	entry := CompletedSegmentEntry{
		SegmentID:     glid.New(),
		RecordCount:   1,
		ByteSize:      128,
		FirstIngestTS: now,
		LastIngestTS:  now,
		Checksum:      1,
		OriginNodeID:  "node-a",
		PublishedAt:   now,
	}
	wire := MarshalPublishCompletedSegment(entry)
	applyCmd(t, fsm, wire)
	applyCmd(t, fsm, wire)
	if len(fsm.ListCompletedSegments()) != 1 {
		t.Fatal("idempotent replay must not duplicate")
	}
}

func TestRepublishWithNewPublishedAtIsIdempotent(t *testing.T) {
	t.Parallel()
	fsm := New()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	segID := glid.New()
	first := CompletedSegmentEntry{
		SegmentID:     segID,
		RecordCount:   10,
		ByteSize:      512,
		FirstIngestTS: now,
		LastIngestTS:  now,
		Checksum:      1,
		OriginNodeID:  "node-a",
		PublishedAt:   now,
	}
	applyCmd(t, fsm, MarshalPublishCompletedSegment(first))

	// Distribution catch-up after restart re-publishes with a FRESH attempt
	// timestamp. Same content = same publish: must no-op, not conflict
	// (gastrolog-2usqfx: the old check retried this forever, hammering the
	// vault-ctl leader queue during restart-under-load).
	republish := first
	republish.PublishedAt = now.Add(45 * time.Minute)
	applyCmd(t, fsm, MarshalPublishCompletedSegment(republish))

	entries := fsm.ListCompletedSegments()
	if len(entries) != 1 {
		t.Fatalf("registry entries = %d, want 1", len(entries))
	}
	if !entries[0].PublishedAt.Equal(now) {
		t.Fatalf("PublishedAt = %v, want original %v (first write wins)", entries[0].PublishedAt, now)
	}
}

func TestPublishCompletedSegmentConflict(t *testing.T) {
	t.Parallel()
	fsm := New()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	segID := glid.New()
	first := CompletedSegmentEntry{
		SegmentID:     segID,
		RecordCount:   10,
		ByteSize:      512,
		FirstIngestTS: now,
		LastIngestTS:  now,
		Checksum:      1,
		PublishedAt:   now,
	}
	applyCmd(t, fsm, MarshalPublishCompletedSegment(first))

	conflict := first
	conflict.RecordCount = 11
	result := fsm.Apply(&hraft.Log{Data: MarshalPublishCompletedSegment(conflict)})
	err, ok := result.(error)
	if !ok || !errors.Is(err, ErrCompletedSegmentConflict) {
		t.Fatalf("Apply = %v (%T), want ErrCompletedSegmentConflict", result, result)
	}
}

func TestCompletedSegmentSnapshotRoundTrip(t *testing.T) {
	t.Parallel()
	src := New()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	applyCmd(t, src, MarshalPublishCompletedSegment(CompletedSegmentEntry{
		SegmentID:     glid.New(),
		RecordCount:   5,
		ByteSize:      256,
		FirstIngestTS: now.Add(-2 * time.Minute),
		LastIngestTS:  now.Add(-time.Minute),
		Checksum:      99,
		OriginNodeID:  "origin",
		PublishedAt:   now,
	}))

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
	if len(dst.ListCompletedSegments()) != 1 {
		t.Fatalf("restored segments = %d", len(dst.ListCompletedSegments()))
	}
}

type bytesSink struct {
	buf []byte
}

func (s *bytesSink) Write(p []byte) (int, error) {
	s.buf = append(s.buf, p...)
	return len(p), nil
}

func (s *bytesSink) Close() error  { return nil }
func (s *bytesSink) Cancel() error { return nil }

func TestAckSegmentHolderAppendsAndIsIdempotent(t *testing.T) {
	t.Parallel()
	fsm := New()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	segID := glid.New()
	applyCmd(t, fsm, MarshalPublishCompletedSegment(CompletedSegmentEntry{
		SegmentID:     segID,
		RecordCount:   3,
		ByteSize:      256,
		FirstIngestTS: now,
		LastIngestTS:  now,
		Checksum:      7,
		OriginNodeID:  "node-a",
		PublishedAt:   now,
	}))

	applyCmd(t, fsm, MarshalAckSegmentHolder(segID, "node-b"))
	if got := fsm.GetCompletedSegment(segID).Holders; len(got) != 1 || got[0] != "node-b" {
		t.Fatalf("holders after first ack = %v, want [node-b]", got)
	}

	// Idempotent: re-ack the same node does not duplicate.
	applyCmd(t, fsm, MarshalAckSegmentHolder(segID, "node-b"))
	if got := fsm.GetCompletedSegment(segID).Holders; len(got) != 1 {
		t.Fatalf("holders after duplicate ack = %v, want one entry", got)
	}

	// A second distinct holder appends.
	applyCmd(t, fsm, MarshalAckSegmentHolder(segID, "node-c"))
	if got := fsm.GetCompletedSegment(segID).Holders; len(got) != 2 ||
		got[0] != "node-b" || got[1] != "node-c" {
		t.Fatalf("holders after second ack = %v, want [node-b node-c]", got)
	}
}

func TestAckSegmentHolderUnknownSegmentIsNoOp(t *testing.T) {
	t.Parallel()
	fsm := New()
	// No publish first: ack for an unknown segment tolerated as a no-op.
	applyCmd(t, fsm, MarshalAckSegmentHolder(glid.New(), "node-b"))
	if len(fsm.ListCompletedSegments()) != 0 {
		t.Fatal("ack for unknown segment must not create a registry entry")
	}
}

func TestPublishReplayPreservesGrownHolders(t *testing.T) {
	t.Parallel()
	fsm := New()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	segID := glid.New()
	entry := CompletedSegmentEntry{
		SegmentID:     segID,
		RecordCount:   2,
		ByteSize:      100,
		FirstIngestTS: now,
		LastIngestTS:  now,
		Checksum:      9,
		OriginNodeID:  "node-a",
		PublishedAt:   now,
	}
	wire := MarshalPublishCompletedSegment(entry)
	applyCmd(t, fsm, wire)
	applyCmd(t, fsm, MarshalAckSegmentHolder(segID, "node-b"))

	// A duplicate publish (which carries no holders) must remain idempotent
	// and must not clobber the grown holder set.
	applyCmd(t, fsm, wire)
	if got := fsm.GetCompletedSegment(segID).Holders; len(got) != 1 || got[0] != "node-b" {
		t.Fatalf("holders after publish replay = %v, want [node-b]", got)
	}
}

func TestAckSegmentHolderSurvivesSnapshot(t *testing.T) {
	t.Parallel()
	src := New()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	segID := glid.New()
	applyCmd(t, src, MarshalPublishCompletedSegment(CompletedSegmentEntry{
		SegmentID:     segID,
		RecordCount:   5,
		ByteSize:      256,
		FirstIngestTS: now,
		LastIngestTS:  now,
		Checksum:      99,
		OriginNodeID:  "origin",
		PublishedAt:   now,
	}))
	applyCmd(t, src, MarshalAckSegmentHolder(segID, "node-b"))

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
	if got := dst.GetCompletedSegment(segID).Holders; len(got) != 1 || got[0] != "node-b" {
		t.Fatalf("restored holders = %v, want [node-b]", got)
	}
}

func TestPublishCompletedSegmentsBatchApply(t *testing.T) {
	t.Parallel()
	fsm := New()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	segA, segB := glid.New(), glid.New()
	var callbacks int
	fsm.AddOnPublishCompletedSegment(func(CompletedSegmentEntry) { callbacks++ })

	applyCmd(t, fsm, MarshalPublishCompletedSegments([]CompletedSegmentEntry{
		{SegmentID: segA, RecordCount: 1, ByteSize: 1, FirstIngestTS: now, LastIngestTS: now, Checksum: 1, PublishedAt: now},
		{SegmentID: segB, RecordCount: 2, ByteSize: 2, FirstIngestTS: now, LastIngestTS: now, Checksum: 2, PublishedAt: now},
	}))
	if callbacks != 2 {
		t.Fatalf("callbacks = %d, want 2", callbacks)
	}
	got := fsm.ListCompletedSegments()
	if len(got) != 2 {
		t.Fatalf("registry len = %d, want 2", len(got))
	}
}

func TestPublishCompletedSegmentFanOut(t *testing.T) {
	t.Parallel()
	fsm := New()
	now := time.Unix(0, 1_700_000_000_000).UTC()

	var a, b int
	fsm.AddOnPublishCompletedSegment(func(CompletedSegmentEntry) { a++ })
	removeB := fsm.AddOnPublishCompletedSegment(func(CompletedSegmentEntry) { b++ })

	applyCmd(t, fsm, MarshalPublishCompletedSegment(CompletedSegmentEntry{
		SegmentID: glid.New(), RecordCount: 1, ByteSize: 1,
		FirstIngestTS: now, LastIngestTS: now, Checksum: 1, PublishedAt: now,
	}))
	if a != 1 || b != 1 {
		t.Fatalf("after first publish a=%d b=%d, want 1,1", a, b)
	}

	// Removing one subscriber leaves the other intact.
	removeB()
	applyCmd(t, fsm, MarshalPublishCompletedSegment(CompletedSegmentEntry{
		SegmentID: glid.New(), RecordCount: 1, ByteSize: 1,
		FirstIngestTS: now.Add(time.Minute), LastIngestTS: now.Add(time.Minute), Checksum: 1, PublishedAt: now,
	}))
	if a != 2 || b != 1 {
		t.Fatalf("after second publish a=%d b=%d, want 2,1", a, b)
	}
}

func TestListCompletedSegmentsSortOrder(t *testing.T) {
	t.Parallel()
	fsm := New()
	base := time.Unix(0, 1_700_000_000_000).UTC()
	older := CompletedSegmentEntry{
		SegmentID:     glid.New(),
		RecordCount:   1,
		ByteSize:      1,
		FirstIngestTS: base,
		LastIngestTS:  base,
		Checksum:      1,
		PublishedAt:   base,
	}
	newer := older
	newer.SegmentID = glid.New()
	newer.FirstIngestTS = base.Add(time.Minute)
	newer.LastIngestTS = base.Add(time.Minute)

	applyCmd(t, fsm, MarshalPublishCompletedSegment(newer))
	applyCmd(t, fsm, MarshalPublishCompletedSegment(older))

	list := fsm.ListCompletedSegments()
	if len(list) != 2 {
		t.Fatalf("len = %d", len(list))
	}
	if list[0].SegmentID != older.SegmentID || list[1].SegmentID != newer.SegmentID {
		t.Fatalf("order = [%s, %s]", list[0].SegmentID, list[1].SegmentID)
	}
}

func TestPublishCompletedSegmentIgnoredAfterRelease(t *testing.T) {
	t.Parallel()
	fsm := New()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	segID := glid.New()
	entry := CompletedSegmentEntry{
		SegmentID:     segID,
		RecordCount:   10,
		ByteSize:      512,
		FirstIngestTS: now,
		LastIngestTS:  now,
		Checksum:      1,
		OriginNodeID:  "node-a",
		PublishedAt:   now,
	}
	applyCmd(t, fsm, MarshalPublishCompletedSegment(entry))
	applyCmd(t, fsm, MarshalReleaseSegments([]glid.GLID{segID}))

	// Stale distribution publish after ReleaseSegments must not resurrect registry entry.
	applyCmd(t, fsm, MarshalPublishCompletedSegment(entry))
	if got := fsm.GetCompletedSegment(segID); got != nil {
		t.Fatalf("GetCompletedSegment = %+v, want nil after release + stale publish", got)
	}
	if len(fsm.ListCompletedSegments()) != 0 {
		t.Fatalf("ListCompletedSegments = %d, want 0", len(fsm.ListCompletedSegments()))
	}
}

func TestReleasedSegmentSnapshotRoundTrip(t *testing.T) {
	t.Parallel()
	src := New()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	segID := glid.New()
	applyCmd(t, src, MarshalPublishCompletedSegment(CompletedSegmentEntry{
		SegmentID:     segID,
		RecordCount:   1,
		ByteSize:      1,
		FirstIngestTS: now,
		LastIngestTS:  now,
		Checksum:      1,
		PublishedAt:   now,
	}))
	applyCmd(t, src, MarshalReleaseSegments([]glid.GLID{segID}))

	snap := src.SnapshotProto()
	dst := New()
	dst.RestoreProto(snap)

	applyCmd(t, dst, MarshalPublishCompletedSegment(CompletedSegmentEntry{
		SegmentID:     segID,
		RecordCount:   1,
		ByteSize:      1,
		FirstIngestTS: now,
		LastIngestTS:  now,
		Checksum:      1,
		PublishedAt:   now,
	}))
	if dst.GetCompletedSegment(segID) != nil {
		t.Fatal("released segment tombstone must survive snapshot restore")
	}
}
