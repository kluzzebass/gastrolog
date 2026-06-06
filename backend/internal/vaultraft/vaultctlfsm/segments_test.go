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
