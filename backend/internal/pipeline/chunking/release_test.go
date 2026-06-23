package chunking

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

func TestReleasableSegmentIDsSkipsPartialSlices(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	segFull := glid.New()
	segPartial := glid.New()
	chunkID := chunk.NewChunkID()

	fsm := vaultctlfsm.New()
	for _, spec := range []struct {
		id    glid.GLID
		count uint32
	}{
		{segFull, 2},
		{segPartial, 10},
	} {
		if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
			SegmentID:     spec.id,
			RecordCount:   spec.count,
			ByteSize:      1,
			FirstIngestTS: now,
			LastIngestTS:  now,
			Checksum:      1,
			PublishedAt:   now,
		})}); err != nil {
			t.Fatal(err)
		}
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalOpenChunkManifest(chunkID, now)}); err != nil {
		t.Fatal(err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         segFull,
		FirstRecordNumber: 0,
		LastRecordNumber:  1,
		SliceBytes:        2,
		RefAddedAt:        now,
	})}); err != nil {
		t.Fatal(err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         segPartial,
		FirstRecordNumber: 0,
		LastRecordNumber:  4,
		SliceBytes:        5,
		RefAddedAt:        now,
	})}); err != nil {
		t.Fatal(err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, now.Add(time.Minute))}); err != nil {
		t.Fatal(err)
	}

	got := releasableSegmentIDs(fsm, fsm.SealedManifest())
	if len(got) != 1 || got[0] != segFull {
		t.Fatalf("releasable = %v, want only fully consumed %s", got, segFull)
	}
}

func TestPartitionPendingReleaseWaitsForHolders(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	segID := glid.New()
	fsm := vaultctlfsm.New()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID: segID, RecordCount: 1, ByteSize: 1,
		FirstIngestTS: now, LastIngestTS: now, Checksum: 1, PublishedAt: now,
	})}); err != nil {
		t.Fatal(err)
	}
	chunkID := chunk.NewChunkID()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalOpenChunkManifest(chunkID, now)}); err != nil {
		t.Fatal(err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 0,
		SliceBytes: 1, RefAddedAt: now,
	})}); err != nil {
		t.Fatal(err)
	}

	required := []string{"home-a", "home-b"}
	ready, pending := partitionPendingRelease(fsm, []glid.GLID{segID}, required, true)
	if len(ready) != 0 || len(pending) != 1 {
		t.Fatalf("before ack: ready=%v pending=%v", ready, pending)
	}

	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalAckSegmentHolder(segID, "home-a")}); err != nil {
		t.Fatal(err)
	}
	ready, pending = partitionPendingRelease(fsm, []glid.GLID{segID}, required, true)
	if len(ready) != 0 || len(pending) != 1 {
		t.Fatalf("after one ack: ready=%v pending=%v", ready, pending)
	}

	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalAckSegmentHolder(segID, "home-b")}); err != nil {
		t.Fatal(err)
	}
	ready, pending = partitionPendingRelease(fsm, []glid.GLID{segID}, required, true)
	if len(ready) != 1 || ready[0] != segID || len(pending) != 0 {
		t.Fatalf("after all acks: ready=%v pending=%v", ready, pending)
	}
}
