package chunking

import (
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

func TestRegistryPlanningStatsFromFSM(t *testing.T) {
	fsm := vaultctlfsm.New()
	now := time.Now().UTC().Truncate(time.Millisecond)
	segA := glid.New()
	segB := glid.New()
	applyBacklogCmd(t, fsm, vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID:     segA,
		RecordCount:   100,
		LastIngestTS:  now.Add(-2 * time.Hour),
		FirstIngestTS: now.Add(-2*time.Hour - time.Minute),
		OriginNodeID:  "node-1",
		PublishedAt:   now,
	}))
	applyBacklogCmd(t, fsm, vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID:     segB,
		RecordCount:   50,
		LastIngestTS:  now.Add(-1 * time.Hour),
		FirstIngestTS: now.Add(-1*time.Hour - time.Minute),
		OriginNodeID:  "node-1",
		PublishedAt:   now,
	}))

	stats := RegistryPlanningStatsFromFSM(fsm)
	if stats.TotalSegments != 2 || stats.EligibleSegments != 2 || stats.RegistryRecords != 150 {
		t.Fatalf("initial stats = %+v", stats)
	}
	if !stats.OldestLastIngest.Equal(now.Add(-2 * time.Hour)) {
		t.Fatalf("OldestLastIngest = %v want %v", stats.OldestLastIngest, now.Add(-2*time.Hour))
	}

	// Chunk all of segA — planner treats it as exhausted.
	chunkID := backlogTestChunkID(0x01)
	applyBacklogCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, now))
	applyBacklogCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         segA,
		FirstRecordNumber: 0,
		LastRecordNumber:  99,
		SliceBytes:        1,
		RefAddedAt:        now,
	}))

	stats = RegistryPlanningStatsFromFSM(fsm)
	if stats.EligibleSegments != 1 || stats.RegistryRecords != 150 {
		t.Fatalf("after exhaust A = %+v", stats)
	}
	if !stats.OldestLastIngest.Equal(now.Add(-1 * time.Hour)) {
		t.Fatalf("OldestLastIngest = %v want %v", stats.OldestLastIngest, now.Add(-1*time.Hour))
	}
}

func applyBacklogCmd(t *testing.T, fsm *vaultctlfsm.FSM, data []byte) {
	t.Helper()
	result := fsm.Apply(&hraft.Log{Data: data})
	if err, ok := result.(error); ok && err != nil {
		t.Fatalf("apply: %v", err)
	}
}

func backlogTestChunkID(b byte) chunk.ChunkID {
	var id chunk.ChunkID
	id[0] = b
	return id
}
