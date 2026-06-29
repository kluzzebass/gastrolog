package server

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

func TestPartitionHolderIndexStable(t *testing.T) {
	t.Parallel()
	id, err := chunk.ParseChunkID("01h5kq3g8r0000000000000000")
	if err != nil {
		id = chunk.NewChunkID()
	}
	idx1 := partitionHolderIndex(id, 4)
	idx2 := partitionHolderIndex(id, 4)
	if idx1 != idx2 {
		t.Fatalf("partition index not stable: %d vs %d", idx1, idx2)
	}
	if idx1 < 0 || idx1 >= 4 {
		t.Fatalf("index out of range: %d", idx1)
	}
}

func TestPlanVaultSearchPartitions_DistributesSealed(t *testing.T) {
	t.Parallel()
	holders := []string{"node-a", "node-b", "node-c", "node-d"}
	sealed := make([]chunk.ChunkMeta, 12)
	for i := range sealed {
		sealed[i] = chunk.ChunkMeta{
			ID:    chunk.NewChunkID(),
			State: chunk.ChunkStateSealed,
		}
	}
	targets := planVaultSearchPartitions(holders, "node-d", sealed)
	if len(targets) == 0 {
		t.Fatal("expected partition targets")
	}
	seen := make(map[chunk.ChunkID]string)
	for _, target := range targets {
		for _, id := range target.sealedChunkIDs {
			if prev, ok := seen[id]; ok {
				t.Fatalf("chunk %s assigned to both %s and %s", id, prev, target.nodeID)
			}
			seen[id] = target.nodeID
		}
	}
	if len(seen) != len(sealed) {
		t.Fatalf("expected %d sealed chunks partitioned, got %d", len(sealed), len(seen))
	}
}

func TestPlanVaultSearchPartitions_PipelineOnLeaderOnly(t *testing.T) {
	t.Parallel()
	metas := []chunk.ChunkMeta{
		{ID: chunk.NewChunkID(), State: chunk.ChunkStateSealed},
		{ID: chunk.NewChunkID(), State: chunk.ChunkStateActive, IngestEnd: time.Now()},
	}
	targets := planVaultSearchPartitions([]string{"node-a", "node-b"}, "node-b", metas)
	var leaderPipeline, followerPipeline bool
	for _, target := range targets {
		if target.pipelineChunks && target.nodeID == "node-b" {
			leaderPipeline = true
		}
		if target.pipelineChunks && target.nodeID == "node-a" {
			followerPipeline = true
		}
	}
	if !leaderPipeline {
		t.Fatal("expected pipeline chunks on leader node-b")
	}
	if followerPipeline {
		t.Fatal("follower must not receive pipeline chunks")
	}
}

func TestPlanVaultSearchPartitions_SingleHolder(t *testing.T) {
	t.Parallel()
	metas := []chunk.ChunkMeta{
		{ID: chunk.NewChunkID(), State: chunk.ChunkStateSealed},
		{ID: chunk.NewChunkID(), State: chunk.ChunkStateActive},
	}
	targets := planVaultSearchPartitions([]string{"node-only"}, "node-only", metas)
	if len(targets) != 1 {
		t.Fatalf("expected 1 target, got %d", len(targets))
	}
	if targets[0].nodeID != "node-only" {
		t.Fatalf("unexpected node %s", targets[0].nodeID)
	}
	if len(targets[0].sealedChunkIDs) != 1 {
		t.Fatalf("expected 1 sealed chunk, got %d", len(targets[0].sealedChunkIDs))
	}
	if !targets[0].pipelineChunks {
		t.Fatal("expected pipeline chunks on sole holder")
	}
}
