package orchestrator

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

func applyToFSM(t *testing.T, fsm *vaultctlfsm.FSM, data []byte) {
	t.Helper()
	if err, ok := fsm.Apply(&hraft.Log{Data: data}).(error); ok && err != nil {
		t.Fatalf("apply: %v", err)
	}
}

func publishSegment(t *testing.T, fsm *vaultctlfsm.FSM, origin string, holders ...string) glid.GLID {
	t.Helper()
	id := glid.New()
	now := time.Now()
	applyToFSM(t, fsm, vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID: id, RecordCount: 1, ByteSize: 1, FirstIngestTS: now, LastIngestTS: now,
		Checksum: 1, OriginNodeID: origin, PublishedAt: now,
	}))
	for _, h := range holders {
		applyToFSM(t, fsm, vaultctlfsm.MarshalAckSegmentHolders([]glid.GLID{id}, h))
	}
	return id
}

// chunkSegment puts every record of the segment into one sealed chunk held by
// holders.
func chunkSegment(t *testing.T, fsm *vaultctlfsm.FSM, segID glid.GLID, holders ...string) {
	t.Helper()
	now := time.Now()
	chunkID := chunk.NewChunkID()
	applyToFSM(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, now))
	applyToFSM(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 0, SliceBytes: 1, RefAddedAt: now,
	}))
	applyToFSM(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, now))
	applyToFSM(t, fsm, vaultctlfsm.MarshalSealChunk(chunkID, now, 1, 1, now, now, now, true, now, [32]byte{1}))
	for _, h := range holders {
		ack, err := vaultctlfsm.MarshalAckChunkHolders([]chunk.ChunkID{chunkID}, h)
		if err != nil {
			t.Fatal(err)
		}
		applyToFSM(t, fsm, ack)
	}
}

// Removing a node takes every copy it holds. An unreleased segment the homes
// still collect depends on the node's copy when the node holds it or
// originated it and the rest of the placement holds fewer than HolderQuorum
// copies; a segment a durable chunk supersedes depends on nothing.
func TestHolderLossInCountsSegmentsThePlacementLacks(t *testing.T) {
	t.Parallel()
	remaining := []string{"home-a", "home-b"}
	cases := []struct {
		name  string
		setup func(t *testing.T, fsm *vaultctlfsm.FSM)
		want  int
	}{
		{name: "held by the node only", want: 1, setup: func(t *testing.T, fsm *vaultctlfsm.FSM) {
			publishSegment(t, fsm, "origin", "node")
		}},
		{name: "held by the node and one home", want: 1, setup: func(t *testing.T, fsm *vaultctlfsm.FSM) {
			publishSegment(t, fsm, "origin", "node", "home-a")
		}},
		{name: "originated by the node, collected by no home", want: 1, setup: func(t *testing.T, fsm *vaultctlfsm.FSM) {
			publishSegment(t, fsm, "node")
		}},
		{name: "held by the node and the quorum of homes", want: 0, setup: func(t *testing.T, fsm *vaultctlfsm.FSM) {
			publishSegment(t, fsm, "origin", "node", "home-a", "home-b")
		}},
		{name: "originated by the node, collected by the quorum", want: 0, setup: func(t *testing.T, fsm *vaultctlfsm.FSM) {
			publishSegment(t, fsm, "node", "home-a", "home-b")
		}},
		{name: "held by the node, superseded by a chunk the homes hold", want: 0, setup: func(t *testing.T, fsm *vaultctlfsm.FSM) {
			seg := publishSegment(t, fsm, "origin", "node")
			chunkSegment(t, fsm, seg, "home-a", "home-b")
		}},
		{name: "neither held nor originated by the node", want: 0, setup: func(t *testing.T, fsm *vaultctlfsm.FSM) {
			publishSegment(t, fsm, "origin", "home-a")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fsm := vaultctlfsm.New()
			tc.setup(t, fsm)
			chunks, segments := holderLossIn(fsm, "node", remaining)
			if segments != tc.want || chunks != 0 {
				t.Fatalf("holderLossIn = %d chunks, %d segments; want 0 chunks, %d segments", chunks, segments, tc.want)
			}
		})
	}
}

// A sealed chunk only the node holds counts as a chunk; with no placement left
// besides the node nothing is reported (the orphan gate's case).
func TestHolderLossInCountsChunksAndIgnoresAnEmptyPlacement(t *testing.T) {
	t.Parallel()
	fsm := vaultctlfsm.New()
	seg := publishSegment(t, fsm, "origin", "node")
	chunkSegment(t, fsm, seg, "node")
	if chunks, segments := holderLossIn(fsm, "node", []string{"home-a"}); chunks != 1 || segments != 1 {
		t.Fatalf("holderLossIn = %d chunks, %d segments; want 1 chunk (only the node holds it) and 1 segment it does not supersede", chunks, segments)
	}
	if chunks, segments := holderLossIn(fsm, "node", nil); chunks != 0 || segments != 0 {
		t.Fatalf("holderLossIn with no remaining placement = %d chunks, %d segments; want none", chunks, segments)
	}
}
