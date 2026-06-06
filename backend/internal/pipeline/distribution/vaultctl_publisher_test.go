package distribution

import (
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

func TestVaultCtlPublisherPublish(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	segID := glid.New()
	vaultID := glid.New()

	fsm := vaultctlfsm.New()
	pub := &VaultCtlPublisher{
		Applier:      &fsmApplier{fsm: fsm},
		OriginNodeID: "node-a",
		Now:          func() time.Time { return now },
	}

	meta := Metadata{
		SegmentID:     segID,
		VaultID:       vaultID,
		RecordCount:   42,
		ByteSize:      8192,
		FirstIngestTS: now.Add(-time.Minute),
		LastIngestTS:  now.Add(-time.Second),
		Checksum:      0xDEADBEEF,
	}
	if err := pub.Publish(t.Context(), meta); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	got := fsm.ListCompletedSegments()
	if len(got) != 1 {
		t.Fatalf("ListCompletedSegments len = %d, want 1", len(got))
	}
	if got[0].SegmentID != segID || got[0].RecordCount != 42 || got[0].OriginNodeID != "node-a" {
		t.Fatalf("registry entry = %+v", got[0])
	}
	if !got[0].PublishedAt.Equal(now) {
		t.Fatalf("PublishedAt = %v, want %v", got[0].PublishedAt, now)
	}

	if err := pub.Publish(t.Context(), meta); err != nil {
		t.Fatalf("replay Publish: %v", err)
	}
	if len(fsm.ListCompletedSegments()) != 1 {
		t.Fatal("idempotent replay must not duplicate entry")
	}
}

func TestVaultCtlPublisherReplicatesOnFollowerFSM(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	segID := glid.New()

	leader := vaultctlfsm.New()
	follower := vaultctlfsm.New()
	pub := &VaultCtlPublisher{
		Applier:      &fsmApplier{fsm: leader},
		OriginNodeID: "node-origin",
		Now:          func() time.Time { return now },
	}
	meta := Metadata{
		SegmentID:     segID,
		VaultID:       glid.New(),
		RecordCount:   10,
		ByteSize:      512,
		FirstIngestTS: now.Add(-2 * time.Minute),
		LastIngestTS:  now.Add(-time.Minute),
		Checksum:      1234,
	}
	if err := pub.Publish(t.Context(), meta); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	wire := vaultctlfsm.MarshalPublishCompletedSegment(
		CompletedSegmentEntryFromMetadata(meta, "node-origin", now),
	)
	if err := applyWireToFSM(follower, wire); err != nil {
		t.Fatalf("follower apply: %v", err)
	}

	leaderList := leader.ListCompletedSegments()
	followerList := follower.ListCompletedSegments()
	if len(leaderList) != 1 || len(followerList) != 1 {
		t.Fatalf("leader=%d follower=%d entries", len(leaderList), len(followerList))
	}
	if leaderList[0] != followerList[0] {
		t.Fatalf("leader %+v != follower %+v", leaderList[0], followerList[0])
	}
}

type fsmApplier struct {
	fsm *vaultctlfsm.FSM
}

func (a *fsmApplier) Apply(data []byte) error {
	return applyWireToFSM(a.fsm, data)
}

func applyWireToFSM(fsm *vaultctlfsm.FSM, data []byte) error {
	if result := fsm.Apply(&hraft.Log{Data: data}); result != nil {
		if err, ok := result.(error); ok {
			return err
		}
	}
	return nil
}

func TestVaultCtlPublisherWireRoundTrip(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 1_700_000_000_123).UTC()
	segID := glid.New()
	meta := Metadata{
		SegmentID:     segID,
		RecordCount:   3,
		ByteSize:      99,
		FirstIngestTS: now,
		LastIngestTS:  now.Add(time.Second),
		Checksum:      77,
	}
	entry := CompletedSegmentEntryFromMetadata(meta, "node-z", now)
	wire := vaultctlfsm.MarshalPublishCompletedSegment(entry)

	var cmd gastrologv1.VaultCtlCommand
	if err := proto.Unmarshal(wire, &cmd); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := cmd.GetPublishCompletedSegment()
	if got == nil {
		t.Fatal("expected publish_completed_segment case")
	}
	if string(got.GetSegmentId()) != string(segID[:]) {
		t.Fatalf("segment id mismatch")
	}
	if got.GetRecordCount() != 3 || got.GetChecksum() != 77 || got.GetOriginNodeId() != "node-z" {
		t.Fatalf("fields = %+v", got)
	}
}
