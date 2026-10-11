package vaultctlfsm

import (
	"bytes"
	"testing"
	"time"

	"gastrolog/internal/glid"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

func releasedTestEntry(id glid.GLID) CompletedSegmentEntry {
	now := time.Unix(0, 1_700_000_000_000).UTC()
	return CompletedSegmentEntry{
		SegmentID:     id,
		RecordCount:   10,
		ByteSize:      100,
		FirstIngestTS: now,
		LastIngestTS:  now,
		Checksum:      7,
		OriginNodeID:  "node-d",
		PublishedAt:   now,
	}
}

// publishAndRelease registers a segment and releases it, returning its ID.
func publishAndRelease(t *testing.T, fsm *FSM) glid.GLID {
	t.Helper()
	id := glid.New()
	applyCmd(t, fsm, MarshalPublishCompletedSegment(releasedTestEntry(id)))
	applyCmd(t, fsm, MarshalReleaseSegments([]glid.GLID{id}))
	return id
}

func applyExpectErr(t *testing.T, fsm *FSM, data []byte) {
	t.Helper()
	if err, ok := fsm.Apply(&hraft.Log{Data: data}).(error); !ok || err == nil {
		t.Fatalf("apply succeeded, want an error")
	}
}

func snapshotBytes(t *testing.T, fsm *FSM) []byte {
	t.Helper()
	raw, err := proto.Marshal(fsm.SnapshotProto())
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	return raw
}

var threeMembers = []string{"node-a", "node-b", "node-c"}

func TestReleasedSegmentTombstonesPruneOnceEveryMemberConfirms(t *testing.T) {
	t.Parallel()
	fsm := New()
	first := publishAndRelease(t, fsm)
	second := publishAndRelease(t, fsm)
	if got := fsm.ReleaseSeq(); got != 2 {
		t.Fatalf("ReleaseSeq = %d, want 2", got)
	}

	applyCmd(t, fsm, MarshalConfirmSegmentPurge("node-a", 2))
	applyCmd(t, fsm, MarshalConfirmSegmentPurge("node-b", 1))
	if fsm.ReleasedSegmentsPrunable(threeMembers) {
		t.Fatal("prunable while node-c has confirmed nothing")
	}
	applyCmd(t, fsm, MarshalPruneReleasedSegments(threeMembers))
	if got := fsm.ReleasedSegmentCount(); got != 2 {
		t.Fatalf("tombstones = %d after a prune node-c never confirmed, want 2", got)
	}

	applyCmd(t, fsm, MarshalConfirmSegmentPurge("node-c", 2))
	if !fsm.ReleasedSegmentsPrunable(threeMembers) {
		t.Fatal("not prunable once every member confirmed the first release")
	}
	applyCmd(t, fsm, MarshalPruneReleasedSegments(threeMembers))
	if fsm.SegmentReleased(first) {
		t.Error("first release still tombstoned after every member confirmed it")
	}
	if !fsm.SegmentReleased(second) {
		t.Error("second release pruned although node-b confirmed only the first")
	}

	applyCmd(t, fsm, MarshalConfirmSegmentPurge("node-b", 2))
	applyCmd(t, fsm, MarshalPruneReleasedSegments(threeMembers))
	if got := fsm.ReleasedSegmentCount(); got != 0 {
		t.Fatalf("tombstones = %d after every member confirmed every release, want 0", got)
	}
	if fsm.ReleasedSegmentsPrunable(threeMembers) {
		t.Error("prunable with nothing left to prune")
	}
}

// A late publish of a released segment — the restart republish of a node that
// missed the release — must stay refused for as long as the tombstone lives,
// which is until every member has confirmed purging it.
func TestLatePublishOfReleasedSegmentRefusedUntilEveryMemberConfirms(t *testing.T) {
	t.Parallel()
	fsm := New()
	var published int
	fsm.AddOnPublishCompletedSegment(func(CompletedSegmentEntry) { published++ })
	id := publishAndRelease(t, fsm)
	published = 0

	applyCmd(t, fsm, MarshalConfirmSegmentPurge("node-a", fsm.ReleaseSeq()))
	applyCmd(t, fsm, MarshalConfirmSegmentPurge("node-b", fsm.ReleaseSeq()))
	applyCmd(t, fsm, MarshalPruneReleasedSegments(threeMembers))

	applyCmd(t, fsm, MarshalPublishCompletedSegment(releasedTestEntry(id)))
	applyCmd(t, fsm, MarshalAckSegmentHolders([]glid.GLID{id}, "node-c"))
	if fsm.GetCompletedSegment(id) != nil {
		t.Fatal("late publish re-registered a released segment before node-c confirmed its purge")
	}
	if published != 0 {
		t.Fatalf("late publish fired %d publish callbacks, want 0", published)
	}
	if !fsm.SegmentReleased(id) {
		t.Fatal("tombstone pruned before node-c confirmed")
	}
}

// Confirming a release sequence the log has not reached cannot pre-confirm a
// later release, and an older confirmation never lowers a newer one.
func TestConfirmSegmentPurgeIsClampedAndMonotonic(t *testing.T) {
	t.Parallel()
	fsm := New()
	publishAndRelease(t, fsm)
	applyCmd(t, fsm, MarshalConfirmSegmentPurge("node-a", 99))
	if got := fsm.SegmentPurgeConfirmed("node-a"); got != 1 {
		t.Fatalf("confirmation past the release sequence = %d, want clamped to 1", got)
	}

	later := publishAndRelease(t, fsm)
	applyCmd(t, fsm, MarshalConfirmSegmentPurge("node-b", 2))
	applyCmd(t, fsm, MarshalConfirmSegmentPurge("node-b", 1))
	if got := fsm.SegmentPurgeConfirmed("node-b"); got != 2 {
		t.Fatalf("an older confirmation lowered node-b to %d, want 2", got)
	}
	applyCmd(t, fsm, MarshalPruneReleasedSegments([]string{"node-a", "node-b"}))
	if !fsm.SegmentReleased(later) {
		t.Fatal("a confirmation sent before the release covered it")
	}

	applyExpectErr(t, fsm, MarshalConfirmSegmentPurge("", 1))
	applyExpectErr(t, fsm, MarshalPruneReleasedSegments(nil))
}

// Releasing a segment that is already tombstoned keeps its sequence: the
// purge obligation does not restart, and the release sequence does not move.
func TestReleasingATombstonedSegmentKeepsItsSequence(t *testing.T) {
	t.Parallel()
	fsm := New()
	id := publishAndRelease(t, fsm)
	applyCmd(t, fsm, MarshalReleaseSegments([]glid.GLID{id}))
	if got := fsm.ReleaseSeq(); got != 1 {
		t.Fatalf("ReleaseSeq = %d after re-releasing a tombstoned segment, want 1", got)
	}
	applyCmd(t, fsm, MarshalConfirmSegmentPurge("node-a", 1))
	applyCmd(t, fsm, MarshalPruneReleasedSegments([]string{"node-a"}))
	if fsm.SegmentReleased(id) {
		t.Fatal("tombstone survived a prune its only member confirmed")
	}
}

// A node outside the members a prune names neither holds the floor down nor
// keeps its confirmation: it has left the vault-ctl group.
func TestPruneDropsConfirmationsOfDepartedMembers(t *testing.T) {
	t.Parallel()
	fsm := New()
	publishAndRelease(t, fsm)
	applyCmd(t, fsm, MarshalConfirmSegmentPurge("node-a", 1))
	applyCmd(t, fsm, MarshalConfirmSegmentPurge("node-old", 1))
	members := []string{"node-a"}
	if !fsm.ReleasedSegmentsPrunable(members) {
		t.Fatal("not prunable with every member confirmed")
	}
	applyCmd(t, fsm, MarshalPruneReleasedSegments(members))
	if got := fsm.ReleasedSegmentCount(); got != 0 {
		t.Fatalf("tombstones = %d, want 0: a departed node held the floor", got)
	}
	if got := fsm.SegmentPurgeConfirmed("node-old"); got != 0 {
		t.Fatalf("departed node's confirmation kept (%d)", got)
	}
	if fsm.ReleasedSegmentsPrunable(members) {
		t.Error("prunable after the prune applied")
	}
}

// The vault-ctl snapshot stops growing with total ingest: after every
// release cycle is confirmed and pruned, the snapshot is no larger than after
// the first cycle.
func TestSnapshotStaysBoundedAcrossReleaseCycles(t *testing.T) {
	t.Parallel()
	fsm := New()
	cycle := func() {
		for range 8 {
			publishAndRelease(t, fsm)
		}
		for _, m := range threeMembers {
			applyCmd(t, fsm, MarshalConfirmSegmentPurge(m, fsm.ReleaseSeq()))
		}
		applyCmd(t, fsm, MarshalPruneReleasedSegments(threeMembers))
	}
	cycle()
	firstSize := len(snapshotBytes(t, fsm))
	for range 100 {
		cycle()
	}
	if got := fsm.ReleasedSegmentCount(); got != 0 {
		t.Fatalf("tombstones = %d after 101 confirmed cycles, want 0", got)
	}
	// The release sequence and each member's confirmation are varints that
	// grow by a few bytes over the whole run, independent of how many
	// segments were released.
	varintGrowth := (1 + len(threeMembers)) * 2
	if size := len(snapshotBytes(t, fsm)); size > firstSize+varintGrowth {
		t.Fatalf("snapshot grew from %d to %d bytes across 100 pruned release cycles", firstSize, size)
	}
}

// Tombstones, their sequences, and purge confirmations survive a snapshot
// round trip, and a node restored from the snapshot ends where a node that
// replayed the whole log ends.
func TestReleasedSegmentStateSurvivesSnapshotAndReplaysDeterministically(t *testing.T) {
	t.Parallel()
	ids := []glid.GLID{glid.New(), glid.New(), glid.New()}
	prefix := [][]byte{
		MarshalPublishCompletedSegment(releasedTestEntry(ids[0])),
		MarshalPublishCompletedSegment(releasedTestEntry(ids[1])),
		MarshalPublishCompletedSegment(releasedTestEntry(ids[2])),
		MarshalReleaseSegments([]glid.GLID{ids[0]}),
		MarshalReleaseSegments([]glid.GLID{ids[1]}),
		MarshalConfirmSegmentPurge("node-a", 2),
		MarshalConfirmSegmentPurge("node-b", 1),
		MarshalConfirmSegmentPurge("node-c", 2),
	}
	suffix := [][]byte{
		MarshalPruneReleasedSegments(threeMembers),
		MarshalReleaseSegments([]glid.GLID{ids[2]}),
		MarshalConfirmSegmentPurge("node-a", 3),
		MarshalPruneReleasedSegments(threeMembers),
		MarshalPublishCompletedSegment(releasedTestEntry(ids[1])),
	}

	replayed := New()
	for _, cmd := range append(append([][]byte{}, prefix...), suffix...) {
		applyCmd(t, replayed, cmd)
	}

	mid := New()
	for _, cmd := range prefix {
		applyCmd(t, mid, cmd)
	}
	restored := New()
	restored.RestoreProto(mid.SnapshotProto())
	if !bytes.Equal(snapshotBytes(t, restored), snapshotBytes(t, mid)) {
		t.Fatal("restored snapshot differs from its source")
	}
	if got := restored.SegmentPurgeConfirmed("node-b"); got != 1 {
		t.Fatalf("restored node-b confirmation = %d, want 1", got)
	}
	for _, cmd := range suffix {
		applyCmd(t, restored, cmd)
	}

	if !bytes.Equal(snapshotBytes(t, restored), snapshotBytes(t, replayed)) {
		t.Fatal("snapshot-restored node diverged from the node that replayed the whole log")
	}
	if replayed.SegmentReleased(ids[0]) {
		t.Error("first release not pruned after every member confirmed it")
	}
	if !replayed.SegmentReleased(ids[1]) || !replayed.SegmentReleased(ids[2]) {
		t.Error("a release pruned before every member confirmed it")
	}
	if replayed.GetCompletedSegment(ids[1]) != nil {
		t.Error("late publish re-registered a still-tombstoned segment")
	}
}
