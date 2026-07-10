package chunking

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)


// releaseScanFor snapshots the FSM the way a release pass does.
func releaseScanFor(fsm *vaultctlfsm.FSM, minChunkHolders int) *vaultctlfsm.ReleaseScan {
	return fsm.SnapshotReleaseScan(minChunkHolders)
}

// testMayRelease evaluates the registry-release gate for one segment the way
// production does: one scan per pass, pure decision over it.
func testMayRelease(fsm *vaultctlfsm.FSM, segID glid.GLID, required []string, holdersWired bool, minChunkHolders int, giveUpTTL time.Duration, now time.Time) bool {
	scan := releaseScanFor(fsm, minChunkHolders)
	entry := scan.Entry(segID)
	if entry == nil {
		return false
	}
	return scanMayRelease(scan, entry, required, holdersWired, giveUpTTL, now)
}

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
	sealedAt := now.Add(time.Minute)
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, sealedAt)}); err != nil {
		t.Fatal(err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(chunkID, sealedAt, 1, 1, now, now, now, true, sealedAt)}); err != nil {
		t.Fatal(err)
	}

	required := []string{"home-a", "home-b"}
	ready, pending := partitionPendingRelease(releaseScanFor(fsm, 2), []glid.GLID{segID}, required, true, 0, time.Time{})
	if len(ready) != 0 || len(pending) != 1 {
		t.Fatalf("before ack: ready=%v pending=%v", ready, pending)
	}

	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalAckSegmentHolder(segID, "home-a")}); err != nil {
		t.Fatal(err)
	}
	ready, pending = partitionPendingRelease(releaseScanFor(fsm, 2), []glid.GLID{segID}, required, true, 0, time.Time{})
	if len(ready) != 0 || len(pending) != 1 {
		t.Fatalf("after one ack: ready=%v pending=%v", ready, pending)
	}

	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalAckSegmentHolder(segID, "home-b")}); err != nil {
		t.Fatal(err)
	}
	ready, pending = partitionPendingRelease(releaseScanFor(fsm, 2), []glid.GLID{segID}, required, true, 0, time.Time{})
	if len(ready) != 1 || ready[0] != segID || len(pending) != 0 {
		t.Fatalf("after all acks: ready=%v pending=%v", ready, pending)
	}
}

func TestSegmentReadyForRegistryReleaseBlockedInSealedManifest(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	segID := glid.New()
	chunkID := chunk.NewChunkID()
	fsm := vaultctlfsm.New()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID: segID, RecordCount: 1, ByteSize: 1,
		FirstIngestTS: now, LastIngestTS: now, Checksum: 1, PublishedAt: now,
	})}); err != nil {
		t.Fatal(err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalOpenChunkManifest(chunkID, now)}); err != nil {
		t.Fatal(err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 0, SliceBytes: 1, RefAddedAt: now,
	})}); err != nil {
		t.Fatal(err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, now.Add(time.Minute))}); err != nil {
		t.Fatal(err)
	}
	if testMayRelease(fsm, segID, nil, false, 2, 0, time.Time{}) {
		t.Fatal("segment in sealed manifest awaiting build must not be releasable")
	}
}

// TestReleaseUnpinsDeadHolderViaSupersession is the completed/ leak fix: a
// segment whose chunk reached RF among the LIVE homes releases even though a
// required home ("dead-home") never ack'd the raw segment. The old gate
// (holdersCover over every required home) pinned it forever on the dead home.
func TestReleaseUnpinsDeadHolderViaSupersession(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	segID := glid.New()
	chunkID := chunk.NewChunkID()
	fsm := vaultctlfsm.New()

	apply := func(data []byte) {
		t.Helper()
		if err, ok := fsm.Apply(&hraft.Log{Data: data}).(error); ok && err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	apply(vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID: segID, RecordCount: 1, ByteSize: 1,
		FirstIngestTS: now, LastIngestTS: now, Checksum: 1, PublishedAt: now,
	}))
	apply(vaultctlfsm.MarshalOpenChunkManifest(chunkID, now))
	apply(vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 0, SliceBytes: 1, RefAddedAt: now,
	}))
	sealedAt := now.Add(time.Minute)
	apply(vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, sealedAt))
	apply(vaultctlfsm.MarshalSealChunk(chunkID, sealedAt, 1, 1, now, now, now, true, sealedAt))

	// dead-home is required but will never ack the segment or the chunk.
	required := []string{"live-a", "live-b", "dead-home"}

	// Before RF: neither the fast path (dead-home missing) nor supersession fires.
	if testMayRelease(fsm, segID, required, true, 2, 0, time.Time{}) {
		t.Fatal("released before chunk reached RF")
	}

	// Two live homes hold the chunk — RF=2 reached without the dead home.
	ackChunk := func(node string) {
		data, err := vaultctlfsm.MarshalAckChunkHolders([]chunk.ChunkID{chunkID}, node)
		if err != nil {
			t.Fatalf("marshal ack chunk holders: %v", err)
		}
		apply(data)
	}
	ackChunk("live-a")
	ackChunk("live-b")

	if !testMayRelease(fsm, segID, required, true, 2, 0, time.Time{}) {
		t.Fatal("segment not releasable after chunk reached RF among live homes — dead home pins it")
	}
}

// TestReleaseGiveUpOnRetentionExpiry pins the counted give-up expiry
// (design-notes 28): an island-origin segment — never collected, never
// chunked, holders never satisfied — releases once its records out-age the
// vault's delete-disposition retention TTL. Had chunking succeeded, retention
// would already have deleted the records; holding the registry entry forever
// only leaks. A segment still inside the TTL, or referenced by an unbuilt
// manifest, never gives up.
func TestReleaseGiveUpOnRetentionExpiry(t *testing.T) {
	t.Parallel()
	ingested := time.Unix(0, 1_700_000_000_000).UTC()
	segID := glid.New()
	fsm := vaultctlfsm.New()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID: segID, RecordCount: 5, ByteSize: 100,
		FirstIngestTS: ingested, LastIngestTS: ingested, Checksum: 1, PublishedAt: ingested,
	})}); err != nil {
		t.Fatal(err)
	}
	required := []string{"live-a", "dead-home"}
	ttl := 3 * time.Minute

	// Inside the TTL: no give-up, and the normal gates refuse (not exhausted,
	// holders never acked).
	if testMayRelease(fsm, segID, required, true, 2, ttl, ingested.Add(time.Minute)) {
		t.Fatal("segment inside the retention TTL must not give up")
	}

	// No bound configured: never gives up regardless of age.
	if testMayRelease(fsm, segID, required, true, 2, 0, ingested.Add(24*time.Hour)) {
		t.Fatal("without a TTL bound the segment must not give up")
	}

	// Records out-age the TTL: released even though nothing else is satisfied.
	if !testMayRelease(fsm, segID, required, true, 2, ttl, ingested.Add(ttl+time.Second)) {
		t.Fatal("segment past the retention TTL must give up (counted expiry)")
	}

	// Placement lookup unresolved (holdersWired, empty required): give-up
	// still fires — that broken state is exactly what it exists for.
	if !testMayRelease(fsm, segID, nil, true, 2, ttl, ingested.Add(ttl+time.Second)) {
		t.Fatal("give-up must fire even with unresolved placement")
	}

	// Referenced by an unbuilt manifest: the build still needs the bytes.
	chunkID := chunk.NewChunkID()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalOpenChunkManifest(chunkID, ingested)}); err != nil {
		t.Fatal(err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 4, SliceBytes: 100, RefAddedAt: ingested,
	})}); err != nil {
		t.Fatal(err)
	}
	if testMayRelease(fsm, segID, required, true, 2, ttl, ingested.Add(ttl+time.Second)) {
		t.Fatal("segment referenced by an unbuilt manifest must never give up")
	}
}
