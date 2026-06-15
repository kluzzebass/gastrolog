package chunking_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/record"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

type fsmApplier struct {
	fsm *vaultctlfsm.FSM
	log *[][]byte
}

func (a *fsmApplier) Apply(data []byte) error {
	cp := append([]byte(nil), data...)
	if a.log != nil {
		*a.log = append(*a.log, cp)
	}
	if err := a.fsm.Apply(&hraft.Log{Data: cp}); err != nil {
		return fmt.Errorf("apply: %v", err)
	}
	return nil
}

func writeCompletedSegment(t *testing.T, vaultRoot string, segID, vaultID glid.GLID, recs []recordForSeg) {
	t.Helper()
	if err := os.MkdirAll(paths.CompletedDir(vaultRoot), 0o750); err != nil {
		t.Fatal(err)
	}
	records := make([]record.Record, len(recs))
	for i, r := range recs {
		records[i] = makeRecordForSeg(segID, r.seq, r.ts, r.raw)
	}
	src := writeSegment(t, segID, vaultID, records)
	dest := paths.CompletedSegment(vaultRoot, segID)
	if err := os.Rename(src, dest); err != nil {
		t.Fatal(err)
	}
}

func publishSegment(t *testing.T, fsm *vaultctlfsm.FSM, segID glid.GLID, pubAt time.Time, recordCount uint32, firstTS, lastTS time.Time) {
	t.Helper()
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID:     segID,
		RecordCount:   recordCount,
		ByteSize:      1024,
		FirstIngestTS: firstTS,
		LastIngestTS:  lastTS,
		Checksum:      1,
		OriginNodeID:  "origin",
		PublishedAt:   pubAt,
	}))
}

func TestLeaderPlannerOpensAndAddsRefOnPublish(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	pubAt := base.Add(time.Minute)
	segID := glid.New()
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	writeCompletedSegment(t, vaultRoot, segID, vaultID, []recordForSeg{
		{0, base, "a"},
		{1, base.Add(time.Second), "b"},
	})

	fsm := vaultctlfsm.New()
	chunkID := chunk.NewChunkID()
	applier := &fsmApplier{fsm: fsm}

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot:  vaultRoot,
		ChunkRoot:  filepath.Join(vaultRoot, "chunks"),
		FSM:        fsm,
		Locate:     chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:    applier,
		IsLeader:   func() bool { return true },
		NewChunkID: func() chunk.ChunkID { return chunkID },
		Policy:     chunking.ManifestRotationPolicy{MaxRecords: 100},
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(20 * time.Millisecond)
		publishSegment(t, fsm, segID, pubAt, 2, base, base.Add(time.Second))
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	if err := mgr.Run(ctx); err != nil && err != context.Canceled {
		t.Fatalf("Run: %v", err)
	}

	open := fsm.OpenChunk()
	if open == nil {
		t.Fatal("expected open manifest")
	}
	if open.ChunkID != chunkID {
		t.Fatalf("chunk ID = %s, want %s", open.ChunkID, chunkID)
	}
	if len(open.Refs) != 1 {
		t.Fatalf("refs = %d, want 1", len(open.Refs))
	}
	if open.Refs[0].FirstRecordNumber != 0 || open.Refs[0].LastRecordNumber != 1 {
		t.Fatalf("ref = [%d,%d], want [0,1]", open.Refs[0].FirstRecordNumber, open.Refs[0].LastRecordNumber)
	}
}

func planUntilSealed(t *testing.T, mgr *chunking.Manager, vaultID glid.GLID, fsm *vaultctlfsm.FSM) *vaultctlfsm.OpenChunkManifest {
	t.Helper()
	ctx := t.Context()
	for step := 0; step < 16; step++ {
		if sealed := fsm.SealedManifest(); sealed != nil {
			return sealed
		}
		if err := mgr.PlanOnce(ctx, vaultID); err != nil {
			t.Fatalf("PlanOnce step %d: %v", step, err)
		}
	}
	t.Fatal("expected sealed manifest after planner catch-up")
	return nil
}

func TestLeaderPlannerRotatesAtMaxRecords(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	pubAt := base.Add(time.Minute)
	segID := glid.New()
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	writeCompletedSegment(t, vaultRoot, segID, vaultID, []recordForSeg{
		{0, base, "a"},
		{1, base.Add(time.Second), "b"},
		{2, base.Add(2 * time.Second), "c"},
		{3, base.Add(3 * time.Second), "d"},
	})

	fsm := vaultctlfsm.New()
	chunkID := chunk.NewChunkID()
	applier := &fsmApplier{fsm: fsm}

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot:  vaultRoot,
		ChunkRoot:  filepath.Join(vaultRoot, "chunks"),
		FSM:        fsm,
		Locate:     chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:    applier,
		IsLeader:   func() bool { return true },
		NewChunkID: func() chunk.ChunkID { return chunkID },
		Policy:     chunking.ManifestRotationPolicy{MaxRecords: 2},
	}); err != nil {
		t.Fatal(err)
	}

	publishSegment(t, fsm, segID, pubAt, 4, base, base.Add(3*time.Second))
	sealed := planUntilSealed(t, mgr, vaultID, fsm)
	if sealed.TotalRecords != 2 {
		t.Fatalf("sealed records = %d, want 2", sealed.TotalRecords)
	}
	if fsm.OpenChunk() != nil {
		t.Fatal("open manifest must clear on rotate")
	}
}

func TestLeaderPlannerFollowerDoesNotPropose(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	writeCompletedSegment(t, vaultRoot, segID, vaultID, []recordForSeg{{0, base, "a"}})

	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot: vaultRoot,
		ChunkRoot: filepath.Join(vaultRoot, "chunks"),
		FSM:       fsm,
		Locate:    chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:   applier,
		IsLeader:  func() bool { return false },
		Policy:    chunking.ManifestRotationPolicy{MaxRecords: 10},
	}); err != nil {
		t.Fatal(err)
	}

	publishSegment(t, fsm, segID, base, 1, base, base)
	if err := mgr.PlanOnce(t.Context(), vaultID); err != nil {
		t.Fatal(err)
	}
	if fsm.OpenChunk() != nil {
		t.Fatal("follower must not open manifest")
	}
}

func TestLeaderPlannerReplicatedManifestSequence(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	pubAt := base.Add(time.Minute)
	segA := glid.New()
	segB := glid.New()
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	writeCompletedSegment(t, vaultRoot, segA, vaultID, []recordForSeg{{0, base, "a"}})
	writeCompletedSegment(t, vaultRoot, segB, vaultID, []recordForSeg{{0, base.Add(time.Second), "b"}})

	fsmLeader := vaultctlfsm.New()
	fsmFollower := vaultctlfsm.New()
	var wireLog [][]byte
	chunkID := chunk.NewChunkID()

	applier := &fsmApplier{fsm: fsmLeader, log: &wireLog}

	publishSegment(t, fsmLeader, segA, pubAt, 1, base, base)
	publishSegment(t, fsmLeader, segB, pubAt.Add(time.Second), 1, base.Add(time.Second), base.Add(time.Second))
	applyChunkCmd(t, fsmFollower, vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID: segA, RecordCount: 1, ByteSize: 1024, FirstIngestTS: base, LastIngestTS: base,
		PublishedAt: pubAt, OriginNodeID: "origin", Checksum: 1,
	}))
	applyChunkCmd(t, fsmFollower, vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID: segB, RecordCount: 1, ByteSize: 1024,
		FirstIngestTS: base.Add(time.Second), LastIngestTS: base.Add(time.Second),
		PublishedAt: pubAt.Add(time.Second), OriginNodeID: "origin", Checksum: 1,
	}))

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot:  vaultRoot,
		ChunkRoot:  filepath.Join(vaultRoot, "chunks"),
		FSM:        fsmLeader,
		Locate:     chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:    applier,
		IsLeader:   func() bool { return true },
		NewChunkID: func() chunk.ChunkID { return chunkID },
		Policy:     chunking.ManifestRotationPolicy{MaxRecords: 100},
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	if err := mgr.Run(ctx); err != nil && err != context.Canceled {
		t.Fatalf("Run: %v", err)
	}

	for _, data := range wireLog {
		if err := fsmFollower.Apply(&hraft.Log{Data: data}); err != nil {
			t.Fatalf("follower apply: %v", err)
		}
	}

	leaderOpen := fsmLeader.OpenChunk()
	followerOpen := fsmFollower.OpenChunk()
	if leaderOpen == nil || followerOpen == nil {
		t.Fatal("expected open manifest on both replicas")
	}
	if len(leaderOpen.Refs) != len(followerOpen.Refs) {
		t.Fatalf("leader refs = %d follower refs = %d", len(leaderOpen.Refs), len(followerOpen.Refs))
	}
	for i := range leaderOpen.Refs {
		l := leaderOpen.Refs[i]
		f := followerOpen.Refs[i]
		if l.SegmentID != f.SegmentID || l.FirstRecordNumber != f.FirstRecordNumber || l.LastRecordNumber != f.LastRecordNumber {
			t.Fatalf("ref %d diverged: leader %+v follower %+v", i, l, f)
		}
	}
}

func planUntilOpenRecords(t *testing.T, mgr *chunking.Manager, vaultID glid.GLID, fsm *vaultctlfsm.FSM, wantRecords uint64) {
	t.Helper()
	ctx := t.Context()
	for step := 0; step < 16; step++ {
		open := fsm.OpenChunk()
		if open != nil && open.TotalRecords == wantRecords {
			return
		}
		if err := mgr.PlanOnce(ctx, vaultID); err != nil {
			t.Fatalf("PlanOnce step %d: %v", step, err)
		}
	}
	t.Fatalf("expected open manifest with %d records", wantRecords)
}

func TestLeaderPlannerFailoverContinuesManifest(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	pubAt := base.Add(time.Minute)
	segID := glid.New()
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	writeCompletedSegment(t, vaultRoot, segID, vaultID, []recordForSeg{
		{0, base, "a"},
		{1, base.Add(time.Second), "b"},
		{2, base.Add(2 * time.Second), "c"},
		{3, base.Add(3 * time.Second), "d"},
	})

	fsmA := vaultctlfsm.New()
	fsmB := vaultctlfsm.New()
	chunkID := chunk.NewChunkID()
	var leader atomic.Bool
	leader.Store(true)

	applierA := &fsmApplier{fsm: fsmA}
	mgrA := chunking.New(chunking.Config{})
	if err := mgrA.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot:  vaultRoot,
		ChunkRoot:  filepath.Join(vaultRoot, "chunks"),
		FSM:        fsmA,
		Locate:     chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:    applierA,
		IsLeader:   leader.Load,
		NewChunkID: func() chunk.ChunkID { return chunkID },
		Policy:     chunking.ManifestRotationPolicy{MaxRecords: 100},
	}); err != nil {
		t.Fatal(err)
	}

	publishSegment(t, fsmA, segID, pubAt, 4, base, base.Add(3*time.Second))
	applyChunkCmd(t, fsmB, vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID: segID, RecordCount: 4, ByteSize: 1024,
		FirstIngestTS: base, LastIngestTS: base.Add(3 * time.Second),
		PublishedAt: pubAt, OriginNodeID: "origin", Checksum: 1,
	}))
	planUntilOpenRecords(t, mgrA, vaultID, fsmA, 4)

	fsmB.RestoreProto(fsmA.SnapshotProto())
	leader.Store(false)

	applierB := &fsmApplier{fsm: fsmB}
	mgrB := chunking.New(chunking.Config{})
	if err := mgrB.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot:  vaultRoot,
		ChunkRoot:  filepath.Join(vaultRoot, "chunks"),
		FSM:        fsmB,
		Locate:     chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:    applierB,
		IsLeader:   func() bool { return true },
		NewChunkID: func() chunk.ChunkID { return chunkID },
		Policy:     chunking.ManifestRotationPolicy{MaxRecords: 2},
	}); err != nil {
		t.Fatal(err)
	}

	sealed := planUntilSealed(t, mgrB, vaultID, fsmB)
	if sealed.TotalRecords != 4 {
		t.Fatalf("sealed records = %d, want 4", sealed.TotalRecords)
	}
}

// TestLeaderPlannerSecondManifestSkipsConsumedSegments is the regression
// test for the duplicate-chunk bug: after a manifest seals and its chunk
// builds (CmdSealChunk clears the pending manifest), the next manifest must
// resume from the persisted segmentResume positions — NOT re-add records the
// sealed chunk already consumed. Before the fix the planner only consulted
// resume positions for refs in the current open manifest, so manifest 2
// restarted every fully-consumed segment from record 0.
func TestLeaderPlannerSecondManifestSkipsConsumedSegments(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	pubAt := base.Add(time.Minute)
	segA := glid.New()
	segB := glid.New()
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	writeCompletedSegment(t, vaultRoot, segA, vaultID, []recordForSeg{
		{0, base, "a0"},
		{1, base.Add(time.Second), "a1"},
	})
	writeCompletedSegment(t, vaultRoot, segB, vaultID, []recordForSeg{
		{0, base.Add(2 * time.Second), "b0"},
		{1, base.Add(3 * time.Second), "b1"},
	})

	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot: vaultRoot,
		ChunkRoot: filepath.Join(vaultRoot, "chunks"),
		FSM:       fsm,
		Locate:    chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:   applier,
		IsLeader:  func() bool { return true },
		Policy:    chunking.ManifestRotationPolicy{MaxRecords: 2},
	}); err != nil {
		t.Fatal(err)
	}

	publishSegment(t, fsm, segA, pubAt, 2, base, base.Add(time.Second))
	publishSegment(t, fsm, segB, pubAt.Add(time.Second), 2, base.Add(2*time.Second), base.Add(3*time.Second))

	first := planUntilSealed(t, mgr, vaultID, fsm)
	if len(first.Refs) != 1 || first.Refs[0].SegmentID != segA {
		t.Fatalf("first manifest refs = %+v, want one ref to segA", first.Refs)
	}

	// Build completes: CmdSealChunk clears the pending sealed manifest.
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealChunk(
		first.ChunkID, base.Add(time.Minute), 2, 1024,
		base, base.Add(time.Second), base.Add(time.Second), true,
	))

	second := planUntilSealed(t, mgr, vaultID, fsm)
	if second.ChunkID == first.ChunkID {
		t.Fatal("second manifest reused first chunk ID")
	}
	if len(second.Refs) != 1 || second.Refs[0].SegmentID != segB {
		t.Fatalf("second manifest refs = %+v, want one ref to segB only", second.Refs)
	}
	if second.Refs[0].FirstRecordNumber != 0 || second.Refs[0].LastRecordNumber != 1 {
		t.Fatalf("second manifest ref = [%d,%d], want [0,1]",
			second.Refs[0].FirstRecordNumber, second.Refs[0].LastRecordNumber)
	}
}

// TestLoadSegmentViewsCachesIndexesAcrossPlanSteps guards gastrolog-3bn3q:
// planOnce must not re-open every active registry segment on each step.
func TestLoadSegmentViewsCachesIndexesAcrossPlanSteps(t *testing.T) {
	t.Parallel()
	const segmentCount = 20
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	pubAt := base.Add(time.Minute)
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	segIDs := make([]glid.GLID, segmentCount)
	for i := range segIDs {
		segIDs[i] = glid.New()
		ts := base.Add(time.Duration(i) * time.Second)
		writeCompletedSegment(t, vaultRoot, segIDs[i], vaultID, []recordForSeg{{0, ts, fmt.Sprintf("s%d", i)}})
	}

	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}
	var indexOpens atomic.Int64
	indexOpener := func(path string) (*chunking.OrderedIndex, error) {
		indexOpens.Add(1)
		return chunking.BuildOrderedIndex(path)
	}

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot:   vaultRoot,
		ChunkRoot:   filepath.Join(vaultRoot, "chunks"),
		FSM:         fsm,
		Locate:      chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:     applier,
		IsLeader:    func() bool { return true },
		Policy:      chunking.ManifestRotationPolicy{MaxRecords: 1000},
		IndexOpener: indexOpener,
	}); err != nil {
		t.Fatal(err)
	}

	for i, segID := range segIDs {
		ts := base.Add(time.Duration(i) * time.Second)
		publishSegment(t, fsm, segID, pubAt.Add(time.Duration(i)*time.Millisecond), 1, ts, ts)
	}

	ctx := t.Context()
	indexOpens.Store(0)
	if err := mgr.PlanOnce(ctx, vaultID); err != nil {
		t.Fatal(err)
	}
	if got := indexOpens.Load(); got != segmentCount {
		t.Fatalf("first PlanOnce index opens = %d, want %d (cold cache)", got, segmentCount)
	}

	indexOpens.Store(0)
	if err := mgr.PlanOnce(ctx, vaultID); err != nil {
		t.Fatal(err)
	}
	if got := indexOpens.Load(); got != 0 {
		t.Fatalf("second PlanOnce index opens = %d, want 0 (warm cache)", got)
	}
}

// TestLoadSegmentViewsIndexesAllActiveSegments ensures the planner sees every
// non-exhausted registry segment, not an arbitrary prefix cap.
func TestLoadSegmentViewsIndexesAllActiveSegments(t *testing.T) {
	t.Parallel()
	const segmentCount = 40
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	pubAt := base.Add(time.Minute)
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	segIDs := make([]glid.GLID, segmentCount)
	for i := range segIDs {
		segIDs[i] = glid.New()
		ts := base.Add(time.Duration(i) * time.Second)
		writeCompletedSegment(t, vaultRoot, segIDs[i], vaultID, []recordForSeg{{0, ts, fmt.Sprintf("s%d", i)}})
	}

	fsm := vaultctlfsm.New()
	chunkID := chunk.NewChunkID()
	applier := &fsmApplier{fsm: fsm}

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot:  vaultRoot,
		ChunkRoot:  filepath.Join(vaultRoot, "chunks"),
		FSM:        fsm,
		Locate:     chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:    applier,
		IsLeader:   func() bool { return true },
		NewChunkID: func() chunk.ChunkID { return chunkID },
		Policy:     chunking.ManifestRotationPolicy{MaxRecords: 1000},
	}); err != nil {
		t.Fatal(err)
	}

	for i, segID := range segIDs {
		ts := base.Add(time.Duration(i) * time.Second)
		publishSegment(t, fsm, segID, pubAt.Add(time.Duration(i)*time.Millisecond), 1, ts, ts)
	}

	ctx := t.Context()
	if err := mgr.PlanOnce(ctx, vaultID); err != nil {
		t.Fatal(err)
	}
	if err := mgr.PlanOnce(ctx, vaultID); err != nil {
		t.Fatal(err)
	}
	open := fsm.OpenChunk()
	if open == nil {
		t.Fatal("expected open manifest")
	}
	if len(open.Refs) != 1 {
		t.Fatalf("refs = %d, want 1", len(open.Refs))
	}
	if open.Refs[0].SegmentID != segIDs[0] {
		t.Fatalf("first ref segment = %s, want earliest EventID segment %s", open.Refs[0].SegmentID, segIDs[0])
	}
}
