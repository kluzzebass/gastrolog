package chunking_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	chunkcloud "gastrolog/internal/chunk/cloud"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)


// publishSegForTest registers a completed segment in the FSM registry —
// required before any manifest ref since the apply-time ghost-ref guard.
func publishSegForTest(t *testing.T, fsm *vaultctlfsm.FSM, segID glid.GLID, count uint32, ts time.Time) {
	t.Helper()
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID: segID, RecordCount: count, ByteSize: 1,
		FirstIngestTS: ts, LastIngestTS: ts, Checksum: 1, PublishedAt: ts,
	}))
}

func TestManagerBuildOnceBuildsGLCBAndAnnouncesSeal(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "one"}, {1, base.Add(time.Second), "two"}})

	fsm := vaultctlfsm.New()
	openedAt := base
	publishSegForTest(t, fsm, segID, 1, openedAt)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 0,
		LastRecordNumber:  1,
		SliceBytes:        4096,
		RefAddedAt:        openedAt,
	}))
	sealedAt := base.Add(time.Minute)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, sealedAt))

	var applied [][]byte
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot: home,
		ChunkRoot: filepath.Join(home, "chunks"),
		FSM:       fsm,
		Locate:    chunking.HeadSegmentLocator{Root: home},
		Applier:   &recordingApplier{out: &applied, fsm: fsm},
		IsLeader:  func() bool { return true },
	}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("BuildOnce: %v", err)
	}
	// SealChunk, and — now that the fixture registers its segment like
	// production — the post-seal ReleaseSegments proposal for the fully
	// consumed segment.
	if len(applied) < 1 || len(applied) > 2 {
		t.Fatalf("applied commands = %d, want SealChunk (+ optional ReleaseSegments)", len(applied))
	}

	glcbPath := chunking.ChunkGLCBPath(filepath.Join(home, "chunks"), chunkID)
	f, err := os.Open(glcbPath)
	if err != nil {
		t.Fatalf("open GLCB: %v", err)
	}
	defer f.Close()
	rd, err := chunkcloud.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	if rd.Meta().RecordCount != 2 {
		t.Fatalf("GLCB records = %d, want 2", rd.Meta().RecordCount)
	}

	entry := fsm.Get(chunkID)
	if entry == nil || entry.State != chunk.ChunkStateSealed {
		t.Fatalf("chunk entry after BuildOnce = %+v", entry)
	}
	if fsm.SealedManifest() != nil {
		t.Fatal("sealed manifest must clear after SealChunk")
	}

	// Stage throughput counters (gastrolog-10n6k8): this home materialized
	// the sealed GLCB, so the seal counters must reflect it.
	stats := mgr.SealStats()
	if len(stats) != 1 || stats[0].VaultID != vaultID {
		t.Fatalf("SealStats = %+v, want one entry for %s", stats, vaultID)
	}
	if stats[0].SealedRecords != 2 || stats[0].SealedBytes == 0 {
		t.Fatalf("sealed = %d records / %d bytes, want 2 records and > 0 bytes",
			stats[0].SealedRecords, stats[0].SealedBytes)
	}
}

func TestManagerBuildOnceReleasesCompletedRegistry(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "one"}})

	fsm := vaultctlfsm.New()
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID:     segID,
		RecordCount:   1,
		ByteSize:      1,
		FirstIngestTS: base,
		LastIngestTS:  base,
		Checksum:      1,
		PublishedAt:   base,
	}))
	openedAt := base
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 0,
		LastRecordNumber:  0,
		SliceBytes:        4096,
		RefAddedAt:        openedAt,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, base.Add(time.Minute)))

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot: home,
		ChunkRoot: filepath.Join(home, "chunks"),
		FSM:       fsm,
		Locate:    chunking.HeadSegmentLocator{Root: home},
		Applier:   &fsmApplier{fsm: fsm},
		IsLeader:  func() bool { return true },
	}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("BuildOnce: %v", err)
	}
	if fsm.GetCompletedSegment(segID) != nil {
		t.Fatal("completed segment registry entry should be released")
	}
}

func TestManagerBuildOncePurgesHeadStaging(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "one"}})
	headPath := filepath.Join(home, "head", segID.String())

	fsm := vaultctlfsm.New()
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID:     segID,
		RecordCount:   1,
		ByteSize:      1,
		FirstIngestTS: base,
		LastIngestTS:  base,
		Checksum:      1,
		PublishedAt:   base,
	}))
	openedAt := base
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 0,
		LastRecordNumber:  0,
		SliceBytes:        4096,
		RefAddedAt:        openedAt,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, base.Add(time.Minute)))

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot: home,
		ChunkRoot: filepath.Join(home, "chunks"),
		FSM:       fsm,
		Locate:    chunking.HeadSegmentLocator{Root: home},
		Applier:   &fsmApplier{fsm: fsm},
		IsLeader:  func() bool { return true },
	}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("BuildOnce: %v", err)
	}
	if _, err := os.Stat(headPath); !os.IsNotExist(err) {
		t.Fatalf("head segment should be purged after build+seal, stat err=%v", err)
	}
}

func TestManagerPurgesHeadWhenSealWinsElsewhere(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	homeA := t.TempDir()
	homeB := t.TempDir()
	writeHeadSegment(t, homeA, segID, vaultID, []recordForSeg{{0, base, "one"}})
	writeHeadSegment(t, homeB, segID, vaultID, []recordForSeg{{0, base, "one"}})
	headB := filepath.Join(homeB, "head", segID.String())

	fsm := vaultctlfsm.New()
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID: segID, RecordCount: 1, ByteSize: 1,
		FirstIngestTS: base, LastIngestTS: base, Checksum: 1, PublishedAt: base,
	}))
	openedAt := base
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 0,
		SliceBytes: 4096, RefAddedAt: openedAt,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, base.Add(time.Minute)))

	spec := func(home string, applier chunking.VaultCtlApplier, leader bool) chunking.VaultConfig {
		return chunking.VaultConfig{
			VaultRoot: home,
			ChunkRoot: filepath.Join(home, "chunks"),
			FSM:       fsm,
			Locate:    chunking.HeadSegmentLocator{Root: home},
			Applier:   applier,
			IsLeader:  func() bool { return leader },
		}
	}

	mgrA := chunking.New(chunking.Config{})
	if err := mgrA.RegisterVault(vaultID, spec(homeA, &fsmApplier{fsm: fsm}, true)); err != nil {
		t.Fatal(err)
	}
	mgrB := chunking.New(chunking.Config{})
	if err := mgrB.RegisterVault(vaultID, spec(homeB, &flakyFSMApplier{fsm: fsm, fail: 1}, false)); err != nil {
		t.Fatal(err)
	}
	// B's worker must run: the ReleaseSegments callback is wake-only and the
	// worker's release branch performs the purge (gastrolog-38snf4).
	ctxB, cancelB := context.WithCancel(context.Background())
	doneB := make(chan struct{})
	go func() { _ = mgrB.Run(ctxB); close(doneB) }()
	t.Cleanup(func() {
		cancelB()
		<-doneB
	})

	// Home B builds locally; only the vault-ctl leader proposes CmdSealChunk.
	if err := mgrB.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("home B BuildOnce: %v", err)
	}
	if _, err := os.Stat(headB); err != nil {
		t.Fatalf("home B head should remain before leader seal: %v", err)
	}

	if err := mgrA.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("home A BuildOnce: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(headB); os.IsNotExist(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("home B head should purge when peer seals")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestManagerBuildOnceWaitsForHoldersBeforeRelease(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "one"}})

	fsm := vaultctlfsm.New()
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID: segID, RecordCount: 1, ByteSize: 1,
		FirstIngestTS: base, LastIngestTS: base, Checksum: 1, PublishedAt: base,
	}))
	openedAt := base
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 0,
		SliceBytes: 4096, RefAddedAt: openedAt,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, base.Add(time.Minute)))

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot: home,
		ChunkRoot: filepath.Join(home, "chunks"),
		FSM:       fsm,
		Locate:    chunking.HeadSegmentLocator{Root: home},
		Applier:   &fsmApplier{fsm: fsm},
		IsLeader:  func() bool { return true },
		RequiredHolders: func() []string {
			return []string{"home-a", "home-b"}
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("BuildOnce: %v", err)
	}
	if fsm.GetCompletedSegment(segID) == nil {
		t.Fatal("registry entry must remain until all holders ack")
	}

	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAckSegmentHolder(segID, "home-a"))
	if err := mgr.ReleaseOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("ReleaseOnce after one ack: %v", err)
	}
	if fsm.GetCompletedSegment(segID) == nil {
		t.Fatal("registry entry must remain until every required holder acks")
	}

	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAckSegmentHolder(segID, "home-b"))
	if err := mgr.ReleaseOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("ReleaseOnce after all acks: %v", err)
	}
	if fsm.GetCompletedSegment(segID) != nil {
		t.Fatal("completed segment registry entry should be released after holder acks")
	}
}

func TestManagerWorkerReleasesAfterBuildWithoutNewHolderAck(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "one"}})

	fsm := vaultctlfsm.New()
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID: segID, RecordCount: 1, ByteSize: 1,
		FirstIngestTS: base, LastIngestTS: base, Checksum: 1, PublishedAt: base,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAckSegmentHolder(segID, "home-a"))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAckSegmentHolder(segID, "home-b"))
	openedAt := base
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 0,
		SliceBytes: 4096, RefAddedAt: openedAt,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, base.Add(time.Minute)))

	mgr := chunking.New(chunking.Config{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- mgr.Run(ctx) }()

	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot: home,
		ChunkRoot: filepath.Join(home, "chunks"),
		FSM:       fsm,
		Locate:    chunking.HeadSegmentLocator{Root: home},
		Applier:   &fsmApplier{fsm: fsm},
		IsLeader:  func() bool { return true },
		RequiredHolders: func() []string {
			return []string{"home-a", "home-b"}
		},
	}); err != nil {
		t.Fatal(err)
	}

	if err := mgr.BuildOnce(ctx, vaultID); err != nil {
		t.Fatalf("BuildOnce: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fsm.GetCompletedSegment(segID) == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("registry entry must release after build without a new holder ack")
}

func TestBuildMaterializesMissingSegments(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	present := glid.New()
	missing := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()
	writeHeadSegment(t, home, present, vaultID, []recordForSeg{{0, base, "ok"}})

	fsm := vaultctlfsm.New()
	openedAt := base
	publishSegForTest(t, fsm, present, 1, openedAt)
	publishSegForTest(t, fsm, missing, 1, openedAt)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         present,
		FirstRecordNumber: 0,
		LastRecordNumber:  0,
		SliceBytes:        2048,
		RefAddedAt:        openedAt,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         missing,
		FirstRecordNumber: 0,
		LastRecordNumber:  0,
		SliceBytes:        2048,
		RefAddedAt:        openedAt,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, base.Add(time.Minute)))

	var collects atomic.Int32
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot: home,
		ChunkRoot: filepath.Join(home, "chunks"),
		FSM:       fsm,
		Locate:    chunking.HeadSegmentLocator{Root: home},
		Collector: collectorSpy{&collects},
		IsLeader:  func() bool { return true },
	}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.BuildOnce(t.Context(), vaultID); err == nil {
		t.Fatal("expected missing segment error on leader")
	}
	if collects.Load() != 1 {
		t.Fatalf("segment collects = %d, want 1", collects.Load())
	}
}

func TestManagerFollowerSkipsQuietlyWhenSegmentsMissing(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	present := glid.New()
	missing := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()
	writeHeadSegment(t, home, present, vaultID, []recordForSeg{{0, base, "ok"}})

	fsm := vaultctlfsm.New()
	openedAt := base
	publishSegForTest(t, fsm, present, 1, openedAt)
	publishSegForTest(t, fsm, missing, 1, openedAt)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         present,
		FirstRecordNumber: 0,
		LastRecordNumber:  0,
		SliceBytes:        2048,
		RefAddedAt:        openedAt,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         missing,
		FirstRecordNumber: 0,
		LastRecordNumber:  0,
		SliceBytes:        2048,
		RefAddedAt:        openedAt,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, base.Add(time.Minute)))

	var collects atomic.Int32
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot: home,
		ChunkRoot: filepath.Join(home, "chunks"),
		FSM:       fsm,
		Locate:    chunking.HeadSegmentLocator{Root: home},
		Collector: collectorSpy{&collects},
		IsLeader:  func() bool { return false },
	}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("follower BuildOnce should skip quietly: %v", err)
	}
	if collects.Load() != 1 {
		t.Fatalf("segment collects = %d, want 1", collects.Load())
	}
}

func TestManagerBuildsOnSealEvent(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "one"}})

	fsm := vaultctlfsm.New()
	openedAt := base
	publishSegForTest(t, fsm, segID, 1, openedAt)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 0,
		LastRecordNumber:  0,
		SliceBytes:        1024,
		RefAddedAt:        openedAt,
	}))

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot: home,
		ChunkRoot: filepath.Join(home, "chunks"),
		FSM:       fsm,
		Locate:    chunking.HeadSegmentLocator{Root: home},
		IsLeader:  func() bool { return false },
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	sealedAt := base.Add(time.Minute)
	go func() {
		time.Sleep(20 * time.Millisecond)
		applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, sealedAt))
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	if err := mgr.Run(ctx); err != nil && err != context.Canceled {
		t.Fatalf("Run: %v", err)
	}

	glcbPath := chunking.ChunkGLCBPath(filepath.Join(home, "chunks"), chunkID)
	if _, err := os.Stat(glcbPath); err != nil {
		t.Fatalf("GLCB not built after sealed-manifest callback: %v", err)
	}
}

func TestManagerFollowerHomeBuildsWithoutProposingSealChunk(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "one"}})

	fsm := vaultctlfsm.New()
	openedAt := base
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt))
	publishSegForTest(t, fsm, segID, 1, base)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 0,
		LastRecordNumber:  0,
		SliceBytes:        1024,
		RefAddedAt:        openedAt,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, base.Add(time.Minute)))

	var applied [][]byte
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot: home,
		ChunkRoot: filepath.Join(home, "chunks"),
		FSM:       fsm,
		Locate:    chunking.HeadSegmentLocator{Root: home},
		Applier:   &recordingApplier{out: &applied, fsm: fsm},
		IsLeader:  func() bool { return false },
	}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("BuildOnce: %v", err)
	}
	if len(applied) != 0 {
		t.Fatalf("follower applied %d commands, want 0 (leader seals)", len(applied))
	}
	glcbPath := chunking.ChunkGLCBPath(filepath.Join(home, "chunks"), chunkID)
	if _, err := os.Stat(glcbPath); err != nil {
		t.Fatalf("follower GLCB: %v", err)
	}
	if fsm.SealedManifest() == nil {
		t.Fatal("sealed manifest must remain until vault-ctl leader seals")
	}
}

type recordingApplier struct {
	out *[][]byte
	fsm *vaultctlfsm.FSM
}

func (a *recordingApplier) Apply(data []byte) error {
	cp := append([]byte(nil), data...)
	*a.out = append(*a.out, cp)
	if a.fsm != nil {
		if result := a.fsm.Apply(&hraft.Log{Data: data}); result != nil {
			if err, ok := result.(error); ok {
				return err
			}
		}
	}
	return nil
}

type collectorSpy struct {
	n *atomic.Int32
}

func (c collectorSpy) CollectSegments(_ context.Context, _ []glid.GLID) error {
	c.n.Add(1)
	return nil
}

func (c collectorSpy) Nudge() {
	c.n.Add(1)
}

type segmentCollectorRecorder struct {
	calls atomic.Int32
	ids   []glid.GLID
}

func (n *segmentCollectorRecorder) Nudge() {
	n.calls.Add(1)
}

func (n *segmentCollectorRecorder) CollectSegments(_ context.Context, segmentIDs []glid.GLID) error {
	n.calls.Add(1)
	n.ids = append([]glid.GLID(nil), segmentIDs...)
	return nil
}

func TestBuildMaterializesSpecificSegmentIDs(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	present := glid.New()
	missing := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()
	writeHeadSegment(t, home, present, vaultID, []recordForSeg{{0, base, "ok"}})

	fsm := vaultctlfsm.New()
	openedAt := base
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt))
	publishSegForTest(t, fsm, present, 1, base)
	publishSegForTest(t, fsm, missing, 1, base)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         present,
		FirstRecordNumber: 0,
		LastRecordNumber:  0,
		SliceBytes:        2048,
		RefAddedAt:        openedAt,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         missing,
		FirstRecordNumber: 0,
		LastRecordNumber:  0,
		SliceBytes:        2048,
		RefAddedAt:        openedAt,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, base.Add(time.Minute)))

	rec := &segmentCollectorRecorder{}
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot: home,
		ChunkRoot: filepath.Join(home, "chunks"),
		FSM:       fsm,
		Locate:    chunking.HeadSegmentLocator{Root: home},
		Collector: rec,
		IsLeader:  func() bool { return true },
	}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.BuildOnce(t.Context(), vaultID); err == nil {
		t.Fatal("expected missing segment error on leader")
	}
	if rec.calls.Load() != 1 {
		t.Fatalf("segment collects = %d, want 1", rec.calls.Load())
	}
	if len(rec.ids) != 1 || rec.ids[0] != missing {
		t.Fatalf("CollectSegments ids = %v, want [%s]", rec.ids, missing)
	}
}

func applyChunkCmd(t *testing.T, fsm *vaultctlfsm.FSM, data []byte) {
	t.Helper()
	if err := fsm.Apply(&hraft.Log{Data: data}); err != nil {
		t.Fatalf("apply: %v", err)
	}
}

func TestSealChunkClearsSealedManifest(t *testing.T) {
	t.Parallel()
	fsm := vaultctlfsm.New()
	chunkID := chunk.NewChunkID()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, now))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, now.Add(time.Minute)))
	if fsm.SealedManifest() == nil {
		t.Fatal("expected sealed manifest before SealChunk")
	}
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealChunk(chunkID, now.Add(2*time.Minute), 1, 100, now, now, now, true, now.Add(2*time.Minute)))
	if fsm.SealedManifest() != nil {
		t.Fatal("sealed manifest must clear after SealChunk")
	}
}

func TestRecordingApplierSealChunkProto(t *testing.T) {
	t.Parallel()
	id := chunk.NewChunkID()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	data := vaultctlfsm.MarshalSealChunk(id, now, 10, 500, now, now, now, true, now)
	var cmd gastrologv1.VaultCtlCommand
	if err := proto.Unmarshal(data, &cmd); err != nil {
		t.Fatal(err)
	}
	if cmd.GetSealChunk() == nil {
		t.Fatal("expected SealChunk command")
	}
}

func TestManagerBuildOnceFollowerHomeBuildsGLCBWithoutSealing(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "one"}})

	fsm := vaultctlfsm.New()
	openedAt := base
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt))
	publishSegForTest(t, fsm, segID, 1, base)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 0,
		LastRecordNumber:  0,
		SliceBytes:        1024,
		RefAddedAt:        openedAt,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, base.Add(time.Minute)))

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot: home,
		ChunkRoot: filepath.Join(home, "chunks"),
		FSM:       fsm,
		Locate:    chunking.HeadSegmentLocator{Root: home},
		Applier:   &flakyFSMApplier{fsm: fsm},
		IsLeader:  func() bool { return false },
	}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("follower BuildOnce: %v", err)
	}
	if fsm.SealedManifest() == nil {
		t.Fatal("sealed manifest must remain until vault-ctl leader seals")
	}
	glcbPath := chunking.ChunkGLCBPath(filepath.Join(home, "chunks"), chunkID)
	if _, err := os.Stat(glcbPath); err != nil {
		t.Fatalf("follower GLCB: %v", err)
	}
	entry := fsm.Get(chunkID)
	if entry == nil || entry.State != chunk.ChunkStateSealing {
		t.Fatalf("chunk entry = %+v, want sealing", entry)
	}
}

func TestManagerBuildOnceUsesExistingGLCBWithoutSegments(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "one"}})

	fsm := vaultctlfsm.New()
	openedAt := base
	publishSegForTest(t, fsm, segID, 1, openedAt)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 0,
		LastRecordNumber:  0,
		SliceBytes:        1024,
		RefAddedAt:        openedAt,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, base.Add(time.Minute)))

	follower := chunking.New(chunking.Config{})
	if err := follower.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot: home,
		ChunkRoot: filepath.Join(home, "chunks"),
		FSM:       fsm,
		Locate:    chunking.HeadSegmentLocator{Root: home},
		Applier:   &flakyFSMApplier{fsm: fsm},
		IsLeader:  func() bool { return false },
	}); err != nil {
		t.Fatal(err)
	}
	if err := follower.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("follower BuildOnce: %v", err)
	}
	glcbPath := chunking.ChunkGLCBPath(filepath.Join(home, "chunks"), chunkID)
	if _, err := os.Stat(glcbPath); err != nil {
		t.Fatalf("follower GLCB: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(home, "head")); err != nil {
		t.Fatal(err)
	}

	leader := chunking.New(chunking.Config{})
	if err := leader.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot: home,
		ChunkRoot: filepath.Join(home, "chunks"),
		FSM:       fsm,
		Locate:    chunking.HeadSegmentLocator{Root: home},
		Applier:   &flakyFSMApplier{fsm: fsm},
		IsLeader:  func() bool { return true },
	}); err != nil {
		t.Fatal(err)
	}
	if err := leader.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("leader BuildOnce with existing GLCB: %v", err)
	}
	entry := fsm.Get(chunkID)
	if entry == nil || entry.State != chunk.ChunkStateSealed {
		t.Fatalf("chunk entry = %+v, want Sealed", entry)
	}
	if fsm.SealedManifest() != nil {
		t.Fatal("sealed manifest must clear after leader seals")
	}
}

func TestManagerBuildOnceSealsAfterLeadershipTransfer(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "one"}})

	fsm := vaultctlfsm.New()
	openedAt := base
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt))
	publishSegForTest(t, fsm, segID, 1, base)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 0,
		LastRecordNumber:  0,
		SliceBytes:        1024,
		RefAddedAt:        openedAt,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, base.Add(time.Minute)))

	var leader atomic.Bool
	leader.Store(false)
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot: home,
		ChunkRoot: filepath.Join(home, "chunks"),
		FSM:       fsm,
		Locate:    chunking.HeadSegmentLocator{Root: home},
		Applier:   &flakyFSMApplier{fsm: fsm},
		IsLeader:  leader.Load,
	}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("follower BuildOnce: %v", err)
	}
	if fsm.SealedManifest() == nil {
		t.Fatal("sealed manifest must remain until vault-ctl leader seals")
	}

	leader.Store(true)
	if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("leader BuildOnce after transfer: %v", err)
	}
	entry := fsm.Get(chunkID)
	if entry == nil || entry.State != chunk.ChunkStateSealed {
		t.Fatalf("chunk entry = %+v, want Sealed after leadership transfer", entry)
	}
	if fsm.SealedManifest() != nil {
		t.Fatal("sealed manifest must clear after leader seals")
	}
}

func TestManagerBuildOnceRetriesSealApplyAfterFailure(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "one"}})

	fsm := vaultctlfsm.New()
	openedAt := base
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt))
	publishSegForTest(t, fsm, segID, 1, base)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 0,
		LastRecordNumber:  0,
		SliceBytes:        1024,
		RefAddedAt:        openedAt,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, base.Add(time.Minute)))

	applier := &flakyFSMApplier{fsm: fsm, fail: 1}
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot: home,
		ChunkRoot: filepath.Join(home, "chunks"),
		FSM:       fsm,
		Locate:    chunking.HeadSegmentLocator{Root: home},
		Applier:   applier,
		IsLeader:  func() bool { return true },
	}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.BuildOnce(t.Context(), vaultID); err == nil {
		t.Fatal("expected first SealChunk apply to fail")
	}
	if fsm.SealedManifest() == nil {
		t.Fatal("sealed manifest must remain until SealChunk applies")
	}
	if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("second BuildOnce: %v", err)
	}
	if fsm.SealedManifest() != nil {
		t.Fatal("sealed manifest must clear after successful SealChunk retry")
	}
}

type flakyFSMApplier struct {
	fsm  *vaultctlfsm.FSM
	fail int
}

func (a *flakyFSMApplier) Apply(data []byte) error {
	if a.fail > 0 {
		a.fail--
		return errors.New("transient raft apply failure")
	}
	if result := a.fsm.Apply(&hraft.Log{Data: data}); result != nil {
		if err, ok := result.(error); ok {
			return err
		}
	}
	return nil
}

func TestManagerUnregisterVault(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	fsm := vaultctlfsm.New()
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot: t.TempDir(),
		ChunkRoot: filepath.Join(t.TempDir(), "chunks"),
		FSM:       fsm,
		Locate:    chunking.HeadSegmentLocator{Root: t.TempDir()},
		IsLeader:  func() bool { return true },
	}); err != nil {
		t.Fatal(err)
	}
	mgr.UnregisterVault(vaultID)
	if err := mgr.PlanOnce(context.Background(), vaultID); !errors.Is(err, chunking.ErrUnknownVault) {
		t.Fatalf("PlanOnce() = %v, want ErrUnknownVault", err)
	}
}

func TestRewireVaultFSMRebindsLiveRegistry(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "one"}})

	staleFSM := vaultctlfsm.New()
	liveFSM := vaultctlfsm.New()
	applyChunkCmd(t, liveFSM, vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID:   segID,
		RecordCount: 1,
		PublishedAt: base,
	}))

	var applied [][]byte
	applier := &recordingApplier{out: &applied, fsm: liveFSM}
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot: home,
		ChunkRoot: filepath.Join(home, "chunks"),
		FSM:       staleFSM,
		Locate:    chunking.HeadSegmentLocator{Root: home},
		Applier:   applier,
		IsLeader:  func() bool { return true },
	}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.RewireVaultFSM(vaultID, liveFSM, applier); err != nil {
		t.Fatalf("RewireVaultFSM: %v", err)
	}
	if err := mgr.PlanOnce(context.Background(), vaultID); err != nil {
		t.Fatalf("PlanOnce after rewire: %v", err)
	}
	if len(applied) == 0 {
		t.Fatal("expected OpenChunkManifest apply after rewire")
	}
}
