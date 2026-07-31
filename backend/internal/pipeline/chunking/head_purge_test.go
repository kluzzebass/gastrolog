package chunking_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

func TestFlushHeadPurgeWaitsForAllHolderReceipts(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "one"}})
	headPath := filepath.Join(home, "head", segID.String())

	const localNode = "node-a"
	required := []string{localNode, "node-b", "node-c"}

	fsm := vaultctlfsm.New()
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID: segID, RecordCount: 1, ByteSize: 1,
		FirstIngestTS: base, LastIngestTS: base, Checksum: 1, PublishedAt: base,
		OriginNodeID: localNode, Holders: []string{localNode},
	}))
	openedAt := base
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 0,
		SliceBytes: 4096, RefAddedAt: openedAt,
	}))

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot:       home,
		ChunkRoot:       filepath.Join(home, "chunks"),
		FSM:             fsm,
		Locate:          chunking.HeadSegmentLocator{Root: home},
		Applier:         &fsmApplier{fsm: fsm},
		IsLeader:        func() bool { return true },
		RequiredHolders: func() ([]string, bool) { return required, true },
	}); err != nil {
		t.Fatal(err)
	}

	sealedAt := base.Add(time.Minute)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, sealedAt))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealChunk(chunkID, sealedAt.Add(time.Second), 1, 100, base, base, base, true, sealedAt.Add(time.Second)))
	if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("BuildOnce: %v", err)
	}
	if _, err := os.Stat(headPath); err != nil {
		t.Fatalf("head must remain until all holders ack: %v", err)
	}
}

func TestFlushHeadPurgeWaitsForLocalBuild(t *testing.T) {
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
		SegmentID: segID, RecordCount: 1, ByteSize: 1,
		FirstIngestTS: base, LastIngestTS: base, Checksum: 1, PublishedAt: base,
	}))
	openedAt := base
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 0,
		SliceBytes: 4096, RefAddedAt: openedAt,
	}))

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       home,
		ChunkRoot:       filepath.Join(home, "chunks"),
		FSM:             fsm,
		Locate:          chunking.HeadSegmentLocator{Root: home},
		Applier:         &fsmApplier{fsm: fsm},
		IsLeader:        func() bool { return true },
	}); err != nil {
		t.Fatal(err)
	}

	sealedAt := base.Add(time.Minute)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, sealedAt))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealChunk(chunkID, sealedAt.Add(time.Second), 1, 100, base, base, base, true, sealedAt.Add(time.Second)))
	if _, err := os.Stat(headPath); err != nil {
		t.Fatalf("head must remain before local build: %v", err)
	}

	if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("BuildOnce after peer seal: %v", err)
	}
	if _, err := os.Stat(headPath); !os.IsNotExist(err) {
		t.Fatalf("head should purge after local build, stat err=%v", err)
	}
}

func TestPurgeStaleHeadCatchUpDropsOrphans(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	orphan := glid.New()
	exhausted := glid.New()
	active := glid.New()
	vaultID := glid.New()
	home := t.TempDir()
	writeHeadSegment(t, home, orphan, vaultID, []recordForSeg{{0, base, "orphan"}})
	writeHeadSegment(t, home, exhausted, vaultID, []recordForSeg{{0, base, "done"}})
	writeHeadSegment(t, home, active, vaultID, []recordForSeg{{0, base, "active"}})

	fsm := vaultctlfsm.New()
	for _, spec := range []struct {
		id    glid.GLID
		count uint32
	}{
		{exhausted, 1},
		{active, 10},
	} {
		if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
			SegmentID: spec.id, RecordCount: spec.count, ByteSize: 1,
			FirstIngestTS: base, LastIngestTS: base, Checksum: 1, PublishedAt: base,
		})}); err != nil {
			t.Fatal(err)
		}
	}
	chunkID := chunk.NewChunkID()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalOpenChunkManifest(chunkID, base)}); err != nil {
		t.Fatal(err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID: active, FirstRecordNumber: 0, LastRecordNumber: 0,
		SliceBytes: 1, RefAddedAt: base,
	})}); err != nil {
		t.Fatal(err)
	}
	// Exhausted segment fully consumed by a prior sealed chunk.
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID: exhausted, FirstRecordNumber: 0, LastRecordNumber: 0,
		SliceBytes: 1, RefAddedAt: base,
	})}); err != nil {
		t.Fatal(err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, base.Add(time.Minute))}); err != nil {
		t.Fatal(err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(chunkID, base.Add(2*time.Minute), 2, 100, base, base, base, true, base.Add(2*time.Minute))}); err != nil {
		t.Fatal(err)
	}

	mgr := chunking.New(chunking.Config{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = mgr.Run(ctx)
	}()
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       home,
		ChunkRoot:       filepath.Join(home, "chunks"),
		FSM:             fsm,
		Locate:          chunking.HeadSegmentLocator{Root: home},
		IsLeader:        func() bool { return false },
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	cancel()
	wg.Wait()

	assertHeadMissing(t, home, orphan)
	assertHeadPresent(t, home, exhausted)
	assertHeadPresent(t, home, active)
}

func TestReleaseSegmentsPurgesHeadOnFSMCallback(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "one"}})

	fsm := vaultctlfsm.New()
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID: segID, RecordCount: 1, ByteSize: 1,
		FirstIngestTS: base, LastIngestTS: base, Checksum: 1, PublishedAt: base,
	}))

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       home,
		ChunkRoot:       filepath.Join(home, "chunks"),
		FSM:             fsm,
		Locate:          chunking.HeadSegmentLocator{Root: home},
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = mgr.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	applyChunkCmd(t, fsm, vaultctlfsm.MarshalReleaseSegments([]glid.GLID{segID}))
	// The ReleaseSegments callback is wake-only (purging on the Raft apply
	// goroutine deadlocked teardown); the worker's release branch performs
	// the purge — poll instead of asserting synchronously.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(paths.HeadSegment(home, segID)); os.IsNotExist(err) {
			break
		}
		if time.Now().After(deadline) {
			assertHeadMissing(t, home, segID)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func assertHeadMissing(t *testing.T, root string, id glid.GLID) {
	t.Helper()
	if _, err := os.Stat(paths.HeadSegment(root, id)); !os.IsNotExist(err) {
		t.Fatalf("head %s should be purged, stat err=%v", id, err)
	}
}

func assertHeadPresent(t *testing.T, root string, id glid.GLID) {
	t.Helper()
	if _, err := os.Stat(paths.HeadSegment(root, id)); err != nil {
		t.Fatalf("head %s should remain: %v", id, err)
	}
}

// Regression: vault-ctl leader CmdSealChunk can replicate before a follower
// home finishes building. OnSealedManifestCleared must not consume the
// post-seal slot until doneBuild, and finishBuildOnce must purge head/ once
// the local GLCB lands.
func TestManagerPurgesHeadWhenPeerSealBeforeLocalBuild(t *testing.T) {
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
		SegmentID: segID, RecordCount: 1, ByteSize: 1,
		FirstIngestTS: base, LastIngestTS: base, Checksum: 1, PublishedAt: base,
	}))
	openedAt := base
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 0,
		SliceBytes: 4096, RefAddedAt: openedAt,
	}))
	sealedAt := base.Add(time.Minute)

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       home,
		ChunkRoot:       filepath.Join(home, "chunks"),
		FSM:             fsm,
		Locate:          chunking.HeadSegmentLocator{Root: home},
		Applier:         &fsmApplier{fsm: fsm},
		IsLeader:        func() bool { return false },
	}); err != nil {
		t.Fatal(err)
	}
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, sealedAt))
	// Leader seals cluster-wide while the follower has not built yet.
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealChunk(chunkID, sealedAt.Add(time.Second), 1, 100, base, base, base, true, sealedAt.Add(time.Second)))
	if _, err := os.Stat(headPath); err != nil {
		t.Fatalf("head must remain before follower build: %v", err)
	}

	if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("follower BuildOnce after peer seal: %v", err)
	}
	if _, err := os.Stat(headPath); !os.IsNotExist(err) {
		t.Fatalf("head should purge after follower build, stat err=%v", err)
	}
}
