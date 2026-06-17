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
		VaultRoot: home,
		ChunkRoot: filepath.Join(home, "chunks"),
		FSM:       fsm,
		Locate:    chunking.HeadSegmentLocator{Root: home},
		Applier:   &fsmApplier{fsm: fsm},
		IsLeader:  func() bool { return true },
	}); err != nil {
		t.Fatal(err)
	}

	sealedAt := base.Add(time.Minute)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, sealedAt))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealChunk(chunkID, sealedAt.Add(time.Second), 1, 100, base, base, base, true))
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
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(chunkID, base.Add(2*time.Minute), 2, 100, base, base, base, true)}); err != nil {
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
		VaultRoot: home,
		ChunkRoot: filepath.Join(home, "chunks"),
		FSM:       fsm,
		Locate:    chunking.HeadSegmentLocator{Root: home},
		IsLeader:  func() bool { return false },
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
