package chunking_test

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

func TestRecoverOnceSealsFromExistingGLCB(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "one"}, {1, base.Add(time.Second), "two"}})

	fsm := vaultctlfsm.New()
	openedAt := base
	sealedAt := base.Add(time.Minute)
	publishSegForTest(t, fsm, segID, 1, openedAt)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 0,
		LastRecordNumber:  1,
		SliceBytes:        4096,
		RefAddedAt:        openedAt,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, sealedAt))

	_, err := chunking.BuildSealedChunk(chunking.BuildInput{
		Manifest: chunking.SealedManifest{
			ChunkID:  chunkID,
			OpenedAt: openedAt,
			SealedAt: sealedAt,
			Refs: []chunking.ManifestRef{{
				SegmentID:         segID,
				FirstRecordNumber: 0,
				LastRecordNumber:  1,
			}},
		},
		VaultID:   vaultID,
		ChunkRoot: filepath.Join(home, "chunks"),
		Locate:    chunking.HeadSegmentLocator{Root: home},
	})
	if err != nil {
		t.Fatalf("BuildSealedChunk: %v", err)
	}

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       home,
		ChunkRoot:       filepath.Join(home, "chunks"),
		FSM:             fsm,
		Locate:          chunking.HeadSegmentLocator{Root: home},
		Applier:         &flakyFSMApplier{fsm: fsm},
		IsLeader:        func() bool { return true },
	}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.RecoverOnce(context.Background(), vaultID); err != nil {
		t.Fatalf("RecoverOnce: %v", err)
	}
	entry := fsm.Get(chunkID)
	if entry == nil || entry.State != chunk.ChunkStateSealed {
		t.Fatalf("chunk entry = %+v, want Sealed", entry)
	}
	if entry.RecordCount != 2 {
		t.Fatalf("RecordCount = %d, want 2", entry.RecordCount)
	}
	if fsm.SealedManifest() != nil {
		t.Fatal("sealed manifest must clear after recovery seal")
	}
}

func TestRecoverOnceSealsOrphanActiveGLCB(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "alpha"}})

	fsm := vaultctlfsm.New()
	openedAt := base
	publishSegForTest(t, fsm, segID, 1, openedAt)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 0,
		LastRecordNumber:  0,
		SliceBytes:        2048,
		RefAddedAt:        openedAt,
	}))
	// GLCB built locally but SealOpenChunkManifest never applied — crash mid-flight.
	sealedAt := base.Add(2 * time.Minute)
	_, err := chunking.BuildSealedChunk(chunking.BuildInput{
		Manifest: chunking.SealedManifest{
			ChunkID:  chunkID,
			OpenedAt: openedAt,
			SealedAt: sealedAt,
			Refs: []chunking.ManifestRef{{
				SegmentID:         segID,
				FirstRecordNumber: 0,
				LastRecordNumber:  0,
			}},
		},
		VaultID:   vaultID,
		ChunkRoot: filepath.Join(home, "chunks"),
		Locate:    chunking.HeadSegmentLocator{Root: home},
	})
	if err != nil {
		t.Fatalf("BuildSealedChunk: %v", err)
	}

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       home,
		ChunkRoot:       filepath.Join(home, "chunks"),
		FSM:             fsm,
		Locate:          chunking.HeadSegmentLocator{Root: home},
		Applier:         &flakyFSMApplier{fsm: fsm},
		IsLeader:        func() bool { return true },
	}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.RecoverOnce(context.Background(), vaultID); err != nil {
		t.Fatalf("RecoverOnce: %v", err)
	}
	entry := fsm.Get(chunkID)
	if entry == nil || entry.State != chunk.ChunkStateSealed {
		t.Fatalf("chunk entry = %+v, want Sealed", entry)
	}
	if entry.RecordCount != 1 {
		t.Fatalf("RecordCount = %d, want 1", entry.RecordCount)
	}
}

// TestRecoverOnceSkipsSealedEntries: a chunk already Sealed cluster-wide
// whose GLCB survives on disk needs NO recovery work — registration is
// lazy (the chunk manager's on-miss resolver, gastrolog-2kmgj6), so
// recovery must not fire OnBuilt per sealed chunk. The eager re-fire this
// replaces was O(all chunks) of boot work and raced FSM replay; servability
// now comes from the resolver at first lookup, independent of recovery.
func TestRecoverOnceSkipsSealedEntries(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "one"}, {1, base.Add(time.Second), "two"}})

	fsm := vaultctlfsm.New()
	openedAt := base
	sealedAt := base.Add(time.Minute)
	publishSegForTest(t, fsm, segID, 1, openedAt)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 0,
		LastRecordNumber:  1,
		SliceBytes:        4096,
		RefAddedAt:        openedAt,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, sealedAt))

	if _, err := chunking.BuildSealedChunk(chunking.BuildInput{
		Manifest: chunking.SealedManifest{
			ChunkID:  chunkID,
			OpenedAt: openedAt,
			SealedAt: sealedAt,
			Refs: []chunking.ManifestRef{{
				SegmentID:         segID,
				FirstRecordNumber: 0,
				LastRecordNumber:  1,
			}},
		},
		VaultID:   vaultID,
		ChunkRoot: filepath.Join(home, "chunks"),
		Locate:    chunking.HeadSegmentLocator{Root: home},
	}); err != nil {
		t.Fatalf("BuildSealedChunk: %v", err)
	}
	// Seal cluster-wide BEFORE the restart-simulating manager exists.
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealChunk(chunkID, sealedAt, 2, 4096, base, base.Add(time.Second), base, true, sealedAt))

	var rebuilt []chunk.ChunkID
	var mu sync.Mutex
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       home,
		ChunkRoot:       filepath.Join(home, "chunks"),
		FSM:             fsm,
		Locate:          chunking.HeadSegmentLocator{Root: home},
		Applier:         &flakyFSMApplier{fsm: fsm},
		IsLeader:        func() bool { return false },
		OnBuilt: func(id chunk.ChunkID) {
			mu.Lock()
			rebuilt = append(rebuilt, id)
			mu.Unlock()
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.RecoverOnce(context.Background(), vaultID); err != nil {
		t.Fatalf("RecoverOnce: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(rebuilt) != 0 {
		t.Fatalf("OnBuilt fired for %v; sealed entries must skip recovery (lazy resolution owns servability)", rebuilt)
	}
}

// TestRecoveryWaitsForFSMReplay: the worker starts before the vault-ctl
// FSM has replayed (production boot order). Recovery against the empty
// FSM must not consume the once-only opportunity — it retries until the
// FSM is ready and then completes SEAL recovery: a chunk whose GLCB was
// built but whose CmdSealChunk never committed (crash mid-PostSealProcess)
// gets its seal proposed. The unguarded version scanned an empty registry
// at boot and never ran again.
func TestRecoveryWaitsForFSMReplay(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "one"}})

	fsm := vaultctlfsm.New() // NOT ready: nothing applied yet

	// GLCB already on disk from the previous "process".
	if _, err := chunking.BuildSealedChunk(chunking.BuildInput{
		Manifest: chunking.SealedManifest{
			ChunkID:  chunkID,
			OpenedAt: base,
			SealedAt: base.Add(time.Minute),
			Refs: []chunking.ManifestRef{{
				SegmentID:         segID,
				FirstRecordNumber: 0,
				LastRecordNumber:  0,
			}},
		},
		VaultID:   vaultID,
		ChunkRoot: filepath.Join(home, "chunks"),
		Locate:    chunking.HeadSegmentLocator{Root: home},
	}); err != nil {
		t.Fatalf("BuildSealedChunk: %v", err)
	}

	var mu sync.Mutex
	var rebuilt []chunk.ChunkID
	// Production ordering: a node cannot hold vault-ctl leadership before its
	// FSM has replayed — Raft serializes replay ahead of any proposal this
	// node could make. Model that here: leadership arrives only after the
	// fake replay below completes. With leadership from the start, the
	// worker's leader planner raced the direct applies and proposed its OWN
	// open manifest between publish and open ("open chunk manifest already
	// exists"), an interleaving real replay forbids (gastrolog-4cxvdi).
	var leader atomic.Bool
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       home,
		ChunkRoot:       filepath.Join(home, "chunks"),
		FSM:             fsm,
		Locate:          chunking.HeadSegmentLocator{Root: home},
		Applier:         &flakyFSMApplier{fsm: fsm},
		// Seal recovery proposes CmdSealChunk, which only the vault-ctl
		// leader commits — this scenario is the recovering leader.
		IsLeader: leader.Load,
		OnBuilt: func(id chunk.ChunkID) {
			mu.Lock()
			rebuilt = append(rebuilt, id)
			mu.Unlock()
		},
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { _ = mgr.Run(ctx); close(done) }()

	// Let the worker start and (correctly) find the FSM not ready.
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	if len(rebuilt) != 0 {
		mu.Unlock()
		t.Fatal("recovery ran against an unreplayed FSM")
	}
	mu.Unlock()

	// "Raft replay" arrives: the manifest lands SEALING (GLCB built, but
	// CmdSealChunk never committed — the crash-mid-PostSealProcess shape).
	// FSM becomes ready; recovery must now propose the missing seal.
	publishSegForTest(t, fsm, segID, 1, base)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, base))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 0, SliceBytes: 1024, RefAddedAt: base,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, base.Add(time.Minute)))

	// Replay complete — the node wins vault-ctl leadership. Wake the worker
	// so a leader-aware pass (or the ticker recovery) proposes the seal.
	leader.Store(true)
	mgr.NotifyVault(vaultID)

	deadline := time.Now().Add(5 * time.Second)
	for {
		if e := fsm.Get(chunkID); e != nil && e.IsSealed() {
			break
		}
		if time.Now().After(deadline) {
			e := fsm.Get(chunkID)
			t.Fatalf("recovery never proposed CmdSealChunk after FSM replay; entry=%+v", e)
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	<-done
}
