package chunking_test

import (
	"context"
	"path/filepath"
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
			Refs: []chunking.ManifestRefEntry{{
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
		VaultRoot: home,
		ChunkRoot: filepath.Join(home, "chunks"),
		FSM:       fsm,
		Locate:    chunking.HeadSegmentLocator{Root: home},
		Applier:   &flakyFSMApplier{fsm: fsm},
		IsLeader:  func() bool { return true },
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
			Refs: []chunking.ManifestRefEntry{{
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
		VaultRoot: home,
		ChunkRoot: filepath.Join(home, "chunks"),
		FSM:       fsm,
		Locate:    chunking.HeadSegmentLocator{Root: home},
		Applier:   &flakyFSMApplier{fsm: fsm},
		IsLeader:  func() bool { return true },
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
