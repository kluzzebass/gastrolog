package chunking_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkcloud "gastrolog/internal/chunk/cloud"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// These tests pin the CURRENT divergence between the build pass and restart
// recovery when the existing data.glcb on disk is corrupt (gastrolog-5do8sh
// gap 4). Do not read them as endorsing either behavior — they document it so
// a deliberate follow-up can unify the two paths:
//
//   - Build path SWALLOWS the read error: adoptExistingGLCBIfPresent
//     (build_pass.go) returns (BuildResult{}, false, nil) on a corrupt GLCB
//     and runBuildOncePass falls through to a full rebuild from segments, so
//     BuildOnce silently repairs the corruption.
//   - Recover path ERRORS: recoverBuiltGLCB (recover.go) propagates the
//     BuildResultFromExistingGLCB error, so RecoverOnce fails and never
//     rebuilds — the corrupt blob stays on disk.

// glcbCorruptions are the on-disk shapes a damaged data.glcb can take. Each
// mutation keeps the file present (os.Stat succeeds) but makes
// chunkcloud.OpenMappedBlob fail: truncation trips the minimum-size check,
// a garbaged header fails preamble validation, and a zero-length file is
// rejected as "empty GLCB" before parsing.
var glcbCorruptions = []struct {
	name    string
	corrupt func(t *testing.T, glcbPath string)
}{
	{
		name: "truncated",
		corrupt: func(t *testing.T, glcbPath string) {
			t.Helper()
			if err := os.Truncate(glcbPath, 4); err != nil {
				t.Fatalf("truncate GLCB: %v", err)
			}
		},
	},
	{
		name: "garbage header",
		corrupt: func(t *testing.T, glcbPath string) {
			t.Helper()
			f, err := os.OpenFile(glcbPath, os.O_WRONLY, 0)
			if err != nil {
				t.Fatalf("open GLCB for corruption: %v", err)
			}
			defer func() { _ = f.Close() }()
			garbage := make([]byte, 32)
			for i := range garbage {
				garbage[i] = 0xFF
			}
			if _, err := f.WriteAt(garbage, 0); err != nil {
				t.Fatalf("overwrite GLCB header: %v", err)
			}
		},
	},
	{
		name: "empty",
		corrupt: func(t *testing.T, glcbPath string) {
			t.Helper()
			if err := os.Truncate(glcbPath, 0); err != nil {
				t.Fatalf("truncate GLCB to zero: %v", err)
			}
		},
	},
}

type builtGLCBFixture struct {
	home     string
	fsm      *vaultctlfsm.FSM
	vaultID  glid.GLID
	chunkID  chunk.ChunkID
	glcbPath string
}

// setupSealingChunkWithBuiltGLCB reproduces the TestRecoverOnceSealsFromExistingGLCB
// fixture: head segments on disk, a sealed manifest pending in the FSM
// (chunk in Sealing), and a valid GLCB already materialized at
// ChunkGLCBPath — the state right before CmdSealChunk would commit.
func setupSealingChunkWithBuiltGLCB(t *testing.T) builtGLCBFixture {
	t.Helper()
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
			Refs: []chunking.ManifestRefEntry{{
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
	return builtGLCBFixture{
		home:     home,
		fsm:      fsm,
		vaultID:  vaultID,
		chunkID:  chunkID,
		glcbPath: chunking.ChunkGLCBPath(filepath.Join(home, "chunks"), chunkID),
	}
}

func registerFixtureVault(t *testing.T, fx builtGLCBFixture) *chunking.Manager {
	t.Helper()
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(fx.vaultID, chunking.VaultConfig{
		VaultRoot: fx.home,
		ChunkRoot: filepath.Join(fx.home, "chunks"),
		FSM:       fx.fsm,
		Locate:    chunking.HeadSegmentLocator{Root: fx.home},
		Applier:   &flakyFSMApplier{fsm: fx.fsm},
		IsLeader:  func() bool { return true },
	}); err != nil {
		t.Fatal(err)
	}
	return mgr
}

// TestRecoverOnceErrorsOnCorruptExistingGLCB pins the recover-path half of the
// gap 4 divergence: RecoverOnce propagates the corrupt-GLCB read error and
// performs no rebuild — the damaged blob stays on disk and the chunk stays
// unsealed. CURRENT behavior, not necessarily desired behavior.
func TestRecoverOnceErrorsOnCorruptExistingGLCB(t *testing.T) {
	t.Parallel()
	for _, tc := range glcbCorruptions {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fx := setupSealingChunkWithBuiltGLCB(t)
			tc.corrupt(t, fx.glcbPath)

			mgr := registerFixtureVault(t, fx)
			if err := mgr.RecoverOnce(context.Background(), fx.vaultID); err == nil {
				t.Fatal("RecoverOnce succeeded on a corrupt GLCB; want the read error propagated (recover path never rebuilds)")
			}

			// No adoption and no seal: the chunk must still be Sealing with the
			// manifest pending.
			entry := fx.fsm.Get(fx.chunkID)
			if entry == nil || entry.State != chunk.ChunkStateSealing {
				t.Fatalf("chunk entry = %+v, want Sealing (recovery must not seal from a corrupt GLCB)", entry)
			}
			if fx.fsm.SealedManifest() == nil {
				t.Fatal("sealed manifest must remain pending after failed recovery")
			}

			// No rebuild either: the corrupt blob is still unreadable on disk.
			if _, err := chunkcloud.OpenMappedBlob(fx.glcbPath); err == nil {
				t.Fatal("GLCB opened cleanly after RecoverOnce; recover path must not have rebuilt it")
			}
		})
	}
}

// TestBuildOnceRebuildsCorruptExistingGLCB pins the build-path half of the
// gap 4 divergence: adoptExistingGLCBIfPresent swallows the corrupt-GLCB read
// error (nolint:nilerr) and the pass falls through to a full rebuild from the
// still-present head segments — BuildOnce returns nil and silently repairs
// the corruption. CURRENT behavior, not necessarily desired behavior.
func TestBuildOnceRebuildsCorruptExistingGLCB(t *testing.T) {
	t.Parallel()
	for _, tc := range glcbCorruptions {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fx := setupSealingChunkWithBuiltGLCB(t)
			tc.corrupt(t, fx.glcbPath)

			mgr := registerFixtureVault(t, fx)
			if err := mgr.BuildOnce(t.Context(), fx.vaultID); err != nil {
				t.Fatalf("BuildOnce on a corrupt existing GLCB must fall through to a full rebuild, got: %v", err)
			}

			// The rebuild replaced the corrupt blob atomically: it opens and
			// carries every manifest record.
			blob, err := chunkcloud.OpenMappedBlob(fx.glcbPath)
			if err != nil {
				t.Fatalf("open rebuilt GLCB: %v", err)
			}
			defer func() { _ = blob.Close() }()
			if blob.Meta().RecordCount != 2 {
				t.Fatalf("rebuilt GLCB records = %d, want 2", blob.Meta().RecordCount)
			}

			entry := fx.fsm.Get(fx.chunkID)
			if entry == nil || entry.State != chunk.ChunkStateSealed {
				t.Fatalf("chunk entry = %+v, want Sealed after rebuild", entry)
			}
			if entry.RecordCount != 2 {
				t.Fatalf("RecordCount = %d, want 2", entry.RecordCount)
			}
			if fx.fsm.SealedManifest() != nil {
				t.Fatal("sealed manifest must clear after the rebuild seals")
			}
		})
	}
}
