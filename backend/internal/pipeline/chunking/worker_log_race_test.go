package chunking_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/vaultraft/vaultctlfsm"
	"gastrolog/internal/waittest"
)

// The post-seal goroutine spawned by the sealed-manifest-cleared callback logs
// its head purge through the vault logger while the worker may be starting.
// The test orders the two with nothing the race detector counts as
// synchronization: the purge is observed only through the filesystem before
// Run starts the worker, so under -race any worker write to the logger the
// post-seal goroutine read is reported.
func TestPostSealPurgeLoggingDoesNotRaceWorkerStart(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "one"}})

	fsm := vaultctlfsm.New()
	publishSegForTest(t, fsm, segID, 1, base)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, base))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 0,
		SliceBytes: 1024, RefAddedAt: base,
	}))

	mgr := chunking.New(chunking.Config{})
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

	sealedAt := base.Add(time.Minute)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, sealedAt))
	if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("BuildOnce: %v", err)
	}
	assertHeadPresent(t, home, segID)

	// The leader's seal reaches this follower home: the cleared callback
	// spawns the post-seal goroutine, which purges and logs.
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealChunk(chunkID, sealedAt.Add(time.Second), 1, 100, base, base, base, true, sealedAt.Add(time.Second)))
	headPath := paths.HeadSegment(home, segID)
	waittest.Progress(t, "post-seal goroutine purged the head copy", func() (string, bool) {
		_, err := os.Stat(headPath)
		gone := os.IsNotExist(err)
		return fmt.Sprintf("head-gone=%t", gone), gone
	})

	runManager(t, mgr)
}
