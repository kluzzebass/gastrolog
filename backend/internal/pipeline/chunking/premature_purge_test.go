package chunking_test

// Reproduction for gastrolog-67c9b0: a segment's spans can split across two
// chunks. When chunk A seals first while chunk B is still QUEUED, no purge
// or release may destroy the segment — B's build still needs the bytes.
// Observed live as ~60 chunks pinned in 'sealing' since their referenced
// segments vanished cluster-wide (ENOENT on every home, permanently
// unqueryable records — cardinal-rule territory).

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

func TestSealDoesNotPurgeSegmentReferencedByQueuedManifest(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	chunkA := chunk.NewChunkID()
	chunkB := chunk.NewChunkID()
	home := t.TempDir()
	// Two records: span 0..0 goes to chunk A, span 1..1 to chunk B.
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "for-A"}, {1, base.Add(time.Second), "for-B"}})
	headPath := filepath.Join(home, "head", segID.String())

	fsm := vaultctlfsm.New()
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID:     segID,
		RecordCount:   2,
		ByteSize:      1,
		FirstIngestTS: base,
		LastIngestTS:  base.Add(time.Second),
		Checksum:      1,
		PublishedAt:   base,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkA, base))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkA, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 0,
		LastRecordNumber:  0,
		SliceBytes:        4096,
		RefAddedAt:        base,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkA, base.Add(time.Minute)))
	// Chunk B references the SECOND span of the same segment and is queued
	// behind A before A builds — the exact shape a seal backlog produces.
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkB, base.Add(2*time.Minute)))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkB, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 1,
		LastRecordNumber:  1,
		SliceBytes:        4096,
		RefAddedAt:        base.Add(2 * time.Minute),
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkB, base.Add(3*time.Minute)))

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

	// Build+seal chunk A (queue head).
	if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("BuildOnce (chunk A): %v", err)
	}
	if e := fsm.Get(chunkA); e == nil || e.State != chunk.ChunkStateSealed {
		t.Fatalf("chunk A after first build = %+v, want Sealed", e)
	}

	// THE INVARIANT: chunk B still references this segment from the queue,
	// so A's post-seal release/purge must leave the bytes alone.
	if _, err := os.Stat(headPath); err != nil {
		t.Fatalf("segment purged while queued chunk B still references it: %v — B is now permanently unbuildable (gastrolog-67c9b0)", err)
	}

	// And chunk B must complete from those bytes.
	if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("BuildOnce (chunk B): %v", err)
	}
	if e := fsm.Get(chunkB); e == nil || e.State != chunk.ChunkStateSealed {
		t.Fatalf("chunk B after second build = %+v, want Sealed", e)
	}
}
