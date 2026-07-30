package chunking

// Coverage for the holder gate: RequiredHolders is mandatory at chunking
// registration and rejected outright, the unresolved-placement state fails
// every gate closed, and the
// explicit NoRequiredHolders opt-out reproduces the ungated single-node
// behavior deliberately rather than by omission.

import (
	"strings"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// TestRegisterVaultRejectsNilRequiredHolders: the nil test shortcut is gone —
// registration without a required-holders source must fail loudly instead of
// silently running an ungated release path.
func TestRegisterVaultRejectsNilRequiredHolders(t *testing.T) {
	t.Parallel()
	mgr := New(Config{})
	err := mgr.RegisterVault(glid.New(), VaultConfig{
		VaultRoot: t.TempDir(),
		ChunkRoot: t.TempDir(),
		FSM:       vaultctlfsm.New(),
		Locate:    HeadSegmentLocator{Root: t.TempDir()},
	})
	if err == nil {
		t.Fatal("RegisterVault accepted a nil RequiredHolders")
	}
	if !strings.Contains(err.Error(), "required-holders") {
		t.Fatalf("error = %v, want required-holders rejection", err)
	}
}

// sealedReleasableFSM builds an FSM where segID is fully consumed into a
// sealed chunk that reached RF on two homes — releasable and purge-eligible
// whenever the placement lookup resolves.
func sealedReleasableFSM(t *testing.T, segID glid.GLID) *vaultctlfsm.FSM {
	t.Helper()
	now := time.Unix(0, 1_700_000_000_000).UTC()
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
	apply(vaultctlfsm.MarshalAckSegmentHolder(segID, "home-a"))
	apply(vaultctlfsm.MarshalAckSegmentHolder(segID, "home-b"))
	chunkAt := now.Add(time.Minute)
	apply(vaultctlfsm.MarshalOpenChunkManifest(chunkID, now))
	apply(vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 0, SliceBytes: 1, RefAddedAt: now,
	}))
	apply(vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, chunkAt))
	apply(vaultctlfsm.MarshalSealChunk(chunkID, chunkAt, 1, 1, now, now, now, true, chunkAt))
	for _, node := range []string{"home-a", "home-b"} {
		data, err := vaultctlfsm.MarshalAckChunkHolders([]chunk.ChunkID{chunkID}, node)
		if err != nil {
			t.Fatalf("marshal ack chunk holders: %v", err)
		}
		apply(data)
	}
	return fsm
}

// TestUnresolvedPlacementFailsReleaseAndPurgeClosed: a segment that is fully
// ready to go — consumed, sealed, every holder ack'd, chunk at RF — must NOT
// release or purge while the placement lookup is unresolved. Empty-required
// used to read as "no holder gate", which dropped segments on multi-home
// vaults whenever the lookup transiently failed.
func TestUnresolvedPlacementFailsReleaseAndPurgeClosed(t *testing.T) {
	t.Parallel()
	segID := glid.New()
	required := []string{"home-a", "home-b"}
	fsm := sealedReleasableFSM(t, segID)

	// Resolved: the gate opens (sanity for the fail-closed assertions below).
	if !testMayRelease(fsm, segID, required, true, 2, 0, time.Time{}) {
		t.Fatal("fixture broken: segment should release when placement is resolved")
	}
	if !mayPurgeHeadAfterBuild(fsm, segID, required, true, 2) {
		t.Fatal("fixture broken: head purge should pass when placement is resolved")
	}

	// Unresolved: both gates fail closed regardless of readiness.
	if testMayRelease(fsm, segID, nil, false, 2, 0, time.Time{}) {
		t.Fatal("release gate opened on an unresolved placement lookup")
	}
	if mayPurgeHeadAfterBuild(fsm, segID, nil, false, 2) {
		t.Fatal("head purge allowed on an unresolved placement lookup")
	}
}

// TestNoRequiredHoldersReleasesExhaustedSegments: the explicit opt-out is the
// old nil behavior on purpose — a fully consumed segment releases and its
// head copy purges without any holder acks (single-node vault, nothing to
// replicate to).
func TestNoRequiredHoldersReleasesExhaustedSegments(t *testing.T) {
	t.Parallel()
	segID := glid.New()
	fsm := sealedReleasableFSM(t, segID)
	required, resolved := NoRequiredHolders()

	if !testMayRelease(fsm, segID, required, resolved, 0, 0, time.Time{}) {
		t.Fatal("explicit no-holder-gate vault should release exhausted segments")
	}
	if !mayPurgeHeadAfterBuild(fsm, segID, required, resolved, 0) {
		t.Fatal("explicit no-holder-gate vault should purge head after build")
	}
}
