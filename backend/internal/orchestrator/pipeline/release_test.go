package pipeline

import (
	"errors"
	"os"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// A non-home origin holds its completed segments in completed/ until homes
// pull them; the vault-ctl release is the only signal that every home holds
// them in sealed chunks. These tests pin both halves: released segments leave
// completed/, and nothing else does.

func originOnlySpec(vaultID glid.GLID, root string, fsm *vaultctlfsm.FSM) VaultSpec {
	return VaultSpec{
		VaultID:    vaultID,
		Origin:     true,
		OriginRoot: root,
		Publisher:  &recordingPublisher{},
		FSM:        fsm,
	}
}

func applyPublish(t *testing.T, fsm *vaultctlfsm.FSM, ids ...glid.GLID) {
	t.Helper()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	for _, id := range ids {
		if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
			SegmentID: id, RecordCount: 1, ByteSize: 1,
			FirstIngestTS: base, LastIngestTS: base, Checksum: 1, PublishedAt: base,
		})}); err != nil {
			t.Fatalf("PublishCompletedSegment: %v", err)
		}
	}
}

func applyRelease(t *testing.T, fsm *vaultctlfsm.FSM, ids ...glid.GLID) {
	t.Helper()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalReleaseSegments(ids)}); err != nil {
		t.Fatalf("ReleaseSegments: %v", err)
	}
}

func writeCompleted(t *testing.T, root string, ids ...glid.GLID) {
	t.Helper()
	if err := paths.EnsureSegmentationDirs(root); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if err := os.WriteFile(paths.CompletedSegment(root, id), []byte("segment"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func assertCompleted(t *testing.T, root string, id glid.GLID, want bool) {
	t.Helper()
	_, err := os.Stat(paths.CompletedSegment(root, id))
	switch {
	case want && err != nil:
		t.Fatalf("completed/%s must still exist: %v", id, err)
	case !want && !os.IsNotExist(err):
		t.Fatalf("completed/%s must be purged, stat err=%v", id, err)
	}
}

func TestSupervisorNonHomeOriginPurgesCompletedOnRelease(t *testing.T) {
	t.Parallel()
	vaultID, released := glid.New(), glid.New()
	root := t.TempDir()
	fsm := vaultctlfsm.New()
	sup := New(Config{NodeID: glid.New(), Table: allRoute(t, vaultID)})
	if err := sup.RegisterVault(originOnlySpec(vaultID, root, fsm)); err != nil {
		t.Fatalf("RegisterVault: %v", err)
	}
	writeCompleted(t, root, released)
	applyPublish(t, fsm, released)

	applyRelease(t, fsm, released)

	assertCompleted(t, root, released, false)
}

// TestSupervisorNonHomeOriginKeepsUnreleasedSegments: a published segment
// that homes may still need to pull, and a segment awaiting its publish, both
// survive registration and an unrelated release.
func TestSupervisorNonHomeOriginKeepsUnreleasedSegments(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	published, unpublished, other := glid.New(), glid.New(), glid.New()
	root := t.TempDir()
	fsm := vaultctlfsm.New()
	writeCompleted(t, root, published, unpublished, other)
	applyPublish(t, fsm, published, other)

	sup := New(Config{NodeID: glid.New(), Table: allRoute(t, vaultID)})
	if err := sup.RegisterVault(originOnlySpec(vaultID, root, fsm)); err != nil {
		t.Fatalf("RegisterVault: %v", err)
	}
	assertCompleted(t, root, published, true)
	assertCompleted(t, root, unpublished, true)

	applyRelease(t, fsm, other)

	assertCompleted(t, root, other, false)
	assertCompleted(t, root, published, true)
	assertCompleted(t, root, unpublished, true)
}

// TestSupervisorRegisterPurgesSegmentsReleasedBeforeRegistration is the
// restart case: the release applied while this origin was down (or before
// the vault registered), so no release event reaches the new subscription.
func TestSupervisorRegisterPurgesSegmentsReleasedBeforeRegistration(t *testing.T) {
	t.Parallel()
	vaultID, released, live := glid.New(), glid.New(), glid.New()
	root := t.TempDir()
	fsm := vaultctlfsm.New()
	writeCompleted(t, root, released, live)
	applyPublish(t, fsm, released, live)
	applyRelease(t, fsm, released)

	sup := New(Config{NodeID: glid.New(), Table: allRoute(t, vaultID)})
	if err := sup.RegisterVault(originOnlySpec(vaultID, root, fsm)); err != nil {
		t.Fatalf("RegisterVault: %v", err)
	}

	assertCompleted(t, root, released, false)
	assertCompleted(t, root, live, true)
}

// TestSupervisorRewireAfterCtlRestoreRebindsRelease: a snapshot install
// replaces the vault-ctl sub-FSM. The non-home origin must consume releases
// the new FSM carries (applied before the rebind) and releases applied to it
// afterwards; the replaced FSM no longer drives purges.
func TestSupervisorRewireAfterCtlRestoreRebindsRelease(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	inSnapshot, afterRestore, stale := glid.New(), glid.New(), glid.New()
	root := t.TempDir()
	oldFSM := vaultctlfsm.New()
	sup := New(Config{NodeID: glid.New(), Table: allRoute(t, vaultID)})
	if err := sup.RegisterVault(originOnlySpec(vaultID, root, oldFSM)); err != nil {
		t.Fatalf("RegisterVault: %v", err)
	}
	writeCompleted(t, root, inSnapshot, afterRestore, stale)

	newFSM := vaultctlfsm.New()
	applyPublish(t, newFSM, inSnapshot, afterRestore, stale)
	applyRelease(t, newFSM, inSnapshot)

	if err := sup.RewireVaultAfterCtlRestore(vaultID, RewireVaultConfig{FSM: newFSM}); err != nil {
		t.Fatalf("RewireVaultAfterCtlRestore on a non-home origin: %v", err)
	}
	assertCompleted(t, root, inSnapshot, false)

	applyRelease(t, newFSM, afterRestore)
	assertCompleted(t, root, afterRestore, false)

	applyPublish(t, oldFSM, stale)
	applyRelease(t, oldFSM, stale)
	assertCompleted(t, root, stale, true)
}

func TestSupervisorRewireAfterCtlRestoreUnknownVault(t *testing.T) {
	t.Parallel()
	sup := New(Config{NodeID: glid.New(), Table: allRoute(t, glid.New())})
	err := sup.RewireVaultAfterCtlRestore(glid.New(), RewireVaultConfig{FSM: vaultctlfsm.New()})
	if !errors.Is(err, chunking.ErrUnknownVault) {
		t.Fatalf("RewireVaultAfterCtlRestore(unregistered) = %v, want chunking.ErrUnknownVault", err)
	}
}

// TestSupervisorUnregisterStopsReleaseConsumption: a vault that left this
// node no longer reacts to releases.
func TestSupervisorUnregisterStopsReleaseConsumption(t *testing.T) {
	t.Parallel()
	vaultID, seg := glid.New(), glid.New()
	root := t.TempDir()
	fsm := vaultctlfsm.New()
	sup := New(Config{NodeID: glid.New(), Table: allRoute(t, vaultID)})
	if err := sup.RegisterVault(originOnlySpec(vaultID, root, fsm)); err != nil {
		t.Fatalf("RegisterVault: %v", err)
	}
	sup.UnregisterVault(vaultID)
	writeCompleted(t, root, seg)
	applyPublish(t, fsm, seg)
	applyRelease(t, fsm, seg)
	assertCompleted(t, root, seg, true)
}
