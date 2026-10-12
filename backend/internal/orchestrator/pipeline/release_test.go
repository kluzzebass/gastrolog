package pipeline

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
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

func writeStaging(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("segment"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertExists(t *testing.T, path string, want bool) {
	t.Helper()
	_, err := os.Stat(path)
	switch {
	case want && err != nil:
		t.Errorf("%s must still exist: %v", path, err)
	case !want && !os.IsNotExist(err):
		t.Errorf("%s must be purged, stat err=%v", path, err)
	}
}

// lockDir makes dir read-only so deletes inside it fail, and returns the undo.
func lockDir(t *testing.T, dir string) func() {
	t.Helper()
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatalf("chmod %s: %v", dir, err)
	}
	unlock := func() { _ = os.Chmod(dir, 0o750) }
	t.Cleanup(unlock)
	return unlock
}

// The catch-up deletes every staging copy of a released segment under every
// root; a live segment and one awaiting its publish stay (registry absence is
// not release); the pass covers the release sequence it read.
func TestPurgeReleasedStagingRemovesEveryStagingCopy(t *testing.T) {
	t.Parallel()
	originRoot, homeRoot := t.TempDir(), t.TempDir()
	roots := []string{originRoot, homeRoot}
	released, live, unpublished := glid.New(), glid.New(), glid.New()
	fsm := vaultctlfsm.New()
	applyPublish(t, fsm, released, live)
	applyRelease(t, fsm, released)

	var releasedCopies []string
	for _, root := range roots {
		releasedCopies = append(releasedCopies,
			paths.CompletedSegment(root, released), paths.HeadSegment(root, released), paths.PreHeadSegment(root, released))
	}
	kept := []string{paths.CompletedSegment(originRoot, live), paths.HeadSegment(homeRoot, live), paths.CompletedSegment(originRoot, unpublished)}
	for _, path := range append(slices.Clone(releasedCopies), kept...) {
		writeStaging(t, path)
	}

	covered, purged, err := PurgeReleasedStaging(fsm, roots)
	if err != nil {
		t.Fatalf("PurgeReleasedStaging: %v", err)
	}
	if covered == 0 || covered != fsm.ReleaseSeq() {
		t.Errorf("covered = %d, want the applied release sequence %d", covered, fsm.ReleaseSeq())
	}
	if !slices.Equal(purged, []glid.GLID{released}) {
		t.Errorf("purged = %v, want [%s]", purged, released)
	}
	for _, path := range releasedCopies {
		assertExists(t, path, false)
	}
	for _, path := range kept {
		assertExists(t, path, true)
	}

	if _, purged, err := PurgeReleasedStaging(fsm, roots); err != nil || len(purged) != 0 {
		t.Errorf("second pass = (%v, %v), want nothing to purge", purged, err)
	}
	for _, path := range kept {
		assertExists(t, path, true)
	}
}

// Before the FSM has applied anything there is no release evidence: the pass
// touches nothing and covers nothing.
func TestPurgeReleasedStagingBeforeAnyApplyTouchesNothing(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	seg := glid.New()
	writeStaging(t, paths.CompletedSegment(root, seg))

	covered, purged, err := PurgeReleasedStaging(vaultctlfsm.New(), []string{root})
	if err != nil || covered != 0 || len(purged) != 0 {
		t.Fatalf("PurgeReleasedStaging = (%d, %v, %v), want (0, [], nil)", covered, purged, err)
	}
	assertExists(t, paths.CompletedSegment(root, seg), true)
}

// A staging copy that cannot be deleted fails the pass, so the sweep confirms
// no release sequence while the bytes are still on disk.
func TestPurgeReleasedStagingReportsAFailedDelete(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	released := glid.New()
	fsm := vaultctlfsm.New()
	applyPublish(t, fsm, released)
	applyRelease(t, fsm, released)
	headFile := paths.HeadSegment(root, released)
	writeStaging(t, headFile)
	lockDir(t, paths.HeadDir(root))

	if _, _, err := PurgeReleasedStaging(fsm, []string{root}); err == nil {
		t.Fatal("purge reported success while the released segment's head/ copy could not be deleted")
	}
	assertExists(t, headFile, true)
}

// A staging directory that cannot be listed fails the pass too: what it holds
// is unknown, so the pass cannot account for it.
func TestPurgeReleasedStagingReportsAFailedListing(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	released := glid.New()
	fsm := vaultctlfsm.New()
	applyPublish(t, fsm, released)
	applyRelease(t, fsm, released)
	writeStaging(t, paths.PreHeadSegment(root, released))
	unreadable := paths.PreHeadDir(root)
	if err := os.Chmod(unreadable, 0o000); err != nil {
		t.Fatalf("chmod pre-head/: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(unreadable, 0o750) })

	if _, _, err := PurgeReleasedStaging(fsm, []string{root}); err == nil {
		t.Fatal("purge reported success while pre-head/ could not be listed")
	}
}

// The release event and the catch-up delete through the same purge. A delete
// the event could not make stays on disk for the catch-up, which reports it
// until the delete succeeds and then accounts for the release.
func TestReleaseEventFailedDeleteIsLeftForTheCatchUp(t *testing.T) {
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
	unlock := lockDir(t, paths.CompletedDir(root))

	applyRelease(t, fsm, released)
	assertCompleted(t, root, released, true)
	if _, _, err := PurgeReleasedStaging(fsm, []string{root}); err == nil {
		t.Fatal("catch-up reported success while completed/ still held the released segment")
	}

	unlock()
	covered, purged, err := PurgeReleasedStaging(fsm, []string{root})
	if err != nil || covered != fsm.ReleaseSeq() || !slices.Equal(purged, []glid.GLID{released}) {
		t.Fatalf("catch-up = (%d, %v, %v), want (%d, [%s], nil)", covered, purged, err, fsm.ReleaseSeq(), released)
	}
	assertCompleted(t, root, released, false)
}
