package orchestrator

// Coverage for where a vault's pipeline areas live on a node: on the file
// storage the vault's placement chose there, so the vault's storage class
// decides which volume its segments and GLCBs fill; under the node home when
// the node only originates segments for the vault. A placement change moves
// the root, and the files under the previous root move with it.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segmentation"
	"gastrolog/internal/system"
	"gastrolog/internal/waittest"
)

const rootTestNode = "node-root"

// rootTestCluster is a node with three file storages: class 1 (fast), class 3
// (archive) and a second class-3 storage, plus a peer node with its own
// class-3 storage. Each storage lives in its own directory, standing in for a
// separate volume.
type rootTestCluster struct {
	home      string // node home; vaultsDir resolves relative storage paths here
	fast      system.FileStorage
	archive   system.FileStorage
	archive2  system.FileStorage
	peerStore system.FileStorage
}

func newRootTestCluster(t *testing.T) rootTestCluster {
	t.Helper()
	home := t.TempDir()
	return rootTestCluster{
		home: home,
		fast: system.FileStorage{ID: glid.New(), StorageClass: 1, Name: "fast", Path: filepath.Join(t.TempDir(), "fast")},
		// Relative, the operator-facing form: resolves against the node home.
		archive:   system.FileStorage{ID: glid.New(), StorageClass: 3, Name: "archive", Path: "storage/archive"},
		archive2:  system.FileStorage{ID: glid.New(), StorageClass: 3, Name: "archive-2", Path: filepath.Join(t.TempDir(), "archive-2")},
		peerStore: system.FileStorage{ID: glid.New(), StorageClass: 3, Name: "peer-archive", Path: filepath.Join(t.TempDir(), "peer")},
	}
}

func (c rootTestCluster) storageDir(fs system.FileStorage) string {
	return resolveStoragePath(fs.Path, c.home)
}

func (c rootTestCluster) homeRoot(vaultID glid.GLID) string {
	return pipelineVaultRoot(filepath.Join(c.home, "segments"), vaultID)
}

func (c rootTestCluster) storageRoot(fs system.FileStorage, vaultID glid.GLID) string {
	return filepath.Join(c.storageDir(fs), "segments", vaultID.String())
}

// system returns the cluster's config with one class-3 vault, routed to by a
// match-all route, placed on placement.
func (c rootTestCluster) system(vaultID glid.GLID, placement []system.VaultPlacement) *system.System {
	return &system.System{
		Config: system.Config{
			Vaults: []system.VaultConfig{{
				ID: vaultID, Name: "archive-vault", Type: system.VaultTypeFile, StorageClass: 3, Enabled: true,
			}},
			Routes: []system.RouteConfig{{
				ID:           glid.New(),
				Name:         "match-all",
				Stages:       []system.RouteStage{{Match: &system.MatchStage{Expression: "*"}}},
				Destinations: []glid.GLID{vaultID},
				Enabled:      true,
			}},
		},
		Runtime: system.Runtime{
			NodeStorageConfigs: []system.NodeStorageConfig{
				{NodeID: rootTestNode, FileStorages: []system.FileStorage{c.fast, c.archive, c.archive2}},
				{NodeID: "node-peer", FileStorages: []system.FileStorage{c.peerStore}},
			},
			VaultPlacements: map[glid.GLID][]system.VaultPlacement{vaultID: placement},
		},
	}
}

func onStorage(fs system.FileStorage) []system.VaultPlacement {
	return []system.VaultPlacement{{StorageID: fs.ID.String(), Leader: true}}
}

func (c rootTestCluster) newOrch(t *testing.T, loader *staticSystemLoader) *Orchestrator {
	t.Helper()
	orch := newTestOrch(t, Config{
		LocalNodeID:           rootTestNode,
		SystemLoader:          loader,
		SegmentsDir:           filepath.Join(c.home, "segments"),
		SegmentCompletePolicy: segmentation.CompletePolicy{MaxBytes: 1},
	})
	orch.vaultsDir = c.home
	return orch
}

func (l *staticSystemLoader) set(sys *system.System) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sys = sys
}

func TestPipelineRootFollowsPlacementStorage(t *testing.T) {
	t.Parallel()
	c := newRootTestCluster(t)
	vaultID := glid.New()
	orch := c.newOrch(t, &staticSystemLoader{})

	for _, tc := range []struct {
		name      string
		placement []system.VaultPlacement
		want      string
	}{
		{"placed on a local class-3 storage", onStorage(c.archive), c.storageRoot(c.archive, vaultID)},
		{"placed on another local storage", onStorage(c.archive2), c.storageRoot(c.archive2, vaultID)},
		{"placed only on a peer's storage: origin under the node home", onStorage(c.peerStore), c.homeRoot(vaultID)},
		{"synthetic placement on this node", []system.VaultPlacement{{StorageID: system.SyntheticStorageID(rootTestNode), Leader: true}}, c.homeRoot(vaultID)},
		{"unplaced", nil, c.homeRoot(vaultID)},
	} {
		got, err := orch.pipelineRootFor(c.system(vaultID, tc.placement), vaultID)
		if err != nil {
			t.Fatalf("%s: pipelineRootFor: %v", tc.name, err)
		}
		if got != tc.want {
			t.Errorf("%s: root = %s, want %s", tc.name, got, tc.want)
		}
	}
	if want := filepath.Join(c.home, "storage", "archive", "segments", vaultID.String()); c.storageRoot(c.archive, vaultID) != want {
		t.Fatalf("premise: relative storage path resolves against the node home, got %s", c.storageRoot(c.archive, vaultID))
	}
}

// ingestOne submits one record through routing and waits for its durability
// ack, then for a completed segment under root.
func ingestOne(t *testing.T, orch *Orchestrator, root, body string) {
	t.Helper()
	ack := make(chan error, 1)
	rec := chunk.Record{IngestTS: time.Now().UTC(), Attrs: map[string]string{"k": "v"}, Raw: []byte(body)}
	if err := orch.SubmitIngest(context.Background(), rec, ack); err != nil {
		t.Fatalf("SubmitIngest: %v", err)
	}
	progress := func() string { return pipelineProgress(orch) }
	if err := waittest.Recv(t, "durability ack", ack, progress); err != nil {
		t.Fatalf("durability ack: %v", err)
	}
	waittest.Progress(t, "a completed segment under "+root, func() (string, bool) {
		ids, err := paths.ListSegmentIDs(paths.CompletedDir(root))
		return fmt.Sprintf("completed=%d err=%v %s", len(ids), err, progress()), err == nil && len(ids) > 0
	})
}

func segmentFiles(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return filepath.SkipDir
			}
			return err
		}
		if !d.IsDir() {
			rel, _ := filepath.Rel(root, path)
			out = append(out, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	slices.Sort(out)
	return out
}

func startPipeline(t *testing.T, orch *Orchestrator) {
	t.Helper()
	if err := orch.ReloadFilters(context.Background()); err != nil {
		t.Fatalf("ReloadFilters: %v", err)
	}
	if err := orch.pipeline.Start(context.Background()); err != nil {
		t.Fatalf("pipeline Start: %v", err)
	}
	t.Cleanup(func() { _ = orch.pipeline.Stop() })
}

// Records for a vault placed on a class-3 storage land on that storage's
// volume, not the node home, the disk guard watching that storage covers
// them, and a restarted node finds them there.
func TestPipelineSegmentsLandOnPlacementStorage(t *testing.T) {
	t.Parallel()
	c := newRootTestCluster(t)
	vaultID := glid.New()
	loader := &staticSystemLoader{sys: c.system(vaultID, onStorage(c.archive))}
	orch := c.newOrch(t, loader)
	startPipeline(t, orch)

	root := c.storageRoot(c.archive, vaultID)
	if got, _ := orch.originRoot(vaultID); got != root {
		t.Fatalf("registered root = %s, want the archive storage's %s", got, root)
	}
	ingestOne(t, orch, root, "lands-on-archive")
	for _, other := range []string{c.homeRoot(vaultID), c.storageRoot(c.fast, vaultID), c.storageRoot(c.archive2, vaultID)} {
		if files := segmentFiles(t, other); len(files) > 0 {
			t.Fatalf("pipeline files under %s, which the placement did not choose: %v", other, files)
		}
	}

	// The per-storage disk guard samples the archive storage's directory:
	// the root sits inside it, so its bytes are on the volume it watches,
	// and the vault's footprint counts them.
	orch.refreshVaultDiskGuards(context.Background())
	orch.diskGuard.mu.Lock()
	guard := orch.diskGuard.storages[c.archive.ID.String()]
	orch.diskGuard.mu.Unlock()
	if guard == nil {
		t.Fatal("no disk guard entry for the archive storage")
	}
	if !strings.HasPrefix(root, guard.path+string(filepath.Separator)) {
		t.Fatalf("pipeline root %s is outside the guarded storage path %s", root, guard.path)
	}
	if b := orch.localVaultFootprintBytes(vaultID); b <= 0 {
		t.Fatalf("vault footprint = %d bytes with a completed segment on its storage", b)
	}
	before, err := orch.LocalPipelineDiskSegmentCounts(vaultID)
	if err != nil || before.CompletedStaging == 0 {
		t.Fatalf("disk counts = %+v, err=%v; want the completed segment counted", before, err)
	}

	// Restart: a fresh orchestrator over the same home and storages.
	_ = orch.pipeline.Stop()
	restarted := c.newOrch(t, loader)
	startPipeline(t, restarted)
	after, err := restarted.LocalPipelineDiskSegmentCounts(vaultID)
	if err != nil {
		t.Fatalf("disk counts after restart: %v", err)
	}
	if after.CompletedStaging < before.CompletedStaging {
		t.Fatalf("after restart %d completed segments found, want at least the %d written before", after.CompletedStaging, before.CompletedStaging)
	}
	if got, _ := restarted.originRoot(vaultID); got != root {
		t.Fatalf("restarted root = %s, want %s", got, root)
	}
}

// A placement change moves the root, and the files under the previous root
// move into it: no segment is orphaned on a volume the vault left.
func TestPipelineRootGathersFilesOnPlacementChange(t *testing.T) {
	t.Parallel()
	c := newRootTestCluster(t)
	vaultID := glid.New()
	loader := &staticSystemLoader{sys: c.system(vaultID, onStorage(c.peerStore))}
	orch := c.newOrch(t, loader)
	startPipeline(t, orch)
	ctx := context.Background()

	// Not a home: the node originates under its home.
	ingestOne(t, orch, c.homeRoot(vaultID), "origin-only")
	originFiles := segmentFiles(t, c.homeRoot(vaultID))

	// Becoming a home on the archive storage takes the origin backlog along.
	loader.set(c.system(vaultID, onStorage(c.archive)))
	if err := orch.ReloadFilters(ctx); err != nil {
		t.Fatalf("ReloadFilters: %v", err)
	}
	archiveRoot := c.storageRoot(c.archive, vaultID)
	assertGathered(t, archiveRoot, c.homeRoot(vaultID), completedOnly(originFiles))

	// A home's own areas: a head copy and a built chunk.
	headSeg := filepath.Join(paths.Head, glid.New().String())
	glcb := filepath.Join("chunks", chunk.NewChunkID().String(), "data.glcb")
	for _, rel := range []string{headSeg, glcb} {
		writeRootFile(t, filepath.Join(archiveRoot, rel))
	}
	// working/ is excluded: re-registration completes the open segment.
	homeFiles := withoutWorking(segmentFiles(t, archiveRoot))

	// Moving to the other class-3 storage on this node takes every area.
	loader.set(c.system(vaultID, onStorage(c.archive2)))
	if err := orch.ReloadFilters(ctx); err != nil {
		t.Fatalf("ReloadFilters: %v", err)
	}
	archive2Root := c.storageRoot(c.archive2, vaultID)
	assertGathered(t, archive2Root, archiveRoot, homeFiles)

	// Leaving the node: the origin areas come home, the former home's head/
	// and chunks/ stay on the storage.
	ingestOne(t, orch, archive2Root, "before-leaving")
	beforeLeaving := segmentFiles(t, archive2Root)
	loader.set(c.system(vaultID, onStorage(c.peerStore)))
	if err := orch.ReloadFilters(ctx); err != nil {
		t.Fatalf("ReloadFilters: %v", err)
	}
	if got, _ := orch.originRoot(vaultID); got != c.homeRoot(vaultID) {
		t.Fatalf("root after leaving = %s, want the home root", got)
	}
	gotHome := segmentFiles(t, c.homeRoot(vaultID))
	for _, rel := range completedOnly(beforeLeaving) {
		if !slices.Contains(gotHome, rel) {
			t.Errorf("origin file %s did not follow the vault home: home root holds %v", rel, gotHome)
		}
	}
	left := segmentFiles(t, archive2Root)
	for _, rel := range []string{headSeg, glcb} {
		if !slices.Contains(left, rel) {
			t.Errorf("former home's %s left the storage it was held on: %v", rel, left)
		}
	}
	for _, rel := range left {
		if strings.HasPrefix(rel, paths.Completed+string(filepath.Separator)) || strings.HasPrefix(rel, paths.Working+string(filepath.Separator)) {
			t.Errorf("origin file %s orphaned on the storage the vault left", rel)
		}
	}
}

func withoutWorking(files []string) []string {
	var out []string
	for _, f := range files {
		if !strings.HasPrefix(f, paths.Working+string(filepath.Separator)) {
			out = append(out, f)
		}
	}
	return out
}

func completedOnly(files []string) []string {
	var out []string
	for _, f := range files {
		if strings.HasPrefix(f, paths.Completed+string(filepath.Separator)) {
			out = append(out, f)
		}
	}
	return out
}

func writeRootFile(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(path), 0o600); err != nil {
		t.Fatal(err)
	}
}

// assertGathered checks every listed file is under root and none is left
// under old.
func assertGathered(t *testing.T, root, old string, files []string) {
	t.Helper()
	if len(files) == 0 {
		t.Fatal("premise: nothing to gather")
	}
	got := segmentFiles(t, root)
	for _, rel := range files {
		if !slices.Contains(got, rel) {
			t.Errorf("%s not gathered into %s (holds %v)", rel, root, got)
		}
	}
	if leftover := segmentFiles(t, old); len(leftover) > 0 {
		t.Errorf("files orphaned under the previous root %s: %v", old, leftover)
	}
}

// A gather that cannot finish keeps the vault registered — ingest continues
// on the new root — and publishes the registration failed, so the next
// reload finishes the gather once the old root is readable again.
func TestPipelineRootGatherFailureRetries(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	c := newRootTestCluster(t)
	vaultID := glid.New()
	loader := &staticSystemLoader{sys: c.system(vaultID, onStorage(c.peerStore))}
	orch := c.newOrch(t, loader)
	startPipeline(t, orch)
	ctx := context.Background()

	ingestOne(t, orch, c.homeRoot(vaultID), "stranded-for-a-while")
	stranded := completedOnly(segmentFiles(t, c.homeRoot(vaultID)))
	completedDir := paths.CompletedDir(c.homeRoot(vaultID))
	if err := os.Chmod(completedDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(completedDir, 0o750) })

	loader.set(c.system(vaultID, onStorage(c.archive)))
	if err := orch.ReloadFilters(ctx); err != nil {
		t.Fatalf("ReloadFilters: %v", err)
	}
	reg, ok := orch.lookupPipelineVault(vaultID)
	if !ok || !reg.failed {
		t.Fatalf("registration after an unfinished gather = %+v ok=%v, want it published failed", reg, ok)
	}
	archiveRoot := c.storageRoot(c.archive, vaultID)
	ingestOne(t, orch, archiveRoot, "ingest-continues")

	if err := os.Chmod(completedDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := orch.ReloadFilters(ctx); err != nil {
		t.Fatalf("ReloadFilters: %v", err)
	}
	if reg, _ := orch.lookupPipelineVault(vaultID); reg.failed {
		t.Fatalf("registration still failed after the old root became readable: %+v", reg)
	}
	got := segmentFiles(t, archiveRoot)
	for _, rel := range stranded {
		if !slices.Contains(got, rel) {
			t.Errorf("stranded segment %s not gathered on retry (root holds %v)", rel, got)
		}
	}
}

// The released-segment sweep reaches every root a vault's staging copies can
// be under on this node — the home's and each local storage's — not just the
// root the vault is registered with now.
func TestReleasedSegmentStagingRootsCoverEveryLocalStorage(t *testing.T) {
	t.Parallel()
	c := newRootTestCluster(t)
	vaultID := glid.New()
	sys := c.system(vaultID, onStorage(c.peerStore))
	orch := c.newOrch(t, &staticSystemLoader{sys: sys})
	roots := orch.releasedSegmentStagingRoots(sys, vaultID)
	for _, want := range []string{
		c.homeRoot(vaultID),
		c.storageRoot(c.fast, vaultID),
		c.storageRoot(c.archive, vaultID),
		c.storageRoot(c.archive2, vaultID),
	} {
		if !slices.Contains(roots, want) {
			t.Errorf("staging roots %v miss %s", roots, want)
		}
	}
	if slices.Contains(roots, c.storageRoot(c.peerStore, vaultID)) {
		t.Errorf("staging roots include a peer's storage: %v", roots)
	}
}
