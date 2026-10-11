package orchestrator

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/system"
)

// errNoSegmentsDir rejects pipeline registration before the node home is known.
var errNoSegmentsDir = errors.New("segments directory unset: configure orchestrator.Config.SegmentsDir from node home")

// pipelineRootFor resolves the vault's pipeline root on this node — the one
// directory holding every storage area the node keeps for the vault
// (working/, completed/, pre-head/, head/ and chunks/). On a node with a
// placement for the vault on a file storage, the root lives on that storage,
// so the vault's storage class decides which volume its bytes land on. A node
// without such a placement only originates segments for the vault, and its
// root is under the node home's segments directory.
//
// Every area shares the one root because promotion between areas is a rename.
func (o *Orchestrator) pipelineRootFor(sys *system.System, vaultID glid.GLID) (string, error) {
	if o.segmentsDir == "" {
		return "", errNoSegmentsDir
	}
	if dir := o.localPlacementStorageDir(sys, vaultID); dir != "" {
		return storagePipelineRoot(dir, vaultID), nil
	}
	return pipelineVaultRoot(o.segmentsDir, vaultID), nil
}

// localPlacementStorageDir is the resolved path of the file storage holding
// this node's placement for the vault, or "" when the node has none (no
// placement here, or a synthetic one). A node holds at most one placement per
// vault.
func (o *Orchestrator) localPlacementStorageDir(sys *system.System, vaultID glid.GLID) string {
	if sys == nil {
		return ""
	}
	nscs := sys.Runtime.NodeStorageConfigs
	for _, p := range sys.PlacementsFor(vaultID) {
		if system.NodeIDForStorage(p.StorageID, nscs) != o.localNodeID {
			continue
		}
		if fs := findFileStorageByID(&sys.Runtime, p.StorageID); fs != nil && fs.Path != "" {
			return resolveStoragePath(fs.Path, o.vaultsDir)
		}
	}
	return ""
}

// storagePipelineRoot is a vault's pipeline root on a file storage.
func storagePipelineRoot(storageDir string, vaultID glid.GLID) string {
	return pipelineVaultRoot(filepath.Join(storageDir, "segments"), vaultID)
}

// pipelineRootCandidates lists every directory that can hold the vault's
// pipeline areas on this node: the node home's root and a root on each of the
// node's file storages. A placement change moves the resolved root between
// them; files left under another candidate are this node's to move or purge.
func (o *Orchestrator) pipelineRootCandidates(sys *system.System, vaultID glid.GLID) []string {
	var roots []string
	if o.segmentsDir != "" {
		roots = append(roots, pipelineVaultRoot(o.segmentsDir, vaultID))
	}
	if sys == nil {
		return roots
	}
	for _, nsc := range sys.Runtime.NodeStorageConfigs {
		if nsc.NodeID != o.localNodeID {
			continue
		}
		for _, fs := range nsc.FileStorages {
			if fs.Path == "" {
				continue
			}
			root := storagePipelineRoot(resolveStoragePath(fs.Path, o.vaultsDir), vaultID)
			if !slices.Contains(roots, root) {
				roots = append(roots, root)
			}
		}
	}
	return roots
}

// gatherPipelineRoot moves the vault's files from every other candidate root
// into root, so a placement change — the vault moving to another storage on
// this node, the node becoming or ceasing to be a home — orphans nothing. A
// home takes every area. A node that only originates segments for the vault
// takes working/ and completed/, the areas it serves and publishes from; a
// former home's head/ and chunks/ stay where they are, as they are no longer
// this node's to hold, and the released-segment sweep purges their staging
// copies on every candidate root.
//
// The pipeline must not be registered for the vault while this runs: the
// moved files are recovered (working/), republished (completed/) and served
// by the registration that follows.
func gatherPipelineRoot(root string, home bool, candidates []string) error {
	var errs []error
	for _, old := range candidates {
		if old == "" || old == root {
			continue
		}
		if nestedPaths(old, root) {
			errs = append(errs, fmt.Errorf("pipeline roots %s and %s nest; not moving", old, root))
			continue
		}
		if home {
			errs = append(errs, paths.MoveTree(old, root))
			continue
		}
		for _, area := range []string{paths.Working, paths.Completed} {
			errs = append(errs, paths.MoveTree(filepath.Join(old, area), filepath.Join(root, area)))
		}
		_ = os.Remove(old)
	}
	return errors.Join(errs...)
}

// nestedPaths reports whether either path is inside the other.
func nestedPaths(a, b string) bool {
	inside := func(parent, child string) bool {
		rel, err := filepath.Rel(parent, child)
		return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
	}
	return inside(a, b) || inside(b, a)
}
