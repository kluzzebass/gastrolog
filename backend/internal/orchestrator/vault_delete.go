package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"syscall"

	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// DeleteVault is this node's share of a vault deletion. It stops the vault's
// pipeline, closes the vault, retires its vault-ctl Raft group — WAL state
// and directory — and removes the vault's directories on this node.
//
// The files that hold the vault's records — staging segments, sealed chunks,
// and a cloud-backed vault's cloud objects — go only with deleteData. Without
// it they stay where they are, unreachable; what goes is the vault's
// chunk-manager bookkeeping (lock file, cloud index) and every directory left
// empty.
//
// It acts whether or not the vault is registered here: a node that only
// originated segments for the vault, or that already tore it down, still
// holds files of it.
func (o *Orchestrator) DeleteVault(id glid.GLID, deleteData bool) error {
	sys, err := o.loadSystem(context.Background())
	if errors.Is(err, ErrNoSystemLoader) {
		err = nil
	}
	if err != nil {
		err = fmt.Errorf("load system for vault %s file roots: %w", id, err)
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	dirs := o.vaultDirsLocked(sys, id)
	o.unregisterPipelineVaultLocked(id)
	vault, registered := o.vaults[id]
	if deleteData && registered && vault.Instance != nil {
		if purger, ok := vault.Instance.Chunks.(cloudObjectPurger); ok {
			if perr := purger.PurgeCloudObjects(context.Background()); perr != nil {
				err = errors.Join(err, fmt.Errorf("purge cloud objects of vault %s: %w", id, perr))
			}
		}
	}
	if registered {
		o.teardownVault(id, vault)
	} else {
		o.cancelPendingChunkJobs(id)
		o.decommissionVaultControlPlaneRaftGroup(id)
	}

	for _, dir := range dirs {
		err = errors.Join(err, removeVaultDir(dir, deleteData))
	}
	o.logger.Info("vault deleted on this node", "id", id, "delete_data", deleteData)
	return err
}

// cloudObjectPurger is the chunk-manager seam that deletes a vault's cloud
// objects once no upload of it can still land.
type cloudObjectPurger interface {
	PurgeCloudObjects(ctx context.Context) error
}

// vaultDirsLocked lists every directory this node can hold files of the vault
// under. Each is named by the vault's ID, inside a directory every vault
// shares: its pipeline roots (pipelineRootCandidates, and the root it is
// registered with) and its chunk-manager directories — under the vaults
// directory, and under each of this node's file storages.
func (o *Orchestrator) vaultDirsLocked(sys *system.System, id glid.GLID) []string {
	dirs := o.pipelineRootCandidates(sys, id)
	if reg, ok := o.lookupPipelineVault(id); ok && reg.root != "" {
		dirs = append(dirs, reg.root)
	}
	if o.vaultsDir != "" {
		dirs = append(dirs, filepath.Join(o.vaultsDir, "vaults", id.String()))
	}
	if sys != nil {
		for _, nsc := range sys.Runtime.NodeStorageConfigs {
			if nsc.NodeID != o.localNodeID {
				continue
			}
			for _, st := range nsc.FileStorages {
				if st.Path != "" {
					dirs = append(dirs, filepath.Join(resolveStoragePath(st.Path, o.vaultsDir), "vaults", id.String()))
				}
			}
		}
	}
	slices.Sort(dirs)
	return slices.Compact(dirs)
}

// unregisterPipelineVaultLocked stops the vault's pipeline managers, its cron
// rotation job and its registration. Caller holds o.mu.
func (o *Orchestrator) unregisterPipelineVaultLocked(id glid.GLID) {
	if o.pipeline != nil {
		o.pipeline.UnregisterVault(id)
	}
	o.reconcileChunkCron(id, false, "")
	o.deletePipelineVaultLocked(id)
}

// removeVaultDir removes a deleted vault's directory: all of it with
// deleteData, otherwise its chunk-manager bookkeeping files and the
// directories left empty, keeping every file that holds records.
func removeVaultDir(dir string, deleteData bool) error {
	if deleteData {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("remove vault directory %s: %w", dir, err)
		}
		return nil
	}
	root, err := os.OpenRoot(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open vault directory %s: %w", dir, err)
	}
	defer func() { _ = root.Close() }()
	var subdirs []string
	err = fs.WalkDir(root.FS(), ".", func(path string, d fs.DirEntry, err error) error {
		switch {
		case err != nil:
			return err
		case d.IsDir():
			subdirs = append(subdirs, path)
		case chunkfile.IsBookkeepingFile(d.Name()):
			return root.Remove(path)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("clear vault directory %s: %w", dir, err)
	}
	for _, sub := range slices.Backward(subdirs) {
		if sub == "." {
			err = os.Remove(dir)
		} else {
			err = root.Remove(sub)
		}
		if err != nil && !errors.Is(err, syscall.ENOTEMPTY) && !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("remove empty directory %s/%s: %w", dir, sub, err)
		}
	}
	return nil
}
