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
	"gastrolog/internal/orchestrator/pipeline"
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
	o.vaultDeletions.Add(1)
	retired := o.retireVaultLocked(sys, id, deleteData)
	o.mu.Unlock()

	return errors.Join(err, o.finishRetiredVault(retired))
}

// retiredVault is a vault dropped from this node's registries whose teardown
// is still to run: stopping its pipeline, purging its cloud objects, retiring
// its vault-ctl group, closing it and removing its files. Each of those waits
// on goroutines, I/O, the network or Raft, so it runs without o.mu; the
// deletion count keeps a reload from registering the vault in between.
type retiredVault struct {
	id         glid.GLID
	deleteData bool
	dirs       []string
	pipeline   *pipeline.Supervisor
	vault      *Vault // nil when the vault was not registered here
}

// retireVaultLocked drops the vault from this node's registries — its
// pipeline registration and cron job, its retention runner, the vault itself
// — and returns its remaining teardown for finishRetiredVault. sys is the
// config naming this node's file storages. Caller holds o.mu.
func (o *Orchestrator) retireVaultLocked(sys *system.System, id glid.GLID, deleteData bool) retiredVault {
	r := retiredVault{
		id:         id,
		deleteData: deleteData,
		dirs:       o.vaultDirsLocked(sys, id),
		pipeline:   o.pipeline,
		vault:      o.vaults[id],
	}
	o.reconcileChunkCron(id, false, "")
	o.deletePipelineVaultLocked(id)
	if r.vault != nil {
		if inst := r.vault.Instance; inst != nil {
			delete(o.retention, retentionKey(inst.VaultID, inst.StorageID))
		}
		delete(o.vaults, id)
	}
	return r
}

// finishRetiredVault runs a retired vault's teardown. It must not hold o.mu:
// the vault's pipeline goroutines take it, and every step here can wait on
// I/O, the network or Raft.
func (o *Orchestrator) finishRetiredVault(r retiredVault) error {
	// The group stops first: its applies then fail at once instead of waiting
	// out a quorum the other nodes' deletes are dissolving, so the pipeline
	// goroutines waited for below return promptly, and no trailing apply
	// callback reaches the chunk manager closed after them.
	o.decommissionVaultControlPlaneRaftGroup(r.id)
	if r.pipeline != nil {
		r.pipeline.UnregisterVault(r.id)()
	}
	var err error
	if r.deleteData && r.vault != nil && r.vault.Instance != nil {
		if purger, ok := r.vault.Instance.Chunks.(cloudObjectPurger); ok {
			if perr := purger.PurgeCloudObjects(context.Background()); perr != nil {
				err = errors.Join(err, fmt.Errorf("purge cloud objects of vault %s: %w", r.id, perr))
			}
		}
	}
	o.cancelPendingChunkJobs(r.id)
	if r.vault != nil {
		if cerr := r.vault.Close(); cerr != nil {
			o.logger.Warn("close deleted vault", "vault", r.id, "error", cerr)
		}
	}
	for _, dir := range r.dirs {
		err = errors.Join(err, removeVaultDir(dir, r.deleteData))
	}
	o.logger.Info("vault deleted on this node", "id", r.id, "delete_data", r.deleteData)
	return err
}

// loadSystemForReload reads config that the caller applies later under o.mu,
// with the vault-deletion count at the time of the read.
func (o *Orchestrator) loadSystemForReload(ctx context.Context) (*system.System, uint64, error) {
	deletions := o.vaultDeletions.Load()
	sys, err := o.loadSystem(ctx)
	return sys, deletions, err
}

// currentSystemLocked returns sys, read when the vault-deletion count was
// deletions, or config read again when a vault deletion has been applied
// since: applying the earlier read would register the deleted vault again.
// Caller holds o.mu, which DeleteVault holds while it counts a deletion.
func (o *Orchestrator) currentSystemLocked(ctx context.Context, sys *system.System, deletions uint64) (*system.System, error) {
	if o.vaultDeletions.Load() == deletions {
		return sys, nil
	}
	fresh, err := o.loadSystem(ctx)
	if errors.Is(err, ErrNoSystemLoader) {
		return sys, nil
	}
	return fresh, err
}

// registerUnlessDeleted registers a vault built from the config ApplyConfig
// was given, and reports whether it did. A vault deleted since that config
// was read is removed again instead — the files and the vault-ctl group its
// build created go, any records it had stay — so the deletion holds.
func (o *Orchestrator) registerUnlessDeleted(vault *Vault) bool {
	o.mu.Lock()
	o.vaults[vault.ID] = vault
	o.bindVaultInstanceToCtlLocked(vault.ID, vault.Instance)
	if o.vaultDeletions.Load() == 0 {
		o.mu.Unlock()
		return true
	}
	sys, err := o.loadSystem(context.Background())
	if err != nil || sys == nil || findVaultConfig(sys.Config.Vaults, vault.ID) != nil {
		o.mu.Unlock()
		return true
	}
	retired := o.retireVaultLocked(sys, vault.ID, false)
	o.mu.Unlock()
	if err := o.finishRetiredVault(retired); err != nil {
		o.logger.Warn("remove vault deleted while it initialized", "id", vault.ID, "error", err)
	}
	return false
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
// rotation job and its registration, and returns a wait for the pipeline
// goroutines still writing under the vault's root; they take o.mu, so the
// caller waits after releasing it. Caller holds o.mu.
func (o *Orchestrator) unregisterPipelineVaultLocked(id glid.GLID) (wait func()) {
	wait = func() {}
	if o.pipeline != nil {
		wait = o.pipeline.UnregisterVault(id)
	}
	o.reconcileChunkCron(id, false, "")
	o.deletePipelineVaultLocked(id)
	return wait
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
