package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
	"gastrolog/internal/lifecycle"
	"gastrolog/internal/query"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// resolveStoragePath resolves a possibly-relative filesystem path against
// vaultsDir (which defaults to homeDir when --vaults is not set) — the one
// contract every relative path stored in config follows: the stored value
// stays relative so each node resolves it independently against its own
// directory, and a consumer that reaches the filesystem through it must
// join against the same base resolveVaultDir uses, not read it raw. An
// already-absolute path, or an empty vaultsDir (base not known yet), passes
// through unchanged.
func resolveStoragePath(path, vaultsDir string) string {
	if path == "" || filepath.IsAbs(path) || vaultsDir == "" {
		return path
	}
	return filepath.Join(vaultsDir, path)
}

// resolveVaultDir resolves a file vault's "dir" parameter relative to vaultsDir.
// If dir is empty, defaults to "vaults/<vaultName>". Relative paths are joined
// with vaultsDir via resolveStoragePath. The returned map is always a new copy
// — the caller's params are never mutated. The stored config retains the
// original relative path so each node resolves independently against its own
// directory.
func resolveVaultDir(params map[string]string, vaultsDir, vaultID string) map[string]string {
	dir := params["dir"]
	if dir == "" {
		dir = filepath.Join("vaults", vaultID)
	}
	dir = resolveStoragePath(dir, vaultsDir)
	out := maps.Clone(params)
	if out == nil {
		out = make(map[string]string)
	}
	out["dir"] = dir
	return out
}

// AddVault adds a new vault (chunk manager, index manager, query engine) and updates the route table.
// Loads the full config internally to resolve the vault's vault IDs to vault configs.
// Returns ErrDuplicateID if a vault with this ID already exists.
func (o *Orchestrator) AddVault(ctx context.Context, vaultCfg system.VaultConfig, factories Factories) error {
	sys, err := o.loadSystem(ctx)
	if err != nil {
		return fmt.Errorf("load config for AddVault: %w", err)
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if _, exists := o.vaults[vaultCfg.ID]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateID, vaultCfg.ID)
	}

	instance, err := o.buildVaultInstance(sys, vaultCfg, factories)
	if err != nil {
		return fmt.Errorf("build vault instance for %s: %w", vaultCfg.ID, err)
	}

	// Register vault (even when no instance exists locally — placements may
	// land on this node later via handleVaultPut).
	vault := NewVault(vaultCfg.ID, instance)
	vault.Name = vaultCfg.Name
	vault.StorageType = string(vaultCfg.Type)
	o.vaults[vaultCfg.ID] = vault
	o.bindVaultInstanceToCtlLocked(vaultCfg.ID, instance)
	o.signalVaultReadyChange()
	o.scheduleHomeIndexSweep(vaultCfg.ID, instance)

	// Recompile the routing table immediately so the vault can receive
	// records right away. The rotation sweep also reconciles every 15s
	// as a safety net.
	if sys != nil {
		_ = o.reloadRoutesFromConfig(sys)
	}

	// Rotation and retention are reconciled by the discovery-based sweep
	// jobs on their next tick.

	o.logger.Info("vault added", "id", vaultCfg.ID, "name", vaultCfg.Name, "has_instance", instance != nil)
	return nil
}

// Rotation and retention are handled by discovery-based sweep jobs
// (rotationSweep and retentionSweepAll). No per-vault setup needed during AddVault.

func findVaultConfig(vaults []system.VaultConfig, id glid.GLID) *system.VaultConfig {
	for i := range vaults {
		if vaults[i].ID == id {
			return &vaults[i]
		}
	}
	return nil
}

// resolveRetentionRulesFromVault converts vault retention rules to resolved
// retentionRule objects. The second return names every referenced policy
// that resolved with NO trigger (ToRetentionPolicy returned nil, nil — none
// of maxAge/maxSize/maxChunks set): a vault whose RetentionRules are all
// trigger-less has retention configured but enforces nothing. The caller
// uses these labels to name the gap in the retention-unenforceable alarm
// and log line rather than leaving the operator to read code.
func resolveRetentionRulesFromVault(cfg *system.Config, vaultCfg system.VaultConfig) ([]retentionRule, []string, error) {
	// Retention rules carry only the trigger policy: every fired event
	// streams records through the routing engine and always destroys the
	// chunk.
	var rules []retentionRule
	var triggerLess []string
	for _, b := range vaultCfg.RetentionRules {
		retCfg := findRetentionPolicy(cfg.RetentionPolicies, b.RetentionPolicyID)
		if retCfg == nil {
			return nil, nil, fmt.Errorf("vault %s references unknown retention policy: %s", vaultCfg.ID, b.RetentionPolicyID)
		}
		policy, err := retCfg.ToRetentionPolicy()
		if err != nil {
			return nil, nil, fmt.Errorf("invalid retention policy %s for vault %s: %w", b.RetentionPolicyID, vaultCfg.ID, err)
		}
		if policy == nil {
			triggerLess = append(triggerLess, retentionPolicyLabel(*retCfg))
			continue
		}

		// agePolicy/countPolicy are the SAME MaxAge/MaxChunks values
		// `policy` was built from (ToRetentionPolicy), isolated to one
		// dimension apiece so the post-sweep bound check can test "is
		// THIS bound still violated" independent of whatever else the
		// composite policy also bounds.
		// Errors here are unreachable in practice — ToRetentionPolicy
		// above already validated the same expressions — so a parse
		// failure degrades to "this rule states no such bound" rather
		// than aborting rule resolution a second time for the same cause.
		var agePolicy, countPolicy chunk.RetentionPolicy
		if retCfg.MaxAge != nil && !system.IsQuantityUnset(*retCfg.MaxAge) {
			if age, err := system.ParseDuration(*retCfg.MaxAge); err == nil && age > 0 {
				agePolicy = chunk.NewTTLRetentionPolicy(age)
			}
		}
		if retCfg.MaxChunks != nil && *retCfg.MaxChunks > 0 {
			countPolicy = chunk.NewCountRetentionPolicy(int(*retCfg.MaxChunks))
		}

		rules = append(rules, retentionRule{
			policy:      policy,
			refuse:      retCfg.RefuseEnabled(),
			agePolicy:   agePolicy,
			countPolicy: countPolicy,
		})
	}
	return rules, triggerLess, nil
}

// retentionPolicyLabel returns an operator-readable identifier for a
// retention policy config: its name plus ID when named, or just the ID
// otherwise. Used to name trigger-less policies in the
// retention-unenforceable alarm and log line.
func retentionPolicyLabel(c system.RetentionPolicyConfig) string {
	if c.Name != "" {
		return fmt.Sprintf("%s (%s)", c.Name, c.ID)
	}
	return c.ID.String()
}

// RemoveVault removes a vault if it's empty (no chunks with data).
// Returns ErrVaultNotFound if the vault doesn't exist.
// Returns ErrVaultNotEmpty if the vault has any chunks.
//
// Authority: the vault-ctl FSM manifest is canonical for whether a
// vault holds data. The emptiness check consults the FSM first; the
// local Chunks.List() view is corroborating evidence at most (a
// sync-lagged follower or post-recovery node may have less on disk
// than the FSM records). If either source reports non-empty, removal
// is refused — defense in depth against either an FSM read failure or
// a stale-disk-but-fresh-FSM state.
func (o *Orchestrator) RemoveVault(id glid.GLID) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	vault, exists := o.vaults[id]
	if !exists {
		return fmt.Errorf("%w: %s", ErrVaultNotFound, id)
	}

	// Primary: FSM manifest. Returns (nil, nil) when the FSM isn't
	// reachable from this node (memory-mode or no GroupManager); we
	// fall through to the local check in that case rather than
	// quietly allowing removal of a vault we can't authoritatively
	// inspect.
	fsmMetas, err := o.listClusterChunkMetasLocked(id)
	if err != nil {
		return fmt.Errorf("list FSM chunks for vault %s: %w", id, err)
	}
	for _, m := range fsmMetas {
		if m.RecordCount > 0 || !m.Sealed {
			return fmt.Errorf("%w: vault %s has data in FSM manifest", ErrVaultNotEmpty, id)
		}
	}

	// Corroborating: local disk view. Catches the inverse case where
	// the FSM has caught up to empty but residual chunks linger on
	// disk (orphans, partial repatriation, etc.) — the chunk manager's
	// orphan sweep preserves data-bearing orphans rather than deleting
	// them, so refusing removal is the operator-safe stance.
	if vaultInst := vault.Instance; vaultInst != nil {
		metas, err := vaultInst.Chunks.List()
		if err != nil {
			return fmt.Errorf("list chunks for vault %s: %w", id, err)
		}
		for _, m := range metas {
			if m.RecordCount > 0 || !m.Sealed {
				return fmt.Errorf("%w: vault %s has data on local disk", ErrVaultNotEmpty, id)
			}
		}
		if active := vaultInst.Chunks.Active(); active != nil {
			return fmt.Errorf("%w: vault %s has active chunk", ErrVaultNotEmpty, id)
		}
	}

	o.teardownVault(id, vault)
	o.logger.Info("vault removed", "id", id, "name", vault.Name, "type", vault.Type())
	return nil
}

// listClusterChunkMetasLocked is the FSM-manifest variant of
// ListClusterChunkMetas — same body, but assumes the orchestrator
// mutex is already held (RemoveVault holds o.mu for the full
// duration). Inlined to avoid a recursive lock when reading FSM
// chunk metadata as part of mutation guards.
func (o *Orchestrator) listClusterChunkMetasLocked(vaultID glid.GLID) ([]chunk.ChunkMeta, error) {
	if o.groupMgr == nil {
		return nil, nil
	}
	g := o.groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(vaultID))
	if g == nil {
		return nil, nil
	}
	var fsm *vaultctlfsm.FSM
	switch raw := g.FSM.(type) {
	case *vaultctlfsm.FSM:
		fsm = raw
	case *vaultraft.FSM:
		fsm = raw.EnsureVaultFSM(vaultID)
	default:
		return nil, nil
	}
	entries := fsm.List()
	out := make([]chunk.ChunkMeta, 0, len(entries))
	for _, e := range entries {
		out = append(out, manifestEntryToChunkMeta(e, e.IsSealed()))
	}
	return out, nil
}

// removeVaultJobs removes the retention runner for a vault without closing
// managers or unregistering. Used by UnregisterVault and drain.
func (o *Orchestrator) removeVaultJobs(_ glid.GLID, vault *Vault) {
	if vaultInst := vault.Instance; vaultInst != nil {
		delete(o.retention, retentionKey(vaultInst.VaultID, vaultInst.StorageID))
		// Cancel pending cloud-upload jobs (both the catch-up sweep's and the
		// live seal effect's — they share one name now) and drop any stranded
		// backoff/alarm state for this vault. Without this, a vault that
		// leaves this node mid-backoff keeps its cloud-backfill-stuck alarm
		// standing until the next evaluateCloudHealth sweep notices (or
		// forever, if this node never runs that sweep for the vault
		// again).
		o.scheduler.RemoveJobsByPrefix(cloudUploadJobPrefix(vaultInst.VaultID))
		o.purgeBackfillFailuresForVault(vaultInst.VaultID)
	}
}

// teardownVault performs the common cleanup for all vault removal paths:
// cancels pending jobs, closes managers, and removes from the registry. The
// pipeline routing table and Origin registrations are reconciled separately by
// ReloadFilters when the deletion lands in config.
func (o *Orchestrator) teardownVault(id glid.GLID, vault *Vault) {
	o.decommissionVaultControlPlaneRaftGroup(id)

	// Cancel pending post-seal/compress/index jobs to prevent use-after-close.
	vaultPrefix := id.String()
	o.scheduler.RemoveJobsByPrefix("post-seal:" + vaultPrefix)
	o.scheduler.RemoveJobsByPrefix("compress:" + vaultPrefix)
	o.scheduler.RemoveJobsByPrefix("index-build:" + vaultPrefix)
	// Same reasoning as removeVaultJobs: a torn-down vault must not leave a
	// stranded cloud-backfill-stuck alarm behind, and an in-flight upload
	// must not outlive the chunk manager it reads through.
	o.scheduler.RemoveJobsByPrefix(cloudUploadJobPrefix(id))
	o.purgeBackfillFailuresForVault(id)

	// Remove the per-instance retention runner.
	if vaultInst := vault.Instance; vaultInst != nil {
		delete(o.retention, retentionKey(vaultInst.VaultID, vaultInst.StorageID))
	}

	// Close chunk/index managers to release file locks.
	if err := vault.Close(); err != nil {
		o.logger.Warn("failed to close vault during teardown", "vault", id, "error", err)
	}

	delete(o.vaults, id)
	o.signalVaultReadyChange()
}

// DisableVault disables ingestion for a vault.
// Disabled vaults will not receive new records from the ingest pipeline.
// Returns ErrVaultNotFound if the vault doesn't exist.
func (o *Orchestrator) DisableVault(id glid.GLID) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	vault, exists := o.vaults[id]
	if !exists {
		return fmt.Errorf("%w: %s", ErrVaultNotFound, id)
	}

	vault.Enabled = false
	o.logger.Info("vault disabled", "id", id, "name", vault.Name, "type", vault.Type())
	return nil
}

// EnableVault enables ingestion for a vault.
// Returns ErrVaultNotFound if the vault doesn't exist.
func (o *Orchestrator) EnableVault(id glid.GLID) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	vault, exists := o.vaults[id]
	if !exists {
		return fmt.Errorf("%w: %s", ErrVaultNotFound, id)
	}

	vault.Enabled = true
	o.logger.Info("vault enabled", "id", id, "name", vault.Name, "type", vault.Type())
	return nil
}

// IsVaultEnabled returns whether ingestion is enabled for the given vault.
func (o *Orchestrator) IsVaultEnabled(id glid.GLID) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if vault := o.vaults[id]; vault != nil {
		return vault.Enabled
	}
	return false
}

// ForceRemoveVault removes a vault regardless of whether it contains data.
// It seals the active chunk if present, deletes all indexes and chunks,
// closes the chunk manager, and cleans up all associated resources.
// Returns ErrVaultNotFound if the vault doesn't exist.
func (o *Orchestrator) ForceRemoveVault(id glid.GLID) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	vault, exists := o.vaults[id]
	if !exists {
		return fmt.Errorf("%w: %s", ErrVaultNotFound, id)
	}
	// Seal active chunk and delete all data on this node's instance.
	if err := o.forceRemoveVaultData(id, vault.Instance); err != nil {
		return err
	}

	o.teardownVault(id, vault)
	o.logger.Info("vault force-removed", "id", id, "name", vault.Name, "type", vault.Type())
	return nil
}

// forceRemoveVaultData seals the active chunk (if any), deletes all
// indexes, and locally drops every chunk on the given instance. LOCAL
// cleanup only — uses chunk.DeleteNoAnnounce so per-chunk deletes do not
// fan across vault-ctl Raft. See sealAndDeleteAllChunks.
func (o *Orchestrator) forceRemoveVaultData(id glid.GLID, vaultInst *VaultInstance) error {
	if vaultInst == nil {
		return nil
	}
	cm := vaultInst.Chunks
	im := vaultInst.Indexes

	if active := cm.Active(); active != nil {
		if err := cm.Seal(); err != nil {
			return fmt.Errorf("seal active chunk for vault %s: %w", id, err)
		}
	}

	metas, err := cm.List()
	if err != nil {
		return fmt.Errorf("list chunks for vault %s: %w", id, err)
	}
	for _, meta := range metas {
		if im != nil {
			if err := im.DeleteIndexes(meta.ID); err != nil {
				o.logger.Warn("failed to delete indexes during force remove",
					"vault", id, "chunk", meta.ID.String(), "error", err)
			}
		}
		if err := chunk.DeleteNoAnnounce(cm, meta.ID); err != nil {
			return fmt.Errorf("delete chunk %s in vault %s: %w", meta.ID.String(), id, err)
		}
	}
	return nil
}

// sealAndDeleteAllChunks seals the active chunk (if any), then deletes all
// chunks and their indexes. Returns the number of chunks found. Errors are
// logged with the given prefix but do not abort the cleanup.
//
// CRITICAL: this is a LOCAL cleanup path — it removes this node's on-disk
// copies only and MUST NOT drive a cluster-wide delete through the vault-ctl
// FSM receipt protocol. The intended cluster-wide effect (when
// RemoveVaultInstance reacts to placement loss, or DeleteVaultInstance
// reacts to an admin teardown) comes from each node independently running
// its own RemoveVaultInstance as the config change propagates — not from
// a delete proposed out of one node. DeleteNoAnnounce is the local-only
// delete that bypasses the receipt protocol.
//
// The seal obeys the same rule: SealNoAnnounce demotes the local active so it
// can be deleted, where Seal() would announce a cluster-wide Active → Sealing
// transition that this node then never completes (it is discarding the bytes,
// not assembling a sealed form) — leaving every replica's manifest entry
// parked in Sealing.
func (o *Orchestrator) sealAndDeleteAllChunks(vaultInst *VaultInstance, op string, vaultID glid.GLID) int {
	if active := vaultInst.Chunks.Active(); active != nil {
		if err := chunk.SealNoAnnounce(vaultInst.Chunks, active.ID); err != nil {
			o.logger.Warn(op+": seal failed", "vault", vaultID, "error", err)
		}
	}
	metas, err := vaultInst.Chunks.List()
	if err != nil {
		o.logger.Warn(op+": list failed", "vault", vaultID, "error", err)
		return 0
	}
	for _, m := range metas {
		if vaultInst.Indexes != nil {
			if err := vaultInst.Indexes.DeleteIndexes(m.ID); err != nil {
				o.logger.Warn(op+": delete indexes failed", "vault", vaultID, "chunk", m.ID, "error", err)
			}
		}
		if err := chunk.DeleteNoAnnounce(vaultInst.Chunks, m.ID); err != nil {
			o.logger.Warn(op+": delete chunk failed", "vault", vaultID, "chunk", m.ID, "error", err)
		}
	}
	return len(metas)
}

// RemoveVaultInstance unregisters a vault instance from this node WITHOUT
// destroying its on-disk data. Used when placement moves the instance elsewhere
// (transient — the node may well get the instance back seconds later when
// placement flaps back). The instance's Chunks/Indexes managers are closed, jobs
// are cancelled, and the VaultInstance is removed from the vault's vault list,
// but the chunk files and vault directory remain on disk. A subsequent
// AddVaultInstance will re-discover the existing chunks.
//
// For actual instance deletion (admin-driven), use DeleteVaultInstance which
// additionally wipes all chunks and removes the data directory.
//
// Returns true if an instance was removed.
//
// Keeping this path non-destructive matters because placement flaps on
// transient peer-conn teardowns (peers.Invalidate); wiping chunks here
// would destroy data cluster-wide on a flap.
func (o *Orchestrator) RemoveVaultInstance(vaultID glid.GLID) bool {
	return o.removeVaultInstance(vaultID, false)
}

// DeleteVaultInstance unregisters a vault instance AND permanently wipes its
// on-disk data (chunks, indexes, and the vault directory). Used only when a
// instance is being deliberately deleted (admin action via CmdDeleteVault, or
// post-drain cleanup).
//
// Returns true if an instance was removed.
func (o *Orchestrator) DeleteVaultInstance(vaultID glid.GLID) bool {
	return o.removeVaultInstance(vaultID, true)
}

func (o *Orchestrator) removeVaultInstance(vaultID glid.GLID, deleteData bool) bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	vault, exists := o.vaults[vaultID]
	if !exists {
		return false
	}

	vaultInst := vault.Instance
	if vaultInst == nil {
		return false
	}

	// Cancel pending jobs for this vault.
	prefix := vaultID.String()
	o.scheduler.RemoveJobsByPrefix("post-seal:" + prefix + ":" + vaultID.String())
	o.scheduler.RemoveJobsByPrefix("compress:" + prefix + ":" + vaultID.String())
	o.scheduler.RemoveJobsByPrefix("index-build:" + prefix + ":" + vaultID.String())

	// Destructive cleanup — only on explicit deletion.
	if deleteData {
		o.sealAndDeleteAllChunks(vaultInst, "remove vault placement", vaultID)
	}

	// Drop FSM → chunk-manager hooks before Close. Placement can remove the
	// instance while the vault control-plane Raft group still delivers apply
	// callbacks; without clearing, a hook could touch an already-closed
	// file.Manager.
	o.clearVaultFSMChunkCallbacks(vaultID)

	// Close managers.
	if err := vaultInst.Chunks.Close(); err != nil {
		o.logger.Warn("remove vault placement: close chunk manager failed", "vault", vaultID, "error", err)
	}

	// Remove the vault's data directory entirely — not just its chunk subdirs.
	// Without this, leftover files (.lock, cloud.idx) and the directory itself
	// accumulate on disk forever.
	if deleteData {
		if remover, ok := vaultInst.Chunks.(chunk.DirRemover); ok {
			if err := remover.RemoveDir(); err != nil {
				o.logger.Warn("remove vault placement: remove data directory failed", "vault", vaultID, "error", err)
			}
		}
	}

	// Remove the retention runner for this vault.
	delete(o.retention, retentionKey(vaultInst.VaultID, vaultInst.StorageID))

	// Drop the instance from the vault.
	vault.Instance = nil

	// On destructive removal, drop the vault entry entirely. For
	// non-destructive placement-driven removals, the vault shell stays so a
	// subsequent AddVaultInstance can rehydrate.
	if deleteData {
		delete(o.vaults, vaultID)
		o.logger.Info("vault removed", "vault", vaultID)
	}
	o.signalVaultReadyChange()

	o.logger.Info("vault placement removed",
		"vault", vaultID, "deleteData", deleteData)
	return true
}

// AddVaultInstance builds the single VaultInstance for an already-registered
// vault. The orchestrator picks up storage/policy fields from the VaultConfig.
func (o *Orchestrator) AddVaultInstance(ctx context.Context, vaultID glid.GLID, factories Factories) error {
	sys, err := o.loadSystem(ctx)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	o.mu.Lock()
	vault, exists := o.vaults[vaultID]
	if !exists {
		o.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	// Already present?
	if vault.Instance != nil && vault.Instance.VaultID == vaultID {
		o.mu.Unlock()
		return nil
	}
	o.mu.Unlock()

	cfg := &sys.Config
	rt := &sys.Runtime

	vaultCfg := findVaultConfig(cfg.Vaults, vaultID)
	if vaultCfg == nil {
		return fmt.Errorf("vault %s not found in config", vaultID)
	}

	// Every node is a vault-ctl voter, storage placement or not.
	o.ensureVaultControlPlaneRaftGroup(vaultID, rt.Nodes, factories)

	nscs := rt.NodeStorageConfigs
	placements := sys.PlacementsFor(vaultCfg.ID)
	leaderNodeID := system.LeaderNodeID(placements, nscs)
	followerNodeIDs := system.FollowerNodeIDs(placements, nscs)
	isLeader := leaderNodeID == "" || leaderNodeID == o.localNodeID
	isFollower := slices.Contains(followerNodeIDs, o.localNodeID)
	if !isLeader && !isFollower {
		return nil
	}

	var ti *VaultInstance
	if isLeader {
		t, err := o.buildLeaderInstance(sys, *vaultCfg, factories)
		if err != nil {
			return fmt.Errorf("build vault %s: %w", vaultID, err)
		}
		t.FollowerTargets = system.FollowerTargets(placements, nscs)
		ti = t
	} else {
		for _, tgt := range system.FollowerTargets(placements, nscs) {
			if tgt.NodeID != o.localNodeID {
				continue
			}
			t, err := o.buildInstanceForStorage(sys, *vaultCfg, factories, tgt.StorageID, true)
			if err != nil {
				return fmt.Errorf("build vault %s storage %s: %w", vaultID, tgt.StorageID, err)
			}
			t.IsFollower = true
			t.LeaderNodeID = leaderNodeID
			t.StorageID = tgt.StorageID
			t.Chunks.SetRotationPolicy(chunk.NeverRotatePolicy{})
			ti = t
			break
		}
	}

	if ti == nil {
		return nil
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	vault = o.vaults[vaultID]
	if vault == nil {
		_ = ti.Chunks.Close()
		return fmt.Errorf("%w: %s (disappeared during build)", ErrVaultNotFound, vaultID)
	}
	if vault.Instance != nil {
		// Race: someone built the instance ahead of us. Discard the
		// duplicate so we don't leak managers / file handles.
		_ = ti.Chunks.Close()
		return nil
	}
	vault.Instance = ti
	o.bindVaultInstanceToCtlLocked(vaultID, ti)
	o.signalVaultReadyChange()
	o.scheduleHomeIndexSweep(vaultID, ti)
	o.logger.Info("vault placement added", "vault", vaultID)
	return nil
}

// UnregisterVault removes a vault from the orchestrator without deleting any
// data. The chunk manager is closed (releasing connections/locks) but chunks
// and indexes are left intact in storage. This is the correct operation for
// cloud vault reassignment — the data lives in shared object storage and the
// new node will open a fresh Manager pointing to the same bucket.
func (o *Orchestrator) UnregisterVault(id glid.GLID) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	vault, exists := o.vaults[id]
	if !exists {
		return fmt.Errorf("%w: %s", ErrVaultNotFound, id)
	}

	// Cancel pending post-seal/compress/index jobs before closing the chunk manager.
	vaultPrefix := id.String()
	o.scheduler.RemoveJobsByPrefix("post-seal:" + vaultPrefix)
	o.scheduler.RemoveJobsByPrefix("compress:" + vaultPrefix)
	o.scheduler.RemoveJobsByPrefix("index-build:" + vaultPrefix)

	// Stop vault control-plane Raft before closing chunk managers — same
	// ordering as teardownVault. Otherwise trailing apply callbacks can
	// touch a closed Manager. Stop only, never decommission: the vault is
	// being reassigned, not deleted, and its group must recover its WAL
	// state when it re-ensures.
	o.destroyVaultControlPlaneRaftGroup(id)

	if err := vault.Close(); err != nil {
		o.logger.Warn("failed to close vault during unregister",
			"vault", id, "error", err)
	}

	// Remove the per-vault retention runner.
	o.removeVaultJobs(id, vault)

	// Remove from registry.
	delete(o.vaults, id)
	o.signalVaultReadyChange()

	o.logger.Info("vault unregistered (data preserved)", "id", id, "name", vault.Name, "type", vault.Type())
	return nil
}

// VaultConfig returns the effective configuration for a vault.
// This is useful for API responses and debugging.
func (o *Orchestrator) VaultConfig(id glid.GLID) (system.VaultConfig, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if _, exists := o.vaults[id]; !exists {
		return system.VaultConfig{}, fmt.Errorf("%w: %s", ErrVaultNotFound, id)
	}

	cfg := system.VaultConfig{
		ID: id,
	}

	return cfg, nil
}

// buildVaultInstances creates VaultInstance objects for each instance in the vault config.
// Every node joins every instance's Raft group regardless of storage placement.
// Nodes with storage placements also get a VaultInstance with a chunk manager;
// nodes without storage only participate in the Raft group.
func (o *Orchestrator) buildVaultInstance(sys *system.System, vaultCfg system.VaultConfig, factories Factories) (*VaultInstance, error) {
	rt := &sys.Runtime
	// The group comes up before the chunk manager: Raft log replay is fast,
	// the chunk manager's disk scan is slow, so the group elects and catches
	// up while chunks load.
	o.ensureVaultControlPlaneRaftGroup(vaultCfg.ID, rt.Nodes, factories)

	vaultID := vaultCfg.ID

	// Determine this node's role for this vault. With one-replica-per-node
	// (Phase 2 invariant) the node is at most one of: leader, follower, neither.
	nscs := rt.NodeStorageConfigs
	placements := sys.PlacementsFor(vaultCfg.ID)
	leaderNodeID := system.LeaderNodeID(placements, nscs)
	followerNodeIDs := system.FollowerNodeIDs(placements, nscs)
	isLeader := leaderNodeID == "" || leaderNodeID == o.localNodeID
	isFollower := slices.Contains(followerNodeIDs, o.localNodeID)
	if !isLeader && !isFollower {
		return nil, nil
	}

	if isLeader {
		ti, err := o.buildLeaderInstance(sys, vaultCfg, factories)
		if err != nil {
			o.alertVaultInitFailed(vaultID, vaultCfg.Name, err)
			return nil, nil
		}
		ti.FollowerTargets = system.FollowerTargets(placements, nscs)
		return ti, nil
	}

	// Follower: build the instance for this node's placement.
	for _, tgt := range system.FollowerTargets(placements, nscs) {
		if tgt.NodeID != o.localNodeID {
			continue
		}
		sti, err := o.buildInstanceForStorage(sys, vaultCfg, factories, tgt.StorageID, true)
		if err != nil {
			o.alertVaultInitFailed(vaultID, vaultCfg.Name, err)
			return nil, nil
		}
		sti.IsFollower = true
		sti.LeaderNodeID = leaderNodeID
		sti.StorageID = tgt.StorageID
		sti.Chunks.SetRotationPolicy(chunk.NeverRotatePolicy{})
		return sti, nil
	}
	return nil, nil
}

// alertVaultInitFailed logs a warning and raises an alert when a vault
// instance fails to initialize during build. The failed instance is retried on
// the next reconfig cycle.
func (o *Orchestrator) alertVaultInitFailed(vaultID glid.GLID, vaultName string, err error) {
	o.logger.Warn("buildVaultInstances: vaultInst init failed, skipping",
		"vault", vaultID, "name", vaultName, "error", err)
	if o.alerts != nil {
		o.alerts.Raise("vault-init", vaultID.String(),
			fmt.Sprintf("Vault %q failed to initialize: %v", vaultName, err))
	}
}

// buildLeaderInstance creates the leader VaultInstance using the placement's
// storage ID. This avoids directory collisions with same-node follower placements
// that would occur if findLocalFileStorage picked a different storage by class.
func (o *Orchestrator) buildLeaderInstance(sys *system.System, vaultCfg system.VaultConfig, factories Factories) (*VaultInstance, error) {
	// Read placements from VaultConfig (mirrored from vault placements via
	// the FSM bridge).
	storageID := system.LeaderStorageID(sys.PlacementsFor(vaultCfg.ID))
	if storageID != "" && !strings.HasPrefix(storageID, system.SyntheticStoragePrefix) {
		ti, err := o.buildInstanceForStorage(sys, vaultCfg, factories, storageID, false)
		if err != nil {
			return nil, err
		}
		ti.StorageID = storageID
		return ti, nil
	}
	// Synthetic or unplaced — fall back to class-based resolution.
	ti, err := o.buildInstance(sys, vaultCfg, factories, false)
	if err != nil {
		return nil, err
	}
	ti.StorageID = storageID
	return ti, nil
}

// buildInstance creates a single VaultInstance from a VaultConfig.
// When isFollower is true, cloud backing params are stripped so the follower's
// PostSealProcess only runs compress + index without uploading to cloud storage.
// Cloud-backed vaults share a blob key (vault-ID/chunk-ID.glcb) — if the follower
// also uploads, it overwrites the leader's blob with a different-sized version,
// corrupting the leader's stored diskBytes and breaking all future cloud reads.
func (o *Orchestrator) buildInstance(sys *system.System, vaultCfg system.VaultConfig, factories Factories, isFollower bool) (*VaultInstance, error) {
	cfg := &sys.Config
	factoryName := mapVaultTypeToFactory(vaultCfg.Type)

	// Build params from instance system.
	params := buildVaultParams(sys, vaultCfg, o.localNodeID)

	// Followers keep cloud store access for reads (queries) but skip uploads.
	// The leader owns the blob; the follower adopts it lazily via the
	// cloud-backed resolver once the vault-ctl FSM carries the upload.
	if isFollower {
		params["_cloud_read_only"] = "true"
	}

	cmFactory, ok := factories.ChunkManagers[factoryName]
	if !ok {
		return nil, fmt.Errorf("unknown chunk manager type: %s (mapped from vaultInst type %s)", factoryName, vaultCfg.Type)
	}

	var cmLogger = factories.Logger
	if cmLogger != nil {
		cmLogger = cmLogger.With("vault", vaultCfg.ID)
	}

	cmParams := resolveVaultDir(params, factories.VaultsDir, vaultCfg.ID.String())
	cmParams["_expect_existing"] = "true"
	cmParams["_vault_id"] = vaultCfg.ID.String()

	// Resolve JSONL path relative to HomeDir (not VaultsDir).
	if p := cmParams["path"]; p != "" && !filepath.IsAbs(p) && factories.HomeDir != "" {
		cmParams["path"] = filepath.Join(factories.HomeDir, p)
	}

	cm, err := cmFactory(cmParams, cmLogger)
	if err != nil {
		return nil, fmt.Errorf("create chunk manager: %w", err)
	}

	// Apply rotation policy from instance.
	if err := applyRotationPolicy(cm, cfg.RotationPolicies, vaultCfg.RotationPolicyID); err != nil {
		_ = cm.Close()
		return nil, err
	}

	// Wire the FSM-backed integrity verifier so cold-cache cloud downloads
	// reject blobs whose digest doesn't match what the leader stamped at
	// upload time.
	setIntegrityVerifier(cm, o.IntegrityVerifier())

	// JSONL sinks are write-only — no query engine, no indexes.
	if vaultCfg.Type == system.VaultTypeJSONL {
		ti := &VaultInstance{
			VaultID: vaultCfg.ID,
			Type:    string(vaultCfg.Type),
			Chunks:  cm,
		}
		ti.Reconciler = NewVaultLifecycleReconciler(o, vaultCfg.ID, ti, o.localNodeID, o.baseLogger)
		return ti, nil
	}

	imFactory, ok := factories.IndexManagers[factoryName]
	if !ok {
		_ = cm.Close()
		return nil, fmt.Errorf("unknown index manager type: %s (mapped from vaultInst type %s)", factoryName, vaultCfg.Type)
	}
	var imLogger = factories.Logger
	if imLogger != nil {
		imLogger = imLogger.With("vault", vaultCfg.ID)
	}
	im, err := imFactory(cmParams, cm, imLogger)
	if err != nil {
		_ = cm.Close()
		return nil, fmt.Errorf("create index manager: %w", err)
	}

	var qeLogger = factories.Logger
	if qeLogger != nil {
		qeLogger = qeLogger.With("vault", vaultCfg.ID)
	}
	qe := query.New(cm, im, qeLogger)

	// The index manager builds a sealed chunk's secondary indexes; the
	// chunk manager carries it as its post-seal builder, and the startup
	// missing-index sweep only visits vaults whose chunk manager has one.
	if processor, ok := cm.(chunk.ChunkPostSealProcessor); ok {
		processor.SetIndexBuilders([]chunk.ChunkIndexBuilder{im.BuildAdapter()})
	}

	ti := &VaultInstance{
		VaultID: vaultCfg.ID,
		Type:    string(vaultCfg.Type),
		Chunks:  cm,
		Indexes: im,
		Query:   qe,
	}
	ti.Reconciler = NewVaultLifecycleReconciler(o, vaultCfg.ID, ti, o.localNodeID, o.baseLogger)
	o.installPipelineGLCBResolver(ti, vaultCfg.ID)
	return ti, nil
}

// buildInstanceForStorage creates a VaultInstance whose data directory is
// resolved from a specific file storage ID. Used for both leaders with
// explicit storage placements and followers (one per node per instance).
func (o *Orchestrator) buildInstanceForStorage(sys *system.System, vaultCfg system.VaultConfig, factories Factories, storageID string, isFollower bool) (*VaultInstance, error) {
	cfg := &sys.Config
	fs := findFileStorageByID(&sys.Runtime, storageID)
	if fs == nil {
		return nil, fmt.Errorf("file storage %s not found", storageID)
	}

	// Build params normally, then override the dir with this storage's path.
	params := buildVaultParams(sys, vaultCfg, o.localNodeID)
	// Followers keep cloud store access for reads but skip uploads.
	if isFollower {
		params["_cloud_read_only"] = "true"
	}
	params["dir"] = filepath.Join(fs.Path, "vaults", vaultCfg.ID.String(), vaultCfg.ID.String())

	factoryName := mapVaultTypeToFactory(vaultCfg.Type)
	cmFactory, ok := factories.ChunkManagers[factoryName]
	if !ok {
		return nil, fmt.Errorf("unknown chunk manager type: %s", factoryName)
	}

	var cmLogger = factories.Logger
	if cmLogger != nil {
		cmLogger = cmLogger.With("vault", vaultCfg.ID, "storage", storageID)
	}

	cmParams := resolveVaultDir(params, factories.VaultsDir, vaultCfg.ID.String())
	cmParams["_expect_existing"] = "true"
	cmParams["_vault_id"] = vaultCfg.ID.String()

	cm, err := cmFactory(cmParams, cmLogger)
	if err != nil {
		return nil, fmt.Errorf("create chunk manager: %w", err)
	}

	if err := applyRotationPolicy(cm, cfg.RotationPolicies, vaultCfg.RotationPolicyID); err != nil {
		_ = cm.Close()
		return nil, err
	}

	// Follower replicas need index builders for local queries.
	imFactory, ok := factories.IndexManagers[factoryName]
	if !ok {
		_ = cm.Close()
		return nil, fmt.Errorf("unknown index manager type: %s", factoryName)
	}
	var imLogger = factories.Logger
	if imLogger != nil {
		imLogger = imLogger.With("vault", vaultCfg.ID, "storage", storageID)
	}
	im, err := imFactory(cmParams, cm, imLogger)
	if err != nil {
		_ = cm.Close()
		return nil, fmt.Errorf("create index manager: %w", err)
	}

	var qeLogger = factories.Logger
	if qeLogger != nil {
		qeLogger = qeLogger.With("vault", vaultCfg.ID, "storage", storageID)
	}
	qe := query.New(cm, im, qeLogger)

	if processor, ok := cm.(chunk.ChunkPostSealProcessor); ok {
		processor.SetIndexBuilders([]chunk.ChunkIndexBuilder{im.BuildAdapter()})
	}

	ti := &VaultInstance{
		VaultID: vaultCfg.ID,
		Type:    string(vaultCfg.Type),
		Chunks:  cm,
		Indexes: im,
		Query:   qe,
	}
	ti.Reconciler = NewVaultLifecycleReconciler(o, vaultCfg.ID, ti, o.localNodeID, o.baseLogger)
	o.installPipelineGLCBResolver(ti, vaultCfg.ID)
	return ti, nil
}

// findFileStorageByID resolves a file storage ID to its config across all nodes.
func findFileStorageByID(rt *system.Runtime, storageID string) *system.FileStorage {
	for _, nsc := range rt.NodeStorageConfigs {
		for i := range nsc.FileStorages {
			if nsc.FileStorages[i].ID.String() == storageID {
				return &nsc.FileStorages[i]
			}
		}
	}
	return nil
}

// applyRotationPolicy resolves and applies a rotation policy to a chunk manager.
func applyRotationPolicy(cm chunk.ChunkManager, policies []system.RotationPolicyConfig, policyID *glid.GLID) error {
	if policyID == nil {
		return nil
	}
	policyCfg := findRotationPolicy(policies, *policyID)
	if policyCfg == nil {
		return nil
	}
	policy, err := policyCfg.ToRotationPolicy()
	if err != nil {
		return fmt.Errorf("invalid rotation policy %s: %w", *policyID, err)
	}
	if policy != nil {
		cm.SetRotationPolicy(policy)
	}
	return nil
}

// destroyVaultControlPlaneRaftGroup stops the local control-plane Raft group
// without touching its durable state — for a vault leaving this node while
// it lives on elsewhere (reassignment), where the group re-ensures later.
func (o *Orchestrator) destroyVaultControlPlaneRaftGroup(vaultID glid.GLID) {
	if o.vaultCtlLeaders != nil {
		o.vaultCtlLeaders.Stop(vaultID)
	}
	if o.groupMgr == nil {
		return
	}
	gid := raftgroup.VaultControlPlaneGroupID(vaultID)
	if err := o.groupMgr.DestroyGroup(gid); err != nil && !errors.Is(err, raftgroup.ErrGroupNotFound) {
		o.logger.Debug("destroy vault control-plane raft group",
			"vault", vaultID, "error", err)
	}
}

// decommissionVaultControlPlaneRaftGroup permanently retires a deleted
// vault's control-plane group: stops it and drops its state from the shared
// WAL. Vault deletion reaches every node (the delete notification fans out),
// so the WAL forgets the group cluster-wide.
func (o *Orchestrator) decommissionVaultControlPlaneRaftGroup(vaultID glid.GLID) {
	if o.vaultCtlLeaders != nil {
		o.vaultCtlLeaders.Stop(vaultID)
	}
	if o.groupMgr == nil {
		return
	}
	gid := raftgroup.VaultControlPlaneGroupID(vaultID)
	if err := o.groupMgr.DecommissionGroup(gid); err != nil {
		o.logger.Warn("decommission vault control-plane raft group",
			"vault", vaultID, "error", err)
	}
}

// ensureVaultControlPlaneRaftGroup joins this node to the vault's vault-ctl
// Raft group, creating it when the full cluster member set resolves, and
// binds the group (bindVaultCtlGroup). Creation defers while the local node
// is not yet in the cluster node list or a peer is not yet resolvable;
// RefreshVaultCtlMembers retries on the next node-config change. Idempotent;
// safe on every reconfigure sweep.
func (o *Orchestrator) ensureVaultControlPlaneRaftGroup(vaultID glid.GLID, clusterNodes []system.NodeConfig, factories Factories) {
	if factories.GroupManager == nil {
		return
	}
	gid := raftgroup.VaultControlPlaneGroupID(vaultID)
	vfsm := vaultraft.NewFSM()
	vfsm.SetOnAfterRestore(func() { o.afterVaultCtlRestore(vaultID) })
	g, members := o.tryStartClusterWideRaftGroup(gid, vfsm, clusterNodes, factories)
	if g != nil {
		o.vaultCtlLeaders.SetDesiredMembers(vaultID, members)
	} else if g = factories.GroupManager.GetGroup(gid); g == nil {
		return
	}
	o.bindVaultCtlGroup(vaultID, g)
}

// bindVaultCtlGroup wires the vault-level consumers of a vault-ctl group: the
// after-restore hook, the pipeline chunk events on the live sub-FSM, and the
// leader loop. Every node runs them, local instance or not. Idempotent.
func (o *Orchestrator) bindVaultCtlGroup(vaultID glid.GLID, g *raftgroup.Group) {
	if vfsm, ok := g.FSM.(*vaultraft.FSM); ok && vfsm != nil {
		vfsm.SetOnAfterRestore(func() { o.afterVaultCtlRestore(vaultID) })
		wireVaultFSMPipelineChunkEvents(o, vaultID, vfsm.EnsureVaultFSM(vaultID))
	}
	o.vaultCtlLeaders.Start(vaultID, g)
}

// bindVaultInstanceToCtlLocked binds a vault's local instance to its vault's
// vault-ctl group. It runs wherever an instance and its group meet — instance
// registration, group creation by RefreshVaultCtlMembers, and snapshot restore
// — and is idempotent.
//
// The Raft-backed facets and the announcer are installed once, at
// registration, and read the instance's group pointer on every call: before
// the group exists on this node they report not-ready and nothing, and they
// follow the group through its creation and through restores without being
// rewritten. What the sub-FSM pushes — the lifecycle reconciler's callbacks,
// OnCreate, OnUpload — and the cloud-backed resolver attach to the live
// sub-FSM, and move when a restore replaces it.
//
// Returns the live sub-FSM, or nil while the group does not exist. Caller
// holds o.mu.
func (o *Orchestrator) bindVaultInstanceToCtlLocked(vaultID glid.GLID, t *VaultInstance) *vaultctlfsm.FSM {
	if t == nil || o.groupMgr == nil {
		return nil
	}
	g := o.vaultCtlGroup(vaultID)
	if t.IsFSMReady == nil && (o.vaultCtlReplicated || g != nil) {
		applier := instanceVaultCtlChunkApplier{o: o, t: t}
		t.applyRaftCallbacks(instanceVaultRaftCallbacks(t, applier))
		setVaultRaftAnnouncer(t.Chunks, applier, o.phase, o.logger, o.announceResultHook(vaultID))
	}
	if g == nil {
		return nil
	}
	t.vaultCtlGroup.Store(g)
	live := vaultCtlSubFSM(g, vaultID)
	if live == nil || t.vaultCtl == live {
		return live
	}
	if t.Reconciler != nil {
		t.Reconciler.Wire(live)
	}
	wireVaultFSMOnCreate(g, vaultID, t.Chunks, o)
	wireVaultFSMOnUpload(g, vaultID, o)
	o.wireLazyCloudBackedResolver(g, vaultID, t.Chunks)
	t.vaultCtl = live
	return live
}

// vaultCtlGroup returns the vault's vault-ctl group on this node, or nil.
func (o *Orchestrator) vaultCtlGroup(vaultID glid.GLID) *raftgroup.Group {
	if o.groupMgr == nil {
		return nil
	}
	return o.groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(vaultID))
}

// instanceVaultRaftCallbacks builds an instance's Raft-backed callbacks
// against the group the instance is bound to at call time.
func instanceVaultRaftCallbacks(t *VaultInstance, applier vaultctlfsm.Applier) vaultRaftCallbacks {
	raft := func() *hraft.Raft {
		if g := t.vaultCtlGroup.Load(); g != nil {
			return g.Raft
		}
		return nil
	}
	fsm := func() *vaultctlfsm.FSM {
		if g := t.vaultCtlGroup.Load(); g != nil {
			return vaultCtlSubFSM(g, t.VaultID)
		}
		return nil
	}
	return buildVaultRaftCallbacks(raft, fsm, applier)
}

// instanceVaultCtlChunkApplier applies a vault's sub-FSM commands through the
// group its instance is bound to at Apply time.
type instanceVaultCtlChunkApplier struct {
	o *Orchestrator
	t *VaultInstance
}

func (a instanceVaultCtlChunkApplier) Apply(data []byte) error {
	g := a.t.vaultCtlGroup.Load()
	if g == nil {
		return fmt.Errorf("vault control-plane raft group %q not running on this node", raftgroup.VaultControlPlaneGroupID(a.t.VaultID))
	}
	return a.o.vaultCtlChunkApplier(g, a.t.VaultID).Apply(data)
}

// vaultCtlSubFSM returns the vault's sub-FSM inside its vault-ctl group,
// creating it empty when no command has reached it yet.
func vaultCtlSubFSM(g *raftgroup.Group, vaultID glid.GLID) *vaultctlfsm.FSM {
	switch raw := g.FSM.(type) {
	case *vaultctlfsm.FSM:
		return raw
	case *vaultraft.FSM:
		if raw != nil {
			return raw.EnsureVaultFSM(vaultID)
		}
	}
	return nil
}

// vaultCtlChunkApplier returns the applier for a vault's sub-FSM commands:
// forwarded to the group leader when peers are wired, applied through the
// local group otherwise.
func (o *Orchestrator) vaultCtlChunkApplier(g *raftgroup.Group, vaultID glid.GLID) vaultctlfsm.Applier {
	if o.peerConns != nil {
		gid := raftgroup.VaultControlPlaneGroupID(vaultID)
		return cluster.NewVaultCtlChunkApplyForwarder(g.Raft, gid, vaultID, groupApplyWait(g), o.peerConns, cluster.ReplicationTimeout)
	}
	return &vaultCtlApplier{o: o, vaultID: vaultID}
}

// tryStartClusterWideRaftGroup creates or returns an existing cluster-wide Raft group
// (symmetric seeding across all resolvable cluster nodes). The second return is
// the resolved member list when the group is (or will be) active on this node;
// both are nil when creation is deferred or fails.
func (o *Orchestrator) tryStartClusterWideRaftGroup(groupID string, fsm hraft.FSM, clusterNodes []system.NodeConfig, factories Factories) (*raftgroup.Group, []hraft.Server) {
	if factories.GroupManager == nil {
		return nil, nil
	}
	members := o.buildVaultRaftMembers(clusterNodes, factories)
	if len(members) < len(clusterNodes) {
		o.logger.Debug("cluster-wide raft group: not all cluster nodes resolvable, deferring creation",
			"group", groupID,
			"have", len(members),
			"want", len(clusterNodes))
		return nil, nil
	}
	isMember := false
	for _, srv := range members {
		if string(srv.ID) == o.localNodeID {
			isMember = true
			break
		}
	}
	if !isMember {
		return nil, nil
	}
	g := factories.GroupManager.GetGroup(groupID)
	if g != nil {
		return g, members
	}
	g, err := factories.GroupManager.CreateGroup(raftgroup.GroupConfig{
		GroupID:     groupID,
		FSM:         fsm,
		SeedMembers: members,
	})
	if err != nil {
		o.logger.Warn("failed to create cluster-wide raft group", "group", groupID, "error", err)
		return nil, nil
	}
	return g, members
}

type vaultRaftCallbacks struct {
	hasLeader           func() bool
	isLeader            func() bool
	isFSMReady          func() bool
	applyRequestDelete  func(id chunk.ChunkID, reason string, expectedFrom []string) error
	applyAckDelete      func(id chunk.ChunkID, nodeID string) error
	applyFinalizeDelete func(id chunk.ChunkID) error
	applyPruneNode      func(nodeID string) error
	applyRetPending     func(id chunk.ChunkID) error
	// applyAckChunkHolders / applyRevokeChunkHolders commit this node's
	// chunk holder receipts — bytes-earned residency claims (batched, one
	// Raft apply per sweep pass). See vaultctlfsm.FSM.ChunkResidency.
	applyAckChunkHolders    func(ids []chunk.ChunkID, nodeID string) error
	applyRevokeChunkHolders func(ids []chunk.ChunkID, nodeID string) error
	isTombstoned            func(id chunk.ChunkID) bool
	listChunks              func() []chunk.ChunkID
	listRetPending          func() []chunk.ChunkID
	listPendingDeletes      func() []chunk.ChunkID
	manifestEntries         func() []vaultctlfsm.ManifestEntry
	manifestEntry           func(id chunk.ChunkID) (vaultctlfsm.ManifestEntry, bool)
}

// buildVaultRaftCallbacks constructs the callback struct for replicated instance
// chunk metadata (vault control-plane Raft in cluster mode). raft and fsm
// resolve the vault-ctl group's Raft and the vault's sub-FSM at call time;
// either may return nil, and leadership and readiness then report false and
// reads report nothing.
//
// Readiness uses the Raft applied index, not the FSM's ready flag. hraft
// filters LogNoop and LogConfiguration entries before calling FSM.Apply —
// only LogCommand entries hit the FSM — so a fresh cluster (bootstrap config
// + post-election no-op) advances r.AppliedIndex but never touches
// FSM.Apply. The vault FSM's own Ready flag only flips on user-triggered
// commands, which never fire before the first ingestion. Raft's applied
// index is the authoritative "this group is live on this node" signal:
// it advances on bootstrap, elections, snapshot restore, and normal
// replication alike.
func buildVaultRaftCallbacks(raft func() *hraft.Raft, fsm func() *vaultctlfsm.FSM, applier vaultctlfsm.Applier) vaultRaftCallbacks {
	return vaultRaftCallbacks{
		hasLeader: func() bool {
			r := raft()
			return r != nil && r.Leader() != ""
		},
		isLeader: func() bool {
			r := raft()
			return r != nil && r.State() == hraft.Leader
		},
		isFSMReady: func() bool {
			r := raft()
			return r != nil && r.AppliedIndex() > 0
		},
		applyRequestDelete: func(id chunk.ChunkID, reason string, expectedFrom []string) error {
			return applier.Apply(vaultctlfsm.MarshalRequestDelete(id, time.Now(), reason, expectedFrom))
		},
		applyAckDelete: func(id chunk.ChunkID, nodeID string) error {
			return applier.Apply(vaultctlfsm.MarshalAckDelete(id, nodeID))
		},
		applyFinalizeDelete: func(id chunk.ChunkID) error {
			return applier.Apply(vaultctlfsm.MarshalFinalizeDelete(id))
		},
		applyPruneNode: func(nodeID string) error {
			return applier.Apply(vaultctlfsm.MarshalPruneNode(nodeID))
		},
		applyRetPending: func(id chunk.ChunkID) error {
			return applier.Apply(vaultctlfsm.MarshalRetentionPending(id))
		},
		applyAckChunkHolders: func(ids []chunk.ChunkID, nodeID string) error {
			data, err := vaultctlfsm.MarshalAckChunkHolders(ids, nodeID)
			if err != nil {
				return err
			}
			return applier.Apply(data)
		},
		applyRevokeChunkHolders: func(ids []chunk.ChunkID, nodeID string) error {
			data, err := vaultctlfsm.MarshalRevokeChunkHolders(ids, nodeID)
			if err != nil {
				return err
			}
			return applier.Apply(data)
		},
		isTombstoned: func(id chunk.ChunkID) bool {
			f := fsm()
			return f != nil && f.IsTombstoned(id)
		},
		listChunks: func() []chunk.ChunkID {
			f := fsm()
			if f == nil {
				return nil
			}
			entries := f.List()
			ids := make([]chunk.ChunkID, len(entries))
			for i := range entries {
				ids[i] = entries[i].ID
			}
			return ids
		},
		listRetPending: func() []chunk.ChunkID {
			f := fsm()
			if f == nil {
				return nil
			}
			var ids []chunk.ChunkID
			for _, e := range f.List() {
				if e.RetentionPending {
					ids = append(ids, e.ID)
				}
			}
			return ids
		},
		listPendingDeletes: func() []chunk.ChunkID {
			f := fsm()
			if f == nil {
				return nil
			}
			return f.PendingDeleteIDs()
		},
		manifestEntries: func() []vaultctlfsm.ManifestEntry {
			f := fsm()
			if f == nil {
				return nil
			}
			return f.List()
		},
		manifestEntry: func(id chunk.ChunkID) (vaultctlfsm.ManifestEntry, bool) {
			f := fsm()
			if f == nil {
				return vaultctlfsm.ManifestEntry{}, false
			}
			e := f.Get(id)
			if e == nil {
				return vaultctlfsm.ManifestEntry{}, false
			}
			return *e, true
		},
	}
}

// setIntegrityVerifier wires the manifest-backed digest verifier into a chunk
// manager that supports it. Cold-cache cloud downloads consult the FSM-recorded
// hash and reject blobs whose actual digest doesn't match.
func setIntegrityVerifier(cm chunk.ChunkManager, v chunk.IntegrityVerifier) {
	if v == nil {
		return
	}
	setter, ok := cm.(chunk.IntegrityVerifierSetter)
	if !ok {
		return
	}
	setter.SetIntegrityVerifier(v)
}

// setVaultRaftAnnouncer wires the Raft announcer to a chunk manager after both
// the Raft group and chunk manager have been created. The applier handles
// routing to the vault ctl Raft leader when peers are configured. The phase parameter lets
// the announcer short-circuit during shutdown so trailing applies don't
// fire "raft is already shutdown" warnings.
func setVaultRaftAnnouncer(cm chunk.ChunkManager, applier vaultctlfsm.Applier, phase *lifecycle.Phase, logger *slog.Logger, onResult func(op string, id chunk.ChunkID, err error)) {
	if applier == nil {
		return
	}
	setter, ok := cm.(chunk.AnnouncerSetter)
	if !ok {
		return
	}
	ann := vaultctlfsm.NewAnnouncer(applier, phase, logger)
	if onResult != nil {
		ann.SetResultHook(onResult)
	}
	setter.SetAnnouncer(ann)
}

// clearVaultFSMChunkCallbacks clears OnUpload for a vault's FSM slice
// in the vault control-plane group. Used before closing that vault's chunk
// manager when the Raft group may still deliver log entries
// (e.g. RemoveVaultInstance during placement loss).
func (o *Orchestrator) clearVaultFSMChunkCallbacks(vaultID glid.GLID) {
	if o.groupMgr == nil {
		return
	}
	g := o.groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(vaultID))
	if g == nil {
		return
	}
	var fsm *vaultctlfsm.FSM
	switch raw := g.FSM.(type) {
	case *vaultctlfsm.FSM:
		fsm = raw
	case *vaultraft.FSM:
		fsm = raw.EnsureVaultFSM(vaultID)
	default:
		return
	}
	fsm.SetOnUpload(nil)
	if o.logger != nil {
		o.logger.Debug("cleared vault-ctl FSM chunk callbacks before manager close",
			"vault", vaultID)
	}
}

// wireVaultFSMOnCreate sets up the vault-ctl FSM's OnCreate callback so the
// WatchChunks event bus gets CREATED events as soon as a new active chunk is
// announced via CmdCreateChunk — the inspector shows the chunk immediately
// rather than only after seal. Fired on every node where the apply ran;
// followers learn about the new chunk via Raft replication.
//
// Chunk-file deletion is NOT wired here: it flows exclusively through the
// vault-ctl FSM's receipt protocol (CmdRequestDelete + acks + finalize),
// executed by VaultLifecycleReconciler.
//
// Safe to call with nil group or nil cm: the callback is simply not wired.
func wireVaultFSMOnCreate(g *raftgroup.Group, vaultID glid.GLID, cm chunk.ChunkManager, o *Orchestrator) {
	if g == nil || cm == nil {
		return
	}
	var fsm *vaultctlfsm.FSM
	switch raw := g.FSM.(type) {
	case *vaultctlfsm.FSM:
		fsm = raw
	case *vaultraft.FSM:
		fsm = raw.EnsureVaultFSM(vaultID)
	default:
		return
	}
	fsm.SetOnCreate(func(e vaultctlfsm.ManifestEntry) {
		if o == nil {
			return
		}
		o.EmitChunkCreated(vaultID, manifestEntryToChunkMeta(e, false)) // logs + event bus
	})
}

// wireVaultFSMPipelineChunkEvents connects vault-ctl pipeline manifest
// lifecycle callbacks to the WatchChunks event bus. Legacy CmdCreateChunk
// already emits CREATED via SetOnCreate; open/sealing pipeline chunks use
// OpenChunkManifest / SealOpenChunkManifest instead and need their own
// path so the inspector chunk list shows active and sealing chunks without
// polling ListChunks. Uses AddOn* so the chunking manager's SetOn* slot-0
// callbacks are not disturbed. Idempotent per sub-FSM: a snapshot restore's
// replacement sub-FSM gets its own subscribers.
func wireVaultFSMPipelineChunkEvents(o *Orchestrator, vaultID glid.GLID, fsm *vaultctlfsm.FSM) {
	if o == nil || fsm == nil {
		return
	}
	if prev, loaded := o.vaultCtlPipelineChunkEvents.Swap(vaultID, fsm); loaded && prev == fsm {
		return
	}
	fsm.AddOnOpenChunkManifest(func(m *vaultctlfsm.OpenChunkManifest) {
		if m == nil || len(m.Refs) == 0 {
			return
		}
		o.EmitChunkCreated(vaultID, openChunkManifestToChunkMeta(m, chunk.ChunkStateActive))
	})
	fsm.AddOnOpenChunkRefAdded(func(m *vaultctlfsm.OpenChunkManifest) {
		if m == nil {
			return
		}
		meta := openChunkManifestToChunkMeta(m, chunk.ChunkStateActive)
		if len(m.Refs) == 1 {
			o.EmitChunkCreated(vaultID, meta)
			return
		}
		o.EmitChunkProgress(vaultID, meta)
	})
	fsm.AddOnSealedManifest(func(m *vaultctlfsm.OpenChunkManifest) {
		if m == nil {
			return
		}
		o.EmitChunkSealing(vaultID, openChunkManifestToChunkMeta(m, chunk.ChunkStateSealing))
	})
	fsm.AddOnSeal(func(e vaultctlfsm.ManifestEntry) {
		if e.State != chunk.ChunkStateSealed {
			return
		}
		o.EmitChunkSealed(vaultID, manifestEntryToChunkMeta(e, true))
	})
	// Residency is receipt-only, so holder-set changes
	// are chunk-state changes the inspector must see live: emit PROGRESS
	// so the WatchChunks relay re-stamps the event with the FSM's current
	// holder set. Without this edge, a sealed chunk's honest pre-receipt
	// amber pips only turn green on a full snapshot refetch.
	fsm.SetOnHoldersChanged(func(e vaultctlfsm.ManifestEntry) {
		o.EmitChunkProgress(vaultID, manifestEntryToChunkMeta(e, e.State == chunk.ChunkStateSealed))
	})
}

// wireVaultFSMOnUpload connects the vault-ctl FSM's OnUpload callback to the
// orchestrator's UPLOADED chunk event. Cloud-index registration deliberately
// does NOT happen here: the chunk manager's lazy cloud-backed resolver
// (wireLazyCloudBackedResolver) fills the cloud index from the FSM manifest on
// the first lookup miss, covering live CmdUploadChunk replication and snapshot
// install through the same single path.
func wireVaultFSMOnUpload(g *raftgroup.Group, vaultID glid.GLID, o *Orchestrator) {
	if g == nil {
		return
	}
	var fsm *vaultctlfsm.FSM
	switch raw := g.FSM.(type) {
	case *vaultctlfsm.FSM:
		fsm = raw
	case *vaultraft.FSM:
		fsm = raw.EnsureVaultFSM(vaultID)
	default:
		return
	}
	fsm.SetOnUpload(func(e vaultctlfsm.ManifestEntry) {
		// Emit an UPLOADED event with the FSM's authoritative state —
		// every cluster node's FSM applies the same CmdUploadChunk
		// payload, so every node emits the same RecordCount /
		// DiskBytes / CloudBacked. Using local Manager.Meta instead
		// produced per-node variance and inspector flicker.
		if o == nil {
			return
		}
		meta := manifestEntryToChunkMeta(e, true)
		meta.CloudBacked = true
		o.EmitChunkUploaded(vaultID, meta)
	})
}

// RefreshVaultCtlMembers re-derives the desired vault-ctl Raft group member
// list from the current cluster node list and applies it to every local
// vault's leader manager. The next reconcile pass on each vault-ctl Raft
// leader then AddVoter's new members and RemoveServer's departed ones.
//
// Called by the config dispatcher on NotifyNodeConfigPut / NotifyNodeConfigDeleted
// so cluster scale-out and scale-in propagate into per-vault Raft groups.
// Without this, vault-ctl groups stay frozen at their bootstrap membership
// and a freshly-joined node loops forever in pre-vote campaigns rejected
// by the original members with "node is not in configuration", blocking
// chunk catchup and RF expansion.
//
// It is also where a deferred vault-ctl group gets created: every local vault
// whose group is missing is ensured, and each local instance is bound to its
// group (bindVaultInstanceToCtlLocked).
//
// Partial resolution short-circuits: if any cluster node's address can't
// be resolved (transient — the cluster-ctl Raft config has not caught up yet),
// the refresh is skipped rather than passing a partial set to the
// reconciler, which would otherwise RemoveServer the missing entries.
// The next NotifyNodeConfigPut retries once the address is in cluster-ctl Raft.
func (o *Orchestrator) RefreshVaultCtlMembers(clusterNodes []system.NodeConfig, factories Factories) {
	if o.vaultCtlLeaders == nil {
		return
	}
	members := o.buildVaultRaftMembers(clusterNodes, factories)
	if len(members) == 0 {
		// Single-node mode (nil NodeAddressResolver) or empty node list —
		// no vault-ctl groups to refresh.
		return
	}
	if len(members) < len(clusterNodes) {
		o.logger.Debug("refresh vault-ctl members: not all nodes resolvable, skipping",
			"have", len(members), "want", len(clusterNodes))
		return
	}

	o.mu.RLock()
	vaultIDs := make([]glid.GLID, 0, len(o.vaults))
	for id := range o.vaults {
		vaultIDs = append(vaultIDs, id)
	}
	o.mu.RUnlock()

	for _, vaultID := range vaultIDs {
		// A vault registered before this node's NodeConfig reached the
		// cluster node list, or while a peer was unresolvable, has no group
		// here yet; the complete member set now lets it be created.
		o.ensureVaultControlPlaneRaftGroup(vaultID, clusterNodes, factories)
		o.vaultCtlLeaders.SetDesiredMembers(vaultID, members)
	}

	o.mu.Lock()
	for _, vaultID := range vaultIDs {
		if v := o.vaults[vaultID]; v != nil {
			o.bindVaultInstanceToCtlLocked(vaultID, v.Instance)
		}
	}
	o.signalVaultReadyChange()
	o.mu.Unlock()
}

// buildVaultRaftMembers returns ALL cluster nodes as Raft members for a vault
// control-plane Raft group. Every node participates regardless of which vaults
// it stores — nodes without local instance data still replicate instance metadata.
func (o *Orchestrator) buildVaultRaftMembers(clusterNodes []system.NodeConfig, factories Factories) []hraft.Server {
	if factories.NodeAddressResolver == nil || len(clusterNodes) == 0 {
		return nil
	}
	var members []hraft.Server
	for _, node := range clusterNodes {
		nodeID := node.ID.String()
		if addr, ok := factories.NodeAddressResolver(nodeID); ok {
			members = append(members, hraft.Server{
				ID:      hraft.ServerID(nodeID),
				Address: hraft.ServerAddress(addr),
			})
		}
	}
	return members
}

func mapVaultTypeToFactory(t system.VaultType) string {
	switch t {
	case system.VaultTypeMemory:
		return "memory"
	case system.VaultTypeFile:
		return "file"
	case system.VaultTypeJSONL:
		return "jsonl"
	default:
		return string(t)
	}
}

// buildVaultParams builds a params map from a VaultConfig suitable for factory consumption.
func buildVaultParams(sys *system.System, vaultCfg system.VaultConfig, localNodeID string) map[string]string {
	rt := &sys.Runtime
	params := make(map[string]string)

	switch vaultCfg.Type {
	case system.VaultTypeMemory:
		// The expression passes through verbatim — the params map is already
		// strings and the factory resolves it with the shared parser, so
		// nothing converts here.
		//
		// Defense in depth: creation defaults an unset budget and rejects an
		// explicit "0", so an unset one here is a bug-produced config. Bound
		// it with the default rather than run unbounded in RAM.
		budget := vaultCfg.MemoryBudget
		if system.IsQuantityUnset(budget) {
			budget = system.DefaultVaultMemoryBudget
		}
		params["budgetBytes"] = budget

	case system.VaultTypeFile:
		// Single storage class for all file vaults — local-only and
		// cloud-backed alike. The active chunk and warm cache live at
		// the same chunkDir path.
		if vaultCfg.IsCloud() {
			addCloudParams(params, &sys.Config, vaultCfg)
			addCacheParams(params, vaultCfg)
		}
		if fs := findLocalFileStorage(rt, localNodeID, vaultCfg.StorageClass); fs != nil {
			params["dir"] = filepath.Join(fs.Path, "vaults", vaultCfg.ID.String(), vaultCfg.ID.String())
		}

	case system.VaultTypeJSONL:
		if vaultCfg.Path != "" {
			params["path"] = vaultCfg.Path
		} else {
			// Default: jsonl/<vault-id>/<instance-id>.jsonl
			params["path"] = filepath.Join("jsonl", vaultCfg.ID.String(), vaultCfg.ID.String()+".jsonl")
		}
	}

	return params
}

// addCacheParams threads the vault's warm-cache config into the factory param
// map. Without these keys the file manager reads empty and runs with an
// unbounded warm cache regardless of config. The expressions pass through
// verbatim: the params map is strings and the factory resolves them with
// the shared parser.
func addCacheParams(params map[string]string, vaultCfg system.VaultConfig) {
	if vaultCfg.CacheEviction != "" {
		params["cache_eviction"] = vaultCfg.CacheEviction
	}
	if !system.IsQuantityUnset(vaultCfg.CacheBudget) {
		params["cache_budget"] = vaultCfg.CacheBudget
	}
	if !system.IsQuantityUnset(vaultCfg.CacheTTL) {
		params["cache_ttl"] = vaultCfg.CacheTTL
	}
}

// addCloudParams writes cloud-store credentials + bucket info into params
// for a cloud-backed file instance. Always records the cloud_service_id
// (snapshotted onto every CmdUploadChunk); no-op for the rest if the
// referenced cloud service entry is missing — the chunk manager will start
// without a CloudStore wired but still knows which service it would pin to.
//
// The provider fields come from CloudService.StoreParams — the same mapping
// PutCloudService validates against at config-accept time, so an accepted
// config carries exactly the params store creation needs.
func addCloudParams(params map[string]string, cfg *system.Config, vaultCfg system.VaultConfig) {
	params["cloud_service_id"] = vaultCfg.CloudServiceID.String()
	cs := findCloudService(cfg, *vaultCfg.CloudServiceID)
	if cs == nil {
		return
	}
	params["sealed_backing"] = cs.Provider
	maps.Copy(params, cs.StoreParams())
}

// findLocalFileStorage finds a FileStorage on the given node with the given storage class.
func findLocalFileStorage(rt *system.Runtime, nodeID string, storageClass uint32) *system.FileStorage {
	if storageClass == 0 {
		return nil
	}
	for _, nsc := range rt.NodeStorageConfigs {
		if nsc.NodeID != nodeID {
			continue
		}
		for i := range nsc.FileStorages {
			if nsc.FileStorages[i].StorageClass == storageClass {
				return &nsc.FileStorages[i]
			}
		}
	}
	return nil
}

// findCloudService finds a CloudService by ID in the system.
func findCloudService(cfg *system.Config, id glid.GLID) *system.CloudService {
	for i := range cfg.CloudServices {
		if cfg.CloudServices[i].ID == id {
			return &cfg.CloudServices[i]
		}
	}
	return nil
}

// announceResultHook returns the callback the vault-ctl announcer uses to
// report each announce attempt's outcome for one vault.
//
// One standing alarm per VAULT, not per chunk. A node that cannot commit
// announces cannot commit them for any chunk, so a per-chunk alarm would turn a
// single condition into a flood — and the alarm system exists to reduce, not to
// enumerate. The alarm clears on the next announce that commits, which is the
// condition genuinely ending rather than a timer expiring.
//
// The MetadataAnnouncer contract stays best-effort by design — the calls fire
// from deferred closures with no caller to return an error to, deferred
// precisely so a Raft round trip cannot deadlock against the FSM apply path.
// This hook is what keeps "does not block the local operation" from also
// meaning "is invisible": without it, seals and uploads complete locally while
// the replicated manifest never learns about them, silently.
func (o *Orchestrator) announceResultHook(vaultID glid.GLID) func(string, chunk.ChunkID, error) {
	return func(op string, id chunk.ChunkID, err error) {
		if o == nil || o.alerts == nil {
			return
		}
		if err == nil {
			o.alerts.Clear("vault-announce-failing", vaultID.String())
			return
		}
		o.alerts.Raise("vault-announce-failing", vaultID.String(),
			fmt.Sprintf("Vault %s cannot record what this node has sealed — the cluster's view of the vault is falling behind what is actually on disk here (last failure: %s for chunk %s: %v).",
				o.VaultAlarmLabel(vaultID), op, id, err))
	}
}
