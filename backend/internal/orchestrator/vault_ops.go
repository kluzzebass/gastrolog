package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"sync"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
	"gastrolog/internal/index"
	"gastrolog/internal/index/analyzer"
	"gastrolog/internal/system"
)

// IndexInfo describes a single index for a chunk.
type IndexInfo struct {
	Name       string
	Exists     bool
	EntryCount int64
	SizeBytes  int64
}

// ChunkIndexReport aggregates chunk seal status and per-index info.
type ChunkIndexReport struct {
	Sealed  bool
	Indexes []IndexInfo
}

// activeTierManagers returns chunk and index managers for the vault's active
// (ingest) tier — Tiers[0]. Returns ErrVaultNotFound if the vault doesn't exist
// or has no tiers.
func (o *Orchestrator) activeTierManagers(vaultID glid.GLID) (chunk.ChunkManager, index.IndexManager, error) {
	o.mu.RLock()
	s := o.vaults[vaultID]
	o.mu.RUnlock()
	if s == nil {
		return nil, nil, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if err := vaultReplicationReadinessErr(vaultID, s); err != nil {
		return nil, nil, err
	}
	cm, im := s.ActiveTierChunkManager(), s.ActiveTierIndexManager()
	if cm == nil {
		return nil, nil, fmt.Errorf("%w: %s (no tiers)", ErrVaultNotFound, vaultID)
	}
	return cm, im, nil
}

// activeTierChunkManager returns the chunk manager for the vault's active tier.
func (o *Orchestrator) activeTierChunkManager(vaultID glid.GLID) (chunk.ChunkManager, error) {
	o.mu.RLock()
	s := o.vaults[vaultID]
	o.mu.RUnlock()
	if s == nil {
		return nil, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if err := vaultReplicationReadinessErr(vaultID, s); err != nil {
		return nil, err
	}
	cm := s.ActiveTierChunkManager()
	if cm == nil {
		return nil, fmt.Errorf("%w: %s (no tiers)", ErrVaultNotFound, vaultID)
	}
	return cm, nil
}

// activeTierIndexManager returns the index manager for the vault's active tier.
func (o *Orchestrator) activeTierIndexManager(vaultID glid.GLID) (index.IndexManager, error) {
	o.mu.RLock()
	s := o.vaults[vaultID]
	o.mu.RUnlock()
	if s == nil {
		return nil, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if err := vaultReplicationReadinessErr(vaultID, s); err != nil {
		return nil, err
	}
	im := s.ActiveTierIndexManager()
	if im == nil {
		return nil, fmt.Errorf("%w: %s (no tiers)", ErrVaultNotFound, vaultID)
	}
	return im, nil
}

// findManagersForChunk returns the chunk and index managers for the tier that
// owns the given chunk (metadata match or active chunk ID). IndexManager may
// be nil if the tier has no index backend.
func (o *Orchestrator) findManagersForChunk(vaultID glid.GLID, chunkID chunk.ChunkID) (chunk.ChunkManager, index.IndexManager, error) {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil {
		return nil, nil, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if err := vaultReplicationReadinessErr(vaultID, vault); err != nil {
		return nil, nil, err
	}
	for _, tier := range vault.Tiers {
		if _, err := tier.Chunks.Meta(chunkID); err == nil {
			return tier.Chunks, tier.Indexes, nil
		}
		if active := tier.Chunks.Active(); active != nil && active.ID == chunkID {
			return tier.Chunks, tier.Indexes, nil
		}
	}
	return nil, nil, fmt.Errorf("%w: chunk %s in vault %s", chunk.ErrChunkNotFound, chunkID, vaultID)
}

// findChunkManagerForChunk searches all tiers in a vault for the chunk manager
// that owns the given chunk ID.
func (o *Orchestrator) findChunkManagerForChunk(vaultID glid.GLID, chunkID chunk.ChunkID) (chunk.ChunkManager, error) {
	cm, _, err := o.findManagersForChunk(vaultID, chunkID)
	return cm, err
}

// ArchiveChunk transitions a cloud-backed sealed chunk to an offline storage class.
func (o *Orchestrator) ArchiveChunk(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID, storageClass string) error {
	cm, err := o.findChunkManagerForChunk(vaultID, chunkID)
	if err != nil {
		return err
	}
	archiver, ok := cm.(chunk.ChunkArchiver)
	if !ok {
		return errors.New("chunk manager does not support archival")
	}
	return archiver.ArchiveChunk(ctx, chunkID, storageClass)
}

// RestoreChunk initiates retrieval of an archived chunk.
func (o *Orchestrator) RestoreChunk(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID, tier string, days int) error {
	cm, err := o.findChunkManagerForChunk(vaultID, chunkID)
	if err != nil {
		return err
	}
	archiver, ok := cm.(chunk.ChunkArchiver)
	if !ok {
		return errors.New("chunk manager does not support restore")
	}
	return archiver.RestoreChunk(ctx, chunkID, tier, days)
}

// --- Chunk read ---

// TieredChunkMeta pairs a chunk with the tier it belongs to.
type TieredChunkMeta struct {
	chunk.ChunkMeta
	TierID   glid.GLID
	TierType string
}

// ListChunkMetas returns all chunk metadata for a vault (active tier only).
// Use ListAllChunkMetas for a multi-tier view.
func (o *Orchestrator) ListChunkMetas(vaultID glid.GLID) ([]chunk.ChunkMeta, error) {
	cm, err := o.activeTierChunkManager(vaultID)
	if err != nil {
		return nil, err
	}
	return cm.List()
}

// ListAllChunkMetas returns chunk metadata from ALL local tiers of a vault,
// each tagged with its tier ID and type.
// ListAllChunkMetas returns chunk metadata from all local tier instances.
// When a vault has multiple tier instances for the same tier on the same
// node (leader + same-node follower storages), the leader's view is preferred
// to avoid double-counting chunks. Follower-only tiers are still included
// (the leader node is elsewhere; this node contributes replica presence).
//
// Caller-side deduplication across nodes happens in the server's ListChunks.
func (o *Orchestrator) ListAllChunkMetas(vaultID glid.GLID) ([]TieredChunkMeta, error) {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil {
		return nil, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if err := vaultReplicationReadinessErr(vaultID, vault); err != nil {
		return nil, err
	}

	// If a tier has both a leader and follower instance on this node, prefer
	// the leader. Same-node followers exist during tier draining or when
	// replication factor requires multiple local storages.
	hasLeader := make(map[glid.GLID]bool)
	for _, tier := range vault.Tiers {
		if !tier.IsFollower {
			hasLeader[tier.TierID] = true
		}
	}

	var result []TieredChunkMeta
	for _, tier := range vault.Tiers {
		// Skip same-node follower if the leader instance is also here.
		if tier.IsFollower && hasLeader[tier.TierID] {
			continue
		}
		metas, err := tier.Chunks.List()
		if err != nil {
			return nil, fmt.Errorf("list chunks for tier %s: %w", tier.TierID, err)
		}
		for _, m := range metas {
			// Override CloudBacked / Archived / NumFrames from the tier
			// Raft FSM (the cluster-wide source of truth). Without this,
			// follower nodes always report CloudBacked=false because their
			// local chunk manager has CloudStore=nil. See gastrolog-asg4l.
			if tier.OverlayFromFSM != nil {
				m = tier.OverlayFromFSM(m)
			}
			result = append(result, TieredChunkMeta{
				ChunkMeta: m,
				TierID:    tier.TierID,
				TierType:  tier.Type,
			})
		}
	}
	return result, nil
}

// GetChunkMeta returns metadata for a specific chunk. The result is overlaid
// from the tier Raft FSM if the chunk belongs to a tier with a Raft group, so
// CloudBacked / Archived / NumFrames reflect the cluster-wide truth rather
// than this node's local chunk-manager view. See gastrolog-asg4l.
func (o *Orchestrator) GetChunkMeta(vaultID glid.GLID, chunkID chunk.ChunkID) (chunk.ChunkMeta, error) {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil {
		return chunk.ChunkMeta{}, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if err := vaultReplicationReadinessErr(vaultID, vault); err != nil {
		return chunk.ChunkMeta{}, err
	}
	for _, tier := range vault.Tiers {
		m, err := tier.Chunks.Meta(chunkID)
		if err != nil {
			continue // not in this tier, try the next
		}
		if tier.OverlayFromFSM != nil {
			m = tier.OverlayFromFSM(m)
		}
		return m, nil
	}
	return chunk.ChunkMeta{}, chunk.ErrChunkNotFound
}

// GetTieredChunkMeta returns metadata for a specific chunk with tier info.
func (o *Orchestrator) GetTieredChunkMeta(vaultID glid.GLID, chunkID chunk.ChunkID) (TieredChunkMeta, error) {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil {
		return TieredChunkMeta{}, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if err := vaultReplicationReadinessErr(vaultID, vault); err != nil {
		return TieredChunkMeta{}, err
	}
	for _, tier := range vault.Tiers {
		m, err := tier.Chunks.Meta(chunkID)
		if err != nil {
			continue
		}
		if tier.OverlayFromFSM != nil {
			m = tier.OverlayFromFSM(m)
		}
		return TieredChunkMeta{
			ChunkMeta: m,
			TierID:    tier.TierID,
			TierType:  tier.Type,
		}, nil
	}
	return TieredChunkMeta{}, chunk.ErrChunkNotFound
}

// OpenCursor opens a record cursor for the given chunk on the tier that owns it.
func (o *Orchestrator) OpenCursor(vaultID glid.GLID, chunkID chunk.ChunkID) (chunk.RecordCursor, error) {
	cm, err := o.findChunkManagerForChunk(vaultID, chunkID)
	if err != nil {
		return nil, err
	}
	return cm.OpenCursor(chunkID)
}

// VaultExists returns true if a vault with the given ID is registered.
func (o *Orchestrator) VaultExists(vaultID glid.GLID) bool {
	o.mu.RLock()
	s := o.vaults[vaultID]
	o.mu.RUnlock()
	return s != nil
}

// VaultType returns the type string for a registered vault, or "" if not found.
func (o *Orchestrator) VaultType(vaultID glid.GLID) string {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if v := o.vaults[vaultID]; v != nil {
		return v.Type()
	}
	return ""
}

// HasMissingTiers returns true if the vault's local tier list differs from the
// given tier IDs — either tiers were added or removed.
func (o *Orchestrator) HasMissingTiers(vaultID glid.GLID, tierIDs []glid.GLID) bool {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil {
		return false
	}
	// Collect unique local tier IDs (multiple instances per tier due to same-node replication).
	local := make(map[glid.GLID]bool, len(vault.Tiers))
	for _, t := range vault.Tiers {
		local[t.TierID] = true
	}
	expected := make(map[glid.GLID]bool, len(tierIDs))
	for _, id := range tierIDs {
		expected[id] = true
		if !local[id] {
			return true // tier added
		}
	}
	for id := range local {
		if !expected[id] {
			return true // tier removed
		}
	}
	return false
}

// LocalTierIDs returns the tier IDs currently instantiated for the given vault.
func (o *Orchestrator) LocalTierIDs(vaultID glid.GLID) []glid.GLID {
	o.mu.RLock()
	defer o.mu.RUnlock()
	vault := o.vaults[vaultID]
	if vault == nil {
		return nil
	}
	seen := make(map[glid.GLID]bool, len(vault.Tiers))
	var ids []glid.GLID
	for _, t := range vault.Tiers {
		if !seen[t.TierID] {
			seen[t.TierID] = true
			ids = append(ids, t.TierID)
		}
	}
	return ids
}

func (o *Orchestrator) FindLocalTierExported(vaultID, tierID glid.GLID) *TierInstance {
	return o.findLocalTier(vaultID, tierID)
}

// findLocalTier returns the TierInstance for a specific tier in a vault,
// or nil if the tier is not local.
func (o *Orchestrator) findLocalTier(vaultID, tierID glid.GLID) *TierInstance {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil {
		return nil
	}
	for _, t := range vault.Tiers {
		if t.TierID == tierID {
			return t
		}
	}
	return nil
}

// AppendToTier appends a record to a specific tier's chunk manager.
// Used by inter-tier transition to target a downstream tier directly,
// bypassing the vault's active tier. Includes seal detection.
// AppendToTier appends a record to a specific tier. primaryChunkID, when
// non-zero on secondaries, syncs the chunk ID with the primary so the
// follower has real, queryable, promotable chunks.
func (o *Orchestrator) AppendToTier(vaultID, tierID glid.GLID, primaryChunkID chunk.ChunkID, rec chunk.Record) error {
	o.mu.RLock()
	// NOTE: manually unlocked below — remote forwards happen outside the lock.

	vault := o.vaults[vaultID]
	if vault == nil {
		o.mu.RUnlock()
		return fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}

	// Block appends to tiers that are draining.
	if _, draining := o.tierDraining[tierDrainKey(vaultID, tierID)]; draining {
		o.mu.RUnlock()
		return ErrTierDraining
	}

	for _, tier := range vault.Tiers {
		if tier.TierID != tierID {
			continue
		}
		cm := tier.Chunks

		// Reject writes targeting a tombstoned chunk ID — a stale replication
		// RPC that would otherwise recreate a chunk the cluster has already
		// deleted. Caller translates this into a benign ack on the receive
		// path. See gastrolog-11rzz.
		if primaryChunkID != (chunk.ChunkID{}) && tier.IsTombstoned != nil && tier.IsTombstoned(primaryChunkID) {
			o.mu.RUnlock()
			return fmt.Errorf("%w: append to tombstoned chunk %s", chunk.ErrChunkTombstoned, primaryChunkID)
		}

		// On followers, sync chunk ID with the leader. If the active
		// chunk has a different ID (left over from a previous leader cycle),
		// seal it so the next append opens a new chunk with the synced ID.
		if tier.IsFollower && primaryChunkID != (chunk.ChunkID{}) {
			if active := cm.Active(); active != nil && active.ID != primaryChunkID {
				_ = cm.Seal()
			}
			cm.SetNextChunkID(primaryChunkID)
		}

		activeBefore := cm.Active()
		if _, _, err := cm.Append(rec); err != nil {
			return err
		}

		// Leader: collect remote forward targets (local appends happen under lock).
		var remotes []remoteForwardTarget
		if tier.ShouldForwardToFollowers() {
			remotes = o.forwardToFollowers(vault, vaultID, tier, cm, rec)
		}

		activeAfter := cm.Active()
		sealed := activeBefore != nil && (activeAfter == nil || activeAfter.ID != activeBefore.ID)
		if sealed && !tier.IsFollower {
			o.schedulePostSeal(vaultID, cm, activeBefore.ID)
		}
		o.mu.RUnlock()

		// Post-rotation: seal followers, then forward the record.
		if sealed {
			o.sealRemoteFollowers(remotes, activeBefore.ID)
		}
		o.fireAndForgetRemote(remotes, rec)
		return nil
	}
	o.mu.RUnlock()
	return fmt.Errorf("tier %s not found in vault %s", tierID, vaultID)
}

// remoteForwardTarget captures the parameters for a fire-and-forget forward
// to a cross-node follower. Collected under o.mu.RLock, executed after release.
type remoteForwardTarget struct {
	nodeID        string
	vaultID       glid.GLID
	tierID        glid.GLID
	activeChunkID chunk.ChunkID
}

// forwardToFollowers forwards a record to all follower targets for active-chunk
// durability. Same-node targets use direct append (under lock); cross-node targets
// are collected and returned for the caller to forward AFTER releasing the lock.
// Called under o.mu.RLock.
func (o *Orchestrator) forwardToFollowers(vault *Vault, vaultID glid.GLID, tier *TierInstance, cm chunk.ChunkManager, rec chunk.Record) []remoteForwardTarget {
	activeNow := cm.Active()
	var activeChunkID chunk.ChunkID
	if activeNow != nil {
		activeChunkID = activeNow.ID
	}
	var remotes []remoteForwardTarget
	for _, tgt := range tier.FollowerTargets {
		if tgt.NodeID == o.localNodeID {
			o.appendToLocalFollower(vault, tier.TierID, tgt.StorageID, activeChunkID, rec)
		} else {
			remotes = append(remotes, remoteForwardTarget{
				nodeID: tgt.NodeID, vaultID: vaultID,
				tierID: tier.TierID, activeChunkID: activeChunkID,
			})
		}
	}
	return remotes
}

// sealRemoteFollowers sends seal commands to remote followers through the
// tier replication stream, ensuring they seal at the same boundary as the
// leader. Must be called BEFORE the next record's append to maintain ordering.
// Seal RPCs to distinct followers run in parallel so the leader does not pay
// sum(latency) per seal boundary (important for remote tier transition streams
// that append one record at a time). Ordering per follower stream is still
// sequential because each follower receives at most one in-flight seal here.
//
// During shutdown (o.shuttingDown()) this is a silent no-op: the local
// chunk is already sealed on disk, peers are racing to shut down alongside
// us, and their replication-catchup on next startup will reseal to the
// same boundary. Trying to push the seal command now would just add noise
// from peers that are unreachable. See gastrolog-1e5ke.
func (o *Orchestrator) sealRemoteFollowers(targets []remoteForwardTarget, chunkID chunk.ChunkID) {
	if o.tierReplicator == nil || len(targets) == 0 || o.shuttingDown() {
		return
	}
	var wg sync.WaitGroup
	for _, tgt := range targets {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), cluster.ForwardingTimeout)
			defer cancel()
			if err := o.tierReplicator.SealTier(ctx, tgt.nodeID, tgt.vaultID, tgt.tierID, chunkID); err != nil {
				o.logger.Warn("replication: failed to seal remote follower",
					"node", tgt.nodeID, "vault", tgt.vaultID, "tier", tgt.tierID,
					"chunk", chunkID.String(), "error", err)
			}
		})
	}
	wg.Wait()
}

// fireAndForgetRemote sends records to remote followers outside any lock.
// Appends to distinct followers run in parallel (WaitGroup) so per-record
// latency is bounded by the slowest follower, not the sum — same ordering
// guarantee as sealRemoteFollowers: each follower tier stream still processes
// one RPC at a time, and AppendToTier does not start the next record until
// this function returns.
//
// During shutdown (o.shuttingDown()) this is a silent no-op: the record
// is already durable on the local leader's disk; peers that are also
// shutting down will reconcile via replication-catchup on next startup.
// Skipping the fanout avoids the log spam storm where each buffered
// record in the drain pipeline tries to reach peers that are gone. This
// is the single biggest source of shutdown noise before the fix —
// see gastrolog-1e5ke.
func (o *Orchestrator) fireAndForgetRemote(targets []remoteForwardTarget, rec chunk.Record) {
	if len(targets) == 0 || o.shuttingDown() || o.tierReplicator == nil {
		return
	}
	var wg sync.WaitGroup
	for _, tgt := range targets {
		// Circuit breaker: skip nodes in backoff.
		if rb := o.getReplicaBackoff(tgt.nodeID); rb != nil && rb.skipUntil.After(o.now()) {
			continue
		}

		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), cluster.ForwardingTimeout)
			defer cancel()
			err := o.tierReplicator.AppendRecords(ctx, tgt.nodeID, tgt.vaultID, tgt.tierID, tgt.activeChunkID, []chunk.Record{rec})
			if err != nil {
				o.bumpReplicaBackoff(tgt.nodeID, err)
			} else {
				o.clearReplicaBackoff(tgt.nodeID)
			}
		})
	}
	wg.Wait()
}

// replicaBackoff tracks consecutive failures and exponential backoff for
// a follower node's replication stream.
type replicaBackoff struct {
	failures  int
	skipUntil time.Time
}

func (o *Orchestrator) getReplicaBackoff(nodeID string) *replicaBackoff {
	v, ok := o.replicaCircuit.Load(nodeID)
	if !ok {
		return nil
	}
	return v.(*replicaBackoff)
}

func (o *Orchestrator) bumpReplicaBackoff(nodeID string, err error) {
	v, _ := o.replicaCircuit.LoadOrStore(nodeID, &replicaBackoff{})
	rb := v.(*replicaBackoff)
	rb.failures++
	backoff := time.Duration(1<<min(rb.failures, 5)) * time.Second // 2s, 4s, 8s, 16s, 32s cap
	rb.skipUntil = o.now().Add(backoff)

	// Log only on the first failure and when backoff increases.
	if rb.failures == 1 || rb.failures&(rb.failures-1) == 0 { // powers of 2
		o.logger.Warn("replication: follower unreachable, backing off",
			"node", nodeID, "failures", rb.failures, "backoff", backoff, "error", err)
	}
}

func (o *Orchestrator) clearReplicaBackoff(nodeID string) {
	if _, loaded := o.replicaCircuit.LoadAndDelete(nodeID); loaded {
		o.logger.Info("replication: follower recovered", "node", nodeID)
	}
}

// appendToLocalFollower appends a record to a same-node follower tier instance,
// identified by storageID. Called under o.mu.RLock — vault is already resolved.
func (o *Orchestrator) appendToLocalFollower(vault *Vault, tierID glid.GLID, storageID string, primaryChunkID chunk.ChunkID, rec chunk.Record) {
	for _, t := range vault.Tiers {
		if t.TierID == tierID && t.StorageID == storageID && t.IsFollower { //nolint:nestif // error handling adds nesting
			if primaryChunkID != (chunk.ChunkID{}) {
				if active := t.Chunks.Active(); active != nil && active.ID != primaryChunkID {
					if err := t.Chunks.Seal(); err != nil {
						o.logger.Warn("replication: local follower seal failed",
							"tier", tierID, "storage", storageID, "error", err)
					}
				}
				t.Chunks.SetNextChunkID(primaryChunkID)
			}
			if _, _, err := t.Chunks.Append(rec); err != nil {
				o.logger.Warn("replication: local follower append failed",
					"tier", tierID, "storage", storageID, "error", err)
			}
			return
		}
	}
}

// deleteFromFollowers removes a chunk from all same-node follower instances
// of a tier. Called by retention after deleting from the leader.
// DeleteChunkFromTier deletes a specific chunk from a tier. If the chunk is
// currently the tier's active chunk, it is sealed first so the delete can
// proceed. This handles the follower case where the leader has moved on to a
// new active chunk but the follower still has the old ID as active (records
// sync via TierReplicator.AppendRecords preserves the leader's chunk ID).
func (o *Orchestrator) DeleteChunkFromTier(vaultID, tierID glid.GLID, chunkID chunk.ChunkID) error {
	tier, err := o.findTierForDelete(vaultID, tierID)
	if err != nil {
		return err
	}
	return o.deleteChunkFromTierInstance(tier, vaultID, tierID, chunkID)
}

// findTierForDelete returns the matching tier instance or an error, releasing
// the orchestrator read lock before returning.
func (o *Orchestrator) findTierForDelete(vaultID, tierID glid.GLID) (*TierInstance, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	vault := o.vaults[vaultID]
	if vault == nil {
		return nil, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	for _, t := range vault.Tiers {
		if t.TierID == tierID {
			return t, nil
		}
	}
	return nil, fmt.Errorf("tier %s not found in vault %s", tierID, vaultID)
}

// deleteChunkFromTierInstance seals the active chunk if it matches, then
// deletes the chunk's indexes and data files.
func (o *Orchestrator) deleteChunkFromTierInstance(t *TierInstance, vaultID, tierID glid.GLID, chunkID chunk.ChunkID) error {
	if active := t.Chunks.Active(); active != nil && active.ID == chunkID {
		if err := t.Chunks.Seal(); err != nil {
			return fmt.Errorf("seal active before delete: %w", err)
		}
	}
	if t.Indexes != nil {
		if err := t.Indexes.DeleteIndexes(chunkID); err != nil {
			o.logger.Warn("delete chunk: delete indexes failed",
				"vault", vaultID, "tier", tierID, "chunk", chunkID, "error", err)
		}
	}
	return t.Chunks.Delete(chunkID)
}

// replaceForwardedChunk seals (if active) and deletes a pre-existing chunk
// on a follower to make room for the canonical sealed version from the leader.
// The pre-existing chunk may come from:
//   - TierReplicator.AppendRecords syncing records as the leader's active
//     chunk fills up (and the follower may have missed some due to a brief
//     network disruption, leaving its copy slightly behind the leader's)
//   - a catchup path that fills follower state from scratch
//
// Retries if a concurrent Append reopens the active chunk between seal and
// delete.
//
// Uses DeleteNoAnnounce: this is a LOCAL cleanup operation on a single
// follower. It must NOT propagate the delete via tier Raft — the canonical
// sealed chunk is about to replace it locally via ImportRecords, which will
// fire its own AnnounceCreate/AnnounceSeal for the replacement.
func replaceForwardedChunk(cm chunk.ChunkManager, chunkID chunk.ChunkID, isActive bool) error {
	if isActive {
		if err := cm.Seal(); err != nil {
			return fmt.Errorf("seal forwarded chunk %s: %w", chunkID, err)
		}
	}
	if err := chunk.DeleteNoAnnounce(cm, chunkID); errors.Is(err, chunk.ErrActiveChunk) {
		if sealErr := cm.Seal(); sealErr != nil {
			return fmt.Errorf("re-seal forwarded chunk %s: %w", chunkID, sealErr)
		}
		if err = chunk.DeleteNoAnnounce(cm, chunkID); err != nil {
			return fmt.Errorf("delete forwarded chunk %s (after re-seal): %w", chunkID, err)
		}
	} else if err != nil {
		return fmt.Errorf("delete forwarded chunk %s: %w", chunkID, err)
	}
	return nil
}

// deleteFromFollowers removes a chunk from same-node follower tier instances.
// Called from retention's expireChunk after applyRaftDelete has already fired
// the global CmdDeleteChunk. Uses DeleteNoAnnounce to avoid a redundant
// second Raft-wide announce (the first one already propagated via OnDelete).
func (o *Orchestrator) deleteFromFollowers(vaultID glid.GLID, tierID glid.GLID, chunkID chunk.ChunkID) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	vault := o.vaults[vaultID]
	if vault == nil {
		return
	}
	for _, t := range vault.Tiers {
		if t.TierID == tierID && t.IsFollower {
			if err := chunk.DeleteNoAnnounce(t.Chunks, chunkID); err != nil {
				o.logger.Warn("delete from followers: failed",
					"vault", vaultID, "tier", tierID, "chunk", chunkID, "error", err)
			}
		}
	}
}

// --- Chunk write ---

// Append appends a record to the vault's active chunk.
// If the append triggers a rotation (e.g. age or size policy), post-seal
// work (compression + index builds) is scheduled automatically.
//
// This is the sole write path for all record sources: local ingesters,
// cluster-forwarded records, and the ImportRecords API.
func (o *Orchestrator) Append(vaultID glid.GLID, rec chunk.Record) (chunk.ChunkID, uint64, error) {
	o.mu.RLock()
	cid, pos, _, remotes, err := o.appendRecord(vaultID, rec)
	o.mu.RUnlock()
	o.fireAndForgetRemote(remotes, rec)
	return cid, pos, err
}

// replicationTask describes a pending sync forward for ack-gated ingestion.
// Returned by appendRecord when rec.WaitForReplica is true, consumed by
// ackAfterReplication outside the orchestrator lock.
type replicationTask struct {
	vaultID glid.GLID
	tierID  glid.GLID
	chunkID chunk.ChunkID
	targets []system.ReplicationTarget
}

// appendRecord is the unified append-with-seal-detection path.
// Caller MUST hold o.mu.RLock.
//
// Returns a replicationTask when the record has WaitForReplica set and
// the tier has secondaries. Also returns remoteForwardTargets for
// fire-and-forget forwarding — the caller fires these AFTER releasing the lock.
func (o *Orchestrator) appendRecord(vaultID glid.GLID, rec chunk.Record) (chunk.ChunkID, uint64, *replicationTask, []remoteForwardTarget, error) {
	vault := o.vaults[vaultID]
	if vault == nil {
		return chunk.ChunkID{}, 0, nil, nil, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if !vault.Enabled {
		return chunk.ChunkID{}, 0, nil, nil, fmt.Errorf("%w: %s", ErrVaultDisabled, vaultID)
	}
	if err := vaultReplicationReadinessErr(vaultID, vault); err != nil {
		return chunk.ChunkID{}, 0, nil, nil, err
	}

	cm := vault.ActiveTierChunkManager()
	if cm == nil {
		return chunk.ChunkID{}, 0, nil, nil, fmt.Errorf("%w: %s (no tiers)", ErrVaultNotFound, vaultID)
	}
	activeBefore := cm.Active()
	cid, pos, err := cm.Append(rec)
	if err != nil {
		return cid, pos, nil, nil, err
	}

	// Forward record to followers for active-chunk durability.
	// Followers append to their ChunkManager — real, queryable chunks
	// that allow immediate promotion if the leader dies.
	//
	// When WaitForReplica is set, skip fire-and-forget — the caller does
	// sync forwarding outside the lock via ackAfterReplication.
	activeTier := vault.ActiveTier()
	var task *replicationTask
	var remotes []remoteForwardTarget
	if activeTier != nil && activeTier.ShouldForwardToFollowers() {
		if rec.WaitForReplica {
			activeNow := cm.Active()
			var activeChunkID chunk.ChunkID
			if activeNow != nil {
				activeChunkID = activeNow.ID
			}
			task = &replicationTask{
				vaultID: vaultID,
				tierID:  activeTier.TierID,
				chunkID: activeChunkID,
				targets: activeTier.FollowerTargets,
			}
		} else {
			remotes = o.forwardToFollowers(vault, vaultID, activeTier, cm, rec)
		}
	}

	// Detect seal: if Active() changed, the previous chunk was sealed.
	// This fires for rotations the chunk manager decided internally during
	// Append (record-count, size, or age policies that trip mid-write) —
	// the hot path for high-rate rotation misconfigurations. Count the
	// rotation for the per-tier rate alerter so the operator sees an alert
	// before the pipeline collapses. See gastrolog-47qyw.
	activeAfter := cm.Active()
	if activeBefore != nil && (activeAfter == nil || activeAfter.ID != activeBefore.ID) {
		if activeTier != nil {
			o.rotationRates.Record(activeTier.TierID, o.now())
		}
		o.schedulePostSeal(vaultID, cm, activeBefore.ID)
	}

	return cid, pos, task, remotes, nil
}

// ImportChunkRecords creates a new sealed chunk from the given records in the
// target vault. Used by the ForwardImportRecords handler to receive cross-node
// chunk migrations. Works with any ChunkManager type (file or memory).
// Compression and index builds are scheduled asynchronously via the scheduler.
func (o *Orchestrator) ImportChunkRecords(ctx context.Context, vaultID glid.GLID, next chunk.RecordIterator) error {
	cm, _, err := o.activeTierManagers(vaultID)
	if err != nil {
		return err
	}

	meta, err := cm.ImportRecords(chunk.ChunkID{}, next)
	if err != nil {
		return fmt.Errorf("import records: %w", err)
	}

	if meta.ID != (chunk.ChunkID{}) {
		o.mu.RLock()
		o.schedulePostSeal(vaultID, cm, meta.ID)
		o.mu.RUnlock()
	}

	return nil
}

// ImportToTier imports records as a sealed chunk into a specific tier,
// preserving the given chunk ID. Used by sealed-chunk replication —
// the follower receives a sealed chunk from the leader with the same ID.
// Schedules postSealWork for local indexing (secondaries need indexes for queries)
// but won't trigger further replication (gated by !IsFollower in tierReplicationInfo).
func (o *Orchestrator) ImportToTier(ctx context.Context, vaultID, tierID glid.GLID, chunkID chunk.ChunkID, next chunk.RecordIterator) error {
	return o.ImportToTierStorage(ctx, vaultID, tierID, "", chunkID, next)
}

// ImportToTierStorage imports a sealed chunk to a specific storage-targeted tier
// instance. When storageID is empty, falls back to the first matching tier (backward compat).
// Used by same-node replication to route to specific file storage instances.
func (o *Orchestrator) ImportToTierStorage(ctx context.Context, vaultID, tierID glid.GLID, storageID string, chunkID chunk.ChunkID, next chunk.RecordIterator) error {
	// Look up the tier under lock, then release BEFORE the import.
	// ImportRecords reads from a network stream and can block — holding
	// RLock during a network read starves writers (FSM dispatcher) and
	// deadlocks the entire orchestrator.
	type tierRef struct {
		cm           chunk.ChunkManager
		isFollower   bool
		isTombstoned func(chunk.ChunkID) bool
	}
	ref := func() *tierRef {
		o.mu.RLock()
		defer o.mu.RUnlock()
		vault := o.vaults[vaultID]
		if vault == nil {
			return nil
		}
		for _, t := range vault.Tiers {
			if t.TierID == tierID && (storageID == "" || t.StorageID == storageID) {
				return &tierRef{cm: t.Chunks, isFollower: t.IsFollower, isTombstoned: t.IsTombstoned}
			}
		}
		return nil
	}()
	if ref == nil {
		return fmt.Errorf("%w: tier %s in vault %s", ErrVaultNotFound, tierID, vaultID)
	}
	// Reject stale ImportSealed RPCs for chunks the cluster already deleted.
	// The race is: leader schedules replication, retention fires, delete is
	// committed via Raft (populates tombstone), then the late replication
	// RPC arrives. Without this check the receiver would recreate the chunk.
	// See gastrolog-11rzz.
	if ref.isTombstoned != nil && ref.isTombstoned(chunkID) {
		return fmt.Errorf("%w: import sealed chunk %s into tier %s", chunk.ErrChunkTombstoned, chunkID, tierID)
	}
	cm := ref.cm

	// Serialize SetNextChunkID + ImportRecords per tier instance to prevent
	// concurrent replication messages from interleaving the two calls.
	// Key includes storageID so same-node replicas can import in parallel.
	importKey := tierID.String() + ":" + storageID
	muVal, _ := o.importMu.LoadOrStore(importKey, &sync.Mutex{})
	tierMu := muVal.(*sync.Mutex)
	tierMu.Lock()
	defer tierMu.Unlock()

	// Check if this chunk already exists (sealed or active).
	_, metaErr := cm.Meta(chunkID)
	activeIsChunk := false
	if active := cm.Active(); active != nil && active.ID == chunkID {
		activeIsChunk = true
	}

	chunkExists := metaErr == nil || activeIsChunk

	// Leader: idempotent skip — canonical version is already here.
	if chunkExists && !ref.isFollower {
		o.logger.Debug("replication: chunk already exists, skipping import",
			"vault", vaultID, "tier", tierID, "chunk", chunkID.String())
		drainIterator(next)
		return nil
	}

	// Follower: replace the forwarded version (may be incomplete due to
	// fire-and-forget drops) with the canonical sealed chunk.
	if chunkExists {
		if err := replaceForwardedChunk(cm, chunkID, activeIsChunk); err != nil {
			drainIterator(next)
			return err
		}
		o.logger.Debug("replication: replacing forwarded chunk with canonical version",
			"vault", vaultID, "tier", tierID, "chunk", chunkID.String())
	}

	meta, err := cm.ImportRecords(chunkID, next)
	if err != nil {
		return fmt.Errorf("import to tier %s: %w", tierID, err)
	}
	o.logger.Debug("replication: sealed chunk imported",
		"vault", vaultID, "tier", tierID,
		"chunk", meta.ID.String(), "records", meta.RecordCount)

	return o.finalizeImportedChunk(vaultID, tierID, cm, meta, ref.isTombstoned)
}

// finalizeImportedChunk handles the post-import steps: tier Raft
// announcement, tombstone re-check, and (if not tombstoned) post-seal
// work scheduling. See gastrolog-11rzz for the ordering rationale.
//
// Ordering matters: announce first, then re-check tombstone. This covers
// the race where DeleteChunk applies between ImportRecords and our check
// — if we checked first we'd miss it; announcing first propagates the
// create through the tier FSM (which rejects it when tombstoned via the
// applyCreate guard), so by the time we re-check the tombstone state is
// authoritative and any on-disk files we wrote are orphans we must
// clean up explicitly because the FSM onDelete callback fired before
// the files existed.
func (o *Orchestrator) finalizeImportedChunk(vaultID, tierID glid.GLID, cm chunk.ChunkManager, meta chunk.ChunkMeta, isTombstoned func(chunk.ChunkID) bool) error {
	if meta.ID == (chunk.ChunkID{}) {
		return nil
	}

	if ann, ok := cm.(chunk.AnnouncerGetter); ok {
		if a := ann.GetAnnouncer(); a != nil {
			a.AnnounceCreate(meta.ID, meta.WriteStart, meta.IngestStart, meta.SourceStart)
			a.AnnounceSeal(meta.ID, meta.WriteEnd, meta.RecordCount, meta.Bytes, meta.IngestEnd, meta.SourceEnd)
		}
	}

	if isTombstoned != nil && isTombstoned(meta.ID) {
		if del, ok := cm.(chunk.SilentDeleter); ok {
			_ = del.DeleteSilent(meta.ID)
		} else {
			_ = cm.Delete(meta.ID)
		}
		o.logger.Debug("replication: post-import tombstone detected, deleted just-created chunk",
			"vault", vaultID, "tier", tierID, "chunk", meta.ID.String())
		return nil
	}

	o.postSealWork(vaultID, cm, meta.ID)
	return nil
}

// StreamAppendToTier appends records from an iterator to a tier's active chunk.
// The tier's rotation policy handles sealing. Used for remote tier transitions.
func (o *Orchestrator) StreamAppendToTier(ctx context.Context, vaultID, tierID glid.GLID, next chunk.RecordIterator) error {
	for {
		rec, iterErr := next()
		if errors.Is(iterErr, chunk.ErrNoMoreRecords) {
			return nil
		}
		if iterErr != nil {
			return iterErr
		}
		if err := o.AppendToTier(vaultID, tierID, chunk.ChunkID{}, rec); err != nil {
			return err
		}
	}
}

// drainIterator reads and discards all remaining records from an iterator.
// Used when skipping an import but the caller (network stream) still needs
// its data consumed.
func drainIterator(next chunk.RecordIterator) {
	for {
		if _, err := next(); err != nil {
			return
		}
	}
}

// SealActive seals the active chunk if it has records. No-op if empty or no active chunk.
// After sealing, schedules compression and index builds (same as ingest-triggered seal).
// SealActive seals the active chunk on matching tiers in the vault. If tierID
// is glid.Nil, all tiers are sealed. Returns the number of tiers sealed.
func (o *Orchestrator) SealActive(vaultID glid.GLID, tierID glid.GLID) (int, error) {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil {
		return 0, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}

	var sealed int
	for _, tier := range vault.Tiers {
		if tierID != glid.Nil && tier.TierID != tierID {
			continue
		}
		active := tier.Chunks.Active()
		if active == nil || active.RecordCount == 0 {
			continue
		}
		chunkID := active.ID
		if err := tier.Chunks.Seal(); err != nil {
			return sealed, fmt.Errorf("seal tier %s: %w", tier.TierID, err)
		}
		sealed++
		o.mu.RLock()
		o.schedulePostSeal(vaultID, tier.Chunks, chunkID)
		o.mu.RUnlock()
	}
	return sealed, nil
}

// --- Index ops ---

// IndexSizes returns the size in bytes for each index of a chunk.
func (o *Orchestrator) IndexSizes(vaultID glid.GLID, chunkID chunk.ChunkID) (map[string]int64, error) {
	_, im, err := o.findManagersForChunk(vaultID, chunkID)
	if err != nil {
		return nil, err
	}
	if im == nil {
		return map[string]int64{}, nil
	}
	return im.IndexSizes(chunkID), nil
}

// IndexesComplete reports whether all indexes exist for the given chunk.
func (o *Orchestrator) IndexesComplete(vaultID glid.GLID, chunkID chunk.ChunkID) (bool, error) {
	_, im, err := o.findManagersForChunk(vaultID, chunkID)
	if err != nil {
		return false, err
	}
	if im == nil {
		return false, nil
	}
	return im.IndexesComplete(chunkID)
}

// BuildIndexes builds all indexes for a sealed chunk.
func (o *Orchestrator) BuildIndexes(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID) error {
	_, im, err := o.findManagersForChunk(vaultID, chunkID)
	if err != nil {
		return err
	}
	if im == nil {
		return errors.New("no index manager for chunk tier")
	}
	return im.BuildIndexes(ctx, chunkID)
}

// DeleteIndexes removes all indexes for a chunk.
func (o *Orchestrator) DeleteIndexes(vaultID glid.GLID, chunkID chunk.ChunkID) error {
	_, im, err := o.findManagersForChunk(vaultID, chunkID)
	if err != nil {
		return err
	}
	if im == nil {
		return nil
	}
	return im.DeleteIndexes(chunkID)
}

// --- Composite ---

// ChunkIndexInfos returns seal status and per-index info for a chunk.
func (o *Orchestrator) ChunkIndexInfos(vaultID glid.GLID, chunkID chunk.ChunkID) (*ChunkIndexReport, error) {
	cm, im, err := o.findManagersForChunk(vaultID, chunkID)
	if err != nil {
		return nil, err
	}

	meta, err := cm.Meta(chunkID)
	if err != nil {
		return nil, err
	}
	if im == nil {
		return nil, errors.New("no index manager for chunk tier")
	}

	sizes := im.IndexSizes(chunkID)

	report := &ChunkIndexReport{
		Sealed:  meta.Sealed,
		Indexes: make([]IndexInfo, 0, 7),
	}

	// Token index
	if idx, err := im.OpenTokenIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "token", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["token"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "token"})
	}

	// Attr key index
	if idx, err := im.OpenAttrKeyIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "attr_key", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["attr_key"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "attr_key"})
	}

	// Attr value index
	if idx, err := im.OpenAttrValueIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "attr_val", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["attr_val"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "attr_val"})
	}

	// Attr kv index
	if idx, err := im.OpenAttrKVIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "attr_kv", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["attr_kv"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "attr_kv"})
	}

	// KV key index
	if idx, _, err := im.OpenKVKeyIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "kv_key", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["kv_key"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "kv_key"})
	}

	// KV value index
	if idx, _, err := im.OpenKVValueIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "kv_val", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["kv_val"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "kv_val"})
	}

	// KV combined index
	if idx, _, err := im.OpenKVIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "kv_kv", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["kv_kv"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "kv_kv"})
	}

	return report, nil
}

// NewAnalyzer returns an index analyzer backed by the vault's active (ingest)
// tier. For a specific chunk (possibly on a non-active tier), use
// NewAnalyzerForChunk.
func (o *Orchestrator) NewAnalyzer(vaultID glid.GLID) (*analyzer.Analyzer, error) {
	cm, im, err := o.activeTierManagers(vaultID)
	if err != nil {
		return nil, err
	}
	return analyzer.New(cm, im), nil
}

// NewAnalyzerForChunk returns an analyzer backed by the tier that owns chunkID.
func (o *Orchestrator) NewAnalyzerForChunk(vaultID glid.GLID, chunkID chunk.ChunkID) (*analyzer.Analyzer, error) {
	cm, im, err := o.findManagersForChunk(vaultID, chunkID)
	if err != nil {
		return nil, err
	}
	if im == nil {
		return nil, errors.New("no index manager for chunk tier")
	}
	return analyzer.New(cm, im), nil
}

// SupportsChunkMove returns true if both vaults support filesystem-level chunk moves.
func (o *Orchestrator) SupportsChunkMove(srcID, dstID glid.GLID) bool {
	o.mu.RLock()
	srcVault := o.vaults[srcID]
	dstVault := o.vaults[dstID]
	o.mu.RUnlock()
	if srcVault == nil || dstVault == nil {
		return false
	}
	_, srcOK := srcVault.ActiveTierChunkManager().(chunk.ChunkMover)
	_, dstOK := dstVault.ActiveTierChunkManager().(chunk.ChunkMover)
	return srcOK && dstOK
}
