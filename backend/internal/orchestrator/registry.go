package orchestrator

import (
	"bytes"
	"fmt"
	"gastrolog/internal/glid"
	"slices"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/manifest"
	"gastrolog/internal/query"
)

// RegisterVault adds a vault to the registry.
func (o *Orchestrator) RegisterVault(vault *Vault) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.vaults[vault.ID] = vault
}

// RegisterDigester appends a digester to the processing pipeline.
// Digesters run in registration order on each message before storage.
// Must be called before Start().
func (o *Orchestrator) RegisterDigester(d Digester) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.digesters = append(o.digesters, d)
}

// RegisterIngester adds an ingester to the registry.
// Must be called before Start().
func (o *Orchestrator) RegisterIngester(id glid.GLID, name, ingType string, r Ingester) {
	o.registerIngester(id, name, ingType, false, r)
}

func (o *Orchestrator) registerIngester(id glid.GLID, name, ingType string, passive bool, r Ingester) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ingesters[id] = r
	o.ingesterMeta[id] = ingesterInfo{Name: name, Type: ingType, Passive: passive}
	if o.ingesterStats[id] == nil {
		o.ingesterStats[id] = &IngesterStats{}
	}
}

// SetFilterSet sets the filter set for attribute-based message filtering.
// Must be called before Start() or Ingest().
// If not set, messages are sent to all vaults (legacy fan-out behavior).
func (o *Orchestrator) SetFilterSet(fs *FilterSet) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.filterSet = fs
}

// UnregisterIngester removes a ingester from the registry.
// Must be called before Start() or after Stop().
func (o *Orchestrator) UnregisterIngester(id glid.GLID) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.ingesters, id)
	delete(o.ingesterMeta, id)
}

// ChunkManager implements manifest.VaultRegistry: returns the active (ingest)
// tier's chunk manager for the vault keyed by key. Returns nil if not found.
func (o *Orchestrator) ChunkManager(key glid.GLID) chunk.ChunkManager {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if s := o.vaults[key]; s != nil {
		return s.ActiveTierChunkManager()
	}
	return nil
}

// IndexManager implements manifest.VaultRegistry: returns the active tier's index
// manager for the vault keyed by key. Returns nil if not found.
func (o *Orchestrator) IndexManager(key glid.GLID) index.IndexManager {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if s := o.vaults[key]; s != nil {
		return s.ActiveTierIndexManager()
	}
	return nil
}

// TriggerIngester triggers a one-shot emission on a Triggerable ingester.
// Returns an error if the ingester is not found or doesn't support triggering.
func (o *Orchestrator) TriggerIngester(id glid.GLID) error {
	o.mu.RLock()
	ing, ok := o.ingesters[id]
	o.mu.RUnlock()
	if !ok {
		return fmt.Errorf("ingester not found: %s", id)
	}
	trig, ok := ing.(Triggerable)
	if !ok {
		return fmt.Errorf("ingester %s does not support triggering", id)
	}
	trig.Trigger()
	return nil
}

// IsRunning returns true if the orchestrator is running.
func (o *Orchestrator) IsRunning() bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.running
}

// ListVaults returns all registered vault IDs in deterministic order.
func (o *Orchestrator) ListVaults() []glid.GLID {
	o.mu.RLock()
	defer o.mu.RUnlock()
	keys := make([]glid.GLID, 0, len(o.vaults))
	for k := range o.vaults {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b glid.GLID) int { return bytes.Compare(a[:], b[:]) })
	return keys
}

// LocalLeaderTierIDs returns the set of tier IDs where this node is the
// leader. Used by search fan-out: follower tiers are NOT included because
// they may be incomplete (missing active chunk, replication lag). The search
// always fans out to the leader for authoritative results.
func (o *Orchestrator) LocalLeaderTierIDs() map[glid.GLID]bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	ids := make(map[glid.GLID]bool)
	for _, v := range o.vaults {
		if err := vaultReplicationReadinessErr(v.ID, v); err != nil {
			continue
		}
		for _, t := range v.Tiers {
			if !t.IsFollower {
				ids[t.TierID] = true
			}
		}
	}
	return ids
}

// LocalLeaderVaultIDs returns the set of vault IDs where this node is
// the leader for at least one of the vault's tiers. Vault-keyed
// equivalent of LocalLeaderTierIDs — search fan-out and remote-vault
// resolution can use this directly without translating from tier IDs.
//
// During the vault refactor (gastrolog-257l7) this iterates the same
// data as LocalLeaderTierIDs; once the 1:1 vault/tier model is enforced
// and tiers go away, the inner loop collapses to "is leader" without
// the per-tier walk.
func (o *Orchestrator) LocalLeaderVaultIDs() map[glid.GLID]bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	ids := make(map[glid.GLID]bool)
	for _, v := range o.vaults {
		if err := vaultReplicationReadinessErr(v.ID, v); err != nil {
			continue
		}
		for _, t := range v.Tiers {
			if !t.IsFollower {
				ids[v.ID] = true
				break
			}
		}
	}
	return ids
}

// HasLocalQueryEngine returns true if the vault has at least one tier with
// a query engine on this node (i.e., actual searchable data, not just a
// routing entry). Used by search fan-out to decide local vs. remote.
func (o *Orchestrator) HasLocalQueryEngine(vaultID glid.GLID) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	v := o.vaults[vaultID]
	if v == nil {
		return false
	}
	if err := vaultReplicationReadinessErr(vaultID, v); err != nil {
		return false
	}
	for _, t := range v.Tiers {
		if t.Query != nil {
			return true
		}
	}
	return false
}

// ListIngesters returns all registered ingester IDs in deterministic order.
func (o *Orchestrator) ListIngesters() []glid.GLID {
	o.mu.RLock()
	defer o.mu.RUnlock()
	keys := make([]glid.GLID, 0, len(o.ingesters))
	for k := range o.ingesters {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b glid.GLID) int { return bytes.Compare(a[:], b[:]) })
	return keys
}

// QueryEngine returns the query engine registered under the given key.
// Returns nil if not found.
func (o *Orchestrator) QueryEngine(key glid.GLID) *query.Engine {
	o.mu.RLock()
	defer o.mu.RUnlock()
	s := o.vaults[key]
	if s == nil {
		return nil
	}
	if err := vaultReplicationReadinessErr(key, s); err != nil {
		return nil
	}
	return s.QueryEngine()
}

// MultiVaultQueryEngine returns a query engine that searches across all vaults.
// Vault predicates in queries (e.g., "vault_id=<uuid>") filter which vaults are searched.
func (o *Orchestrator) MultiVaultQueryEngine() *query.Engine {
	return query.NewWithRegistry(&searchReadyRegistry{o: o}, o.logger)
}

// searchReadyRegistry implements manifest.VaultRegistry for multi-vault search,
// exposing only replication-ready vaults so partially applied tier metadata
// cannot be queried (gastrolog-4ip1o).
type searchReadyRegistry struct {
	o *Orchestrator
}

func (r *searchReadyRegistry) ListVaults() []glid.GLID {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	keys := make([]glid.GLID, 0, len(r.o.vaults))
	for k, v := range r.o.vaults {
		if err := vaultReplicationReadinessErr(k, v); err != nil {
			continue
		}
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b glid.GLID) int { return bytes.Compare(a[:], b[:]) })
	return keys
}

func (r *searchReadyRegistry) ChunkManager(key glid.GLID) chunk.ChunkManager {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	v := r.o.vaults[key]
	if v == nil || vaultReplicationReadinessErr(key, v) != nil {
		return nil
	}
	return v.ActiveTierChunkManager()
}

func (r *searchReadyRegistry) IndexManager(key glid.GLID) index.IndexManager {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	v := r.o.vaults[key]
	if v == nil || vaultReplicationReadinessErr(key, v) != nil {
		return nil
	}
	return v.ActiveTierIndexManager()
}

func (r *searchReadyRegistry) TransitionStreamedChunks(key glid.GLID) map[chunk.ChunkID]bool {
	return r.o.TransitionStreamedChunks(key)
}

func (r *searchReadyRegistry) Reader() manifest.Reader { return r.o.ManifestReader() }

func (r *searchReadyRegistry) IndexReader() manifest.IndexReader { return r.o.IndexReader() }

// LeaderVaultQueryEngine returns a query engine that only searches leader
// tiers (not follower replicas). Used by ForwardSearch handlers to avoid
// double-counting when the requesting node already searches its own followers.
func (o *Orchestrator) LeaderVaultQueryEngine() *query.Engine {
	return query.NewWithRegistry(&leaderTierRegistry{o: o}, o.logger)
}

// LocalTierQueryEngine returns a query engine that searches every locally
// available tier — leader OR follower. Suitable for approximate aggregations
// (notably histogram bucket counts) where consistency with leader is not
// required and follower-replica data is "good enough." Avoids cross-node
// gRPC fan-out when chunks are already replicated locally (RF > 1). Records
// must NOT use this engine — use LeaderVaultQueryEngine for authoritative
// reads. See gastrolog-66b7x.
func (o *Orchestrator) LocalTierQueryEngine() *query.Engine {
	return query.NewWithRegistry(&localTierRegistry{o: o}, o.logger)
}

// localTierRegistry exposes every tier this node holds (as leader or
// follower) as a searchable unit keyed by tier ID. See LocalTierQueryEngine.
type localTierRegistry struct {
	o *Orchestrator
}

func (r *localTierRegistry) ListVaults() []glid.GLID {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	var ids []glid.GLID
	for _, v := range r.o.vaults {
		if err := vaultReplicationReadinessErr(v.ID, v); err != nil {
			continue
		}
		for _, t := range v.Tiers {
			if t.Query != nil {
				ids = append(ids, t.TierID)
			}
		}
	}
	return ids
}

func (r *localTierRegistry) ChunkManager(key glid.GLID) chunk.ChunkManager {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	for _, v := range r.o.vaults {
		if err := vaultReplicationReadinessErr(v.ID, v); err != nil {
			continue
		}
		for _, t := range v.Tiers {
			if t.TierID == key && t.Query != nil {
				return t.Chunks
			}
		}
	}
	return nil
}

func (r *localTierRegistry) IndexManager(key glid.GLID) index.IndexManager {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	for _, v := range r.o.vaults {
		if err := vaultReplicationReadinessErr(v.ID, v); err != nil {
			continue
		}
		for _, t := range v.Tiers {
			if t.TierID == key && t.Query != nil {
				return t.Indexes
			}
		}
	}
	return nil
}

func (r *localTierRegistry) QueryEngine(_ glid.GLID) *query.Engine { return nil }

// TransitionStreamedChunks returns the streamed-but-not-yet-expired set for
// any tier on this node (leader or follower). Followers don't apply the
// flag locally; their callbacks will return empty, which is fine — the
// flag set is read primarily to filter source chunks during transitions
// on the leader. See gastrolog-66b7x.
func (r *localTierRegistry) Reader() manifest.Reader { return r.o.ManifestReader() }

func (r *localTierRegistry) IndexReader() manifest.IndexReader { return r.o.IndexReader() }

func (r *localTierRegistry) TransitionStreamedChunks(key glid.GLID) map[chunk.ChunkID]bool {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	for _, v := range r.o.vaults {
		if err := vaultReplicationReadinessErr(v.ID, v); err != nil {
			continue
		}
		for _, t := range v.Tiers {
			if t.TierID != key || t.Query == nil || t.ListTransitionStreamed == nil {
				continue
			}
			ids := t.ListTransitionStreamed()
			if len(ids) == 0 {
				return nil
			}
			out := make(map[chunk.ChunkID]bool, len(ids))
			for _, cid := range ids {
				out[cid] = true
			}
			return out
		}
	}
	return nil
}

// leaderTierRegistry provides a flat view of all leader vaults. Each
// vault is a searchable unit keyed by VAULT ID (post-tier model — the
// 1:1 vault/tier collapse means tier ID and vault ID are equivalent
// for query routing). Resume tokens emitted from this registry tag
// positions with vault IDs, so the local-vs-remote split in
// QueryServer.splitResumeToken collapses to a single vault-keyed
// dispatch.
//
// Renamed for clarity in a later commit; still leaderTierRegistry
// during the refactor (gastrolog-257l7).
type leaderTierRegistry struct {
	o *Orchestrator
}

// findLeaderTier returns the leader TierInstance for the given vault ID,
// or nil if the vault is unknown / unready / has no leader tier.
func (r *leaderTierRegistry) findLeaderTier(vaultID glid.GLID) *TierInstance {
	v := r.o.vaults[vaultID]
	if v == nil {
		return nil
	}
	if err := vaultReplicationReadinessErr(vaultID, v); err != nil {
		return nil
	}
	for _, t := range v.Tiers {
		if !t.IsFollower && t.Query != nil {
			return t
		}
	}
	return nil
}

func (r *leaderTierRegistry) ListVaults() []glid.GLID {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	var ids []glid.GLID
	for vid, v := range r.o.vaults {
		if err := vaultReplicationReadinessErr(vid, v); err != nil {
			continue
		}
		for _, t := range v.Tiers {
			if !t.IsFollower && t.Query != nil {
				ids = append(ids, vid)
				break
			}
		}
	}
	return ids
}

func (r *leaderTierRegistry) ChunkManager(vaultID glid.GLID) chunk.ChunkManager {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	if t := r.findLeaderTier(vaultID); t != nil {
		return t.Chunks
	}
	return nil
}

func (r *leaderTierRegistry) IndexManager(vaultID glid.GLID) index.IndexManager {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	if t := r.findLeaderTier(vaultID); t != nil {
		return t.Indexes
	}
	return nil
}

func (r *leaderTierRegistry) QueryEngine(_ glid.GLID) *query.Engine { return nil }

func (r *leaderTierRegistry) Reader() manifest.Reader { return r.o.ManifestReader() }

func (r *leaderTierRegistry) IndexReader() manifest.IndexReader { return r.o.IndexReader() }

// TransitionStreamedChunks returns the streamed-but-not-yet-expired
// chunk set for the given vault. Resolves the leader tier instance and
// reads its ListTransitionStreamed callback (which reads the per-vault
// FSM). See gastrolog-4xusf.
func (r *leaderTierRegistry) TransitionStreamedChunks(vaultID glid.GLID) map[chunk.ChunkID]bool {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	t := r.findLeaderTier(vaultID)
	if t == nil || t.ListTransitionStreamed == nil {
		return nil
	}
	ids := t.ListTransitionStreamed()
	if len(ids) == 0 {
		return nil
	}
	out := make(map[chunk.ChunkID]bool, len(ids))
	for _, cid := range ids {
		out[cid] = true
	}
	return out
}

// LeaderTierQueryEngineForVault returns a query engine scoped to leader
// tiers of a single vault. Used by ForwardSearch — the vault is already
// selected, no vault_id= filtering needed.
func (o *Orchestrator) LeaderTierQueryEngineForVault(vaultID glid.GLID) (*query.Engine, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	v := o.vaults[vaultID]
	if err := vaultReplicationReadinessErr(vaultID, v); err != nil {
		return nil, err
	}
	var leaders []*TierInstance
	for _, t := range v.Tiers {
		if !t.IsFollower && t.Query != nil {
			leaders = append(leaders, t)
		}
	}
	if len(leaders) == 0 {
		return nil, nil
	}
	return query.NewWithRegistry(&tierRegistry{tiers: leaders}, o.logger), nil
}
