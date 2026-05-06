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

// ChunkManager implements manifest.VaultRegistry: returns the vault's
// chunk manager. Returns nil if not found.
func (o *Orchestrator) ChunkManager(key glid.GLID) chunk.ChunkManager {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if s := o.vaults[key]; s != nil {
		return s.ChunkManager()
	}
	return nil
}

// IndexManager implements manifest.VaultRegistry: returns the vault's
// index manager. Returns nil if not found.
func (o *Orchestrator) IndexManager(key glid.GLID) index.IndexManager {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if s := o.vaults[key]; s != nil {
		return s.IndexManager()
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

// LocalLeaderVaultIDs returns the set of vault IDs where this node is
// the leader for the vault. Used by search fan-out and remote-vault
// resolution: follower-only vaults are NOT included because their
// replicas may be incomplete (missing active chunk, replication lag);
// search always fans out to the leader for authoritative results.
func (o *Orchestrator) LocalLeaderVaultIDs() map[glid.GLID]bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	ids := make(map[glid.GLID]bool)
	for _, v := range o.vaults {
		if err := vaultReplicationReadinessErr(v.ID, v); err != nil {
			continue
		}
		if t := v.Instance; t != nil && !t.IsFollower {
			ids[v.ID] = true
		}
	}
	return ids
}

// HasLocalQueryEngine returns true if the vault has an instance with a
// query engine on this node (i.e., actual searchable data, not just a
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
	return v.Instance != nil && v.Instance.Query != nil
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
	return v.ChunkManager()
}

func (r *searchReadyRegistry) IndexManager(key glid.GLID) index.IndexManager {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	v := r.o.vaults[key]
	if v == nil || vaultReplicationReadinessErr(key, v) != nil {
		return nil
	}
	return v.IndexManager()
}

func (r *searchReadyRegistry) TransitionStreamedChunks(key glid.GLID) map[chunk.ChunkID]bool {
	return r.o.TransitionStreamedChunks(key)
}

func (r *searchReadyRegistry) Reader() manifest.Reader { return r.o.ManifestReader() }

func (r *searchReadyRegistry) IndexReader() manifest.IndexReader { return r.o.IndexReader() }

// LeaderVaultQueryEngine returns a query engine that only searches leader
// instances (not follower replicas). Used by ForwardSearch handlers to
// avoid double-counting when the requesting node already searches its own
// followers.
func (o *Orchestrator) LeaderVaultQueryEngine() *query.Engine {
	return query.NewWithRegistry(&leaderVaultRegistry{o: o}, o.logger)
}

// LocalVaultQueryEngine returns a query engine that searches every locally
// available vault — leader OR follower. Suitable for approximate aggregations
// (notably histogram bucket counts) where consistency with leader is not
// required and follower-replica data is "good enough." Avoids cross-node
// gRPC fan-out when chunks are already replicated locally (RF > 1). Records
// must NOT use this engine — use LeaderVaultQueryEngine for authoritative
// reads. See gastrolog-66b7x.
func (o *Orchestrator) LocalVaultQueryEngine() *query.Engine {
	return query.NewWithRegistry(&localVaultRegistry{o: o}, o.logger)
}

// localVaultRegistry exposes every vault this node holds (as leader or
// follower) as a searchable unit keyed by VAULT ID.
// See LocalVaultQueryEngine.
type localVaultRegistry struct {
	o *Orchestrator
}

// findLocalInstance returns the local VaultInstance for the given vault if
// it has a query engine wired (leader OR follower). Locked-context helper.
func (r *localVaultRegistry) findLocalInstance(vaultID glid.GLID) *VaultInstance {
	v := r.o.vaults[vaultID]
	if v == nil {
		return nil
	}
	if err := vaultReplicationReadinessErr(vaultID, v); err != nil {
		return nil
	}
	if v.Instance != nil && v.Instance.Query != nil {
		return v.Instance
	}
	return nil
}

func (r *localVaultRegistry) ListVaults() []glid.GLID {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	var ids []glid.GLID
	for vid, v := range r.o.vaults {
		if err := vaultReplicationReadinessErr(vid, v); err != nil {
			continue
		}
		if v.Instance != nil && v.Instance.Query != nil {
			ids = append(ids, vid)
		}
	}
	return ids
}

func (r *localVaultRegistry) ChunkManager(vaultID glid.GLID) chunk.ChunkManager {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	if t := r.findLocalInstance(vaultID); t != nil {
		return t.Chunks
	}
	return nil
}

func (r *localVaultRegistry) IndexManager(vaultID glid.GLID) index.IndexManager {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	if t := r.findLocalInstance(vaultID); t != nil {
		return t.Indexes
	}
	return nil
}

func (r *localVaultRegistry) QueryEngine(_ glid.GLID) *query.Engine { return nil }

func (r *localVaultRegistry) Reader() manifest.Reader { return r.o.ManifestReader() }

func (r *localVaultRegistry) IndexReader() manifest.IndexReader { return r.o.IndexReader() }

// TransitionStreamedChunks returns the streamed-but-not-yet-expired set
// for the given vault. Followers don't apply the flag locally (their
// callbacks return empty), which is fine — the flag set is read primarily
// to filter source chunks during transitions on the leader.
// See gastrolog-66b7x.
func (r *localVaultRegistry) TransitionStreamedChunks(vaultID glid.GLID) map[chunk.ChunkID]bool {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	t := r.findLocalInstance(vaultID)
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

// leaderVaultRegistry provides a flat view of all leader vaults. Each
// vault is a searchable unit keyed by VAULT ID. Resume tokens emitted
// from this registry tag positions with vault IDs.
type leaderVaultRegistry struct {
	o *Orchestrator
}

// findLeaderInstance returns the leader VaultInstance for the given vault
// ID, or nil if the vault is unknown / unready / has no leader instance.
func (r *leaderVaultRegistry) findLeaderInstance(vaultID glid.GLID) *VaultInstance {
	v := r.o.vaults[vaultID]
	if v == nil {
		return nil
	}
	if err := vaultReplicationReadinessErr(vaultID, v); err != nil {
		return nil
	}
	if v.Instance != nil && !v.Instance.IsFollower && v.Instance.Query != nil {
		return v.Instance
	}
	return nil
}

func (r *leaderVaultRegistry) ListVaults() []glid.GLID {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	var ids []glid.GLID
	for vid, v := range r.o.vaults {
		if err := vaultReplicationReadinessErr(vid, v); err != nil {
			continue
		}
		if v.Instance != nil && !v.Instance.IsFollower && v.Instance.Query != nil {
			ids = append(ids, vid)
		}
	}
	return ids
}

func (r *leaderVaultRegistry) ChunkManager(vaultID glid.GLID) chunk.ChunkManager {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	if t := r.findLeaderInstance(vaultID); t != nil {
		return t.Chunks
	}
	return nil
}

func (r *leaderVaultRegistry) IndexManager(vaultID glid.GLID) index.IndexManager {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	if t := r.findLeaderInstance(vaultID); t != nil {
		return t.Indexes
	}
	return nil
}

func (r *leaderVaultRegistry) QueryEngine(_ glid.GLID) *query.Engine { return nil }

func (r *leaderVaultRegistry) Reader() manifest.Reader { return r.o.ManifestReader() }

func (r *leaderVaultRegistry) IndexReader() manifest.IndexReader { return r.o.IndexReader() }

// TransitionStreamedChunks returns the streamed-but-not-yet-expired
// chunk set for the given vault. Resolves the leader instance and reads
// its ListTransitionStreamed callback (which reads the per-vault FSM).
// See gastrolog-4xusf.
func (r *leaderVaultRegistry) TransitionStreamedChunks(vaultID glid.GLID) map[chunk.ChunkID]bool {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	t := r.findLeaderInstance(vaultID)
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

// LeaderTierQueryEngineForVault returns a query engine scoped to the
// leader instance of a single vault. Used by ForwardSearch — the vault
// is already selected, no vault_id= filtering needed.
func (o *Orchestrator) LeaderTierQueryEngineForVault(vaultID glid.GLID) (*query.Engine, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	v := o.vaults[vaultID]
	if err := vaultReplicationReadinessErr(vaultID, v); err != nil {
		return nil, err
	}
	if v == nil || v.Instance == nil || v.Instance.IsFollower || v.Instance.Query == nil {
		return nil, nil
	}
	return query.NewWithRegistry(&singleVaultRegistry{o: o, vaultID: vaultID}, o.logger), nil
}

// singleVaultRegistry exposes a single vault's leader instance as a
// searchable unit. Used by ForwardSearch where the vault is already
// selected and the query engine just needs to dispatch to one vault.
type singleVaultRegistry struct {
	o       *Orchestrator
	vaultID glid.GLID
}

func (r *singleVaultRegistry) ListVaults() []glid.GLID { return []glid.GLID{r.vaultID} }

func (r *singleVaultRegistry) ChunkManager(key glid.GLID) chunk.ChunkManager {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	if key != r.vaultID {
		return nil
	}
	v := r.o.vaults[r.vaultID]
	if v == nil || v.Instance == nil || v.Instance.IsFollower {
		return nil
	}
	return v.Instance.Chunks
}

func (r *singleVaultRegistry) IndexManager(key glid.GLID) index.IndexManager {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	if key != r.vaultID {
		return nil
	}
	v := r.o.vaults[r.vaultID]
	if v == nil || v.Instance == nil || v.Instance.IsFollower {
		return nil
	}
	return v.Instance.Indexes
}

func (r *singleVaultRegistry) QueryEngine(_ glid.GLID) *query.Engine { return nil }

func (r *singleVaultRegistry) Reader() manifest.Reader { return r.o.ManifestReader() }

func (r *singleVaultRegistry) IndexReader() manifest.IndexReader { return r.o.IndexReader() }

func (r *singleVaultRegistry) TransitionStreamedChunks(key glid.GLID) map[chunk.ChunkID]bool {
	if key != r.vaultID {
		return nil
	}
	return r.o.TransitionStreamedChunks(key)
}
