package orchestrator

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	"gastrolog/internal/manifest"
	"gastrolog/internal/query"
	"gastrolog/internal/system"
)

// HolderSearchScope limits which chunks a holder node searches for a vault.
// SealedChunkIDs is an allowlist of sealed chunks; PipelineChunks includes
// active/sealing chunks (placement leader only).
type HolderSearchScope struct {
	SealedChunkIDs []chunk.ChunkID
	PipelineChunks bool
}

// HolderQueryEngineForVault returns a query engine scoped to the local vault
// instance (leader or follower) with chunk subset filtering for distributed
// sealed search (gastrolog-2qj7m).
func (o *Orchestrator) HolderQueryEngineForVault(vaultID glid.GLID, scope HolderSearchScope) (*query.Engine, error) {
	o.mu.RLock()
	v := o.vaults[vaultID]
	var repErr error
	ready := false
	if v != nil {
		repErr = vaultReplicationReadinessErr(vaultID, v)
		ready = v.Instance != nil && v.Instance.Query != nil
	}
	o.mu.RUnlock()
	if repErr != nil {
		return nil, repErr
	}
	if !ready {
		return nil, nil
	}
	scope = o.gateHolderScopeToLocalReplica(vaultID, scope)
	if len(scope.SealedChunkIDs) == 0 && !scope.PipelineChunks {
		return nil, nil
	}
	return query.NewWithRegistry(&holderVaultRegistry{
		o:       o,
		vaultID: vaultID,
		scope:   scope,
	}, o.logger), nil
}

// gateHolderScopeToLocalReplica drops sealed chunks that are missing or
// incomplete on this node's local chunk manager.
func (o *Orchestrator) gateHolderScopeToLocalReplica(vaultID glid.GLID, scope HolderSearchScope) HolderSearchScope {
	if len(scope.SealedChunkIDs) == 0 {
		return scope
	}
	metas, err := o.ListLocalChunkMetas(vaultID)
	if err != nil || len(metas) == 0 {
		scope.SealedChunkIDs = nil
		return scope
	}
	local := make(map[chunk.ChunkID]chunk.ChunkMeta, len(metas))
	for _, m := range metas {
		local[m.ID] = m
	}
	kept := scope.SealedChunkIDs[:0]
	fsm := o.vaultCtlFSMForVault(vaultID)
	for _, id := range scope.SealedChunkIDs {
		m, ok := local[id]
		if !ok || !chunkMetaIsSealedLocal(m) {
			continue
		}
		if fsm != nil {
			if e := fsm.Get(id); e != nil && e.IsSealed() && m.RecordCount != e.RecordCount {
				continue
			}
		}
		kept = append(kept, id)
	}
	scope.SealedChunkIDs = kept
	return scope
}

// SearchChunkMetasForHolderScope returns chunk metas for a distributed-search
// holder slice without scanning the full vault manifest.
func (o *Orchestrator) SearchChunkMetasForHolderScope(vaultID glid.GLID, scope HolderSearchScope) []chunk.ChunkMeta {
	if fsm := o.vaultCtlFSMForVault(vaultID); fsm != nil {
		var out []chunk.ChunkMeta
		if scope.PipelineChunks {
			for _, e := range fsm.ListIncludingPipelineManifest() {
				if e.IsSealed() {
					continue
				}
				m := e.ToChunkMeta()
				o.overlayPipelineChunkMetaBounds(vaultID, &m)
				out = append(out, m)
			}
		}
		for _, id := range scope.SealedChunkIDs {
			e := fsm.Get(id)
			if e == nil || !e.IsSealed() {
				continue
			}
			out = append(out, e.ToChunkMeta())
		}
		return out
	}
	metas := o.SearchChunkMetasForVault(vaultID)
	if len(scope.SealedChunkIDs) == 0 && scope.PipelineChunks {
		return metas
	}
	allow := make(map[chunk.ChunkID]struct{}, len(scope.SealedChunkIDs))
	for _, id := range scope.SealedChunkIDs {
		allow[id] = struct{}{}
	}
	var out []chunk.ChunkMeta
	for _, m := range metas {
		if chunkMetaIsPipelineLocal(m) {
			if scope.PipelineChunks {
				out = append(out, m)
			}
			continue
		}
		if _, ok := allow[m.ID]; ok {
			out = append(out, m)
		}
	}
	return out
}

func chunkMetaIsSealedLocal(m chunk.ChunkMeta) bool {
	switch m.State {
	case chunk.ChunkStateSealed:
		return true
	case chunk.ChunkStateActive, chunk.ChunkStateSealing:
		return false
	case chunk.ChunkStateUnknown:
		return m.Sealed
	default:
		return m.Sealed
	}
}

// holderVaultRegistry exposes a local vault instance with sealed-chunk
// subset filtering for distributed search.
type holderVaultRegistry struct {
	o       *Orchestrator
	vaultID glid.GLID
	scope   HolderSearchScope
}

func (r *holderVaultRegistry) findLocalInstance(vaultID glid.GLID) *VaultInstance {
	if vaultID != r.vaultID {
		return nil
	}
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	v := r.o.vaults[r.vaultID]
	if v == nil {
		return nil
	}
	if err := vaultReplicationReadinessErr(r.vaultID, v); err != nil {
		return nil
	}
	if v.Instance != nil && v.Instance.Query != nil {
		return v.Instance
	}
	return nil
}

func (r *holderVaultRegistry) ListVaults() []glid.GLID { return []glid.GLID{r.vaultID} }

func (r *holderVaultRegistry) ChunkManager(key glid.GLID) chunk.ChunkManager {
	if inst := r.findLocalInstance(key); inst != nil {
		return inst.Chunks
	}
	return nil
}

func (r *holderVaultRegistry) IndexManager(key glid.GLID) index.IndexManager {
	if inst := r.findLocalInstance(key); inst != nil {
		return inst.Indexes
	}
	return nil
}

func (r *holderVaultRegistry) QueryEngine(_ glid.GLID) *query.Engine { return nil }

func (r *holderVaultRegistry) Reader() manifest.Reader { return r.o.ManifestReader() }

func (r *holderVaultRegistry) SearchChunkMetas(vaultID glid.GLID) []chunk.ChunkMeta {
	if vaultID != r.vaultID {
		return nil
	}
	return r.o.SearchChunkMetasForHolderScope(vaultID, r.scope)
}

func chunkMetaIsPipelineLocal(m chunk.ChunkMeta) bool {
	switch m.State {
	case chunk.ChunkStateActive, chunk.ChunkStateSealing:
		return true
	case chunk.ChunkStateSealed:
		return false
	case chunk.ChunkStateUnknown:
		return !m.Sealed
	default:
		return !m.Sealed
	}
}

func (r *holderVaultRegistry) OpenPipelineChunkCursor(vaultID glid.GLID, chunkID chunk.ChunkID) (chunk.RecordCursor, error) {
	if vaultID == r.vaultID && r.scope.PipelineChunks {
		return r.o.OpenPipelineChunkCursor(vaultID, chunkID)
	}
	return nil, chunk.ErrChunkNotFound
}

func (r *holderVaultRegistry) ScanPipelineChunkIngestTS(vaultID glid.GLID, chunkID chunk.ChunkID, cb func(tsNanos int64) bool) error {
	if vaultID == r.vaultID && r.scope.PipelineChunks {
		return r.o.ScanPipelineChunkIngestTS(vaultID, chunkID, cb)
	}
	return chunk.ErrChunkNotFound
}

func (r *holderVaultRegistry) IndexReader() manifest.IndexReader { return r.o.IndexReader() }

// LocalHolderVaultIDs returns vault IDs with a searchable local instance
// (leader or follower). Used for resume-token routing under distributed search.
func (o *Orchestrator) LocalHolderVaultIDs() map[glid.GLID]bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	ids := make(map[glid.GLID]bool)
	for _, v := range o.vaults {
		if err := vaultReplicationReadinessErr(v.ID, v); err != nil {
			continue
		}
		if v.Instance != nil && v.Instance.Query != nil {
			ids[v.ID] = true
		}
	}
	return ids
}

// UsesDistributedSearch returns true when the vault has multiple placement
// holders and should use partitioned sealed search.
func UsesDistributedSearch(placements []system.VaultPlacement, nscs []system.NodeStorageConfig) bool {
	return len(system.PlacementNodeIDs(placements, nscs)) > 1
}
