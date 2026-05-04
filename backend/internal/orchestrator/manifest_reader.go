package orchestrator

import (
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	"gastrolog/internal/manifest"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/vaultraft"
	"gastrolog/internal/vaultraft/tierfsm"
)

// ManifestReader returns a manifest.Reader backed by this orchestrator's
// vaults. Walks the per-tier sub-FSMs to project a global view of sealed
// chunk manifests; honors the active-chunk exception by filtering on
// Sealed=true.
//
// Memory-mode tiers (no FSM, no replication) are projected from the local
// chunk manager's List() so callers see a uniform view regardless of how
// the tier is backed.
func (o *Orchestrator) ManifestReader() manifest.Reader {
	return &orchestratorManifestReader{o: o}
}

// orchestratorManifestReader implements manifest.Reader by walking the
// orchestrator's vaults and their tiers. Sealed entries from the tier FSM
// are returned verbatim; memory-mode tiers project from chunk.ChunkManager
// because those tiers are their own source of truth (no replication).
type orchestratorManifestReader struct {
	o *Orchestrator
}

var _ manifest.Reader = (*orchestratorManifestReader)(nil)

// Entry returns the sealed manifest entry for the given chunk ID. ChunkIDs
// are globally unique, so this fans out across every vault and tier until
// it finds the chunk; it does NOT return active chunks (Sealed=false).
func (r *orchestratorManifestReader) Entry(id chunk.ChunkID) (tierfsm.ManifestEntry, bool) {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	for _, v := range r.o.vaults {
		for _, t := range v.Tiers {
			if e, ok := tierManifestEntry(t, id); ok && e.Sealed {
				return e, true
			}
		}
	}
	return tierfsm.ManifestEntry{}, false
}

// EntriesForVault returns every sealed manifest entry under the given key.
// Resolves the key as a vault ID first; if no vault matches, falls back
// to interpreting the key as a tier ID (used by leader/local tier
// registries that present tiers as searchable units). Returns nil if no
// vault or tier matches.
func (r *orchestratorManifestReader) EntriesForVault(key glid.GLID) []tierfsm.ManifestEntry {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	if v := r.o.vaults[key]; v != nil {
		return collectSealedEntries(v.Tiers)
	}
	for _, v := range r.o.vaults {
		for _, t := range v.Tiers {
			if t.TierID == key {
				return collectSealedEntries([]*TierInstance{t})
			}
		}
	}
	return nil
}

// VaultManifestEntriesFromCtlFSM returns every manifest entry (sealed and
// active) for the given vault, read directly from the replicated vault-ctl
// Raft FSM rather than from local TierInstances. Every node participates as
// a voter in every vault-ctl Raft group (gastrolog-292yi), so the FSM is
// authoritative cluster-wide and visible on nodes that don't host any tier
// instance for the vault — the case where ManifestReader().EntriesForVault
// returns nil because o.vaults has no entry. Returns nil when there is no
// GroupManager (single-node / memory mode) or when this node hasn't joined
// the vault-ctl group yet.
func (o *Orchestrator) VaultManifestEntriesFromCtlFSM(vaultID glid.GLID) []tierfsm.ManifestEntry {
	if o.groupMgr == nil {
		return nil
	}
	g := o.groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(vaultID))
	if g == nil {
		return nil
	}
	vfsm, ok := g.FSM.(*vaultraft.FSM)
	if !ok || vfsm == nil {
		return nil
	}
	var out []tierfsm.ManifestEntry
	for _, t := range vfsm.Tiers() {
		out = append(out, t.List()...)
	}
	return out
}

func collectSealedEntries(tiers []*TierInstance) []tierfsm.ManifestEntry {
	var out []tierfsm.ManifestEntry
	for _, t := range tiers {
		for _, e := range tierManifestEntries(t) {
			if e.Sealed {
				out = append(out, e)
			}
		}
	}
	return out
}

// tierManifestEntry returns the manifest entry for a chunk on this tier.
// Prefers the FSM callback (cluster-replicated truth) and falls back to
// projecting from the local chunk manager for memory-mode tiers.
func tierManifestEntry(t *TierInstance, id chunk.ChunkID) (tierfsm.ManifestEntry, bool) {
	if t.ManifestEntry != nil {
		return t.ManifestEntry(id)
	}
	if t.Chunks == nil {
		return tierfsm.ManifestEntry{}, false
	}
	meta, err := t.Chunks.Meta(id)
	if err != nil {
		return tierfsm.ManifestEntry{}, false
	}
	return chunkMetaToManifestEntry(meta), true
}

// tierManifestEntries returns every manifest entry on this tier. FSM-backed
// tiers go through the callback; memory-mode tiers project from List().
func tierManifestEntries(t *TierInstance) []tierfsm.ManifestEntry {
	if t.ManifestEntries != nil {
		return t.ManifestEntries()
	}
	if t.Chunks == nil {
		return nil
	}
	metas, err := t.Chunks.List()
	if err != nil || len(metas) == 0 {
		return nil
	}
	out := make([]tierfsm.ManifestEntry, len(metas))
	for i, m := range metas {
		out[i] = chunkMetaToManifestEntry(m)
	}
	return out
}

// IndexReader returns a manifest.IndexReader that resolves IngestTS rank /
// position lookups against this orchestrator's locally-hosted tiers. Phase 1
// implementation delegates to the existing layered fallback
// (chunk.ChunkManager.FindIngestEntryIndex → index.IndexManager.FindIngestEntryIndex).
// A future pass will collapse those two file-access paths onto a single
// FSM-grounded GLCB section reader; the interface boundary is in place
// either way so callers (notably the histogram) don't have to know.
func (o *Orchestrator) IndexReader() manifest.IndexReader {
	return &orchestratorIndexReader{o: o}
}

// orchestratorIndexReader implements manifest.IndexReader by walking the
// orchestrator's local tier instances to find the chunk's owning tier,
// then dispatching to that tier's chunk manager (and index manager) for
// the actual rank/pos lookup. Same fallback logic as the legacy
// findIngestRank/findIngestPos helpers in internal/query/histogram.go,
// just behind the manifest.IndexReader interface.
type orchestratorIndexReader struct {
	o *Orchestrator
}

var _ manifest.IndexReader = (*orchestratorIndexReader)(nil)

// FindIngestRank returns the rank of the first IngestTS-sorted entry with
// TS >= ts. Tries the chunk manager (active chunk B+ tree, cloud chunk
// cached index) first; falls back to the index manager (sealed local
// chunk sidecar). Returns (0, false) when neither serves the lookup.
func (r *orchestratorIndexReader) FindIngestRank(chunkID chunk.ChunkID, ts time.Time) (uint64, bool) {
	cm, im := r.lookupTierManagers(chunkID)
	if cm != nil {
		if rank, found, err := cm.FindIngestEntryIndex(chunkID, ts); err == nil && found {
			return rank, true
		}
	}
	if im != nil {
		if rank, found, err := im.FindIngestEntryIndex(chunkID, ts); err == nil && found {
			return rank, true
		}
	}
	return 0, false
}

// FindIngestPos returns the physical record position for the same query.
// Same dispatch shape as FindIngestRank.
func (r *orchestratorIndexReader) FindIngestPos(chunkID chunk.ChunkID, ts time.Time) (uint64, bool) {
	cm, im := r.lookupTierManagers(chunkID)
	if cm != nil {
		if pos, found, err := cm.FindIngestStartPosition(chunkID, ts); err == nil && found {
			return pos, true
		}
	}
	if im != nil {
		if pos, found, err := im.FindIngestStartPosition(chunkID, ts); err == nil && found {
			return pos, true
		}
	}
	return 0, false
}

// lookupTierManagers walks the orchestrator's local tiers to find the
// (chunk, index) manager pair owning the given chunk. Returns (nil, nil)
// when the chunk isn't on any local tier — a signal that the histogram
// caller should fall back to FSM-based proportional distribution.
func (r *orchestratorIndexReader) lookupTierManagers(chunkID chunk.ChunkID) (chunk.ChunkManager, index.IndexManager) {
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	for _, v := range r.o.vaults {
		for _, t := range v.Tiers {
			if t.Chunks == nil {
				continue
			}
			if _, err := t.Chunks.Meta(chunkID); err == nil {
				return t.Chunks, t.Indexes
			}
		}
	}
	return nil, nil
}

// chunkMetaToManifestEntry projects a chunk.ChunkMeta into the FSM-shaped
// tierfsm.ManifestEntry. Used only for memory-mode tiers, which have no
// FSM and no replication — the local chunk manager IS the source of truth
// there. RetentionPending / TransitionStreamed / IngestIdx*/SourceIdx*
// fields stay zero (memory-mode tiers don't track them).
func chunkMetaToManifestEntry(m chunk.ChunkMeta) tierfsm.ManifestEntry {
	return tierfsm.ManifestEntry{
		ID:          m.ID,
		WriteStart:  m.WriteStart,
		WriteEnd:    m.WriteEnd,
		RecordCount: m.RecordCount,
		Bytes:       m.Bytes,
		Sealed:      m.Sealed,
		DiskBytes:   m.DiskBytes,
		IngestStart: m.IngestStart,
		IngestEnd:   m.IngestEnd,
		SourceStart: m.SourceStart,
		SourceEnd:   m.SourceEnd,
		CloudBacked: m.CloudBacked,
		Archived:    m.Archived,
		NumFrames:   m.NumFrames,
	}
}
