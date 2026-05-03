package orchestrator

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/manifest"
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
