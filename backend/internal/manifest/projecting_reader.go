package manifest

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/tierfsm"
)

// NewProjectingReader returns a Reader that projects manifest entries from
// each vault's chunk manager via List() / Meta(). Used when no FSM is
// wired (memory-mode tiers, unit-test registries) — those tiers are their
// own source of truth and the local chunk manager view is authoritative.
//
// Sealed-only filtering is honored: ChunkMeta.Sealed=false entries are
// excluded from EntriesForVault and Entry. RetentionPending /
// TransitionStreamed / TS-index TOC offsets are zero in the projected
// entries (memory-mode tiers don't track them).
func NewProjectingReader(reg VaultRegistry) Reader {
	return &projectingReader{reg: reg}
}

type projectingReader struct {
	reg VaultRegistry
}

func (p *projectingReader) Entry(id chunk.ChunkID) (tierfsm.ManifestEntry, bool) {
	for _, vaultID := range p.reg.ListVaults() {
		cm := p.reg.ChunkManager(vaultID)
		if cm == nil {
			continue
		}
		meta, err := cm.Meta(id)
		if err != nil {
			continue
		}
		if !meta.Sealed {
			return tierfsm.ManifestEntry{}, false
		}
		return projectChunkMeta(meta), true
	}
	return tierfsm.ManifestEntry{}, false
}

func (p *projectingReader) EntriesForVault(vaultID glid.GLID) []tierfsm.ManifestEntry {
	cm := p.reg.ChunkManager(vaultID)
	if cm == nil {
		return nil
	}
	metas, err := cm.List()
	if err != nil || len(metas) == 0 {
		return nil
	}
	out := make([]tierfsm.ManifestEntry, 0, len(metas))
	for _, m := range metas {
		if !m.Sealed {
			continue
		}
		out = append(out, projectChunkMeta(m))
	}
	return out
}

func projectChunkMeta(m chunk.ChunkMeta) tierfsm.ManifestEntry {
	return tierfsm.ManifestEntry{
		ID:          m.ID,
		WriteStart:  m.WriteStart,
		WriteEnd:    m.WriteEnd,
		RecordCount: m.RecordCount,
		Bytes:       m.Bytes,
		Sealed:      m.Sealed,
		Compressed:  m.Compressed,
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
