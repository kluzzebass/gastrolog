package manifest

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// NewProjectingReader returns a Reader that projects manifest entries from
// each vault's chunk manager via List() / Meta(). Used when no FSM is
// wired (memory-mode vaults, unit-test registries) — those vaults are their
// own source of truth and the local chunk manager view is authoritative.
//
// Sealed-only filtering is honored: ChunkMeta.Sealed=false entries are
// excluded from EntriesForVault and Entry. RetentionPending and TS-index
// TOC offsets are zero in the projected entries (memory-mode vaults
// don't track them).
func NewProjectingReader(reg VaultRegistry) Reader {
	return &projectingReader{reg: reg}
}

type projectingReader struct {
	reg VaultRegistry
}

func (p *projectingReader) Entry(id chunk.ChunkID) (vaultctlfsm.ManifestEntry, bool) {
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
			return vaultctlfsm.ManifestEntry{}, false
		}
		return projectChunkMeta(meta), true
	}
	return vaultctlfsm.ManifestEntry{}, false
}

func (p *projectingReader) EntriesForVault(vaultID glid.GLID) []vaultctlfsm.ManifestEntry {
	cm := p.reg.ChunkManager(vaultID)
	if cm == nil {
		return nil
	}
	metas, err := cm.List()
	if err != nil || len(metas) == 0 {
		return nil
	}
	out := make([]vaultctlfsm.ManifestEntry, 0, len(metas))
	for _, m := range metas {
		if !m.Sealed {
			continue
		}
		out = append(out, projectChunkMeta(m))
	}
	return out
}

func projectChunkMeta(m chunk.ChunkMeta) vaultctlfsm.ManifestEntry {
	state := m.State
	if state == chunk.ChunkStateUnknown {
		// projecting_reader runs in single-node / memory-mode where the
		// FSM doesn't populate State. Derive from the local Sealed bool
		// — m.Sealed=true on a memory-mode chunk means "fully sealed"
		// (no Sealing intermediate exists without an FSM).
		if m.Sealed {
			state = chunk.ChunkStateSealed
		} else {
			state = chunk.ChunkStateActive
		}
	}
	return vaultctlfsm.ManifestEntry{
		ID:          m.ID,
		WriteStart:  m.WriteStart,
		WriteEnd:    m.WriteEnd,
		RecordCount: m.RecordCount,
		Bytes:       m.Bytes,
		State:       state,
		DiskBytes:   m.DiskBytes,
		IngestStart: m.IngestStart,
		IngestEnd:   m.IngestEnd,
		SourceStart: m.SourceStart,
		SourceEnd:   m.SourceEnd,
		CloudBacked: m.CloudBacked,
		Archived:    m.Archived,
	}
}
