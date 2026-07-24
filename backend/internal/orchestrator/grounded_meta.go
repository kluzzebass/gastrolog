package orchestrator

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// groundChunkMeta returns a copy of a node-local chunk meta with the
// cluster-wide lifecycle fields grounded in the replicated vault-ctl FSM
// manifest. It is the single orchestrator-level seam that replaces per-call-site
// OverlayFromFSM patching (gastrolog-2lfjk): the chunk manager stays local-truth
// only, and every producer-side reader that needs cluster-wide truth grounds
// the local meta here.
//
// The local chunk manager only reflects THIS node's view. For the cluster-wide
// fields that view is wrong on follower nodes: followers strip sealed_backing
// from their chunk-manager params (reconfig_vaults.go), so their CloudStore is
// nil and their local CloudBacked stays false even after the cluster uploaded
// the chunk to S3. The replicated FSM carries the authoritative truth via the
// CmdSealChunk / CmdUploadChunk / CmdArchiveChunk commands, so these fields are
// sourced from the manifest entry:
//
//   - State        cluster lifecycle (Active/Sealing/Sealed). Local meta.Sealed
//                  flips at sealActiveLocked time, but the cluster only sees the
//                  chunk as Sealed once sealToGLCB commits (gastrolog-1huz5), so
//                  producer-side iteration must branch on the FSM state to avoid
//                  jumping the gun on Sealing chunks.
//   - Sealed       kept in sync with State (Sealing reads as not-yet-sealed) so
//                  the unaudited legacy .Sealed read sites stay correct.
//   - CloudBacked  } cloud upload / archive effects a follower cannot observe
//   - Archived     } locally.
//   - SealedAt     retention MaxAge anchor. Pipeline / file managers often leave
//                  local SealedAt zero and only populate WriteEnd, so the FSM's
//                  wall-clock seal completion is applied when present.
//
// Every node-local fact is preserved untouched: DiskBytes (per-node warm-cache
// footprint), CloudBytes, and every other field the local manager owns. The
// FSM's ManifestEntry has no per-node disk claim to overlay, so DiskBytes must
// never be clobbered (gastrolog-33ul6h; pinned by
// TestGroundMetaFromEntryDoesNotClobberLocalDiskBytes).
//
// When this node participates in no vault-ctl FSM for the vault (memory-mode
// vaults, single-node mode, or a chunk the FSM has not recorded yet), the local
// meta is returned unchanged — the local manager is already authoritative there.
func (o *Orchestrator) groundChunkMeta(vaultID glid.GLID, m chunk.ChunkMeta) chunk.ChunkMeta {
	e, ok := o.groundingEntry(vaultID, m.ID)
	if !ok {
		return m
	}
	return groundMetaFromEntry(m, e)
}

// chunkMetaGrounder returns groundChunkMeta bound to a single vault, for call
// sites that hand the grounding step around as a value (streaming projections,
// transfer planners). The returned function returns its argument unchanged
// whenever the vault has no local vault-ctl FSM — matching the nil-OverlayFromFSM
// behavior it replaces, so callers no longer need a nil guard.
func (o *Orchestrator) chunkMetaGrounder(vaultID glid.GLID) func(chunk.ChunkMeta) chunk.ChunkMeta {
	return func(m chunk.ChunkMeta) chunk.ChunkMeta {
		return o.groundChunkMeta(vaultID, m)
	}
}

// groundMetaFromEntry merges the cluster-wide lifecycle fields of a replicated
// manifest entry onto a node-local chunk meta, preserving every node-local
// fact. Pure — the FSM lookup lives in groundChunkMeta — so the DiskBytes /
// CloudBytes non-clobber invariant is unit-testable without cluster scaffolding.
func groundMetaFromEntry(m chunk.ChunkMeta, e vaultctlfsm.ManifestEntry) chunk.ChunkMeta {
	m.State = e.State
	m.Sealed = e.State == chunk.ChunkStateSealed
	m.CloudBacked = e.CloudBacked
	m.Archived = e.Archived
	if !e.SealedAt.IsZero() {
		m.SealedAt = e.SealedAt
	}
	return m
}

// groundingEntry resolves the replicated vault-ctl manifest entry used to ground
// a chunk meta, mirroring the read core's dual (manifestEntryByChunk) scoped to
// one vault:
//
//  1. The per-vault FSM via the group manager — resolvable on any voter
//     (gastrolog-292yi), even on a node hosting no instance for the vault. Every
//     node is a voter of every vault-ctl group, so this is the authoritative
//     cluster-wide source. A present-but-absent chunk (FSM has the vault, not
//     this chunk) reports false so local truth is preserved — matching the
//     retired OverlayFromFSM, which read this same FSM.
//  2. The local instance's own vault-ctl FSM callback (ManifestEntry), for a
//     node with an instance whose group-manager handle isn't reachable here —
//     the same replicated FSM, read through the instance closure.
//
// Both tiers read the FSM only; the seam never re-derives cluster-wide fields
// from the local chunk manager (memory-mode vaults have no FSM callback, so
// they report false and their already-authoritative local truth passes through
// unchanged — exactly the nil-OverlayFromFSM behavior this replaces). Reports
// false when neither tier resolves; the meta is returned unchanged.
func (o *Orchestrator) groundingEntry(vaultID glid.GLID, id chunk.ChunkID) (vaultctlfsm.ManifestEntry, bool) {
	if f := o.vaultCtlFSMForVault(vaultID); f != nil {
		e := f.Get(id)
		if e == nil {
			return vaultctlfsm.ManifestEntry{}, false
		}
		return *e, true
	}
	o.mu.RLock()
	v := o.vaults[vaultID]
	o.mu.RUnlock()
	if v != nil && v.Instance != nil && v.Instance.ManifestEntry != nil {
		return v.Instance.ManifestEntry(id)
	}
	return vaultctlfsm.ManifestEntry{}, false
}
