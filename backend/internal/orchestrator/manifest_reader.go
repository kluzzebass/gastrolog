package orchestrator

import (
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	filetsidx "gastrolog/internal/index/file/tsidx"
	"gastrolog/internal/manifest"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/vaultraft"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// ManifestReader returns a manifest.Reader backed by the replicated vault-ctl
// FSMs. Every node is a voter of every vault-ctl Raft group,
// so the sealed-chunk manifest resolves on ANY node — including nodes that
// host no instance for the vault. Honors the active-chunk exception by
// filtering on IsSealed().
//
// Memory-mode vaults (no FSM, no replication) are projected from the local
// chunk manager's List() so callers see a uniform view regardless of how
// the instance is backed.
func (o *Orchestrator) ManifestReader() manifest.Reader {
	return &orchestratorManifestReader{o: o}
}

// IntegrityVerifier returns a chunk.IntegrityVerifier backed by the same
// FSM-projected manifest as ManifestReader. The chunk manager wires this
// in to verify cold-cache cloud downloads against the FSM-recorded digest.
func (o *Orchestrator) IntegrityVerifier() chunk.IntegrityVerifier {
	return &orchestratorManifestReader{o: o}
}

// orchestratorManifestReader implements manifest.Reader on the unified
// manifest read core (manifestEntryByChunk / manifestEntriesForVault):
// vault-ctl FSM first, local memory-mode projection as fallback. Sealed
// entries from the vault-ctl FSM are returned verbatim; memory-mode vaults
// project from chunk.ChunkManager because those vaults are their own source
// of truth (no replication).
type orchestratorManifestReader struct {
	o *Orchestrator
}

var _ manifest.Reader = (*orchestratorManifestReader)(nil)
var _ chunk.IntegrityVerifier = (*orchestratorManifestReader)(nil)

// ExpectedDigest implements chunk.IntegrityVerifier. Returns the FSM-recorded
// GLCB whole-blob digest for a chunk; (zero, false) when the chunk isn't
// in the manifest yet (pre-upload race) or carries no recorded hash (zero
// Hash on the entry, treated as "no expectation"). Cold-cache downloads
// consult this to reject blobs
// whose actual digest doesn't match what the leader stamped at upload time.
func (r *orchestratorManifestReader) ExpectedDigest(id chunk.ChunkID) ([32]byte, bool) {
	e, ok := r.Entry(id)
	if !ok {
		return [32]byte{}, false
	}
	if e.Hash == ([32]byte{}) {
		return [32]byte{}, false
	}
	return e.Hash, true
}

// Entry returns the sealed manifest entry for the given chunk ID. ChunkIDs
// are globally unique, so this resolves across every vault-ctl FSM this node
// participates in (any voter — no local instance required) before falling
// back to memory-mode local projection; it does NOT return active chunks.
func (r *orchestratorManifestReader) Entry(id chunk.ChunkID) (vaultctlfsm.ManifestEntry, bool) {
	_, e, ok := r.o.manifestEntryByChunk(id)
	if !ok || !e.IsSealed() {
		return vaultctlfsm.ManifestEntry{}, false
	}
	return e, true
}

// EntriesForVault returns every sealed manifest entry for the given vault.
// Served from the replicated vault-ctl FSM on any voter; returns nil only
// when this node has neither joined the vault's control-plane group nor
// hosts a local instance for it.
func (r *orchestratorManifestReader) EntriesForVault(key glid.GLID) []vaultctlfsm.ManifestEntry {
	var out []vaultctlfsm.ManifestEntry
	for _, e := range r.o.manifestEntriesForVault(key) {
		if e.IsSealed() {
			out = append(out, e)
		}
	}
	return out
}

// VaultManifestEntriesIncludingOpen returns every manifest entry (sealed,
// sealing AND open/active) for the given vault, read from the replicated
// vault-ctl Raft FSM. Every node participates as a voter in every vault-ctl
// Raft group, so the FSM is authoritative cluster-wide and
// visible on nodes that don't host any instance for the vault. This is the
// open-chunk-inclusive projection of the same read core that backs
// ManifestReader (which stays sealed-only per the active-chunk exception).
// Returns nil when there is no GroupManager (single-node / memory mode) or
// when this node hasn't joined the vault-ctl group yet — callers keep their
// own local-projection fallback for that case.
func (o *Orchestrator) VaultManifestEntriesIncludingOpen(vaultID glid.GLID) []vaultctlfsm.ManifestEntry {
	f := o.vaultCtlFSMForVault(vaultID)
	if f == nil {
		return nil
	}
	return f.ListIncludingPipelineManifest()
}

// manifestEntriesForVault is the single read core behind the per-vault
// manifest surfaces: the replicated vault-ctl FSM first (visible on every
// voter), the local instance projection (memory-mode vaults, which have no
// FSM) as fallback. Entries come back in ALL lifecycle states; callers apply
// their own sealed/open filter.
func (o *Orchestrator) manifestEntriesForVault(vaultID glid.GLID) []vaultctlfsm.ManifestEntry {
	if f := o.vaultCtlFSMForVault(vaultID); f != nil {
		return f.ListIncludingPipelineManifest()
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	if v := o.vaults[vaultID]; v != nil && v.Instance != nil {
		return vaultManifestEntries(v.Instance)
	}
	return nil
}

// manifestEntryByChunk resolves the manifest entry and owning vault for a
// chunk on the unified read core: every vault-ctl FSM this node participates
// in first (any voter — no local instance required), then the local instances
// (memory-mode projection). Open pipeline chunks live in the open-chunk
// manifest, not the chunk map, so they do not resolve here; sealed-manifest
// consumers don't want them and open-chunk consumers have
// findPipelineOpenChunk.
func (o *Orchestrator) manifestEntryByChunk(id chunk.ChunkID) (glid.GLID, vaultctlfsm.ManifestEntry, bool) {
	for vid, f := range o.vaultCtlFSMs() {
		if e := f.Get(id); e != nil {
			return vid, *e, true
		}
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	for vid, v := range o.vaults {
		if v.Instance == nil {
			continue
		}
		if e, ok := vaultManifestEntry(v.Instance, id); ok {
			return vid, e, true
		}
	}
	return glid.Nil, vaultctlfsm.ManifestEntry{}, false
}

// vaultCtlFSMs returns every vault-ctl chunk FSM this node participates in,
// keyed by vault ID. With symmetric seeding that is every
// vault in the cluster, whether or not this node hosts an instance for it.
// Nil when there is no GroupManager (single-node / memory mode).
func (o *Orchestrator) vaultCtlFSMs() map[glid.GLID]*vaultctlfsm.FSM {
	if o.groupMgr == nil {
		return nil
	}
	var out map[glid.GLID]*vaultctlfsm.FSM
	for _, gid := range o.groupMgr.Groups() {
		if !raftgroup.IsVaultControlPlaneGroupID(gid) {
			continue
		}
		g := o.groupMgr.GetGroup(gid)
		if g == nil {
			continue
		}
		vfsm, ok := g.FSM.(*vaultraft.FSM)
		if !ok || vfsm == nil {
			continue
		}
		for vid, f := range vfsm.Vaults() {
			if out == nil {
				out = make(map[glid.GLID]*vaultctlfsm.FSM)
			}
			out[vid] = f
		}
	}
	return out
}

// vaultCtlFSMForVault returns the vault-ctl chunk FSM for vaultID when this
// node has joined the vault control-plane Raft group.
func (o *Orchestrator) vaultCtlFSMForVault(vaultID glid.GLID) *vaultctlfsm.FSM {
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
	return vfsm.VaultFSM(vaultID)
}

func collectSealedEntries(vaultInst *VaultInstance) []vaultctlfsm.ManifestEntry {
	if vaultInst == nil {
		return nil
	}
	var out []vaultctlfsm.ManifestEntry
	for _, e := range vaultManifestEntries(vaultInst) {
		if e.IsSealed() {
			out = append(out, e)
		}
	}
	return out
}

// vaultManifestEntry returns the manifest entry for a chunk on this instance.
// Prefers the FSM callback (cluster-replicated truth) and falls back to
// projecting from the local chunk manager for memory-mode vaults.
func vaultManifestEntry(t *VaultInstance, id chunk.ChunkID) (vaultctlfsm.ManifestEntry, bool) {
	if t.ManifestEntry != nil {
		return t.ManifestEntry(id)
	}
	if t.Chunks == nil {
		return vaultctlfsm.ManifestEntry{}, false
	}
	meta, err := t.Chunks.Meta(id)
	if err != nil {
		return vaultctlfsm.ManifestEntry{}, false
	}
	return chunkMetaToManifestEntry(meta), true
}

// vaultManifestEntries returns every manifest entry on this instance.
// FSM-backed instances go through the callback; memory-mode vaults
// project from List().
func vaultManifestEntries(t *VaultInstance) []vaultctlfsm.ManifestEntry {
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
	out := make([]vaultctlfsm.ManifestEntry, len(metas))
	for i, m := range metas {
		out[i] = chunkMetaToManifestEntry(m)
	}
	return out
}

// IndexReader returns a manifest.IndexReader that resolves IngestTS rank /
// position lookups on the unified manifest read core: vault ownership
// comes from the replicated vault-ctl FSM (any voter), and
// the lookup is served from whatever ITSI bytes are locally materialized —
// the owning instance's chunk manager (active chunk B+ tree, cloud-backed
// cached index), its index manager (sealed local sidecar), a pipeline open
// chunk's built GLCB, or a sealed GLCB in the vault chunk root that no
// manager serves (yet). No remote read is fabricated: when the bytes are
// not local, the lookup reports unresolvable and callers fall back to the
// FSM-based estimate.
func (o *Orchestrator) IndexReader() manifest.IndexReader {
	return &orchestratorIndexReader{o: o}
}

// orchestratorIndexReader implements manifest.IndexReader by resolving the
// chunk's owning vault through the replicated manifest, then dispatching to
// that vault's local managers (with byte-local GLCB fallbacks) for the
// actual rank/pos lookup.
type orchestratorIndexReader struct {
	o *Orchestrator
}

var _ manifest.IndexReader = (*orchestratorIndexReader)(nil)

// FindIngestRank returns the rank of the first IngestTS-sorted entry with
// TS >= ts. Tries the chunk manager (active chunk B+ tree, cloud-backed chunk
// cached index) first, then the index manager (sealed local chunk sidecar),
// then a locally built pipeline GLCB, then a sealed GLCB in the vault chunk
// root, then the byte-free FSM-metadata boundary answer (sealed monotonic
// chunks, ts strictly before IngestStart → rank 0). Returns (0, false) when
// none of those serve the lookup.
func (r *orchestratorIndexReader) FindIngestRank(chunkID chunk.ChunkID, ts time.Time) (uint64, bool) {
	cm, im := r.lookupVaultManagers(chunkID)
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
	if rank, ok := r.o.PipelineFindIngestRank(chunkID, ts); ok {
		return rank, true
	}
	if rank, _, ok := r.o.chunkRootFindIngest(chunkID, ts); ok {
		return rank, true
	}
	if rank, ok := r.o.manifestBoundaryIngestRank(chunkID, ts); ok {
		return rank, true
	}
	return 0, false
}

// FindIngestPos returns the physical record position for the same query.
// Same dispatch shape as FindIngestRank.
func (r *orchestratorIndexReader) FindIngestPos(chunkID chunk.ChunkID, ts time.Time) (uint64, bool) {
	cm, im := r.lookupVaultManagers(chunkID)
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
	if _, pos, ok := r.o.chunkRootFindIngest(chunkID, ts); ok {
		return pos, true
	}
	// rank == pos on monotonic chunks, so the metadata boundary answer
	// serves position lookups too.
	if pos, ok := r.o.manifestBoundaryIngestRank(chunkID, ts); ok {
		return pos, true
	}
	return 0, false
}

// lookupVaultManagers resolves the (chunk, index) manager pair for the vault
// owning the given chunk. Ownership comes from the replicated manifest first
// (manifestEntryByChunk — resolvable on every voter, and correct even when
// the local chunk manager hasn't registered the chunk yet); open chunks and
// manifest misses fall back to probing local instances for the chunk.
// Returns (nil, nil) when this node hosts no managers for the owning vault —
// the caller then tries the byte-local GLCB fallbacks and finally reports
// the lookup unresolvable (the histogram's cue to fall back to FSM-based
// proportional distribution).
func (r *orchestratorIndexReader) lookupVaultManagers(chunkID chunk.ChunkID) (chunk.ChunkManager, index.IndexManager) {
	if vid, _, ok := r.o.manifestEntryByChunk(chunkID); ok {
		r.o.mu.RLock()
		v := r.o.vaults[vid]
		var cm chunk.ChunkManager
		var im index.IndexManager
		if v != nil && v.Instance != nil {
			cm, im = v.Instance.Chunks, v.Instance.Indexes
		}
		r.o.mu.RUnlock()
		if cm != nil || im != nil {
			return cm, im
		}
		// The manifest knows the chunk but this node hosts no instance
		// for its vault. Fall through to the local probe — bytes can
		// still be locally materialized under another instance while a
		// retention transfer is in flight.
	}
	r.o.mu.RLock()
	defer r.o.mu.RUnlock()
	for _, v := range r.o.vaults {
		t := v.Instance
		if t == nil || t.Chunks == nil {
			continue
		}
		if _, err := t.Chunks.Meta(chunkID); err == nil {
			return t.Chunks, t.Indexes
		}
	}
	return nil, nil
}

// manifestBoundaryIngestRank answers rank (and, on monotonic chunks,
// position) lookups that need no ITSI bytes at all, from the FSM-replicated
// index metadata. On a sealed monotonic chunk the first
// appended record carries the minimum IngestTS (IngestStart), so a timestamp
// strictly before IngestStart resolves to rank 0 — exactly the answer the
// chunk's own ITSI section would give — on ANY voter, bytes or not.
// Timestamps at or after IngestStart need bytes and stay unresolvable here
// (per-timestamp resolvability; consumers fall back to the FSM estimate).
// Timestamps past IngestEnd already report unresolvable in
// the byte-backed fallbacks, matching the ITSI "past all entries" answer.
// Non-monotonic chunks get no boundary answer: IngestStart is only the first
// APPENDED record's timestamp there, not the minimum.
func (o *Orchestrator) manifestBoundaryIngestRank(chunkID chunk.ChunkID, ts time.Time) (uint64, bool) {
	_, e, ok := o.manifestEntryByChunk(chunkID)
	if !ok || !e.IsSealed() || !e.IngestTSMonotonic || e.RecordCount <= 0 {
		return 0, false
	}
	if e.IngestStart.IsZero() || !ts.Before(e.IngestStart) {
		return 0, false
	}
	return 0, true
}

// chunkRootFindIngest serves an IngestTS index lookup straight from a locally
// materialized GLCB's ITSI section, addressed via the replicated manifest —
// no chunk or index manager involvement. Covers sealed pipeline chunks whose
// GLCB exists in this node's vault chunk root but is not (or not yet)
// registered with a manager. Nodes without local bytes report false; remote
// reads are never fabricated; the caller falls back to the FSM estimate.
func (o *Orchestrator) chunkRootFindIngest(chunkID chunk.ChunkID, ts time.Time) (rank, pos uint64, ok bool) {
	vid, e, found := o.manifestEntryByChunk(chunkID)
	if !found || !e.IsSealed() {
		return 0, 0, false
	}
	var hit bool
	err := o.withPipelineChunkIngestIndex(vid, chunkID, func(mv filetsidx.MmapView) error {
		r, p, found := mv.SearchTS(ts.UnixNano())
		rank, pos, hit = uint64(r), uint64(p), found
		return nil
	})
	if err != nil || !hit {
		return 0, 0, false
	}
	return rank, pos, true
}

// chunkMetaToManifestEntry projects a chunk.ChunkMeta into the FSM-shaped
// vaultctlfsm.ManifestEntry. Used only for memory-mode vaults, which have no
// FSM and no replication — the local chunk manager IS the source of truth
// there. RetentionPending / TransitionStreamed / IngestIdx*/SourceIdx*
// fields stay zero (memory-mode vaults don't track them).
func chunkMetaToManifestEntry(m chunk.ChunkMeta) vaultctlfsm.ManifestEntry {
	state := m.State
	if state == chunk.ChunkStateUnknown {
		// Memory-mode vaults don't carry FSM state. Derive from the
		// local Sealed bool — there's no Sealing intermediate without
		// an FSM driving the announce protocol.
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
		CloudBytes:  m.CloudBytes,
		IngestStart: m.IngestStart,
		IngestEnd:   m.IngestEnd,
		SourceStart: m.SourceStart,
		SourceEnd:   m.SourceEnd,
		CloudBacked: m.CloudBacked,
		Archived:    m.Archived,
	}
}
