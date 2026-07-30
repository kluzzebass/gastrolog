package orchestrator

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/vaultraft"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// cloudBackedChunkResolverSetter is the chunk-manager seam for lazy on-miss
// cloud-backed chunk resolution — the cloud counterpart of
// externalGLCBResolverSetter. The resolver answers targeted by-ID lookups
// (Meta/OpenCursor); the lister answers enumeration (List), returning the
// cloud-backed chunk IDs the resolver would accept so List()-walking
// consumers surface the same chunks the by-ID resolver serves.
type cloudBackedChunkResolverSetter interface {
	SetCloudBackedChunkResolver(func(chunk.ChunkID) (chunk.CloudBackedChunkInfo, bool))
	SetCloudBackedChunkLister(func() []chunk.ChunkID)
}

// wireLazyCloudBackedResolver installs the on-miss cloud-backed resolver on a
// file vault's chunk manager. With it, a meta-lookup miss resolves against the
// replicated vault-ctl FSM manifest at lookup time: a chunk is servable the
// moment the FSM says CloudBacked, whether that fact arrived via live
// CmdUploadChunk replication or a wholesale snapshot install (which fires no
// per-apply effects). This is the single fill path: the cloud index is a
// cache of the manifest's CloudBacked entries, filled from the owner on miss.
//
// Resolution registers metadata only — the blob key is derived by the
// manager's blobKey() at read time, and no bytes are fetched until a cursor
// actually reads (the histogram no-fetch policy holds).
//
// The resolver runs under the chunk manager's mutex, so it must never take
// orchestrator locks (o.mu holders call into the manager — ABBA). Its lock
// footprint is exactly the pipeline resolver's proven chain: vaultCtlHandle
// (o.mu-free) plus the FSM's internal read lock (apply effects fire outside
// FSM locks, so no path holds those locks while entering the manager). No
// file or network I/O.
//
// Skipped when the vault has no vault-ctl group (single-node/memory mode —
// the local manager is already authoritative) or the chunk manager does not
// take a resolver. A manager without a cloud store ignores the installed
// resolver (nil cloudIdx → the miss stays a miss).
func (o *Orchestrator) wireLazyCloudBackedResolver(g *raftgroup.Group, vaultID glid.GLID, cm chunk.ChunkManager) {
	if g == nil || cm == nil {
		return
	}
	setter, ok := cm.(cloudBackedChunkResolverSetter)
	if !ok {
		return
	}
	var fallback *vaultctlfsm.FSM
	switch raw := g.FSM.(type) {
	case *vaultctlfsm.FSM:
		fallback = raw
	case *vaultraft.FSM:
		fallback = raw.EnsureVaultFSM(vaultID)
	default:
		return
	}
	lookupFSM := func() *vaultctlfsm.FSM {
		if f, _, _, ok := o.vaultCtlHandle(vaultID); ok && f != nil {
			return f
		}
		return fallback // pre-restore fallback; a ctl restore swaps the live FSM
	}
	setter.SetCloudBackedChunkResolver(func(id chunk.ChunkID) (chunk.CloudBackedChunkInfo, bool) {
		f := lookupFSM()
		if f == nil {
			return chunk.CloudBackedChunkInfo{}, false
		}
		e := f.Get(id)
		if e == nil || !e.CloudBacked {
			return chunk.CloudBackedChunkInfo{}, false
		}
		return cloudBackedInfoFromEntry(*e), true
	})
	setter.SetCloudBackedChunkLister(func() []chunk.ChunkID {
		f := lookupFSM()
		if f == nil {
			return nil
		}
		entries := f.List()
		ids := make([]chunk.ChunkID, 0, len(entries))
		for i := range entries {
			if entries[i].CloudBacked {
				ids = append(ids, entries[i].ID)
			}
		}
		return ids
	})
}

// cloudBackedInfoFromEntry maps a replicated manifest entry's cloud-relevant
// fields onto the chunk manager's registration payload.
func cloudBackedInfoFromEntry(e vaultctlfsm.ManifestEntry) chunk.CloudBackedChunkInfo {
	return chunk.CloudBackedChunkInfo{
		WriteStart:        e.WriteStart,
		WriteEnd:          e.WriteEnd,
		IngestStart:       e.IngestStart,
		IngestEnd:         e.IngestEnd,
		SourceStart:       e.SourceStart,
		SourceEnd:         e.SourceEnd,
		RecordCount:       e.RecordCount,
		Bytes:             e.Bytes,
		CloudBytes:        e.CloudBytes,
		IngestIdxOffset:   e.IngestIdxOffset,
		IngestIdxSize:     e.IngestIdxSize,
		SourceIdxOffset:   e.SourceIdxOffset,
		SourceIdxSize:     e.SourceIdxSize,
		IngestTSMonotonic: e.IngestTSMonotonic,
	}
}
