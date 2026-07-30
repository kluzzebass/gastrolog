package orchestrator

import (
	"os"
	"path/filepath"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// externalGLCBResolverSetter is the chunk-manager seam for lazy on-miss
// external-GLCB resolution (registration as a cache, not a prerequisite).
//
// The resolver answers targeted by-ID lookups (Meta/OpenCursor). The lister
// answers ENUMERATION (List): it returns the sealed chunk IDs the resolver
// would accept, so a match-all search or holder-scope gate — which enumerate
// the manager rather than looking a chunk up by ID — discovers sealed chunks
// that live only as an FSM entry plus an on-disk GLCB. Without the lister,
// lazy resolution serves a chunk you already name but hides it from
// enumeration, so a restarted home answers a match-all with zero records
// until some other path happens to register the chunk.
type externalGLCBResolverSetter interface {
	SetExternalGLCBResolver(func(chunk.ChunkID) (string, chunk.ExternalGLCBInfo, bool))
	SetExternalGLCBLister(func() []chunk.ChunkID)
}

// installLazyGLCBResolver installs (or, when disabled, clears) the on-miss
// external-GLCB resolver on a pipeline vault's chunk manager. With it, a
// meta-lookup miss resolves against the vault-ctl manifest and the on-disk
// GLCB at lookup time: a chunk is servable the moment its FSM entry and
// file both exist, regardless of process history, sweep timing, or boot
// ordering — no boot-eager registration scan, no warm-up window.
//
// chunkRoot is captured BY VALUE. The resolver runs under the chunk
// manager's mutex, so it must never take orchestrator locks (o.mu holders
// call into the manager — ABBA). Its lock footprint is exactly:
// groupMgr/FSM internal locks (via vaultCtlHandle, which is o.mu-free;
// FSM apply effects fire outside FSM locks, so no path holds those locks
// while entering the manager) plus a one-time on-disk GLCB read
// (externalGLCBInfoForPipeline): pure os.* file I/O that acquires no shared
// lock, so no inversion with m.mu. The manager memoizes the resolved info
// into m.metas, so the read happens ONCE per chunk on the first miss.
//
// Caller holds o.mu (the pipeline reload path).
func (o *Orchestrator) installLazyGLCBResolver(vaultID glid.GLID, enabled bool, fsm *vaultctlfsm.FSM, chunkRoot string) {
	vault := o.vaults[vaultID]
	if vault == nil || vault.Instance == nil {
		return
	}
	o.installLazyGLCBResolverOn(vault.Instance, vaultID, enabled, fsm, chunkRoot)
}

// installLazyGLCBResolverOn is the instance-scoped core of
// installLazyGLCBResolver (separated so fixtures can wire a resolver
// without a fully-populated vault registry).
func (o *Orchestrator) installLazyGLCBResolverOn(inst *VaultInstance, vaultID glid.GLID, enabled bool, fsm *vaultctlfsm.FSM, chunkRoot string) {
	if inst == nil || inst.Chunks == nil {
		return
	}
	setter, ok := inst.Chunks.(externalGLCBResolverSetter)
	if !ok {
		return
	}
	if !enabled || chunkRoot == "" {
		setter.SetExternalGLCBResolver(nil)
		setter.SetExternalGLCBLister(nil)
		return
	}
	lookupFSM := func() *vaultctlfsm.FSM {
		if f, _, _, ok := o.vaultCtlHandle(vaultID); ok && f != nil {
			return f
		}
		return fsm // pre-restore fallback; a ctl restore swaps the live FSM
	}
	setter.SetExternalGLCBResolver(func(id chunk.ChunkID) (string, chunk.ExternalGLCBInfo, bool) {
		f := lookupFSM()
		if f == nil {
			return "", chunk.ExternalGLCBInfo{}, false
		}
		e := f.Get(id)
		if e == nil || (!e.IsSealed() && e.State != chunk.ChunkStateSealing) {
			return "", chunk.ExternalGLCBInfo{}, false
		}
		glcbPath := chunking.ChunkGLCBPath(chunkRoot, id)
		// File-enriched info: the memoized registration must be COMPLETE at
		// first resolution — there is no eager re-registration pass to upgrade
		// it afterward. For a Sealed entry the FSM already carries index
		// offsets and bounds, so the overlay only opens+stats the file and
		// returns immediately (cheap). For a Sealing entry — built on disk but
		// before CmdSealChunk committed its TS index offsets — the overlay
		// reads them (and any missing bounds/counts) from the on-disk blob's
		// TOC, so a query landing in that window still memoizes a servable,
		// index-complete meta. A missing/unreadable file means the bytes are
		// not on this node: fall through to other holders.
		info, err := externalGLCBInfoForPipeline(*e, glcbPath)
		if err != nil {
			return "", chunk.ExternalGLCBInfo{}, false
		}
		return glcbPath, info, true
	})
	// Enumeration companion to the resolver: the sealed chunk IDs of this
	// vault's committed manifest. List() calls the resolver on each ID it
	// does not already hold, so enumeration surfaces exactly the chunks the
	// by-ID resolver would serve — no separate registration sweep. Only
	// sealed entries participate; Sealing/Active chunks are served through
	// the pipeline manifest-cursor path, not manager enumeration.
	setter.SetExternalGLCBLister(func() []chunk.ChunkID {
		f := lookupFSM()
		if f == nil {
			return nil
		}
		entries := f.List()
		ids := make([]chunk.ChunkID, 0, len(entries))
		for i := range entries {
			if entries[i].IsSealed() {
				ids = append(ids, entries[i].ID)
			}
		}
		return ids
	})
}

// externalGLCBInfoForPipeline builds chunk-manager registration metadata for a
// pipeline GLCB. FSM fields are used when present; index section locations and
// missing bounds are read from the on-disk blob (needed while vault-ctl still
// shows Sealing before CmdSealChunk commits index offsets).
func externalGLCBInfoForPipeline(e vaultctlfsm.ManifestEntry, glcbPath string) (chunk.ExternalGLCBInfo, error) {
	info := externalGLCBInfoFromFSM(e)
	if err := overlayPipelineGLCBFromFile(&info, glcbPath, e.WriteEnd); err != nil {
		return info, err
	}
	return info, nil
}

func externalGLCBInfoFromFSM(e vaultctlfsm.ManifestEntry) chunk.ExternalGLCBInfo {
	return chunk.ExternalGLCBInfo{
		WriteStart:  e.WriteStart,
		WriteEnd:    e.WriteEnd,
		IngestStart: e.IngestStart,
		IngestEnd:   e.IngestEnd,
		SourceStart: e.SourceStart,
		SourceEnd:   e.SourceEnd,
		RecordCount: e.RecordCount,
		Bytes:       e.Bytes,
		// No DiskBytes seed here: ManifestEntry carries no per-node local
		// footprint. overlayPipelineGLCBFromBuild's info.DiskBytes==0
		// fallback below recomputes it from the actual local build
		// result — but only when it runs at all:
		// overlayPipelineGLCBFromBuild early-returns once the FSM entry
		// already has RecordCount and both IngestStart/IngestEnd (the
		// steady-state case, once CmdSealChunk has fully committed), so
		// DiskBytes stays 0 there. chunk.DiskClaim's Bytes+index-size
		// fallback covers sizing whenever DiskBytes lands at 0.
		IngestIdxOffset:   e.IngestIdxOffset,
		IngestIdxSize:     e.IngestIdxSize,
		SourceIdxOffset:   e.SourceIdxOffset,
		SourceIdxSize:     e.SourceIdxSize,
		IngestTSMonotonic: e.IngestTSMonotonic,
	}
}

func overlayPipelineGLCBFromFile(info *chunk.ExternalGLCBInfo, glcbPath string, writeEnd time.Time) error {
	f, err := os.Open(filepath.Clean(glcbPath))
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	stat, err := f.Stat()
	if err != nil {
		return err
	}
	overlayPipelineGLCBIndexFromTOC(info, f, stat.Size())
	overlayPipelineGLCBFromBuild(info, glcbPath, writeEnd)
	return nil
}

func overlayPipelineGLCBIndexFromTOC(info *chunk.ExternalGLCBInfo, f *os.File, fileSize int64) {
	if info.IngestIdxOffset != 0 && info.SourceIdxOffset != 0 {
		return
	}
	toc, err := glcb.ReadTOC(f, fileSize)
	if err != nil {
		return
	}
	if info.IngestIdxOffset == 0 {
		if ingest, ok := toc.Find(glcb.SectionIngestTSIndex); ok {
			info.IngestIdxOffset = ingest.Offset
			info.IngestIdxSize = ingest.Size
		}
	}
	if info.SourceIdxOffset == 0 {
		if source, ok := toc.Find(glcb.SectionSourceTSIndex); ok {
			info.SourceIdxOffset = source.Offset
			info.SourceIdxSize = source.Size
		}
	}
}

func overlayPipelineGLCBFromBuild(info *chunk.ExternalGLCBInfo, glcbPath string, writeEnd time.Time) {
	if info.RecordCount != 0 && !info.IngestStart.IsZero() && !info.IngestEnd.IsZero() {
		return
	}
	build, err := chunking.BuildResultFromExistingGLCB(glcbPath, writeEnd)
	if err != nil {
		return
	}
	if info.RecordCount == 0 {
		info.RecordCount = int64(build.RecordCount)
	}
	if info.Bytes == 0 {
		info.Bytes = build.Bytes
	}
	if info.DiskBytes == 0 {
		info.DiskBytes = build.Bytes
	}
	if info.WriteEnd.IsZero() {
		info.WriteEnd = build.WriteEnd
	}
	if info.IngestStart.IsZero() {
		info.IngestStart = build.IngestStart
	}
	if info.IngestEnd.IsZero() {
		info.IngestEnd = build.IngestEnd
	}
	if info.SourceEnd.IsZero() {
		info.SourceEnd = build.SourceEnd
	}
	if !build.IngestTSMonotonic {
		info.IngestTSMonotonic = false
	}
}
