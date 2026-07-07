package orchestrator

import (
	"os"
	"path/filepath"
	"time"

	"gastrolog/internal/chunk"
	chunkcloud "gastrolog/internal/chunk/cloud"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// externalGLCBResolverSetter is the chunk-manager seam for lazy on-miss
// external-GLCB resolution (registration as a cache, not a prerequisite).
type externalGLCBResolverSetter interface {
	SetExternalGLCBResolver(func(chunk.ChunkID) (string, chunk.ExternalGLCBInfo, bool))
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
// while entering the manager) plus one os.Stat.
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
		if _, err := os.Stat(glcbPath); err != nil {
			return "", chunk.ExternalGLCBInfo{}, false
		}
		// Metadata comes from the FSM entry alone — no blob header read
		// under the manager lock. Index offsets and bounds were committed
		// by CmdSealChunk; Sealing entries resolve with what the manifest
		// has (the register sweep's file overlay remains the enrichment
		// path for those).
		return glcbPath, externalGLCBInfoFromFSM(*e), true
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
		WriteStart:        e.WriteStart,
		WriteEnd:          e.WriteEnd,
		IngestStart:       e.IngestStart,
		IngestEnd:         e.IngestEnd,
		SourceStart:       e.SourceStart,
		SourceEnd:         e.SourceEnd,
		RecordCount:       e.RecordCount,
		Bytes:             e.Bytes,
		DiskBytes:         e.DiskBytes,
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
	toc, err := chunkcloud.ReadTOC(f, fileSize)
	if err != nil {
		return
	}
	if info.IngestIdxOffset == 0 {
		if ingest, ok := toc.Find(chunkcloud.SectionIngestTSIndex); ok {
			info.IngestIdxOffset = ingest.Offset
			info.IngestIdxSize = ingest.Size
		}
	}
	if info.SourceIdxOffset == 0 {
		if source, ok := toc.Find(chunkcloud.SectionSourceTSIndex); ok {
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
