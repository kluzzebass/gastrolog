package orchestrator

import (
	"os"
	"path/filepath"
	"time"

	"gastrolog/internal/chunk"
	chunkcloud "gastrolog/internal/chunk/cloud"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

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
