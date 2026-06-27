package query

import (
	"errors"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/manifest"
)

// pipelineOpenChunk reports whether meta is a pipeline active/sealing chunk
// owned by the vault-ctl open-chunk manifest rather than VaultInstance.Chunks
// active head (m.active). Pipeline ingest never appends to m.active.
func pipelineOpenChunk(meta chunk.ChunkMeta, cm chunk.ChunkManager) bool {
	if meta.Sealed {
		return false
	}
	if meta.State != chunk.ChunkStateActive && meta.State != chunk.ChunkStateSealing {
		return false
	}
	if cm == nil {
		return true
	}
	if active := cm.Active(); active != nil && active.ID == meta.ID {
		return false
	}
	return true
}

// scanActiveChunkIngestTS walks an open chunk's records by IngestTS. Pipeline
// open/sealing chunks use the manifest segment path; file/memory vaults use
// the chunk manager's in-memory B+ tree on m.active.
func (e *Engine) scanActiveChunkIngestTS(vaultID glid.GLID, meta chunk.ChunkMeta, cb func(tsNanos int64) bool) {
	cm, _ := e.getVaultManagers(vaultID)
	if pipelineOpenChunk(meta, cm) {
		if e.registry != nil {
			if scanner, ok := e.registry.(manifest.PipelineIngestScanner); ok {
				_ = scanner.ScanPipelineChunkIngestTS(vaultID, meta.ID, cb)
			}
		}
		return
	}
	if cm != nil {
		_ = cm.ScanActiveIngestTS(meta.ID, cb)
	}
}

func scanPipelineChunkByIngestTS(
	registry manifest.VaultRegistry,
	vaultID glid.GLID,
	chunkID chunk.ChunkID,
	cb func(ingestTS time.Time, attrs chunk.Attributes) bool,
) {
	if registry == nil {
		return
	}
	opener, ok := registry.(manifest.PipelineChunkOpener)
	if !ok {
		return
	}
	cursor, err := opener.OpenPipelineChunkCursor(vaultID, chunkID)
	if err != nil {
		return
	}
	defer func() { _ = cursor.Close() }()
	for {
		rec, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			return
		}
		if err != nil {
			return
		}
		if !cb(rec.IngestTS, rec.Attrs) {
			return
		}
	}
}

// scanActiveChunkByIngestTS walks open-chunk records exposing IngestTS and attrs.
func (e *Engine) scanActiveChunkByIngestTS(vaultID glid.GLID, meta chunk.ChunkMeta, cb func(ingestTS time.Time, attrs chunk.Attributes) bool) {
	cm, _ := e.getVaultManagers(vaultID)
	if pipelineOpenChunk(meta, cm) {
		scanPipelineChunkByIngestTS(e.registry, vaultID, meta.ID, cb)
		return
	}
	if cm != nil {
		_ = cm.ScanActiveByIngestTS(meta.ID, cb)
	}
}
