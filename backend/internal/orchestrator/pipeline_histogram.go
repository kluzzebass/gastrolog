package orchestrator

import (
	"errors"
	"os"
	"time"

	"gastrolog/internal/chunk"
	chunkcloud "gastrolog/internal/chunk/cloud"
	"gastrolog/internal/glid"
	filetsidx "gastrolog/internal/index/file/tsidx"
	"gastrolog/internal/pipeline/chunking"
)

// findPipelineOpenChunk returns the vault and FSM state for a pipeline active
// or sealing chunk in the vault-ctl open-chunk manifest.
func (o *Orchestrator) findPipelineOpenChunk(chunkID chunk.ChunkID) (vaultID glid.GLID, state chunk.ChunkState, ok bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	for vid := range o.vaults {
		for _, e := range o.VaultManifestEntriesFromCtlFSM(vid) {
			if e.ID != chunkID {
				continue
			}
			if e.State != chunk.ChunkStateActive && e.State != chunk.ChunkStateSealing {
				continue
			}
			return vid, e.State, true
		}
	}
	return glid.Nil, 0, false
}

func (o *Orchestrator) withPipelineChunkIngestIndex(vaultID glid.GLID, chunkID chunk.ChunkID, fn func(filetsidx.MmapView) error) error {
	root, ok := o.pipelineVaultChunkRoot(vaultID)
	if !ok {
		return os.ErrNotExist
	}
	glcbPath := chunking.ChunkGLCBPath(root, chunkID)
	if _, err := os.Stat(glcbPath); err != nil {
		return err
	}
	blob, err := chunkcloud.OpenMappedBlob(glcbPath)
	if err != nil {
		return err
	}
	defer func() { _ = blob.Close() }()
	blob.Retain()
	defer blob.Release()

	section, ok := blob.Section(chunkcloud.SectionIngestTSIndex)
	if !ok || len(section) == 0 {
		return os.ErrNotExist
	}
	mv, err := filetsidx.ViewFromSection(section)
	if err != nil {
		return err
	}
	return fn(mv)
}

// PipelineFindIngestRank resolves IngestTS rank for a pipeline active/sealing
// chunk via its built data.glcb ITSI section.
func (o *Orchestrator) PipelineFindIngestRank(chunkID chunk.ChunkID, ts time.Time) (uint64, bool) {
	vaultID, _, ok := o.findPipelineOpenChunk(chunkID)
	if !ok {
		return 0, false
	}
	var rank uint64
	var found bool
	err := o.withPipelineChunkIngestIndex(vaultID, chunkID, func(mv filetsidx.MmapView) error {
		r, _, ok := mv.SearchTS(ts.UnixNano())
		rank, found = uint64(r), ok
		return nil
	})
	if err != nil {
		return 0, false
	}
	return rank, found
}

// ScanPipelineChunkIngestTS iterates every record in a pipeline active/sealing
// chunk, calling cb with each record's IngestTS in physical read order.
func (o *Orchestrator) ScanPipelineChunkIngestTS(vaultID glid.GLID, chunkID chunk.ChunkID, cb func(tsNanos int64) bool) error {
	cursor, err := o.OpenPipelineChunkCursor(vaultID, chunkID)
	if err != nil {
		return err
	}
	defer func() { _ = cursor.Close() }()
	for {
		rec, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			return nil
		}
		if err != nil {
			return err
		}
		if !cb(rec.IngestTS.UnixNano()) {
			return nil
		}
	}
}
