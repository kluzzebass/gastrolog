package chunking

import (
	"context"
	"os"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// BuildResultFromExistingGLCB reads seal metadata from a pipeline GLCB that
// already exists on disk. Header-only: IngestTSMonotonic is persisted in the
// blob layout meta at build time (gastrolog-699s7p), so no record frame is
// ever touched. Used on the hot build path and after restart when local
// materialization finished but CmdSealChunk did not apply before shutdown.
func BuildResultFromExistingGLCB(glcbPath string, sealedAt time.Time) (BuildResult, error) {
	meta, fileBytes, err := readGLCBSealMeta(glcbPath)
	if err != nil {
		return BuildResult{}, err
	}
	writeEnd := sealedAt
	if writeEnd.IsZero() {
		writeEnd = meta.WriteEnd
	}
	return BuildResult{
		GLCBPath:          glcbPath,
		RecordCount:       meta.RecordCount,
		Bytes:             fileBytes,
		WriteEnd:          writeEnd,
		IngestStart:       meta.IngestStart,
		IngestEnd:         meta.IngestEnd,
		SourceEnd:         meta.SourceEnd,
		IngestTSMonotonic: meta.IngestTSMonotonic,
	}, nil
}

// RecoverOnce seals any pipeline chunks on this home whose local GLCB is
// present but the vault-ctl FSM still shows Active or Sealing. Idempotent.
func (m *Manager) RecoverOnce(ctx context.Context, vaultID glid.GLID) error {
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if !ok {
		return ErrUnknownVault
	}
	return v.recoverOnce(ctx)
}

func (v *vaultChunking) recoverOnce(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var lastErr error
	if pending := v.fsm().SealedManifest(); pending != nil {
		if len(pending.Refs) == 0 && pending.TotalRecords == 0 {
			if err := v.discardEmptySealedManifest(pending); err != nil {
				lastErr = err
			}
		} else if err := v.recoverBuiltGLCB(ctx, pending); err != nil {
			lastErr = err
		}
	}
	pendingID := chunk.ChunkID{}
	if pending := v.fsm().SealedManifest(); pending != nil {
		pendingID = pending.ChunkID
	}
	for _, e := range v.fsm().List() {
		if e.IsSealed() || e.ID == pendingID {
			continue
		}
		glcbPath := ChunkGLCBPath(v.cfg.ChunkRoot, e.ID)
		if _, err := os.Stat(glcbPath); err != nil {
			continue
		}
		manifest := &vaultctlfsm.OpenChunkManifest{ChunkID: e.ID}
		if err := v.recoverBuiltGLCB(ctx, manifest); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (v *vaultChunking) recoverBuiltGLCB(ctx context.Context, pending *vaultctlfsm.OpenChunkManifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if pending == nil || pending.ChunkID == chunk.ChunkID(glid.Nil) {
		return nil
	}
	entry := v.fsm().Get(pending.ChunkID)
	if entry != nil && entry.IsSealed() {
		return nil
	}
	glcbPath := ChunkGLCBPath(v.cfg.ChunkRoot, pending.ChunkID)
	if _, err := os.Stat(glcbPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	sealedAt := pending.SealedAt
	if sealedAt.IsZero() && entry != nil && !entry.WriteEnd.IsZero() {
		sealedAt = entry.WriteEnd
	}
	result, err := BuildResultFromExistingGLCB(glcbPath, sealedAt)
	if err != nil {
		return err
	}
	key := buildKey{chunkID: pending.ChunkID, sealedAt: sealedAt}
	if key.sealedAt.IsZero() {
		key.sealedAt = result.WriteEnd
	}
	v.progress.markBuilt(key, result)
	applied, err := v.proposeSealOnce(ctx, pending, key, result)
	if err != nil {
		return err
	}
	if applied {
		// Restart lost the in-memory GLCB registration; fire OnBuilt so the
		// recovered chunk is queryable on this home.
		v.fireOnBuiltOnce(pending, key, true)
	}
	return nil
}
