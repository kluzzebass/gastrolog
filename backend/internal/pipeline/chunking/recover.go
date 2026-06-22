package chunking

import (
	"context"
	"fmt"
	"os"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// BuildResultFromExistingGLCB reads seal metadata from a pipeline GLCB that
// already exists on disk. Used after restart when local materialization
// finished but CmdSealChunk did not apply before shutdown.
func BuildResultFromExistingGLCB(glcbPath string, sealedAt time.Time) (BuildResult, error) {
	meta, monotonic, fileBytes, err := readGLCBSealMeta(glcbPath)
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
		IngestTSMonotonic: monotonic,
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
	if pending := v.cfg.FSM.SealedManifest(); pending != nil {
		if err := v.recoverBuiltGLCB(ctx, pending); err != nil {
			lastErr = err
		}
	}
	pendingID := chunk.ChunkID{}
	if pending := v.cfg.FSM.SealedManifest(); pending != nil {
		pendingID = pending.ChunkID
	}
	for _, e := range v.cfg.FSM.List() {
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
	entry := v.cfg.FSM.Get(pending.ChunkID)
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
	v.mu.Lock()
	if v.doneSealProposed == key {
		v.mu.Unlock()
		return nil
	}
	v.mu.Unlock()

	v.mu.Lock()
	v.doneBuild = key
	v.lastBuild = struct {
		key    buildKey
		result BuildResult
		ok     bool
	}{key: key, result: result, ok: true}
	alreadyProposed := v.doneSealProposed == key
	v.mu.Unlock()

	if v.cfg.Applier == nil || alreadyProposed {
		return nil
	}
	if !v.cfg.IsLeader() {
		v.mu.Lock()
		v.doneSealProposed = key
		v.mu.Unlock()
		return nil
	}
	v.mu.Lock()
	v.sealAttemptKey = key
	v.lastSealAttempt = time.Now()
	v.mu.Unlock()
	if err := v.cfg.Applier.Apply(vaultctlfsm.MarshalSealChunk(
		pending.ChunkID,
		result.WriteEnd,
		int64(result.RecordCount),
		result.Bytes,
		result.IngestStart,
		result.IngestEnd,
		result.SourceEnd,
		result.IngestTSMonotonic,
	)); err != nil {
		return err
	}
	if !v.chunkSealCommitted(pending.ChunkID) {
		return fmt.Errorf("chunking: CmdSealChunk did not commit seal for %s", pending.ChunkID)
	}
	v.mu.Lock()
	v.doneSealProposed = key
	v.mu.Unlock()
	v.afterSealBuild(pending)

	if v.cfg.OnBuilt != nil {
		v.mu.Lock()
		fire := v.doneOnBuilt != key
		if fire {
			v.doneOnBuilt = key
		}
		v.mu.Unlock()
		if fire {
			v.cfg.OnBuilt(pending.ChunkID)
		}
	}
	return nil
}
