package chunking

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// BuildResultFromExistingGLCB reads seal metadata from a pipeline GLCB that
// already exists on disk: header and TOC footer only — IngestTSMonotonic is
// persisted in the blob layout meta at build time, so no record frame is ever
// touched. Used on the hot build path and after restart when local
// materialization finished but CmdSealChunk did not apply before shutdown;
// BlobDigest is the footer's, so a seal proposed from it records the digest
// the blob was written with.
func BuildResultFromExistingGLCB(glcbPath string, sealedAt time.Time) (BuildResult, error) {
	meta, digest, fileBytes, err := readGLCBSealMeta(glcbPath)
	if err != nil {
		return BuildResult{}, err
	}
	return buildResultFromSealMeta(glcbPath, meta, digest, fileBytes, sealedAt), nil
}

func buildResultFromSealMeta(glcbPath string, meta glcb.BlobMeta, digest [32]byte, fileBytes int64, sealedAt time.Time) BuildResult {
	writeEnd := sealedAt
	if writeEnd.IsZero() {
		writeEnd = meta.WriteEnd
	}
	return BuildResult{
		GLCBPath:          glcbPath,
		BlobDigest:        digest,
		RecordCount:       meta.RecordCount,
		Bytes:             fileBytes,
		WriteEnd:          writeEnd,
		IngestStart:       meta.IngestStart,
		IngestEnd:         meta.IngestEnd,
		SourceEnd:         meta.SourceEnd,
		IngestTSMonotonic: meta.IngestTSMonotonic,
	}
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
	// Sweep BuildGLCBFile's crash-orphaned staging files before anything
	// else runs. BuildGLCBFile relies on this happening on the recovery
	// path it documents ("re-running with the same inputs is safe"):
	// without it a crash between CreateTemp and the rename leaves
	// ".glcb.tmp.*" in the chunk dir forever. Best-effort: a failed sweep
	// must not block the rest of recovery, which is what actually gets the
	// chunk sealed.
	//
	// Under buildMu: RecoverOnce runs on the vault-registration catch-up
	// goroutine concurrently with the wake-driven worker's build pass, and
	// an unserialized sweep deletes the ".glcb.tmp.*" a live BuildGLCBFile
	// is about to rename (observed: "BuildOnce: rename ... no such file or
	// directory"). With buildMu held no build is mid-staging, so anything
	// matching the tmp pattern is genuinely orphaned. pruneCorruptGLCBs
	// shares the lock for the same reason: corruptGLCBs is written from
	// the build pass under buildMu.
	v.buildMu.Lock()
	v.sweepOrphanGLCBBuildTmp()
	v.pruneCorruptGLCBs()
	v.buildMu.Unlock()
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
		if e.ID == pendingID {
			continue
		}
		if e.IsSealed() {
			// Sealed cluster-wide: nothing to build or propose, and
			// registration is lazy — the chunk manager's on-miss resolver
			// serves the on-disk GLCB at first lookup. Skipping BEFORE
			// the stat keeps recovery O(unsealed), not O(all chunks),
			// which is what keeps startup sub-3s per the vision's
			// 30k-chunk registry target.
			continue
		}
		// Only a seal-queue manifest's record set is final. The open
		// manifest still grows, and after a restart this FSM can lag what
		// this node built before it stopped, so a GLCB is judged once its
		// chunk reaches the seal queue — by the build pass if not here.
		queued := v.fsm().SealQueueManifest(e.ID)
		if queued == nil {
			continue
		}
		if _, err := os.Stat(ChunkGLCBPath(v.cfg.ChunkRoot, e.ID)); err != nil {
			continue
		}
		if err := v.recoverBuiltGLCB(ctx, queued); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// isGLCBBuildTmpName reports whether name matches BuildGLCBFile's exact
// os.CreateTemp naming contract (glcbBuildTmpPrefix + random suffix). Kept
// as a standalone predicate so a writer-sweeper contract test can call it
// directly against BuildGLCBFile's actual produced name rather than a
// hand-typed pattern guess.
func isGLCBBuildTmpName(name string) bool {
	return strings.HasPrefix(name, glcbBuildTmpPrefix)
}

// sweepOrphanGLCBBuildTmp removes BuildGLCBFile staging files
// (glcbBuildTmpPrefix*) left behind by a crash between os.CreateTemp and
// the rename commit. Each chunk lives in its own <ChunkRoot>/<chunkID>/
// directory (ChunkGLCBPath), so this only needs one level of listing.
// Best-effort and owner-local: this package is the only writer of this
// tmp shape, so it is the only package responsible for sweeping it (no
// global janitor).
func (v *vaultChunking) sweepOrphanGLCBBuildTmp() {
	entries, err := os.ReadDir(v.cfg.ChunkRoot)
	if err != nil {
		return
	}
	for _, chunkDirEntry := range entries {
		if !chunkDirEntry.IsDir() {
			continue
		}
		chunkDir := filepath.Join(v.cfg.ChunkRoot, chunkDirEntry.Name())
		files, err := os.ReadDir(chunkDir)
		if err != nil {
			continue
		}
		for _, f := range files {
			if f.IsDir() || !isGLCBBuildTmpName(f.Name()) {
				continue
			}
			path := filepath.Join(chunkDir, f.Name())
			if err := os.Remove(path); err != nil {
				v.logger().Warn("failed to remove orphan GLCB build temp file", "path", path, "error", err)
			} else {
				v.logger().Info("removed orphan GLCB build temp file", "path", path)
			}
		}
	}
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
	sealedAt := pending.SealedAt
	if sealedAt.IsZero() && entry != nil && !entry.WriteEnd.IsZero() {
		sealedAt = entry.WriteEnd
	}
	// Under buildMu: a quarantine decided from a read that raced a live
	// build would rename that build's GLCB aside.
	v.buildMu.Lock()
	result, adopted, err := v.existingGLCBBuildOf(pending, sealedAt)
	v.buildMu.Unlock()
	if err != nil || !adopted {
		// Missing, or quarantined as corrupt or mismatched: nothing to
		// recover here. The build pass builds the seal-queue head from its
		// segments, and the GLCB catch-up pull supplies a sealed chunk whose
		// segments are released.
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
