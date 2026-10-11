package chunking

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"

	"gastrolog/internal/chunk"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// Every home builds every sealed chunk. The vault-ctl FSM keeps a sealed
// chunk's manifest after CmdSealChunk commits, so a home that had not built
// the chunk when the leader sealed it still has the refs to build it from.
// What this home has built is what is in its chunk root.

// sealedChunkNeedsLocalBuild reports whether a chunk whose seal committed is
// still to be built on this home: Sealed, not cloud-backed, without a GLCB in
// this home's chunk root, and not one whose build here diverged from the
// sealed digest — that chunk's agreed copy is pulled from a peer instead.
func (v *vaultChunking) sealedChunkNeedsLocalBuild(id chunk.ChunkID) bool {
	entry := v.fsm().Get(id)
	if entry == nil || entry.State != chunk.ChunkStateSealed || entry.CloudBacked {
		return false
	}
	if v.glcbDivergent(id) {
		return false
	}
	_, err := os.Stat(ChunkGLCBPath(v.cfg.ChunkRoot, id))
	return errors.Is(err, fs.ErrNotExist)
}

func (v *vaultChunking) sealedChunkBuildDue() bool {
	return slices.ContainsFunc(v.fsm().SealedChunkManifestIDs(), v.sealedChunkNeedsLocalBuild)
}

// buildSealedChunksMissingLocally builds, in seal order, every sealed chunk
// this home lacks. A chunk whose segments collection cannot deliver yet is
// skipped and retried on a later pass; once one of its segments is released
// the FSM drops its manifest and the GLCB catch-up pull supplies the chunk.
func (v *vaultChunking) buildSealedChunksMissingLocally(ctx context.Context) error {
	var errs []error
	for _, id := range v.fsm().SealedChunkManifestIDs() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !v.sealedChunkNeedsLocalBuild(id) {
			continue
		}
		sealed := v.fsm().SealedChunkManifest(id)
		if sealed == nil {
			continue
		}
		if err := v.buildSealedChunk(ctx, sealed); err != nil {
			errs = append(errs, fmt.Errorf("build sealed chunk %s: %w", id, err))
		}
	}
	return errors.Join(errs...)
}

func (v *vaultChunking) buildSealedChunk(ctx context.Context, sealed *vaultctlfsm.OpenChunkManifest) error {
	manifest := sealedManifestFromFSM(sealed)
	if !v.collectManifestSegments(ctx, manifest) {
		return nil
	}
	result, err := BuildSealedChunk(v.buildInput(manifest))
	if err != nil {
		return err
	}
	v.clearCorruptGLCB(sealed.ChunkID, "rebuilt from source segments")
	key := buildKey{chunkID: sealed.ChunkID, sealedAt: sealed.SealedAt}
	v.progress.markBuilt(key, result)
	v.sealedRecords.Add(uint64(result.RecordCount))
	v.sealedBytes.Add(uint64(result.Bytes)) //nolint:gosec // sizes are non-negative
	v.chunksBuilt.Add(1)
	v.afterSealBuild(ctx, sealed)
	v.fireOnBuiltOnce(sealed, key, true)
	return nil
}

// collectManifestSegments reports whether every segment the manifest
// references is on this home, asking collection for the missing ones first.
func (v *vaultChunking) collectManifestSegments(ctx context.Context, manifest SealedManifest) bool {
	missing := missingManifestSegmentIDs(manifest, v.cfg.Locate)
	if len(missing) == 0 {
		return true
	}
	if v.cfg.Collector == nil {
		return false
	}
	if err := v.cfg.Collector.CollectSegments(ctx, missing); err != nil {
		v.logger().Debug("collecting segments for a sealed chunk failed; retrying on a later build pass",
			"chunk", manifest.ChunkID, "missing", len(missing), "error", err)
		return false
	}
	return len(missingManifestSegmentIDs(manifest, v.cfg.Locate)) == 0
}
