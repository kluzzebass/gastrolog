package chunking

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// A GLCB already on a chunk's canonical path when the build pass reaches the
// chunk, or when restart recovery finds it, is adopted — its digest becomes
// the one the seal records — only when it is a build of the chunk's manifest:
// it names the chunk, holds the manifest's record count, spans the manifest's
// record timestamps, and, for a chunk already sealed, carries the sealed
// digest. Any other GLCB there is mismatched. It is renamed to
// data.glcb + GLCBMismatchedSuffix, so the canonical path reads as missing and
// the chunk is built from its segments; sealing it would make every home's
// correct build disagree with the sealed digest.

// GLCBMismatchedSuffix is appended to a chunk's canonical GLCB path when the
// GLCB there is not a build of the chunk's manifest.
const GLCBMismatchedSuffix = ".mismatched"

// existingGLCBBuildOf returns the BuildResult of the GLCB on m's canonical
// path when that GLCB is a build of m. An unreadable GLCB is quarantined as
// corrupt and a mismatched one as mismatched; both report not adopted, leaving
// the canonical path missing for the build pass to fill.
func (v *vaultChunking) existingGLCBBuildOf(m *vaultctlfsm.OpenChunkManifest, sealedAt time.Time) (BuildResult, bool, error) {
	glcbPath := ChunkGLCBPath(v.cfg.ChunkRoot, m.ChunkID)
	if _, err := os.Stat(glcbPath); err != nil {
		if os.IsNotExist(err) {
			return BuildResult{}, false, nil
		}
		return BuildResult{}, false, err
	}
	meta, digest, size, err := readGLCBSealMeta(glcbPath)
	if err != nil {
		v.quarantineCorruptGLCB(m.ChunkID, glcbPath, err)
		return BuildResult{}, false, nil
	}
	var sealedDigest [32]byte
	if e := v.fsm().Get(m.ChunkID); e != nil && e.IsSealed() {
		sealedDigest = e.Hash
	}
	if mismatch := glcbMatchesManifest(meta, digest, m, sealedDigest); mismatch != nil {
		v.quarantineMismatchedGLCB(m.ChunkID, glcbPath, mismatch)
		return BuildResult{}, false, nil
	}
	v.clearCorruptGLCB(m.ChunkID, "existing GLCB readable")
	return buildResultFromSealMeta(glcbPath, meta, digest, size, sealedAt), true, nil
}

// glcbMatchesManifest returns nil when a GLCB with this meta and digest is a
// build of m, else the first difference. A zero sealedDigest means the chunk
// has no sealed digest yet. A manifest without refs carries no record facts,
// so only the chunk ID and digest are compared; a zero bound in m is one the
// manifest does not record.
func glcbMatchesManifest(meta glcb.BlobMeta, digest [32]byte, m *vaultctlfsm.OpenChunkManifest, sealedDigest [32]byte) error {
	if meta.ChunkID != m.ChunkID {
		return fmt.Errorf("the GLCB is chunk %s's", meta.ChunkID)
	}
	if sealedDigest != ([32]byte{}) && digest != sealedDigest {
		return fmt.Errorf("the GLCB's digest %x differs from the sealed digest %x", digest[:8], sealedDigest[:8])
	}
	if len(m.Refs) == 0 {
		return nil
	}
	if uint64(meta.RecordCount) != m.TotalRecords {
		return fmt.Errorf("the GLCB holds %d records, the manifest %d", meta.RecordCount, m.TotalRecords)
	}
	bounds := []struct {
		name          string
		blob, records time.Time
	}{
		{"first write time", meta.WriteStart, m.Bounds.WriteStart},
		{"last write time", meta.WriteEnd, m.Bounds.WriteEnd},
		{"first ingest time", meta.IngestStart, m.Bounds.IngestStart},
		{"last ingest time", meta.IngestEnd, m.Bounds.IngestEnd},
		{"first source time", meta.SourceStart, m.Bounds.SourceStart},
		{"last source time", meta.SourceEnd, m.Bounds.SourceEnd},
	}
	var errs []error
	for _, b := range bounds {
		if !b.records.IsZero() && !b.blob.Equal(b.records) {
			errs = append(errs, fmt.Errorf("the GLCB's %s is %s, the manifest's %s",
				b.name, b.blob.UTC().Format(time.RFC3339Nano), b.records.UTC().Format(time.RFC3339Nano)))
		}
	}
	return errors.Join(errs...)
}

// quarantineMismatchedGLCB renames a mismatched GLCB aside. The renamed file
// is kept for investigation and goes when the chunk's directory does.
func (v *vaultChunking) quarantineMismatchedGLCB(chunkID chunk.ChunkID, glcbPath string, mismatch error) {
	quarantine := glcbPath + GLCBMismatchedSuffix
	if err := os.Rename(glcbPath, quarantine); err != nil {
		v.logger().Warn("mismatched GLCB quarantine rename failed — the build from source segments replaces it in place",
			"chunk", chunkID, "path", glcbPath, "error", err)
	}
	v.logger().Error("the GLCB on a chunk's canonical path is not a build of its manifest — quarantined; building the chunk from its segments",
		"chunk", chunkID, "quarantine", quarantine, "mismatch", mismatch)
}
