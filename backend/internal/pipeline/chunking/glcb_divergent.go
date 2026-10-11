package chunking

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// Every home's copy of a sealed chunk is the copy the cluster agreed on at
// seal. Homes build byte-identical GLCBs from the same manifest, and
// CmdSealChunk records the sealing leader's whole-blob digest
// (ManifestEntry.Hash), so every copy on every home carries that digest. A
// copy that does not is divergent: a nondeterministic build, or source segment
// bytes on this home that differ from the leader's.
//
//   - Detect: CheckLandedGLCB, which the orchestrator runs for every GLCB that
//     lands on this home — build, peer pull, transfer copy, seal apply —
//     before the home indexes the copy or claims a holder receipt for it.
//   - Quarantine: the copy is renamed to data.glcb + GLCBDivergentSuffix. The
//     canonical path then reads as missing, so the copy is not served, no
//     receipt is claimed for it, and the GLCB catch-up pull fetches the agreed
//     copy from a peer home, verifying its bytes against the sealed digest
//     before it lands. The orchestrator's landing path makes the vault's
//     chunk manager forget the copy (ForgetPipelineGLCB), so no registration
//     or cached mapping made before the check keeps serving it, and the cloud
//     upload refuses any copy that does not carry the sealed digest.
//   - No rebuild: the build pass skips a divergent chunk. The same manifest
//     over the same local segments builds the same divergent bytes again.
//   - Alarm: one per vault while any chunk on this home is divergent. Two
//     homes building different bytes from one manifest is a defect, so the
//     catalog latches it.
//   - Recover: the agreed copy landing clears the chunk and removes the
//     quarantined copy.

// GLCBDivergentSuffix is appended to a sealed GLCB's canonical path when its
// digest differs from the one the seal recorded.
const GLCBDivergentSuffix = ".divergent"

// buildDivergedAlarmType is the catalog type ID for copies whose digest
// differs from the sealed digest; the instance key is the vault ID.
const buildDivergedAlarmType = "chunking-build-diverged"

// GLCBAgreesWithSeal compares the whole-blob digest the GLCB at path records
// with the digest the seal recorded for e, returning the local digest. A
// chunk sealed without a digest agrees with any readable copy: nothing records
// which bytes were agreed.
func GLCBAgreesWithSeal(path string, e vaultctlfsm.ManifestEntry) ([32]byte, bool, error) {
	local, err := glcb.ReadBlobDigest(path)
	if err != nil {
		return local, false, err
	}
	return local, e.Hash == ([32]byte{}) || local == e.Hash, nil
}

// CheckLandedGLCB reports whether this home's GLCB for the sealed chunk e is
// the agreed copy, so the home may index it and claim a holder receipt. A
// divergent copy is quarantined and alarmed; an unreadable one takes the
// corrupt-GLCB path. False for a chunk not yet sealed (no digest to compare
// against yet; the seal apply lands the copy again), for a copy not on disk,
// and for a vault this manager does not run.
func (m *Manager) CheckLandedGLCB(vaultID glid.GLID, e vaultctlfsm.ManifestEntry) bool {
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if !ok {
		return false
	}
	return v.checkLandedGLCB(e)
}

func (v *vaultChunking) checkLandedGLCB(e vaultctlfsm.ManifestEntry) bool {
	if !e.IsSealed() {
		return false
	}
	path := ChunkGLCBPath(v.cfg.ChunkRoot, e.ID)
	// Read and rename under one hold: a check that read a divergent copy
	// must not rename aside the agreed copy a peer pull landed after it.
	v.corruptMu.Lock()
	defer v.corruptMu.Unlock()
	local, agrees, err := GLCBAgreesWithSeal(path, e)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return false
	case err != nil:
		v.quarantineCorruptGLCBLocked(e.ID, path, err)
		return false
	case !agrees:
		v.quarantineDivergentGLCBLocked(e.ID, path, local, e.Hash)
		return false
	}
	v.clearDivergentGLCBLocked(e.ID, "the copy on disk carries the sealed digest")
	return true
}

// quarantineDivergentGLCBLocked marks the chunk divergent, then renames its
// copy aside. Marked first: the build pass rebuilds a sealed chunk whose
// canonical path is missing unless it is marked. Caller holds corruptMu.
func (v *vaultChunking) quarantineDivergentGLCBLocked(chunkID chunk.ChunkID, path string, local, agreed [32]byte) {
	if v.divergentGLCBs == nil {
		v.divergentGLCBs = make(map[chunk.ChunkID]struct{})
	}
	if _, seen := v.divergentGLCBs[chunkID]; !seen {
		v.logger().Error("sealed chunk's GLCB on this home differs from the copy agreed at seal — quarantined; pulling the agreed copy from a peer home",
			"chunk", chunkID, "local_digest", hex.EncodeToString(local[:]), "sealed_digest", hex.EncodeToString(agreed[:]))
	}
	v.divergentGLCBs[chunkID] = struct{}{}
	v.updateDivergentGLCBAlarmLocked()
	quarantine := path + GLCBDivergentSuffix
	if err := os.Rename(path, quarantine); err != nil {
		v.logger().Warn("divergent GLCB quarantine rename failed — the agreed copy cannot land until the canonical path is clear",
			"chunk", chunkID, "path", path, "error", err)
	}
}

// clearDivergentGLCBLocked marks a flagged chunk recovered: the agreed copy is
// on the canonical path. Removes the quarantined copy. No-op for unflagged
// chunks. Caller holds corruptMu.
func (v *vaultChunking) clearDivergentGLCBLocked(chunkID chunk.ChunkID, via string) {
	if _, ok := v.divergentGLCBs[chunkID]; !ok {
		return
	}
	delete(v.divergentGLCBs, chunkID)
	quarantine := ChunkGLCBPath(v.cfg.ChunkRoot, chunkID) + GLCBDivergentSuffix
	if err := os.Remove(quarantine); err != nil && !errors.Is(err, fs.ErrNotExist) {
		v.logger().Warn("failed to remove a recovered chunk's divergent GLCB",
			"chunk", chunkID, "path", quarantine, "error", err)
	}
	v.logger().Info("divergent GLCB replaced by the agreed copy", "chunk", chunkID, "via", via)
	v.updateDivergentGLCBAlarmLocked()
}

// glcbDivergent reports whether the chunk's copy on this home was quarantined
// as divergent and the agreed copy has not landed yet.
func (v *vaultChunking) glcbDivergent(chunkID chunk.ChunkID) bool {
	v.corruptMu.Lock()
	defer v.corruptMu.Unlock()
	_, ok := v.divergentGLCBs[chunkID]
	return ok
}

// updateDivergentGLCBAlarmLocked reports the divergent-copy condition to the
// alarm collector from the current divergent set. Caller holds corruptMu.
func (v *vaultChunking) updateDivergentGLCBAlarmLocked() {
	if v.cfg.Alerts == nil {
		return
	}
	if len(v.divergentGLCBs) == 0 {
		v.cfg.Alerts.Clear(buildDivergedAlarmType, v.cfg.VaultID.String())
		return
	}
	var example chunk.ChunkID
	for id := range v.divergentGLCBs {
		example = id
		break
	}
	v.cfg.Alerts.Raise(buildDivergedAlarmType, v.cfg.VaultID.String(),
		fmt.Sprintf("Vault %s: %d sealed chunk(s) on this node differ from the copy the cluster agreed on when they sealed (e.g. chunk %s). Each was renamed with a %s suffix so it is not served or counted as a copy; this node pulls the agreed copy from another home.",
			v.vaultLabel(), len(v.divergentGLCBs), example, GLCBDivergentSuffix))
}
