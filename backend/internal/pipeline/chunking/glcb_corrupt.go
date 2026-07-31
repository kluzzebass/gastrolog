package chunking

import (
	"fmt"
	"os"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// This file is the single corrupt-GLCB recovery story shared by the build
// pass (adoptExistingGLCBIfPresent) and restart recovery (recoverBuiltGLCB).
//
// The unified story: detect → quarantine → alert → heal.
//
//   - Detect: BuildResultFromExistingGLCB fails on an existing data.glcb.
//   - Quarantine: rename the file aside to data.glcb + GLCBCorruptSuffix.
//     The rename degrades corrupt to EXACTLY the missing-GLCB state — a
//     stat on the canonical path now fails — so every existing missing-GLCB
//     mechanism applies unchanged: an unsealed chunk rebuilds from source
//     segments on the next build pass (collection pulls any segment bytes
//     this home lacks), and a sealed chunk whose source segments are long
//     released is re-pulled from a peer home by the orchestrator's GLCB
//     catch-up sweep (pullMissingGLCB triggers on the stat-miss).
//   - Alert: a per-vault operator alert names the chunk and the read error,
//     raised while any corrupt chunk remains and cleared when none does.
//     The catalog's DelayOn keeps it out of the alarm list while the heal
//     paths below are still expected to land. Logging is state-transition-
//     only: one Warn when a chunk is first flagged, one Info when it heals
//     — never per retry.
//   - Heal: any path that makes the canonical GLCB readable again clears
//     the state — rebuild from segments, adopting a readable existing file,
//     restart recovery reading it cleanly, or the orchestrator's peer
//     re-pull (Manager.NoteGLCBRestored).
//
// Quarantine-not-delete: the damaged bytes are preserved for forensics in
// the one case that matters — when NO replacement exists (segments released
// and no peer holds a copy), the quarantine file is the last, damaged copy
// of ingested records and an automated path must not destroy it. It cannot
// become unswept litter: healing removes it here, and retention delete
// removes the whole chunk directory (deletePipelineChunkDir RemoveAll), so
// the file only outlives the episode during an unresolved durability
// incident — exactly when it should.

// GLCBCorruptSuffix is appended to a sealed GLCB's canonical path when its
// bytes are detected unreadable, quarantining the damaged file so the
// canonical path reads as missing to every recovery mechanism.
const GLCBCorruptSuffix = ".corrupt"

// glcbCorruptAlarmType is the catalog type ID for quarantined unreadable
// GLCBs; the instance key is the vault ID.
const glcbCorruptAlarmType = "chunking-glcb-corrupt"

// quarantineCorruptGLCB moves an existing-but-unreadable sealed GLCB aside
// and flags the chunk corrupt (alert + one Warn per episode). See the file
// header for the full story. Safe to call again while already flagged.
func (v *vaultChunking) quarantineCorruptGLCB(chunkID chunk.ChunkID, glcbPath string, readErr error) {
	quarantine := glcbPath + GLCBCorruptSuffix
	if err := os.Rename(glcbPath, quarantine); err != nil {
		// A failed rename does not block local healing — BuildGLCBFile
		// renames a fresh temp over the canonical path — but the sealed-
		// chunk peer re-pull needs the stat-miss, so say so.
		v.logger().Warn("corrupt GLCB quarantine rename failed — peer re-pull stays blocked until the canonical path is clear",
			"chunk", chunkID, "path", glcbPath, "error", err)
	}
	v.corruptMu.Lock()
	defer v.corruptMu.Unlock()
	if _, seen := v.corruptGLCBs[chunkID]; !seen {
		v.logger().Warn("sealed GLCB unreadable — quarantined; heals via rebuild from source segments or peer re-pull",
			"chunk", chunkID, "quarantine", quarantine, "error", readErr)
	}
	if v.corruptGLCBs == nil {
		v.corruptGLCBs = make(map[chunk.ChunkID]string)
	}
	v.corruptGLCBs[chunkID] = readErr.Error()
	v.updateCorruptGLCBAlertLocked()
}

// clearCorruptGLCB marks a previously-flagged chunk healed: its canonical
// GLCB is readable again (rebuilt from segments, adopted readable, recovered
// cleanly, or re-pulled from a peer). Removes the quarantine file and clears
// the alert when no corrupt chunk remains. No-op for unflagged chunks.
func (v *vaultChunking) clearCorruptGLCB(chunkID chunk.ChunkID, via string) {
	v.corruptMu.Lock()
	defer v.corruptMu.Unlock()
	if _, ok := v.corruptGLCBs[chunkID]; !ok {
		return
	}
	delete(v.corruptGLCBs, chunkID)
	quarantine := ChunkGLCBPath(v.cfg.ChunkRoot, chunkID) + GLCBCorruptSuffix
	if err := os.Remove(quarantine); err != nil && !os.IsNotExist(err) {
		v.logger().Warn("failed to remove healed chunk's quarantined GLCB",
			"chunk", chunkID, "path", quarantine, "error", err)
	}
	v.logger().Info("corrupt GLCB healed — chunk readable again on this home",
		"chunk", chunkID, "via", via)
	v.updateCorruptGLCBAlertLocked()
}

// pruneCorruptGLCBs drops corrupt state for chunks that left the vault-ctl
// FSM entirely (retention deleted the chunk; the chunk-dir RemoveAll took
// the quarantine file with it) so a deleted chunk cannot pin the alert
// forever. Called from the build and recovery passes; cheap — the map is
// empty outside an active corruption episode.
func (v *vaultChunking) pruneCorruptGLCBs() {
	v.corruptMu.Lock()
	defer v.corruptMu.Unlock()
	if len(v.corruptGLCBs) == 0 {
		return
	}
	fsm := v.fsm()
	changed := false
	for id := range v.corruptGLCBs {
		if fsm.Get(id) == nil {
			delete(v.corruptGLCBs, id)
			changed = true
		}
	}
	if changed {
		v.updateCorruptGLCBAlertLocked()
	}
}

// updateCorruptGLCBAlertLocked reports the corrupt-GLCB condition to the
// alarm collector from the current corrupt set. Raise/Clear dedup and the
// catalog's DelayOn window (the alarm annunciates only if corruption
// outlives the self-healing paths) are the collector's; logging here stays
// transition-edge via the corrupt map itself. Caller holds corruptMu.
func (v *vaultChunking) updateCorruptGLCBAlertLocked() {
	if v.cfg.Alerts == nil {
		return
	}
	if len(v.corruptGLCBs) == 0 {
		v.cfg.Alerts.Clear(glcbCorruptAlarmType, v.cfg.VaultID.String())
		return
	}
	var example chunk.ChunkID
	exampleErr := ""
	for id, e := range v.corruptGLCBs {
		example, exampleErr = id, e
		break
	}
	v.cfg.Alerts.Raise(glcbCorruptAlarmType, v.cfg.VaultID.String(),
		fmt.Sprintf("vault %s: %d sealed chunk GLCB(s) were unreadable on this node (e.g. chunk %s: %s); each file was quarantined with a %s suffix",
			v.cfg.VaultID, len(v.corruptGLCBs), example, exampleErr, GLCBCorruptSuffix))
}

// NoteGLCBRestored tells chunking that a chunk's canonical GLCB became
// readable again through a path outside this package — the orchestrator's
// GLCB catch-up sweep re-pulled a verified copy from a peer home. Clears the
// chunk's corrupt-GLCB state (quarantine file + operator alert) when it was
// flagged; no-op otherwise.
func (m *Manager) NoteGLCBRestored(vaultID glid.GLID, chunkID chunk.ChunkID) {
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if ok {
		v.clearCorruptGLCB(chunkID, "re-pulled from a peer home")
	}
}
