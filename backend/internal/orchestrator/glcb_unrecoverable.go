package orchestrator

import (
	"fmt"
	"maps"
	"slices"
	"sync"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// glcbUnrecoverableAlarmType is the catalog type ID for sealed chunks this
// home should hold but no peer home or holder could supply; the instance key
// is the vault ID.
const glcbUnrecoverableAlarmType = "glcb-unrecoverable"

// glcbUnrecoverable tracks, per vault, the sealed chunks whose replica pull
// found no source. A chunk leaves the set when a pull lands it, when its bytes
// turn up locally, or when it leaves the vault's manifest.
type glcbUnrecoverable struct {
	mu      sync.Mutex
	byVault map[glid.GLID]map[chunk.ChunkID]string
}

// noteGLCBUnrecoverable records that no source supplied the chunk and raises
// the vault's alarm.
func (o *Orchestrator) noteGLCBUnrecoverable(vaultID glid.GLID, chunkID chunk.ChunkID, reason string) {
	u := &o.glcbUnrecoverable
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.byVault == nil {
		u.byVault = make(map[glid.GLID]map[chunk.ChunkID]string)
	}
	chunks := u.byVault[vaultID]
	if chunks == nil {
		chunks = make(map[chunk.ChunkID]string)
		u.byVault[vaultID] = chunks
	}
	chunks[chunkID] = reason
	o.updateGLCBUnrecoverableAlertLocked(vaultID)
}

// noteGLCBRecovered drops the chunk from the vault's unrecoverable set.
func (o *Orchestrator) noteGLCBRecovered(vaultID glid.GLID, chunkID chunk.ChunkID) {
	u := &o.glcbUnrecoverable
	u.mu.Lock()
	defer u.mu.Unlock()
	if _, ok := u.byVault[vaultID][chunkID]; !ok {
		return
	}
	delete(u.byVault[vaultID], chunkID)
	o.updateGLCBUnrecoverableAlertLocked(vaultID)
}

// pruneGLCBUnrecoverable drops the vault's unrecoverable chunks that left its
// manifest, so a chunk retention deleted cannot pin the alarm.
func (o *Orchestrator) pruneGLCBUnrecoverable(vaultID glid.GLID, inManifest func(chunk.ChunkID) bool) {
	u := &o.glcbUnrecoverable
	u.mu.Lock()
	defer u.mu.Unlock()
	chunks := u.byVault[vaultID]
	if len(chunks) == 0 {
		return
	}
	changed := false
	for id := range chunks {
		if !inManifest(id) {
			delete(chunks, id)
			changed = true
		}
	}
	if changed {
		o.updateGLCBUnrecoverableAlertLocked(vaultID)
	}
}

func (o *Orchestrator) updateGLCBUnrecoverableAlertLocked(vaultID glid.GLID) {
	chunks := o.glcbUnrecoverable.byVault[vaultID]
	if len(chunks) == 0 {
		delete(o.glcbUnrecoverable.byVault, vaultID)
	}
	if o.alerts == nil {
		return
	}
	key := vaultID.String()
	if len(chunks) == 0 {
		o.alerts.Clear(glcbUnrecoverableAlarmType, key)
		return
	}
	ids := slices.SortedFunc(maps.Keys(chunks), chunk.ChunkID.Compare)
	o.alerts.Raise(glcbUnrecoverableAlarmType, key, fmt.Sprintf(
		"Vault %s: %d sealed chunk(s) missing on this home with no source to pull from (first: %s, %s)",
		o.VaultAlarmLabel(vaultID), len(ids), ids[0], chunks[ids[0]]))
}
