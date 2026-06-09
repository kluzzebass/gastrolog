package orchestrator

import (
	"fmt"

	"gastrolog/internal/chunk"
	"gastrolog/internal/convert"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/routing"
)

// This file provides test-only seeding primitives that replaced the production
// active-chunk append/seal path removed in Rubicon E2 (gastrolog-358ak). The
// live write path is now the pipeline (Supervisor.Submit / SubmitToVault); the
// per-instance chunk.ChunkManager active-chunk append is no longer driven by
// production code. Tests still need a cheap way to materialize records and
// sealed chunks directly in a vault's chunk store without standing up the full
// pipeline, so these helpers reproduce the *local* portion of the old
// Orchestrator.AppendToVault / SealActiveChunk (no cross-node follower
// forwarding, which E2 deleted).

// AppendToVault appends a single record to the local vault instance's chunk
// manager, preserving an explicit leader chunk ID when provided. Test-only
// seeding primitive — see the file comment.
func (o *Orchestrator) AppendToVault(vaultID glid.GLID, leaderChunkID chunk.ChunkID, rec chunk.Record) error {
	o.mu.RLock()
	defer o.mu.RUnlock()

	vault := o.vaults[vaultID]
	if vault == nil {
		return fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if err := vaultReplicationReadinessErr(vaultID, vault); err != nil {
		return err
	}
	vaultInst := vault.Instance
	if vaultInst == nil {
		return fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if _, draining := o.vaultDraining[vaultDrainKey(vaultID)]; draining {
		return ErrVaultDraining
	}
	cm := vaultInst.Chunks

	if leaderChunkID != (chunk.ChunkID{}) && vaultInst.IsTombstoned != nil && vaultInst.IsTombstoned(leaderChunkID) {
		return fmt.Errorf("%w: append to tombstoned chunk %s", chunk.ErrChunkTombstoned, leaderChunkID)
	}

	if leaderChunkID != (chunk.ChunkID{}) {
		if active := cm.Active(); active != nil && active.ID != leaderChunkID {
			_ = cm.Seal()
		}
		cm.SetNextChunkID(leaderChunkID)
	}

	activeBefore := cm.Active()
	if _, _, err := cm.Append(rec); err != nil {
		return err
	}
	o.progressTrigger.Signal()

	activeAfter := cm.Active()
	sealed := activeBefore != nil && (activeAfter == nil || activeAfter.ID != activeBefore.ID)
	if sealed && !vaultInst.IsFollower {
		o.schedulePostSeal(vaultID, cm, activeBefore.ID)
	}
	return nil
}

// SetTestRoutingTable publishes a routing table directly onto the pipeline
// supervisor, bypassing config reconciliation. Test-only seam used together
// with Ingest to exercise routing without loading a full system config.
func (o *Orchestrator) SetTestRoutingTable(t *routing.Table) {
	o.pipeline.SetRoutingTable(t)
}

// Ingest reproduces the local portion of the removed V0 ingest path as a
// test-only seeding primitive: it evaluates the pipeline's published routing
// table against the record (with an ingest SourceContext) and appends the
// record directly into every matched, enabled local vault's chunk manager.
//
// The production write path is the async pipeline (route → segmentation →
// chunking), which lands records in segments/GLCBs rather than the per-instance
// chunk.ChunkManager. Reconfiguration and vault-lifecycle tests assert delivery
// via the chunk manager (countRecords), so this seam preserves that observable
// contract — exercising config-reconciled routing and the vault Enabled gate —
// without standing up the full async pipeline. Returns ErrNoChunkManagers when
// no vaults are registered, mirroring the old guard; a nil/empty table or an
// unmatched record is a silent drop.
func (o *Orchestrator) Ingest(rec chunk.Record) error {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if len(o.vaults) == 0 {
		return ErrNoChunkManagers
	}
	table := o.pipeline.RoutingTable()
	if table == nil {
		return nil
	}
	prec := convert.ChunkToRecord(rec)
	for _, vid := range table.Match(prec.Attrs, routing.IngestSource(&prec)) {
		vault := o.vaults[vid]
		if vault == nil || !vault.Enabled || vault.Instance == nil {
			continue
		}
		if _, draining := o.vaultDraining[vaultDrainKey(vid)]; draining {
			continue
		}
		if _, _, err := vault.Instance.Chunks.Append(rec); err != nil {
			return err
		}
		o.progressTrigger.Signal()
	}
	return nil
}

// SealActiveChunk seals the active chunk on the local vault instance, gated on
// expectedChunkID. Test-only seeding primitive — see the file comment.
func (o *Orchestrator) SealActiveChunk(vaultID glid.GLID, expectedChunkID chunk.ChunkID) error {
	vaultInst := o.findLocalVaultInstance(vaultID)
	if vaultInst == nil {
		return fmt.Errorf("%w: vault %s", ErrInstanceNotLocal, vaultID)
	}
	active := vaultInst.Chunks.Active()
	if active == nil {
		return nil
	}
	if active.ID != expectedChunkID {
		return nil
	}
	chunkID := active.ID
	if err := vaultInst.Chunks.Seal(); err != nil {
		return err
	}
	o.postSealWork(vaultID, vaultInst.Chunks, chunkID)
	return nil
}
