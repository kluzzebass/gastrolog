package orchestrator

import (
	"errors"
	"fmt"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// orphan_repatriation.go: operator-driven recovery for unknown
// orphan chunks. Unknown orphans are sealed chunks present on
// local disk but absent from the vault-ctl FSM manifest. They are
// never auto-deleted — SweepLocalOrphans only alerts on them, so
// the recovery surface for an FSM glitch survives until an
// operator acts.
//
// The repatriation flow:
//   1. Local Chunks.List() reports a sealed chunk with RecordCount > 0.
//   2. fsm.Get(id) is nil. fsm.IsTombstoned(id) is false.
//      → unknown orphan, alert raised by SweepLocalOrphans.
//   3. Operator runs `gastrolog repatriate <vault-id> <chunk-id>`.
//   4. Orchestrator reconstructs the ManifestEntry from the local
//      ChunkMeta (already read from the chunk's idx.log headers)
//      and proposes CmdRepatriateChunk to the vault-ctl FSM.
//   5. FSM apply inserts the entry in Sealed state, refusing if
//      the entry already exists or is tombstoned.
//   6. The alert clears on the next SweepLocalOrphans tick (the
//      chunk is now FSM-known so it skips the unknown-orphan path).

// ErrOrphanNotFound is returned when the requested chunk ID isn't
// present on local disk for the given vault — the operator passed
// a typo or queried the wrong node.
var ErrOrphanNotFound = errors.New("orphan not found on local disk")

// ErrOrphanNotEligible is returned when the chunk exists locally
// but doesn't match the repatriation profile (FSM already knows
// about it, it's not sealed, or it has zero records). The
// operator-facing message names the specific reason.
var ErrOrphanNotEligible = errors.New("chunk not eligible for repatriation")

// RepatriateOrphan re-introduces a sealed local chunk into the
// vault-ctl FSM manifest. Returns ErrOrphanNotFound if the chunk
// isn't present on local disk for the given vault, or
// ErrOrphanNotEligible if it exists but the FSM already tracks it
// (no recovery needed) or it's tombstoned (the cluster has
// explicitly forgotten it). The FSM-level guards in
// applyRepatriate catch the same conditions, but checking up-front
// gives clearer error messages.
//
// The chunk's ManifestEntry is reconstructed from the local
// chunk's idx.log headers — no record replay required. State is
// always Sealed (active-chunk state isn't reconstructable from
// disk alone).
func (o *Orchestrator) RepatriateOrphan(vaultID glid.GLID, chunkID chunk.ChunkID) error {
	o.mu.RLock()
	vault, exists := o.vaults[vaultID]
	if !exists || vault.Instance == nil {
		o.mu.RUnlock()
		return fmt.Errorf("%w: vault %s not on this node", ErrVaultNotFound, vaultID)
	}
	inst := vault.Instance
	o.mu.RUnlock()

	metas, err := inst.Chunks.List()
	if err != nil {
		return fmt.Errorf("list local chunks for vault %s: %w", vaultID, err)
	}

	var meta *chunk.ChunkMeta
	for i := range metas {
		if metas[i].ID == chunkID {
			meta = &metas[i]
			break
		}
	}
	if meta == nil {
		return fmt.Errorf("%w: chunk %s in vault %s", ErrOrphanNotFound, chunkID, vaultID)
	}
	if !meta.Sealed {
		return fmt.Errorf("%w: chunk %s is not sealed (active-chunk repatriation is out of scope)", ErrOrphanNotEligible, chunkID)
	}
	if meta.RecordCount == 0 {
		return fmt.Errorf("%w: chunk %s has zero records (likely a rotation artifact; not worth repatriating)", ErrOrphanNotEligible, chunkID)
	}

	entry := chunkMetaToManifestEntry(*meta)
	cmdData, err := vaultctlfsm.MarshalRepatriateChunk(entry)
	if err != nil {
		return fmt.Errorf("marshal repatriate command: %w", err)
	}
	if err := o.ApplyVaultControlPlane(vaultID, vaultraft.MarshalVaultChunkCommand(vaultID, cmdData)); err != nil {
		return fmt.Errorf("apply repatriate to vault-ctl FSM: %w", err)
	}
	if o.logger != nil {
		o.logger.Info("orphan chunk repatriated",
			"vault", vaultID, "chunk", chunkID,
			"records", meta.RecordCount, "bytes", meta.Bytes)
	}
	return nil
}

// chunkMetaToManifestEntry is defined in manifest_reader.go; reused here.
