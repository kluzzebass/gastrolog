// Fan-out chunk-manager configuration plumbing (gastrolog-nd6sz).
//
// applyFanOutConfig writes the per-vault WriteModel + initial
// Receiving snapshot to the chunk manager so the NEXT CmdCreateChunk
// announcement stamps both onto the new ChunkPlacement entry. Called
// from buildVaultInstance at instance-build time (and re-called on
// every placement-change reconfig to keep the active chunk-manager's
// snapshot consistent with the operator's current VaultConfig).
//
// LeaderDriven vaults (WriteModel unset or "leader-driven"): pushes
// empty values, leaving the chunk-manager's announce path on the
// legacy CmdCreateChunk codepath unchanged.
//
// FanOut vaults: pushes "fanout" + the placement member node-ID list
// as the initial Receiving snapshot. Both fields are immutable on
// the chunk once stamped (per-chunk cutover semantics).
//
// Implementation note: the chunk-manager exposes the config setter
// via chunk.FanOutConfigSetter (optional interface). Implementations
// that don't support it (memory / jsonl chunk managers) silently
// skip the call — those vaults can't be FanOut anyway because they
// have no cross-node replication path.

package orchestrator

import (
	"slices"

	"gastrolog/internal/chunk"
	"gastrolog/internal/system"
)

func applyFanOutConfig(cm chunk.ChunkManager, vaultCfg system.VaultConfig, placements []system.VaultPlacement, nscs []system.NodeStorageConfig) {
	setter, ok := cm.(chunk.FanOutConfigSetter)
	if !ok {
		return
	}
	writeModel := string(vaultCfg.WriteModel.Resolve())
	if writeModel == string(system.WriteModelLeaderDriven) {
		// LeaderDriven (or unset): no per-chunk placement to stamp.
		// Push empty values so a vault that flips FanOut → LeaderDriven
		// stops emitting FanOut create payloads on subsequent chunks.
		setter.SetFanOutConfig("", nil)
		return
	}
	// FanOut: initial Receiving = every node that has a placement
	// for this vault, deduplicated. The placement list itself encodes
	// the candidate pool (Receiving ⊆ placements per
	// docs/fan-out-data-plane-design.md).
	nodeIDs := make([]string, 0, len(placements))
	for _, sid := range system.StorageIDs(placements) {
		nid := system.NodeIDForStorage(sid, nscs)
		if nid != "" && !slices.Contains(nodeIDs, nid) {
			nodeIDs = append(nodeIDs, nid)
		}
	}
	setter.SetFanOutConfig(writeModel, nodeIDs)
}
