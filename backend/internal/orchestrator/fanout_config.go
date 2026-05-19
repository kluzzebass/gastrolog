// Fan-out chunk-manager configuration plumbing (gastrolog-hshgl).
//
// applyFanOutConfig writes the initial Receiving snapshot to every
// chunk manager that supports it. Called from buildVaultInstance at
// instance-build time + on every placement change.
//
// The chunk-manager exposes the config setter via
// chunk.FanOutConfigSetter (optional interface). Implementations
// that don't support it (memory / jsonl chunk managers) silently
// skip the call — those vaults have no cross-node replication anyway.

package orchestrator

import (
	"slices"

	"gastrolog/internal/chunk"
	"gastrolog/internal/system"
)

func applyFanOutConfig(cm chunk.ChunkManager, _ system.VaultConfig, placements []system.VaultPlacement, nscs []system.NodeStorageConfig) {
	setter, ok := cm.(chunk.FanOutConfigSetter)
	if !ok {
		return
	}
	// Initial Receiving = every node that has a placement for this
	// vault, deduplicated. The placement list itself encodes the
	// candidate pool (Receiving ⊆ placements per
	// docs/fan-out-data-plane-design.md).
	nodeIDs := make([]string, 0, len(placements))
	for _, sid := range system.StorageIDs(placements) {
		nid := system.NodeIDForStorage(sid, nscs)
		if nid != "" && !slices.Contains(nodeIDs, nid) {
			nodeIDs = append(nodeIDs, nid)
		}
	}
	setter.SetFanOutConfig(nodeIDs)
}
