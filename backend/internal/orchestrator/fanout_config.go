// Fan-out chunk-manager configuration plumbing (gastrolog-nd6sz /
// gastrolog-hshgl).
//
// applyFanOutConfig writes the FanOut WriteModel + initial Receiving
// snapshot to every chunk manager that supports it. Called from
// buildVaultInstance at instance-build time + on every placement
// change.
//
// Under gastrolog-hshgl, FanOut is the only WriteModel — every chunk
// gets a placement entry. VaultConfig.WriteModel becomes informational
// (still validated as "fanout" or empty/default-fanout); the empty
// value resolves to fanout, not leader-driven. The legacy
// LeaderDriven branch in the orchestrator dispatch is dead code that
// follow-on commits delete.
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
	// First arg historically encoded the chunk's write-model; post-
	// gastrolog-hshgl it is unused but still part of the
	// FanOutConfigSetter interface (callers may still check non-empty
	// to gate behavior).
	setter.SetFanOutConfig("fanout", nodeIDs)
}
