package server

import (
	"context"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/system"
)

// ForwardSearchEngine resolves the query engine for a ForwardSearch RPC,
// honoring sealed-chunk subset fields when present (gastrolog-2qj7m).
func ForwardSearchEngine(o *orchestrator.Orchestrator, req *apiv1.ForwardSearchRequest) (*query.Engine, error) {
	vaultID := glid.FromBytes(req.GetVaultId())
	if vaultID.IsZero() {
		return nil, nil
	}
	if len(req.GetSealedChunkIds()) == 0 && !req.GetSearchPipelineChunks() {
		return o.LeaderQueryEngineForVault(vaultID)
	}
	scope := orchestrator.HolderSearchScope{
		SealedChunkIDs: chunkIDsFromProto(req.GetSealedChunkIds()),
		PipelineChunks: req.GetSearchPipelineChunks(),
	}
	return o.HolderQueryEngineForVault(vaultID, scope)
}

// ForwardSearchIncludesHistogram reports whether a ForwardSearch handler should
// run the ITSI histogram pre-pass before streaming records. Partitioned holder
// slices skip it — each slice blocked the stream's first message on histogram
// work and N holders multiplied that cost (gastrolog-2qj7m perf).
func ForwardSearchIncludesHistogram(req *apiv1.ForwardSearchRequest, q query.Query) bool {
	if q.BoolExpr != nil {
		return false
	}
	return len(req.GetSealedChunkIds()) == 0 && !req.GetSearchPipelineChunks()
}

// hasMultiHolderVaultsInScope returns true when any vault in the query scope
// has more than one placement holder (RF>1).
func (s *QueryServer) hasMultiHolderVaultsInScope(ctx context.Context, selectedVaults []glid.GLID) bool {
	if s.cfgStore == nil {
		return false
	}
	vaults, err := s.cfgStore.ListVaults(ctx)
	if err != nil {
		return false
	}
	nscs, err := s.cfgStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		return false
	}
	selected := make(map[glid.GLID]bool, len(selectedVaults))
	for _, id := range selectedVaults {
		selected[id] = true
	}
	for _, v := range vaults {
		if len(selected) > 0 && !selected[v.ID] {
			continue
		}
		if len(system.PlacementNodeIDs(v.Placements, nscs)) > 1 {
			return true
		}
	}
	return false
}

// localVaultIDsFromPartitionTargets returns vault IDs searched locally under a
// partition plan.
func localVaultIDsFromPartitionTargets(targets []searchPartitionTarget, localNodeID string) map[glid.GLID]bool {
	if len(targets) == 0 {
		return nil
	}
	ids := make(map[glid.GLID]bool)
	for _, t := range targets {
		if t.nodeID == localNodeID {
			ids[t.vaultID] = true
		}
	}
	return ids
}

func chunkIDsFromProto(raw [][]byte) []chunk.ChunkID {
	if len(raw) == 0 {
		return nil
	}
	out := make([]chunk.ChunkID, 0, len(raw))
	for _, b := range raw {
		if len(b) == 0 {
			continue
		}
		out = append(out, chunk.ChunkID(glid.FromBytes(b)))
	}
	return out
}
