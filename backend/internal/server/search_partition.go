package server

import (
	"context"
	"hash/fnv"
	"slices"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/system"
)

// searchPartitionTarget is one holder node's slice of a vault search.
type searchPartitionTarget struct {
	nodeID         string
	vaultID        glid.GLID
	sealedChunkIDs []chunk.ChunkID
	pipelineChunks bool
}

// partitionHolderIndex picks a stable holder for a sealed chunk.
func partitionHolderIndex(id chunk.ChunkID, holderCount int) int {
	if holderCount <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(id.String()))
	return int(h.Sum32() % uint32(holderCount)) //nolint:gosec // G115: holderCount > 1
}

// planVaultSearchPartitions splits a vault's searchable chunks across replica
// holders. Sealed chunks are hash-partitioned; active/sealing chunks are
// assigned only to the placement leader.
func planVaultSearchPartitions(
	holders []string,
	leaderNodeID string,
	metas []chunk.ChunkMeta,
) []searchPartitionTarget {
	if len(holders) == 0 {
		return nil
	}
	holders = slices.Clone(holders)
	slices.Sort(holders)

	byNode := make(map[string]*searchPartitionTarget, len(holders))
	for _, nodeID := range holders {
		byNode[nodeID] = &searchPartitionTarget{nodeID: nodeID}
	}

	var sealed []chunk.ChunkID
	var hasPipeline bool
	for _, m := range metas {
		if chunkMetaIsPipeline(m) {
			hasPipeline = true
			continue
		}
		if chunkMetaIsSealed(m) {
			sealed = append(sealed, m.ID)
		}
	}

	for _, id := range sealed {
		nodeID := holders[partitionHolderIndex(id, len(holders))]
		byNode[nodeID].sealedChunkIDs = append(byNode[nodeID].sealedChunkIDs, id)
	}
	if hasPipeline && leaderNodeID != "" {
		byNode[leaderNodeID].pipelineChunks = true
	}

	out := make([]searchPartitionTarget, 0, len(byNode))
	for _, nodeID := range holders {
		t := byNode[nodeID]
		if len(t.sealedChunkIDs) == 0 && !t.pipelineChunks {
			continue
		}
		t.sealedChunkIDs = slices.Clone(t.sealedChunkIDs)
		out = append(out, *t)
	}
	return out
}

func chunkMetaIsPipeline(m chunk.ChunkMeta) bool {
	switch m.State {
	case chunk.ChunkStateActive, chunk.ChunkStateSealing:
		return true
	case chunk.ChunkStateSealed:
		return false
	case chunk.ChunkStateUnknown:
		return !m.Sealed
	default:
		return !m.Sealed
	}
}

func chunkMetaIsSealed(m chunk.ChunkMeta) bool {
	switch m.State {
	case chunk.ChunkStateSealed:
		return true
	case chunk.ChunkStateActive, chunk.ChunkStateSealing:
		return false
	case chunk.ChunkStateUnknown:
		return m.Sealed
	default:
		return m.Sealed
	}
}

// buildSearchPartitionTargets resolves per-holder search slices for every
// vault in scope. Vaults with a single placement holder use one target on
// that node (full chunk set). Multi-holder vaults distribute sealed chunks.
func (s *QueryServer) buildSearchPartitionTargets(ctx context.Context, selectedVaults []glid.GLID) []searchPartitionTarget {
	if s.cfgStore == nil || s.orch == nil {
		return nil
	}
	vaults, err := s.cfgStore.ListVaults(ctx)
	if err != nil {
		return nil
	}
	nscs, err := s.cfgStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		return nil
	}

	selected := make(map[glid.GLID]bool, len(selectedVaults))
	for _, id := range selectedVaults {
		selected[id] = true
	}

	var targets []searchPartitionTarget
	for _, v := range vaults {
		if len(selected) > 0 && !selected[v.ID] {
			continue
		}
		placements := s.placementsFor(ctx, v.ID)
		if len(placements) == 0 {
			continue
		}
		holders := system.PlacementNodeIDs(placements, nscs)
		if len(holders) == 0 {
			continue
		}
		leaderNodeID := system.LeaderNodeID(placements, nscs)
		metas := s.vaultPartitionMetas(v.ID)
		if len(metas) == 0 {
			nodeID := leaderNodeID
			if len(holders) == 1 {
				nodeID = holders[0]
			}
			if nodeID != "" {
				targets = append(targets, searchPartitionTarget{
					nodeID:  nodeID,
					vaultID: v.ID,
				})
			}
			continue
		}
		for _, t := range planVaultSearchPartitions(holders, leaderNodeID, metas) {
			t.vaultID = v.ID
			targets = append(targets, t)
		}
	}
	return targets
}

// vaultPartitionMetas returns lightweight chunk metas for partition planning
// only (state + ID). Avoids pipeline bound overlay on every sealed chunk.
func (s *QueryServer) vaultPartitionMetas(vaultID glid.GLID) []chunk.ChunkMeta {
	if entries := s.orch.VaultManifestEntriesIncludingOpen(vaultID); len(entries) > 0 {
		out := make([]chunk.ChunkMeta, 0, len(entries))
		for _, e := range entries {
			out = append(out, e.ToChunkMeta())
		}
		return out
	}
	return s.orch.SearchChunkMetasForVault(vaultID)
}

// holderScopeFromTarget builds an orchestrator holder scope from a partition target.
func holderScopeFromTarget(t searchPartitionTarget) orchestrator.HolderSearchScope {
	return orchestrator.HolderSearchScope{
		SealedChunkIDs: t.sealedChunkIDs,
		PipelineChunks: t.pipelineChunks,
	}
}
