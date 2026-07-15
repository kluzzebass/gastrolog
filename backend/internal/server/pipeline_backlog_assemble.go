package server

import (
	"sort"

	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
)

// AssemblePipelineBacklog merges local vault-ctl state and disk counts with
// peer pipeline disk snapshots from NodeStats broadcasts.
func AssemblePipelineBacklog(
	orch *orchestrator.Orchestrator,
	vaultID glid.GLID,
	localNodeID string,
	peerDisk []cluster.PeerVaultPipelineDisk,
) (orchestrator.PipelineBacklogSnapshot, error) {
	snap, err := orch.LocalPipelineBacklogSnapshot(vaultID)
	if err != nil {
		return snap, err
	}

	nodeSegments := []orchestrator.PipelineNodeDiskSegments{{
		NodeID:                    localNodeID,
		PipelineDiskSegmentCounts: snap.PipelineDiskSegmentCounts,
	}}
	for _, pd := range peerDisk {
		disk := orchestrator.PipelineDiskSegmentCounts{
			Working:               pd.Working,
			CompletedStaging:      pd.CompletedStaging,
			Head:                  pd.Head,
			PreHead:               pd.PreHead,
			WorkingBytes:          pd.WorkingBytes,
			CompletedStagingBytes: pd.CompletedStagingBytes,
			HeadBytes:             pd.HeadBytes,
			PreHeadBytes:          pd.PreHeadBytes,
		}
		snap.AddDiskCounts(disk)
		nodeSegments = append(nodeSegments, orchestrator.PipelineNodeDiskSegments{
			NodeID:                    pd.NodeID,
			PipelineDiskSegmentCounts: disk,
		})
	}

	sort.Slice(nodeSegments, func(i, j int) bool {
		return nodeSegments[i].NodeID < nodeSegments[j].NodeID
	})
	snap.NodeSegments = nodeSegments
	return snap, nil
}
