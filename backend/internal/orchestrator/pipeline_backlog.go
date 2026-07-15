package orchestrator

import (
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/raftgroup"
)

// PipelineNodeDiskSegments is one node's on-disk pipeline segment counts.
type PipelineNodeDiskSegments struct {
	NodeID string
	PipelineDiskSegmentCounts
}

// PipelineBacklogSnapshot is the operator-facing pipeline depth for one vault.
type PipelineBacklogSnapshot struct {
	VaultID glid.GLID

	RegistrySegments         uint32
	EligibleSegments         uint32
	RegistryRecords          uint64
	OpenManifestRefs         uint32
	OpenManifestRecords      uint64
	OpenManifestIngestEnd    time.Time
	SealedManifestPending    bool
	OldestEligibleLastIngest time.Time

	PipelineDiskSegmentCounts
	NodeSegments []PipelineNodeDiskSegments

	VaultCtlLeaderNodeID          string
	ConnectedNodeIsVaultCtlLeader bool
}

// LocalPipelineBacklogSnapshot reads vault-ctl FSM state and local disk counts.
func (o *Orchestrator) LocalPipelineBacklogSnapshot(vaultID glid.GLID) (PipelineBacklogSnapshot, error) {
	out := PipelineBacklogSnapshot{VaultID: vaultID}

	disk, err := o.LocalPipelineDiskSegmentCounts(vaultID)
	if err != nil {
		return out, err
	}
	out.PipelineDiskSegmentCounts = disk

	fsm, _, isLeader, ok := o.vaultCtlHandle(vaultID)
	if !ok || fsm == nil {
		return out, nil
	}

	stats := chunking.RegistryPlanningStatsFromFSM(fsm)
	out.RegistrySegments = uint32(stats.TotalSegments)    //nolint:gosec
	out.EligibleSegments = uint32(stats.EligibleSegments) //nolint:gosec
	out.RegistryRecords = stats.RegistryRecords
	out.OldestEligibleLastIngest = stats.OldestLastIngest

	if summary, hasOpen := fsm.OpenChunkSummary(); hasOpen {
		out.OpenManifestRefs = uint32(summary.RefCount) //nolint:gosec
		out.OpenManifestRecords = summary.TotalRecords
		if open := fsm.OpenChunk(); open != nil && !open.Bounds.IngestEnd.IsZero() {
			out.OpenManifestIngestEnd = open.Bounds.IngestEnd
		}
	}
	out.SealedManifestPending = fsm.SealedManifest() != nil

	out.VaultCtlLeaderNodeID = o.vaultCtlLeaderNodeID(vaultID)
	if isLeader != nil {
		out.ConnectedNodeIsVaultCtlLeader = isLeader()
	}

	return out, nil
}

func (o *Orchestrator) vaultCtlLeaderNodeID(vaultID glid.GLID) string {
	if o.groupMgr == nil {
		return ""
	}
	gid := raftgroup.VaultControlPlaneGroupID(vaultID)
	g := o.groupMgr.GetGroup(gid)
	if g == nil || g.Raft == nil {
		return ""
	}
	_, id := g.Raft.LeaderWithID()
	return string(id)
}
