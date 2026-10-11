package orchestrator

import (
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator/pipeline"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// sweepReleasedSegments runs on every node for every vault: each node purges
// its staging copies of released segments and confirms the release sequence
// the purge covered, and the vault-ctl leader prunes the released-segment
// tombstones every member of the group has confirmed. Every node takes part
// because every node is an origin for every vault and so may hold a released
// segment in completed/. The release consumer purges on each release event
// where the vault is registered with the pipeline; this sweep is what turns a
// purge into a confirmation, and it reaches vaults the pipeline does not run
// here.
func (o *Orchestrator) sweepReleasedSegments() {
	o.mu.RLock()
	vaultIDs := make([]glid.GLID, 0, len(o.vaults))
	for vaultID := range o.vaults {
		vaultIDs = append(vaultIDs, vaultID)
	}
	o.mu.RUnlock()
	for _, vaultID := range vaultIDs {
		o.sweepReleasedSegmentsForVault(vaultID)
	}
}

func (o *Orchestrator) sweepReleasedSegmentsForVault(vaultID glid.GLID) {
	fsm, applier, isLeader, ok := o.vaultCtlHandle(vaultID)
	if !ok || !fsm.Ready() {
		return
	}
	log := o.logger.With("vault", vaultID)
	covered, purged, err := pipeline.PurgeReleasedStaging(fsm, o.releasedSegmentStagingRoots(vaultID))
	if len(purged) > 0 {
		log.Info("released-segment sweep: purged released segments' staging copies", "segments", len(purged))
	}
	switch {
	case err != nil:
		log.Warn("released-segment sweep: purge incomplete; not confirming", "error", err)
	case covered > fsm.SegmentPurgeConfirmed(o.localNodeID):
		if err := applier.Apply(vaultctlfsm.MarshalConfirmSegmentPurge(o.localNodeID, covered)); err != nil {
			log.Warn("released-segment sweep: purge confirmation failed", "release_seq", covered, "error", err)
		}
	}
	if !isLeader() {
		return
	}
	members, ok := o.vaultCtlMembers(vaultID)
	if !ok || !fsm.ReleasedSegmentsPrunable(members) {
		return
	}
	if err := applier.Apply(vaultctlfsm.MarshalPruneReleasedSegments(members)); err != nil {
		log.Warn("released-segment sweep: tombstone prune failed", "error", err)
	}
}

// releasedSegmentStagingRoots is where this node may hold staging copies of a
// vault's segments: <segmentsDir>/<vaultID>, whether or not the vault is
// registered with the pipeline here. None without a segments directory.
func (o *Orchestrator) releasedSegmentStagingRoots(vaultID glid.GLID) []string {
	if o.segmentsDir == "" {
		return nil
	}
	return []string{pipelineVaultRoot(o.segmentsDir, vaultID)}
}

// vaultCtlMembers returns the server IDs in this node's view of the vault-ctl
// group's Raft configuration — on the leader, the latest configuration.
func (o *Orchestrator) vaultCtlMembers(vaultID glid.GLID) ([]string, bool) {
	if o.groupMgr == nil {
		return nil, false
	}
	g := o.groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(vaultID))
	if g == nil || g.Raft == nil {
		return nil, false
	}
	future := g.Raft.GetConfiguration()
	if future.Error() != nil {
		return nil, false
	}
	servers := future.Configuration().Servers
	members := make([]string, 0, len(servers))
	for _, srv := range servers {
		members = append(members, string(srv.ID))
	}
	return members, len(members) > 0
}
