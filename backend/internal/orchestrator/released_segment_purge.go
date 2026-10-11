package orchestrator

import (
	"errors"
	"fmt"
	"os"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// purgeReleasedSegmentStaging deletes this node's staging copies (completed/,
// head/, pre-head/) of every segment the vault-ctl FSM holds a
// released-segment tombstone for. covered is the release sequence the pass
// accounts for: no segment released at or before it is left in this node's
// staging. A non-nil error means some listing or purge failed and the pass
// covers nothing.
//
// Registry absence is never purge evidence — a completed segment awaiting its
// publish is registry-absent too — so only tombstoned segments go.
//
// Lock-free on o.mu: ReconcileFromSnapshot calls it from the Raft apply pump.
func (o *Orchestrator) purgeReleasedSegmentStaging(vaultID glid.GLID, fsm *vaultctlfsm.FSM) (covered uint64, purged int, err error) {
	// Read before listing: every segment released at or before covered is
	// already tombstoned when the directories are read.
	covered = fsm.ReleaseSeq()
	if o.segmentsDir == "" {
		return covered, 0, nil
	}
	root := pipelineVaultRoot(o.segmentsDir, vaultID)
	candidates := make(map[glid.GLID]struct{})
	var errs []error
	for _, dir := range []string{paths.CompletedDir(root), paths.HeadDir(root), paths.PreHeadDir(root)} {
		ids, listErr := paths.ListSegmentIDs(dir)
		if listErr != nil {
			if !os.IsNotExist(listErr) {
				errs = append(errs, fmt.Errorf("list %s: %w", dir, listErr))
			}
			continue
		}
		for id := range ids {
			candidates[id] = struct{}{}
		}
	}
	for id := range candidates {
		if !fsm.SegmentReleased(id) {
			continue
		}
		if purgeErr := paths.PurgeSegmentStaging(root, id); purgeErr != nil {
			errs = append(errs, fmt.Errorf("purge segment %s: %w", id, purgeErr))
			continue
		}
		purged++
	}
	return covered, purged, errors.Join(errs...)
}

// sweepReleasedSegments runs on every node for every vault: each node purges
// its staging copies of released segments and confirms the release sequence
// the purge covered, and the vault-ctl leader prunes the released-segment
// tombstones every member of the group has confirmed. Every node takes part
// because every node is an origin for every vault and so may hold a released
// segment in completed/.
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
	covered, purged, err := o.purgeReleasedSegmentStaging(vaultID, fsm)
	if purged > 0 {
		log.Info("released-segment sweep: purged released segments' staging copies", "segments", purged)
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
