package vaultctlfsm

import (
	"errors"
	"maps"
	"slices"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
)

// A released-segment tombstone outlives the registry entry for as long as
// some node may still hold the segment's bytes in staging: a node that missed
// the release (offline, or caught up by snapshot install) still has the
// segment in completed/ and republishes it on restart, and only the tombstone
// stops that publish from re-registering records that already live in a
// chunk. Each node's released-segment sweep purges every tombstoned segment it
// finds and then confirms the release sequence the sweep covered
// (ConfirmSegmentPurge). The vault-ctl leader prunes the tombstones every
// member of its Raft configuration has confirmed (PruneReleasedSegments), so
// the tombstone set is bounded by the releases some member has not swept yet.

// ReleaseSeq returns the release sequence of the latest ReleaseSegments apply
// that tombstoned a segment.
func (f *FSM) ReleaseSeq() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.releaseSeq
}

// ReleasedSegmentCount returns the number of released-segment tombstones.
func (f *FSM) ReleasedSegmentCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.releasedSegments)
}

// SegmentPurgeConfirmed returns the highest release sequence nodeID has
// confirmed purging through, or 0 when it has confirmed nothing.
func (f *FSM) SegmentPurgeConfirmed(nodeID string) uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.segmentPurgeConfirmed[nodeID]
}

// ReleasedSegmentsPrunable reports whether a PruneReleasedSegments naming
// members would change anything: a tombstone every member has confirmed, or a
// purge confirmation from a node outside members.
func (f *FSM) ReleasedSegmentsPrunable(members []string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	floor := f.purgeFloorLocked(members)
	for _, seq := range f.releasedSegments {
		if seq <= floor {
			return true
		}
	}
	for nodeID := range f.segmentPurgeConfirmed {
		if !slices.Contains(members, nodeID) {
			return true
		}
	}
	return false
}

// purgeFloorLocked is the release sequence every node in members has
// confirmed purging through. A member that has confirmed nothing holds it at
// 0. Caller holds f.mu.
func (f *FSM) purgeFloorLocked(members []string) uint64 {
	floor := f.releaseSeq
	for _, nodeID := range members {
		floor = min(floor, f.segmentPurgeConfirmed[nodeID])
	}
	return floor
}

func (f *FSM) applyConfirmSegmentPurge(c *gastrologv1.ConfirmSegmentPurgeCommand) error {
	nodeID := c.GetNodeId()
	if nodeID == "" {
		return errors.New("confirm segment purge: node id required")
	}
	// A node can only have swept releases its own FSM applied, all of which
	// precede this command in the log.
	seq := min(c.GetReleaseSeq(), f.releaseSeq)
	if seq > f.segmentPurgeConfirmed[nodeID] {
		if f.segmentPurgeConfirmed == nil {
			f.segmentPurgeConfirmed = make(map[string]uint64)
		}
		f.segmentPurgeConfirmed[nodeID] = seq
	}
	return nil
}

func (f *FSM) applyPruneReleasedSegments(c *gastrologv1.PruneReleasedSegmentsCommand) error {
	members := c.GetMembers()
	if len(members) == 0 {
		return errors.New("prune released segments: members required")
	}
	floor := f.purgeFloorLocked(members)
	maps.DeleteFunc(f.releasedSegments, func(_ glid.GLID, seq uint64) bool {
		return seq <= floor
	})
	maps.DeleteFunc(f.segmentPurgeConfirmed, func(nodeID string, _ uint64) bool {
		return !slices.Contains(members, nodeID)
	})
	return nil
}

// snapshotReleasedSegmentsLocked emits the tombstones in segment-ID order and
// the purge confirmations in node-ID order. Caller holds f.mu.
func (f *FSM) snapshotReleasedSegmentsLocked(snap *gastrologv1.VaultCtlSnapshot) {
	snap.ReleaseSeq = f.releaseSeq
	for _, id := range slices.SortedFunc(maps.Keys(f.releasedSegments), glid.Compare) {
		idCopy := id
		snap.ReleasedSegments = append(snap.ReleasedSegments, &gastrologv1.ReleasedSegment{
			SegmentId:  idCopy[:],
			ReleaseSeq: f.releasedSegments[id],
		})
	}
	for _, nodeID := range slices.Sorted(maps.Keys(f.segmentPurgeConfirmed)) {
		snap.SegmentPurgeConfirmations = append(snap.SegmentPurgeConfirmations, &gastrologv1.SegmentPurgeConfirmation{
			NodeId:     nodeID,
			ReleaseSeq: f.segmentPurgeConfirmed[nodeID],
		})
	}
}

// restoreReleasedSegmentsLocked replaces the tombstones and purge
// confirmations from a snapshot. Caller holds f.mu.
func (f *FSM) restoreReleasedSegmentsLocked(snap *gastrologv1.VaultCtlSnapshot) {
	f.releaseSeq = snap.GetReleaseSeq()
	f.releasedSegments = make(map[glid.GLID]uint64, len(snap.GetReleasedSegments()))
	for _, rs := range snap.GetReleasedSegments() {
		f.releasedSegments[glid.FromBytes(rs.GetSegmentId())] = rs.GetReleaseSeq()
	}
	f.segmentPurgeConfirmed = make(map[string]uint64, len(snap.GetSegmentPurgeConfirmations()))
	for _, pc := range snap.GetSegmentPurgeConfirmations() {
		f.segmentPurgeConfirmed[pc.GetNodeId()] = pc.GetReleaseSeq()
	}
}

// NewConfirmSegmentPurge builds a ConfirmSegmentPurge command.
func NewConfirmSegmentPurge(nodeID string, releaseSeq uint64) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_ConfirmSegmentPurge{
		ConfirmSegmentPurge: &gastrologv1.ConfirmSegmentPurgeCommand{NodeId: nodeID, ReleaseSeq: releaseSeq},
	}}
}

// MarshalConfirmSegmentPurge builds Raft log data for ConfirmSegmentPurge.
func MarshalConfirmSegmentPurge(nodeID string, releaseSeq uint64) []byte {
	return mustMarshalCommand(NewConfirmSegmentPurge(nodeID, releaseSeq))
}

// NewPruneReleasedSegments builds a PruneReleasedSegments command.
func NewPruneReleasedSegments(members []string) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_PruneReleasedSegments{
		PruneReleasedSegments: &gastrologv1.PruneReleasedSegmentsCommand{Members: slices.Clone(members)},
	}}
}

// MarshalPruneReleasedSegments builds Raft log data for PruneReleasedSegments.
func MarshalPruneReleasedSegments(members []string) []byte {
	return mustMarshalCommand(NewPruneReleasedSegments(members))
}
