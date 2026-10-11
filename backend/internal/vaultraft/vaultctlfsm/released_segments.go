package vaultctlfsm

import (
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
	return f.releases.seq
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
	return f.releases.confirmed[nodeID]
}

// ReleasedSegmentsPrunable reports whether a PruneReleasedSegments naming
// members would change anything: a tombstone every member has confirmed, or a
// purge confirmation from a node outside members.
func (f *FSM) ReleasedSegmentsPrunable(members []string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return prunable(&f.releases, f.releasedSegments, members)
}

func (f *FSM) applyConfirmSegmentPurge(c *gastrologv1.ConfirmSegmentPurgeCommand) error {
	return f.releases.confirm(c.GetNodeId(), c.GetReleaseSeq())
}

func (f *FSM) applyPruneReleasedSegments(c *gastrologv1.PruneReleasedSegmentsCommand) error {
	return prune(&f.releases, f.releasedSegments, c.GetMembers())
}

// snapshotReleasedSegmentsLocked emits the tombstones in segment-ID order and
// the purge confirmations in node-ID order. Caller holds f.mu.
func (f *FSM) snapshotReleasedSegmentsLocked(snap *gastrologv1.VaultCtlSnapshot) {
	snap.ReleaseSeq = f.releases.seq
	for _, id := range slices.SortedFunc(maps.Keys(f.releasedSegments), glid.Compare) {
		idCopy := id
		snap.ReleasedSegments = append(snap.ReleasedSegments, &gastrologv1.ReleasedSegment{
			SegmentId:  idCopy[:],
			ReleaseSeq: f.releasedSegments[id],
		})
	}
	f.releases.eachConfirmation(func(nodeID string, seq uint64) {
		snap.SegmentPurgeConfirmations = append(snap.SegmentPurgeConfirmations, &gastrologv1.SegmentPurgeConfirmation{
			NodeId:     nodeID,
			ReleaseSeq: seq,
		})
	})
}

// restoreReleasedSegmentsLocked replaces the tombstones and purge
// confirmations from a snapshot. Caller holds f.mu.
func (f *FSM) restoreReleasedSegmentsLocked(snap *gastrologv1.VaultCtlSnapshot) {
	f.releasedSegments = make(map[glid.GLID]uint64, len(snap.GetReleasedSegments()))
	for _, rs := range snap.GetReleasedSegments() {
		f.releasedSegments[glid.FromBytes(rs.GetSegmentId())] = rs.GetReleaseSeq()
	}
	f.releases.reset(snap.GetReleaseSeq(), len(snap.GetSegmentPurgeConfirmations()))
	for _, pc := range snap.GetSegmentPurgeConfirmations() {
		f.releases.confirmed[pc.GetNodeId()] = pc.GetReleaseSeq()
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
