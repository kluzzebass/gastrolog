package vaultctlfsm

import (
	"maps"
	"slices"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
)

// A chunk tombstone outlives the manifest entry of a deleted chunk for as long
// as some node may still hold the chunk's bytes. Every placement member acks a
// delete before it finalizes, but a node outside the expected-acks set — one
// pruned from it, one that left the placement before the delete, one that
// pulled a copy late — can still have the chunk in its chunk store or chunk
// staging, and only the tombstone lets that node delete the bytes instead of
// raising them as an unknown orphan. While the tombstone lives it also refuses
// a create, open-chunk manifest, or repatriation naming the chunk, and defers
// a retention transfer that would re-introduce it.
//
// Each node's chunk tombstone sweep purges every tombstoned chunk it holds and
// then confirms the delete sequence the sweep covered (ConfirmChunkPurge). The
// vault-ctl leader prunes the tombstones every member of its Raft
// configuration has confirmed (PruneChunkTombstones), so the tombstone set is
// bounded by the deletes some member has not swept yet.

// IsTombstoned reports whether a chunk was deleted and its tombstone has not
// been pruned yet.
func (f *FSM) IsTombstoned(id chunk.ChunkID) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, ok := f.tombstones[id]
	return ok
}

// ChunkTombstoneCount returns the number of chunk tombstones.
func (f *FSM) ChunkTombstoneCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.tombstones)
}

// DeleteSeq returns the delete sequence of the latest apply that tombstoned a
// chunk.
func (f *FSM) DeleteSeq() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.deletes.seq
}

// ChunkPurgeConfirmed returns the highest delete sequence nodeID has confirmed
// purging through, or 0 when it has confirmed nothing.
func (f *FSM) ChunkPurgeConfirmed(nodeID string) uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.deletes.confirmed[nodeID]
}

// ChunkTombstonesPrunable reports whether a PruneChunkTombstones naming members
// would change anything: a tombstone every member has confirmed, or a chunk
// purge confirmation from a node outside members.
func (f *FSM) ChunkTombstonesPrunable(members []string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return prunable(&f.deletes, f.tombstones, members)
}

// tombstoneChunksLocked tombstones ids at the next delete sequence. A chunk
// already tombstoned keeps its sequence, and the sequence advances only when
// the apply tombstones a chunk. Caller holds f.mu.
func (f *FSM) tombstoneChunksLocked(ids ...chunk.ChunkID) {
	seq := f.deletes.seq + 1
	for _, id := range ids {
		if _, dead := f.tombstones[id]; dead {
			continue
		}
		f.tombstones[id] = seq
		f.deletes.seq = seq
	}
}

func (f *FSM) applyConfirmChunkPurge(c *gastrologv1.ConfirmChunkPurgeCommand) error {
	return f.deletes.confirm(c.GetNodeId(), c.GetDeleteSeq())
}

func (f *FSM) applyPruneChunkTombstones(c *gastrologv1.PruneChunkTombstonesCommand) error {
	return prune(&f.deletes, f.tombstones, c.GetMembers())
}

// snapshotChunkTombstonesLocked emits the tombstones in chunk-ID order and the
// chunk purge confirmations in node-ID order. Caller holds f.mu.
func (f *FSM) snapshotChunkTombstonesLocked(snap *gastrologv1.VaultCtlSnapshot) {
	snap.DeleteSeq = f.deletes.seq
	snap.Tombstones = make([]*gastrologv1.Tombstone, 0, len(f.tombstones))
	for _, id := range slices.SortedFunc(maps.Keys(f.tombstones), chunk.ChunkID.Compare) {
		idCopy := id
		snap.Tombstones = append(snap.Tombstones, &gastrologv1.Tombstone{
			ChunkId:   idCopy[:],
			DeleteSeq: f.tombstones[id],
		})
	}
	f.deletes.eachConfirmation(func(nodeID string, seq uint64) {
		snap.ChunkPurgeConfirmations = append(snap.ChunkPurgeConfirmations, &gastrologv1.ChunkPurgeConfirmation{
			NodeId:    nodeID,
			DeleteSeq: seq,
		})
	})
}

// restoreChunkTombstonesLocked replaces the tombstones and chunk purge
// confirmations from a snapshot. Caller holds f.mu.
func (f *FSM) restoreChunkTombstonesLocked(snap *gastrologv1.VaultCtlSnapshot) {
	f.tombstones = make(map[chunk.ChunkID]uint64, len(snap.GetTombstones()))
	for _, ts := range snap.GetTombstones() {
		f.tombstones[chunkIDFromProto(ts.GetChunkId())] = ts.GetDeleteSeq()
	}
	f.deletes.reset(snap.GetDeleteSeq(), len(snap.GetChunkPurgeConfirmations()))
	for _, pc := range snap.GetChunkPurgeConfirmations() {
		f.deletes.confirmed[pc.GetNodeId()] = pc.GetDeleteSeq()
	}
}

// NewConfirmChunkPurge builds a ConfirmChunkPurge command.
func NewConfirmChunkPurge(nodeID string, deleteSeq uint64) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_ConfirmChunkPurge{
		ConfirmChunkPurge: &gastrologv1.ConfirmChunkPurgeCommand{NodeId: nodeID, DeleteSeq: deleteSeq},
	}}
}

// MarshalConfirmChunkPurge builds Raft log data for ConfirmChunkPurge.
func MarshalConfirmChunkPurge(nodeID string, deleteSeq uint64) []byte {
	return mustMarshalCommand(NewConfirmChunkPurge(nodeID, deleteSeq))
}

// NewPruneChunkTombstones builds a PruneChunkTombstones command.
func NewPruneChunkTombstones(members []string) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_PruneChunkTombstones{
		PruneChunkTombstones: &gastrologv1.PruneChunkTombstonesCommand{Members: slices.Clone(members)},
	}}
}

// MarshalPruneChunkTombstones builds Raft log data for PruneChunkTombstones.
func MarshalPruneChunkTombstones(members []string) []byte {
	return mustMarshalCommand(NewPruneChunkTombstones(members))
}
