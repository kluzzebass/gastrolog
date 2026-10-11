package vaultctlfsm

import (
	"errors"
	"maps"
	"slices"
)

// tombstoneLedger orders one kind of tombstone and records how far each node
// has confirmed purging its copies of the tombstoned things. Every apply that
// tombstones something advances seq and stamps the new tombstones with it; a
// node's confirmation says it holds no copy of anything tombstoned at or
// before the sequence it names. A tombstone is pruned once every member of the
// vault-ctl Raft configuration has confirmed past it. Sequences and member
// lists come from the log, so every replica prunes the same tombstones at the
// same index.
type tombstoneLedger struct {
	seq       uint64
	confirmed map[string]uint64
}

var errConfirmNodeRequired = errors.New("purge confirmation: node id required")

var errPruneMembersRequired = errors.New("tombstone prune: members required")

// confirm records that nodeID has purged everything tombstoned through seq.
// A node can only have purged tombstones its own FSM applied, all of which
// precede the confirmation in the log, so seq is clamped to the ledger's.
// Confirmations never move backwards.
func (l *tombstoneLedger) confirm(nodeID string, seq uint64) error {
	if nodeID == "" {
		return errConfirmNodeRequired
	}
	seq = min(seq, l.seq)
	if seq > l.confirmed[nodeID] {
		if l.confirmed == nil {
			l.confirmed = make(map[string]uint64)
		}
		l.confirmed[nodeID] = seq
	}
	return nil
}

// floor is the sequence every node in members has confirmed purging through.
// A member that has confirmed nothing holds it at 0.
func (l *tombstoneLedger) floor(members []string) uint64 {
	floor := l.seq
	for _, nodeID := range members {
		floor = min(floor, l.confirmed[nodeID])
	}
	return floor
}

// prunable reports whether a prune naming members would change anything: a
// tombstone every member has confirmed, or a confirmation from a node outside
// members.
func prunable[K comparable](l *tombstoneLedger, tombstones map[K]uint64, members []string) bool {
	floor := l.floor(members)
	for _, seq := range tombstones {
		if seq <= floor {
			return true
		}
	}
	for nodeID := range l.confirmed {
		if !slices.Contains(members, nodeID) {
			return true
		}
	}
	return false
}

// prune drops the tombstones every node in members has confirmed and the
// confirmations of nodes no longer in members.
func prune[K comparable](l *tombstoneLedger, tombstones map[K]uint64, members []string) error {
	if len(members) == 0 {
		return errPruneMembersRequired
	}
	floor := l.floor(members)
	maps.DeleteFunc(tombstones, func(_ K, seq uint64) bool {
		return seq <= floor
	})
	maps.DeleteFunc(l.confirmed, func(nodeID string, _ uint64) bool {
		return !slices.Contains(members, nodeID)
	})
	return nil
}

// eachConfirmation visits the confirmations in node-ID order.
func (l *tombstoneLedger) eachConfirmation(fn func(nodeID string, seq uint64)) {
	for _, nodeID := range slices.Sorted(maps.Keys(l.confirmed)) {
		fn(nodeID, l.confirmed[nodeID])
	}
}

// reset replaces the ledger with a snapshot's sequence and no
// confirmations; the caller adds the snapshot's confirmations.
func (l *tombstoneLedger) reset(seq uint64, confirmations int) {
	l.seq = seq
	l.confirmed = make(map[string]uint64, confirmations)
}
