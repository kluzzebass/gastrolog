package orchestrator

import (
	"context"
	"errors"
	"fmt"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

var (
	// ErrInterimSeqConflict is returned when a replica reports a different
	// seq for an EventID that was already assigned on this node.
	ErrInterimSeqConflict = errors.New("interim seq: EventID seq conflict")
	// ErrInterimMissingEventID is returned when a sequenced write lacks EventID.
	ErrInterimMissingEventID = errors.New("interim seq: record missing EventID")
	// ErrInterimMissingVaultSeq is returned when assignment metadata lacks vault_seq.
	ErrInterimMissingVaultSeq = errors.New("interim seq: missing vault_seq for EventID")
	// ErrInterimStoreUnavailable is returned when the interim store is nil.
	ErrInterimStoreUnavailable = errors.New("interim seq store unavailable")
)

// interimSeqStore holds Phase 2 destination-vault (EventID, seq)
// assignments until Phase 3 spool migration. One store per vault shell.
type interimSeqStore struct {
	byEventID map[chunk.EventID]uint64
	bySeq     map[uint64]chunk.Record
}

func newInterimSeqStore() *interimSeqStore {
	return &interimSeqStore{
		byEventID: make(map[chunk.EventID]uint64),
		bySeq:     make(map[uint64]chunk.Record),
	}
}

func (v *Vault) ensureInterimSeqStore() *interimSeqStore {
	if v.interimSeq == nil {
		v.interimSeq = newInterimSeqStore()
	}
	return v.interimSeq
}

func (o *Orchestrator) vaultInterimSeqStore(vaultID glid.GLID) *interimSeqStore {
	v := o.vaults[vaultID]
	if v == nil {
		return newInterimSeqStore()
	}
	return v.ensureInterimSeqStore()
}

// LookupSeq returns a previously assigned destination sequence for eventID.
func (s *interimSeqStore) LookupSeq(eventID chunk.EventID) (uint64, bool) {
	if s == nil {
		return 0, false
	}
	seq, ok := s.byEventID[eventID]
	return seq, ok
}

// PutLeaderAssignment stores a leader-originated assignment. Idempotent when
// the same EventID maps to the same seq.
func (s *interimSeqStore) PutLeaderAssignment(rec chunk.Record) error {
	if rec.EventID == (chunk.EventID{}) {
		return ErrInterimMissingEventID
	}
	if rec.VaultSeq == 0 {
		return ErrInterimMissingVaultSeq
	}
	if existing, ok := s.byEventID[rec.EventID]; ok {
		if existing != rec.VaultSeq {
			return ErrInterimSeqConflict
		}
		return nil
	}
	if prev, ok := s.bySeq[rec.VaultSeq]; ok && prev.EventID != rec.EventID {
		return ErrInterimSeqConflict
	}
	stored := rec.Copy()
	s.byEventID[rec.EventID] = rec.VaultSeq
	s.bySeq[rec.VaultSeq] = stored
	return nil
}

// PutReplicaAssignment stores a follower-originated assignment. The seq and
// EventID must match any prior leader assignment on this node.
func (s *interimSeqStore) PutReplicaAssignment(rec chunk.Record) error {
	return s.PutLeaderAssignment(rec)
}

// ReadByVaultSeq implements query.SpoolAnchorReader for interim records.
func (s *interimSeqStore) ReadByVaultSeq(_ context.Context, _ glid.GLID, seq uint64) (chunk.Record, error) {
	if s == nil {
		return chunk.Record{}, ErrInterimStoreUnavailable
	}
	rec, ok := s.bySeq[seq]
	if !ok {
		return chunk.Record{}, fmt.Errorf("interim seq: seq %d not found", seq)
	}
	return rec.Copy(), nil
}
