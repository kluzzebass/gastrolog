package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/spool"
	spoolmem "gastrolog/internal/spool/memory"
)

var (
	// ErrSpoolSeqConflict is returned when a replica reports a different
	// seq for an EventID that was already assigned on this node.
	ErrSpoolSeqConflict = errors.New("spool: EventID seq conflict")
	// ErrSpoolMissingEventID is returned when a sequenced write lacks EventID.
	ErrSpoolMissingEventID = errors.New("spool: record missing EventID")
	// ErrSpoolMissingVaultSeq is returned when assignment metadata lacks vault_seq.
	ErrSpoolMissingVaultSeq = errors.New("spool: missing vault_seq for EventID")
	// ErrSpoolStoreUnavailable is returned when the spool store is nil.
	ErrSpoolStoreUnavailable = errors.New("spool store unavailable")
)

// vaultSpoolStore holds destination-vault spool bytes plus accepted-write
// metadata (EventID dedup index and ingest high watermark H).
type vaultSpoolStore struct {
	vaultID   glid.GLID
	store     spool.Store
	mu        sync.RWMutex
	byEventID map[chunk.EventID]uint64
	bySeq     map[uint64]chunk.Record
	ingestH   uint64
	// materializationH is M_r — highest vault_seq fully materialized locally.
	materializationH uint64
	// convergenceH is C_r — highest fence upper bound converge-sealed locally.
	convergenceH uint64
}

func newVaultSpoolStore(vaultID glid.GLID, store spool.Store) *vaultSpoolStore {
	return &vaultSpoolStore{
		vaultID:   vaultID,
		store:     store,
		byEventID: make(map[chunk.EventID]uint64),
		bySeq:     make(map[uint64]chunk.Record),
	}
}

func (v *Vault) ensureSpoolStore(o *Orchestrator) *vaultSpoolStore {
	if v.spool == nil {
		v.spool = o.createVaultSpoolStore(v)
	}
	return v.spool
}

func (o *Orchestrator) vaultSpoolStore(vaultID glid.GLID) *vaultSpoolStore {
	if v := o.vaults[vaultID]; v != nil {
		return v.ensureSpoolStore(o)
	}
	return newVaultSpoolStore(vaultID, spoolmem.NewManager())
}

// ReadVaultSpoolSeq reads one accepted or tentative spool slot by VaultSeq on
// this node. Used by cluster write-path gate tests and inspect tooling.
func (o *Orchestrator) ReadVaultSpoolSeq(vaultID glid.GLID, seq uint64) (chunk.Record, error) {
	ss := o.vaultSpoolStore(vaultID)
	if ss == nil {
		return chunk.Record{}, ErrSpoolStoreUnavailable
	}
	return ss.ReadByVaultSeq(context.Background(), vaultID, seq)
}

// VaultSpoolIngestH returns ingest high watermark H for a vault on this node.
func (o *Orchestrator) VaultSpoolIngestH(vaultID glid.GLID) uint64 {
	ss := o.vaultSpoolStore(vaultID)
	if ss == nil {
		return 0
	}
	return ss.IngestHighWatermark()
}

// LookupSeq returns a previously assigned destination sequence for eventID,
// including tentative spool assignments not yet accepted into H.
func (s *vaultSpoolStore) LookupSeq(eventID chunk.EventID) (uint64, bool) {
	if s == nil {
		return 0, false
	}
	s.mu.RLock()
	seq, ok := s.byEventID[eventID]
	s.mu.RUnlock()
	if ok {
		return seq, true
	}
	return s.store.LookupEventID(eventID)
}

// AppendTentative durably stores one vault_seq slot in spool without advancing H.
func (s *vaultSpoolStore) AppendTentative(rec chunk.Record) error {
	if rec.EventID == (chunk.EventID{}) {
		return ErrSpoolMissingEventID
	}
	if rec.VaultSeq == 0 {
		return ErrSpoolMissingVaultSeq
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkSeqAssignmentLocked(rec); err != nil {
		return err
	}
	return s.store.PutSlot(rec)
}

// PutReplicaWrite stores a follower-originated spool append.
func (s *vaultSpoolStore) PutReplicaWrite(rec chunk.Record) error {
	return s.AppendTentative(rec)
}

// EnsureSwathWindow ensures the local spool contains a window for allocator swath bounds.
func (s *vaultSpoolStore) EnsureSwathWindow(start, end uint64) error {
	if s == nil || s.store == nil {
		return ErrSpoolStoreUnavailable
	}
	return s.store.EnsureWindow(start, end)
}

func (s *vaultSpoolStore) checkSeqAssignmentLocked(rec chunk.Record) error {
	if prev, ok := s.bySeq[rec.VaultSeq]; ok && prev.EventID != rec.EventID {
		return ErrSpoolSeqConflict
	}
	return nil
}

// CommitAcceptance records W-of-N durable success: persists accepted metadata
// and advances H to seq when seq exceeds the prior frontier.
func (s *vaultSpoolStore) CommitAcceptance(rec chunk.Record) error {
	if rec.EventID == (chunk.EventID{}) {
		return ErrSpoolMissingEventID
	}
	if rec.VaultSeq == 0 {
		return ErrSpoolMissingVaultSeq
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkSeqAssignmentLocked(rec); err != nil {
		return err
	}
	stored := rec.Copy()
	s.byEventID[rec.EventID] = rec.VaultSeq
	s.bySeq[rec.VaultSeq] = stored
	if rec.VaultSeq > s.ingestH {
		s.ingestH = rec.VaultSeq
	}
	return nil
}

// IngestHighWatermark returns H — highest accepted vault_seq on this node.
func (s *vaultSpoolStore) IngestHighWatermark() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ingestH
}

// SpoolDurableWatermark returns S_r — highest vault_seq durably present in spool.
func (s *vaultSpoolStore) SpoolDurableWatermark() uint64 {
	if s == nil || s.store == nil {
		return 0
	}
	return s.store.DurableWatermark()
}

// MaterializationWatermark returns M_r — highest vault_seq fully materialized locally.
func (s *vaultSpoolStore) MaterializationWatermark() uint64 {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.materializationH
}

// setMaterializationWatermark advances M_r monotonically.
func (s *vaultSpoolStore) setMaterializationWatermark(seq uint64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if seq > s.materializationH {
		s.materializationH = seq
	}
	s.mu.Unlock()
}

// ConvergenceWatermark returns C_r — highest fence upper bound converge-sealed locally.
func (s *vaultSpoolStore) ConvergenceWatermark() uint64 {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.convergenceH
}

func (s *vaultSpoolStore) setConvergenceWatermark(seq uint64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if seq > s.convergenceH {
		s.convergenceH = seq
	}
	s.mu.Unlock()
}

// ReadByVaultSeq implements query.SpoolAnchorReader.
func (s *vaultSpoolStore) ReadByVaultSeq(_ context.Context, _ glid.GLID, seq uint64) (chunk.Record, error) {
	if s == nil {
		return chunk.Record{}, ErrSpoolStoreUnavailable
	}
	s.mu.RLock()
	if rec, ok := s.bySeq[seq]; ok {
		s.mu.RUnlock()
		return rec.Copy(), nil
	}
	s.mu.RUnlock()
	if rec, ok := s.store.ReadByVaultSeq(seq); ok {
		return rec, nil
	}
	return chunk.Record{}, fmt.Errorf("spool: seq %d not found", seq)
}

func (s *vaultSpoolStore) close() error {
	if s == nil || s.store == nil {
		return nil
	}
	return s.store.Close()
}
