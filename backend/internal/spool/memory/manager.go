package memory

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"gastrolog/internal/chunk"
	"gastrolog/internal/spool"
)

var (
	ErrVaultSeqRequired   = errors.New("spool: record missing vault_seq")
	ErrSegmentSealed      = errors.New("spool: segment is sealed")
	ErrNoActiveSegment    = errors.New("spool: no active segment")
	ErrActiveSegmentEmpty = errors.New("spool: active segment empty")
)

// Manager holds in-memory spool segments keyed by first_seq identity.
type Manager struct {
	mu       sync.RWMutex
	segments map[spool.SegmentID]*segment
	active   spool.SegmentID
}

type segment struct {
	meta    spool.SegmentMeta
	records []chunk.Record
}

// NewManager returns an empty spool manager.
func NewManager() *Manager {
	return &Manager{segments: make(map[spool.SegmentID]*segment)}
}

// Append stores a record in the active segment, opening one keyed by record.VaultSeq when needed.
func (m *Manager) Append(rec chunk.Record) (spool.SegmentMeta, error) {
	if rec.VaultSeq == 0 {
		return spool.SegmentMeta{}, ErrVaultSeqRequired
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	seg := m.activeSegmentLocked(rec.VaultSeq)
	if seg.meta.Sealed {
		return spool.SegmentMeta{}, ErrSegmentSealed
	}
	if len(seg.records) > 0 && rec.VaultSeq < seg.meta.LastSeq {
		return spool.SegmentMeta{}, fmt.Errorf("spool: vault_seq %d precedes segment last %d", rec.VaultSeq, seg.meta.LastSeq)
	}
	stored := rec.Copy()
	seg.records = append(seg.records, stored)
	seg.meta.RecordCount++
	seg.meta.LastSeq = rec.VaultSeq
	return seg.meta, nil
}

func (m *Manager) activeSegmentLocked(firstSeq uint64) *segment {
	id := spool.SegmentID(firstSeq)
	if m.active != 0 {
		if seg, ok := m.segments[m.active]; ok && !seg.meta.Sealed {
			return seg
		}
	}
	if seg, ok := m.segments[id]; ok && !seg.meta.Sealed {
		m.active = id
		return seg
	}
	seg := &segment{
		meta: spool.SegmentMeta{
			ID:       id,
			FirstSeq: firstSeq,
			LastSeq:  firstSeq,
		},
	}
	m.segments[id] = seg
	m.active = id
	return seg
}

// SealActive marks the active segment immutable and clears the active pointer.
func (m *Manager) SealActive() (spool.SegmentMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active == 0 {
		return spool.SegmentMeta{}, ErrNoActiveSegment
	}
	seg := m.segments[m.active]
	if seg == nil || seg.meta.RecordCount == 0 {
		return spool.SegmentMeta{}, ErrActiveSegmentEmpty
	}
	seg.meta.Sealed = true
	meta := seg.meta
	m.active = 0
	return meta, nil
}

// Meta returns metadata for a segment by first_seq identity.
func (m *Manager) Meta(id spool.SegmentID) (spool.SegmentMeta, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	seg, ok := m.segments[id]
	if !ok {
		return spool.SegmentMeta{}, false
	}
	return seg.meta, true
}

// ListSegments returns segment metadata sorted by first_seq ascending.
func (m *Manager) ListSegments() []spool.SegmentMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]spool.SegmentMeta, 0, len(m.segments))
	for _, seg := range m.segments {
		out = append(out, seg.meta)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].FirstSeq < out[j].FirstSeq
	})
	return out
}

// ReadByVaultSeq returns the record with the given acceptance sequence if present.
func (m *Manager) ReadByVaultSeq(seq uint64) (chunk.Record, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, seg := range m.segments {
		if !seg.meta.CoversSeq(seq) {
			continue
		}
		for _, rec := range seg.records {
			if rec.VaultSeq == seq {
				return rec.Copy(), true
			}
		}
	}
	return chunk.Record{}, false
}
