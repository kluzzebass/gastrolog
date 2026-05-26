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
	ErrVaultSeqRequired = errors.New("spool: record missing vault_seq")
	ErrWindowNotFound   = errors.New("spool: no window covers vault_seq")
	ErrSeqOutOfWindow   = errors.New("spool: vault_seq outside window bounds")
	ErrWindowSealed     = errors.New("spool: window is sealed")
)

// Manager holds in-memory spool sequence windows keyed by allocator swath bounds.
type Manager struct {
	mu      sync.RWMutex
	windows map[spool.WindowID]*window
}

type window struct {
	meta  spool.SegmentMeta
	slots map[uint64]chunk.Record
}

// NewManager returns an empty spool manager.
func NewManager() *Manager {
	return &Manager{windows: make(map[spool.WindowID]*window)}
}

// EnsureWindow creates a writable sequence window when absent.
func (m *Manager) EnsureWindow(start, end uint64) error {
	if start == 0 || end == 0 || start > end {
		return fmt.Errorf("spool: invalid window bounds %d..%d", start, end)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	id := spool.WindowID{Start: start, End: end}
	if _, ok := m.windows[id]; ok {
		return nil
	}
	m.windows[id] = &window{
		meta: spool.SegmentMeta{
			ID:       spool.SegmentID(start),
			Window:   id,
			FirstSeq: start,
			EndSeq:   end,
		},
		slots: make(map[uint64]chunk.Record),
	}
	return nil
}

// PutSlot stores one record at rec.VaultSeq inside its covering window.
func (m *Manager) PutSlot(rec chunk.Record) error {
	if rec.VaultSeq == 0 {
		return ErrVaultSeqRequired
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	win := m.windowForSeqLocked(rec.VaultSeq)
	if win == nil {
		return fmt.Errorf("%w: seq %d", ErrWindowNotFound, rec.VaultSeq)
	}
	if win.meta.Sealed {
		return ErrWindowSealed
	}
	if rec.VaultSeq < win.meta.FirstSeq || rec.VaultSeq > win.meta.EndSeq {
		return ErrSeqOutOfWindow
	}
	if _, had := win.slots[rec.VaultSeq]; !had {
		win.meta.RecordCount++
	}
	win.slots[rec.VaultSeq] = rec.Copy()
	if rec.VaultSeq > win.meta.LastSeq {
		win.meta.LastSeq = rec.VaultSeq
	}
	return nil
}

func (m *Manager) windowForSeqLocked(seq uint64) *window {
	for _, win := range m.windows {
		if seq >= win.meta.FirstSeq && seq <= win.meta.EndSeq {
			return win
		}
	}
	return nil
}

// SealWindow marks a window immutable.
func (m *Manager) SealWindow(id spool.WindowID) (spool.SegmentMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	win, ok := m.windows[id]
	if !ok {
		return spool.SegmentMeta{}, spool.ErrSegmentNotFound
	}
	if win.meta.RecordCount == 0 {
		return spool.SegmentMeta{}, errors.New("spool: window empty")
	}
	win.meta.Sealed = true
	return win.meta, nil
}

// Meta returns metadata for a window.
func (m *Manager) Meta(id spool.WindowID) (spool.SegmentMeta, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	win, ok := m.windows[id]
	if !ok {
		return spool.SegmentMeta{}, false
	}
	return win.meta, true
}

// ListWindows returns window metadata sorted by start seq ascending.
func (m *Manager) ListWindows() []spool.SegmentMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]spool.SegmentMeta, 0, len(m.windows))
	for _, win := range m.windows {
		out = append(out, win.meta)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].FirstSeq < out[j].FirstSeq
	})
	return out
}

// Close releases in-memory spool state.
func (m *Manager) Close() error { return nil }

// DurableWatermark returns the highest vault_seq durably present in spool (S_r).
func (m *Manager) DurableWatermark() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var maxSeq uint64
	for _, win := range m.windows {
		for seq := range win.slots {
			if seq > maxSeq {
				maxSeq = seq
			}
		}
	}
	return maxSeq
}

// ReadByVaultSeq returns the record with the given acceptance sequence if present.
func (m *Manager) ReadByVaultSeq(seq uint64) (chunk.Record, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	win := m.windowForSeqLocked(seq)
	if win == nil {
		return chunk.Record{}, false
	}
	rec, ok := win.slots[seq]
	if !ok {
		return chunk.Record{}, false
	}
	return rec.Copy(), true
}

// LookupEventID scans spool windows for a prior assignment of eventID.
func (m *Manager) LookupEventID(id chunk.EventID) (uint64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var latest uint64
	found := false
	for _, win := range m.windows {
		for seq, rec := range win.slots {
			if rec.EventID == id {
				if !found || seq > latest {
					latest = seq
					found = true
				}
			}
		}
	}
	return latest, found
}
