package memory

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/logging"
)

type Config struct {
	// RotationPolicy determines when to rotate to a new chunk.
	// If nil, defaults to a record count policy of 10000 records.
	RotationPolicy chunk.RotationPolicy

	Now       func() time.Time
	MetaStore chunk.MetaStore

	// Logger for structured logging. If nil, logging is disabled.
	// The manager scopes this logger with component="chunk-manager".
	Logger *slog.Logger
}

// Manager manages in-memory chunk storage.
//
// Logging:
//   - Logger is dependency-injected via Config.Logger
//   - Manager owns its scoped logger (component="chunk-manager", type="memory")
//   - Logging is intentionally sparse; only lifecycle events are logged
//   - No logging in hot paths (Append, cursor iteration)
type Manager struct {
	mu            sync.Mutex
	cfg           Config
	active        *chunkState
	chunks        []*chunkState
	indexBuilders []chunk.ChunkIndexBuilder
	nextChunkID   *chunk.ChunkID // if set, used instead of NewChunkID() on next open

	// Logger for this manager instance.
	// Scoped with component="chunk-manager", type="memory" at construction time.
	logger *slog.Logger
}

type chunkState struct {
	meta      chunk.ChunkMeta
	records   []chunk.Record
	size      int64
	createdAt time.Time // Wall-clock time when chunk was created
}

func NewManager(cfg Config) (*Manager, error) {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.MetaStore == nil {
		cfg.MetaStore = NewMetaStore()
	}
	if cfg.RotationPolicy == nil {
		cfg.RotationPolicy = chunk.NewRecordCountPolicy(10000)
	}

	// Scope logger with component identity.
	logger := logging.Default(cfg.Logger).With("component", "chunk-manager", "type", "memory")

	manager := &Manager{
		cfg:    cfg,
		chunks: make([]*chunkState, 0),
		logger: logger,
	}
	return manager, nil
}

func (m *Manager) Append(record chunk.Record) (chunk.ChunkID, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Ensure we have an active chunk.
	if m.active == nil {
		if err := m.openLocked(); err != nil {
			return chunk.ChunkID{}, 0, err
		}
	}

	// Check rotation policy before append.
	state := m.activeChunkState()
	if trigger := m.cfg.RotationPolicy.ShouldRotate(state, record); trigger != nil {
		m.logger.Info("rotating chunk",
			"trigger", *trigger,
			"chunk", state.ChunkID.String(),
			"bytes", state.Bytes,
			"records", state.Records,
			"age", m.cfg.Now().Sub(state.CreatedAt),
		)
		if err := m.sealLocked(); err != nil {
			return chunk.ChunkID{}, 0, err
		}
		if err := m.openLocked(); err != nil {
			return chunk.ChunkID{}, 0, err
		}
	}

	// WriteTS is always assigned by the chunk manager (chunk-local).
	// Monotonic by construction since writes are mutex-serialized.
	record.WriteTS = m.cfg.Now()

	offset := uint64(len(m.active.records))
	m.active.records = append(m.active.records, record)

	// Approximate payload size: raw log line + attribute content.
	recBytes := int64(len(record.Raw))
	for k, v := range record.Attrs {
		recBytes += int64(len(k) + len(v))
	}
	m.active.size += recBytes
	m.active.meta.Bytes = m.active.size
	m.updateMetaLocked(record, int64(len(m.active.records)))

	if err := m.cfg.MetaStore.Save(m.active.meta); err != nil {
		return chunk.ChunkID{}, 0, err
	}

	return m.active.meta.ID, offset, nil
}

// activeChunkState returns an immutable snapshot of the active chunk state.
func (m *Manager) activeChunkState() chunk.ActiveChunkState {
	if m.active == nil {
		return chunk.ActiveChunkState{}
	}
	// For memory manager, Bytes approximates record storage (simplified).
	// In practice, memory chunks don't have the same byte-level concerns as file chunks.
	return chunk.ActiveChunkState{
		ChunkID:     m.active.meta.ID,
		WriteStart:     m.active.meta.WriteStart,
		LastWriteTS: m.active.meta.WriteEnd,
		CreatedAt:   m.active.createdAt,
		Bytes:       uint64(m.active.size), //nolint:gosec // G115: size is always non-negative
		Records:     uint64(len(m.active.records)),
	}
}

func (m *Manager) Seal() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active == nil {
		if err := m.openLocked(); err != nil {
			return err
		}
	}
	return m.sealLocked()
}

func (m *Manager) Active() *chunk.ChunkMeta {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active == nil {
		return nil
	}
	meta := m.active.meta
	return &meta
}

func (m *Manager) Meta(id chunk.ChunkID) (chunk.ChunkMeta, error) {
	m.mu.Lock()
	state := m.findChunkLocked(id)
	m.mu.Unlock()
	if state == nil {
		return chunk.ChunkMeta{}, chunk.ErrChunkNotFound
	}
	return state.meta, nil
}

func (m *Manager) List() ([]chunk.ChunkMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]chunk.ChunkMeta, 0, len(m.chunks))
	for _, state := range m.chunks {
		out = append(out, state.meta)
	}
	return out, nil
}

func (m *Manager) OpenCursor(id chunk.ChunkID) (chunk.RecordCursor, error) {
	m.mu.Lock()
	state := m.findChunkLocked(id)
	m.mu.Unlock()
	if state == nil {
		return nil, chunk.ErrChunkNotFound
	}
	return newRecordReader(state.records, id), nil
}

func (m *Manager) SetNextChunkID(id chunk.ChunkID) {
	m.mu.Lock()
	m.nextChunkID = &id
	m.mu.Unlock()
}

func (m *Manager) openLocked() error {
	var id chunk.ChunkID
	if m.nextChunkID != nil {
		id = *m.nextChunkID
		m.nextChunkID = nil
	} else {
		id = chunk.NewChunkID()
	}
	meta := chunk.ChunkMeta{ID: id}
	if err := m.cfg.MetaStore.Save(meta); err != nil {
		return err
	}
	m.active = &chunkState{
		meta:      meta,
		records:   nil,
		size:      0,
		createdAt: m.cfg.Now(),
	}
	m.chunks = append(m.chunks, m.active)

	return nil
}

func (m *Manager) sealLocked() error {
	if m.active == nil {
		return nil
	}
	m.active.meta.Sealed = true
	if err := m.cfg.MetaStore.Save(m.active.meta); err != nil {
		return err
	}
	m.active = nil
	return nil
}

func (m *Manager) updateMetaLocked(record chunk.Record, recordCount int64) {
	if m.active.meta.WriteStart.IsZero() {
		m.active.meta.WriteStart = record.WriteTS
	}
	m.active.meta.WriteEnd = record.WriteTS
	m.active.meta.RecordCount = recordCount

	// Update IngestTS and SourceTS bounds.
	if m.active.meta.IngestStart.IsZero() || record.IngestTS.Before(m.active.meta.IngestStart) {
		m.active.meta.IngestStart = record.IngestTS
	}
	if m.active.meta.IngestEnd.IsZero() || record.IngestTS.After(m.active.meta.IngestEnd) {
		m.active.meta.IngestEnd = record.IngestTS
	}
	if !record.SourceTS.IsZero() {
		if m.active.meta.SourceStart.IsZero() || record.SourceTS.Before(m.active.meta.SourceStart) {
			m.active.meta.SourceStart = record.SourceTS
		}
		if m.active.meta.SourceEnd.IsZero() || record.SourceTS.After(m.active.meta.SourceEnd) {
			m.active.meta.SourceEnd = record.SourceTS
		}
	}
}

func (m *Manager) findChunkLocked(id chunk.ChunkID) *chunkState {
	for _, state := range m.chunks {
		if state.meta.ID == id {
			return state
		}
	}
	return nil
}

// FindStartPosition binary searches for the record at or before the given timestamp.
// Uses WriteTS for the search since it's monotonically increasing within a chunk.
func (m *Manager) FindStartPosition(id chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	m.mu.Lock()
	state := m.findChunkLocked(id)
	m.mu.Unlock()
	if state == nil {
		return 0, false, chunk.ErrChunkNotFound
	}

	if len(state.records) == 0 {
		return 0, false, nil
	}

	// Quick bounds check.
	if ts.Before(state.records[0].WriteTS) {
		return 0, false, nil
	}

	// Binary search for the latest record with WriteTS <= ts.
	lo, hi := 0, len(state.records)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if state.records[mid].WriteTS.After(ts) {
			hi = mid
		} else {
			lo = mid + 1
		}
	}

	if lo == 0 {
		return 0, false, nil
	}

	return uint64(lo - 1), true, nil //nolint:gosec // G115: lo is always > 0 here (checked above)
}

// FindIngestStartPosition returns the earliest record position with IngestTS >= ts.
// Uses binary search on the in-memory record slice.
func (m *Manager) FindIngestStartPosition(id chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	m.mu.Lock()
	state := m.findChunkLocked(id)
	m.mu.Unlock()
	if state == nil || len(state.records) == 0 {
		return 0, false, nil
	}

	tsNano := ts.UnixNano()
	lo, hi := 0, len(state.records)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if state.records[mid].IngestTS.UnixNano() < tsNano {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo >= len(state.records) {
		return 0, false, nil
	}
	return uint64(lo), true, nil
}

// FindSourceStartPosition returns the earliest record position with SourceTS >= ts.
// Uses binary search on the in-memory record slice.
func (m *Manager) FindSourceStartPosition(id chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	m.mu.Lock()
	state := m.findChunkLocked(id)
	m.mu.Unlock()
	if state == nil || len(state.records) == 0 {
		return 0, false, nil
	}

	tsNano := ts.UnixNano()
	lo, hi := 0, len(state.records)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if state.records[mid].SourceTS.UnixNano() < tsNano {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo >= len(state.records) {
		return 0, false, nil
	}
	return uint64(lo), true, nil
}

// ReadWriteTimestamps reads the WriteTS for each given record position in a chunk.
func (m *Manager) ReadWriteTimestamps(id chunk.ChunkID, positions []uint64) ([]time.Time, error) {
	if len(positions) == 0 {
		return nil, nil
	}

	m.mu.Lock()
	state := m.findChunkLocked(id)
	m.mu.Unlock()
	if state == nil {
		return nil, chunk.ErrChunkNotFound
	}

	results := make([]time.Time, len(positions))
	for i, pos := range positions {
		if pos >= uint64(len(state.records)) {
			return nil, fmt.Errorf("position %d out of range (chunk has %d records)", pos, len(state.records))
		}
		results[i] = state.records[pos].WriteTS
	}

	return results, nil
}

// ScanAttrs iterates records starting from startPos, calling fn with each
// record's WriteTS and Attributes. For memory vaults this is a direct slice
// iteration (no raw body loading since records are already in memory).
func (m *Manager) ScanAttrs(id chunk.ChunkID, startPos uint64, fn func(writeTS time.Time, attrs chunk.Attributes) bool) error {
	m.mu.Lock()
	state := m.findChunkLocked(id)
	m.mu.Unlock()
	if state == nil {
		return chunk.ErrChunkNotFound
	}

	for i := int(startPos); i < len(state.records); i++ { //nolint:gosec // G115: startPos checked by bounds
		if !fn(state.records[i].WriteTS, state.records[i].Attrs) {
			break
		}
	}
	return nil
}

// Delete removes a sealed chunk from memory.
// Returns ErrActiveChunk if the chunk is the current active chunk.
// Returns ErrChunkNotFound if the chunk does not exist.
func (m *Manager) Delete(id chunk.ChunkID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.active != nil && m.active.meta.ID == id {
		return chunk.ErrActiveChunk
	}

	for i, state := range m.chunks {
		if state.meta.ID == id {
			m.chunks = append(m.chunks[:i], m.chunks[i+1:]...)
			return nil
		}
	}

	return chunk.ErrChunkNotFound
}

// SetRotationPolicy updates the rotation policy for future appends.
func (m *Manager) SetRotationPolicy(policy chunk.RotationPolicy) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cfg.RotationPolicy = policy
}

func (m *Manager) CheckRotation() *string {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.active == nil {
		return nil
	}

	state := m.activeChunkState()
	if state.Records == 0 {
		return nil
	}

	var zeroRecord chunk.Record
	trigger := m.cfg.RotationPolicy.ShouldRotate(state, zeroRecord)
	if trigger == nil {
		return nil
	}

	m.logger.Info("rotating chunk",
		"trigger", *trigger,
		"chunk", state.ChunkID.String(),
		"bytes", state.Bytes,
		"records", state.Records,
		"age", m.cfg.Now().Sub(state.CreatedAt),
	)
	if err := m.sealLocked(); err != nil {
		return nil
	}
	return trigger
}

// ImportRecords creates a new sealed chunk by consuming records from the
// iterator, preserving each record's WriteTS. The new chunk is independent
// of the active chunk.
func (m *Manager) ImportRecords(next chunk.RecordIterator) (chunk.ChunkMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var id chunk.ChunkID
	if m.nextChunkID != nil {
		id = *m.nextChunkID
		m.nextChunkID = nil
	} else {
		id = chunk.NewChunkID()
	}
	state := &chunkState{
		meta: chunk.ChunkMeta{
			ID:     id,
			Sealed: true,
		},
		createdAt: m.cfg.Now(),
	}

	for {
		rec, iterErr := next()
		if errors.Is(iterErr, chunk.ErrNoMoreRecords) {
			break
		}
		if iterErr != nil {
			return chunk.ChunkMeta{}, iterErr
		}

		rec.WriteTS = m.cfg.Now()
		state.records = append(state.records, rec)

		recBytes := int64(len(rec.Raw))
		for k, v := range rec.Attrs {
			recBytes += int64(len(k) + len(v))
		}
		state.size += recBytes

		// Track WriteStart/WriteEnd (WriteTS is monotonically non-decreasing).
		if state.meta.WriteStart.IsZero() {
			state.meta.WriteStart = rec.WriteTS
		}
		state.meta.WriteEnd = rec.WriteTS

		// Compute IngestTS and SourceTS bounds inline.
		if state.meta.IngestStart.IsZero() || rec.IngestTS.Before(state.meta.IngestStart) {
			state.meta.IngestStart = rec.IngestTS
		}
		if state.meta.IngestEnd.IsZero() || rec.IngestTS.After(state.meta.IngestEnd) {
			state.meta.IngestEnd = rec.IngestTS
		}
		if !rec.SourceTS.IsZero() {
			if state.meta.SourceStart.IsZero() || rec.SourceTS.Before(state.meta.SourceStart) {
				state.meta.SourceStart = rec.SourceTS
			}
			if state.meta.SourceEnd.IsZero() || rec.SourceTS.After(state.meta.SourceEnd) {
				state.meta.SourceEnd = rec.SourceTS
			}
		}
	}

	if len(state.records) == 0 {
		return chunk.ChunkMeta{}, nil
	}

	state.meta.RecordCount = int64(len(state.records))
	state.meta.Bytes = state.size

	if err := m.cfg.MetaStore.Save(state.meta); err != nil {
		return chunk.ChunkMeta{}, err
	}
	m.chunks = append(m.chunks, state)

	return state.meta, nil
}

// SetIndexBuilders injects index builders into the post-seal pipeline.
func (m *Manager) SetIndexBuilders(builders []chunk.ChunkIndexBuilder) {
	m.indexBuilders = builders
}

// HasIndexBuilders reports whether index builders are injected.
func (m *Manager) HasIndexBuilders() bool {
	return len(m.indexBuilders) > 0
}

// PostSealProcess builds indexes for a sealed chunk.
// Memory vaults don't compress or upload — only index building is needed.
func (m *Manager) PostSealProcess(ctx context.Context, id chunk.ChunkID) error {
	for _, builder := range m.indexBuilders {
		if err := builder.Build(ctx, id); err != nil {
			m.logger.Warn("index build failed", "chunk", id, "error", err)
		}
	}
	return nil
}

// Close releases resources. For memory manager, this clears internal state.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.active = nil
	m.chunks = nil
	return nil
}

var (
	_ chunk.ChunkManager          = (*Manager)(nil)
	_ chunk.ChunkPostSealProcessor = (*Manager)(nil)
)
