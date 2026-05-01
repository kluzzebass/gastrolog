// Package jsonl implements a write-only JSONL sink tier. Every record
// is appended as a single JSON line to a file. No chunking, rotation,
// indexing, or searching — purely for end-to-end verification.
package jsonl

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unicode/utf8"

	"gastrolog/internal/chunk"
)

// Config holds the parameters for a JSONL manager.
type Config struct {
	Path string // full file path for the sink
}

// Manager writes every appended record as a JSON line to a single file.
// It implements chunk.ChunkManager with most methods as no-ops.
type Manager struct {
	mu       sync.Mutex
	file     *os.File
	encoder  *json.Encoder
	id       chunk.ChunkID // single synthetic chunk ID
	count    int64
	earliest time.Time
	latest   time.Time
	closed   bool
}

// record is the JSON structure written per line.
type record struct {
	SourceTS time.Time        `json:"source_ts"`
	IngestTS time.Time        `json:"ingest_ts"`
	WriteTS  time.Time        `json:"write_ts"`
	Attrs    chunk.Attributes `json:"attrs"`
	Raw      string           `json:"raw"`
}

// NewManager creates a JSONL sink manager at the given file path.
func NewManager(cfg Config) (*Manager, error) {
	if err := os.MkdirAll(filepath.Dir(cfg.Path), 0o750); err != nil {
		return nil, fmt.Errorf("create jsonl parent dir: %w", err)
	}
	f, err := os.OpenFile(cfg.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open jsonl sink: %w", err)
	}
	return &Manager{
		file:    f,
		encoder: json.NewEncoder(f),
		id:      chunk.NewChunkID(),
	}, nil
}

// NewFactory returns a chunk.ManagerFactory for JSONL sinks.
func NewFactory() chunk.ManagerFactory {
	return func(params map[string]string, _ *slog.Logger) (chunk.ChunkManager, error) {
		path := params["path"]
		if path == "" {
			return nil, errors.New("jsonl tier requires 'path' parameter")
		}
		return NewManager(Config{Path: path})
	}
}

func (m *Manager) Append(rec chunk.Record) (chunk.ChunkID, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return chunk.ChunkID{}, 0, errors.New("jsonl manager closed")
	}

	raw := rec.Raw
	var rawStr string
	if utf8.Valid(raw) {
		rawStr = string(raw)
	} else {
		rawStr = base64.StdEncoding.EncodeToString(raw)
	}

	if err := m.encoder.Encode(record{
		SourceTS: rec.SourceTS,
		IngestTS: rec.IngestTS,
		WriteTS:  time.Now(),
		Attrs:    rec.Attrs,
		Raw:      rawStr,
	}); err != nil {
		return chunk.ChunkID{}, 0, fmt.Errorf("write jsonl: %w", err)
	}

	pos := uint64(m.count) //nolint:gosec // count is non-negative
	m.count++
	ts := rec.IngestTS
	if m.earliest.IsZero() || ts.Before(m.earliest) {
		m.earliest = ts
	}
	if ts.After(m.latest) {
		m.latest = ts
	}
	return m.id, pos, nil
}

func (m *Manager) Seal() error { return nil }

func (m *Manager) meta() chunk.ChunkMeta {
	var size int64
	if info, err := m.file.Stat(); err == nil {
		size = info.Size()
	}
	return chunk.ChunkMeta{
		ID:          m.id,
		RecordCount: m.count,
		Bytes:       size,
		DiskBytes:   size,
		WriteStart:  m.earliest,
		WriteEnd:    m.latest,
	}
}

func (m *Manager) Active() *chunk.ChunkMeta {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.count == 0 {
		return nil
	}
	meta := m.meta()
	return &meta
}

func (m *Manager) Meta(id chunk.ChunkID) (chunk.ChunkMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if id != m.id {
		return chunk.ChunkMeta{}, chunk.ErrChunkNotFound
	}
	return m.meta(), nil
}

func (m *Manager) List() ([]chunk.ChunkMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.count == 0 {
		return nil, nil
	}
	return []chunk.ChunkMeta{m.meta()}, nil
}

func (m *Manager) Delete(chunk.ChunkID) error { return nil }
func (m *Manager) OpenCursor(chunk.ChunkID) (chunk.RecordCursor, error) {
	return nil, errors.New("jsonl sink does not support reading")
}
func (m *Manager) FindStartPosition(chunk.ChunkID, time.Time) (uint64, bool, error) {
	return 0, false, nil
}
func (m *Manager) FindIngestStartPosition(chunk.ChunkID, time.Time) (uint64, bool, error) {
	return 0, false, nil
}
func (m *Manager) FindIngestEntryIndex(chunk.ChunkID, time.Time) (uint64, bool, error) {
	return 0, false, nil
}
func (m *Manager) ScanActiveByIngestTS(chunk.ChunkID, func(time.Time, chunk.Attributes) bool) error {
	return chunk.ErrChunkNotFound
}
func (m *Manager) ScanActiveIngestTS(chunk.ChunkID, func(int64) bool) error {
	return chunk.ErrChunkNotFound
}
func (m *Manager) FindSourceStartPosition(chunk.ChunkID, time.Time) (uint64, bool, error) {
	return 0, false, nil
}
func (m *Manager) ReadWriteTimestamps(chunk.ChunkID, []uint64) ([]time.Time, error) { return nil, nil }
func (m *Manager) SetRotationPolicy(chunk.RotationPolicy)                           {}
func (m *Manager) CheckRotation() *string                                           { return nil }
func (m *Manager) SetNextChunkID(chunk.ChunkID)                                     {}
func (m *Manager) ScanAttrs(_ chunk.ChunkID, _ uint64, _ func(time.Time, chunk.Attributes) bool) error {
	return nil
}

func (m *Manager) ImportRecords(_ chunk.ChunkID, next chunk.RecordIterator) (chunk.ChunkMeta, error) {
	for {
		rec, err := next()
		if err != nil {
			break
		}
		if _, _, err := m.Append(rec); err != nil {
			return chunk.ChunkMeta{}, err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return chunk.ChunkMeta{
		ID:          m.id,
		RecordCount: m.count,
		WriteStart:  m.earliest,
		WriteEnd:    m.latest,
	}, nil
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	if m.file != nil {
		return m.file.Close()
	}
	return nil
}
