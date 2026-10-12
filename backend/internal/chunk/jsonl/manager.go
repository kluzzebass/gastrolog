// Package jsonl implements a write-only JSONL sink. Every record
// is appended as a single JSON line to a file. No chunking, rotation,
// indexing, or searching.
//
// A JSONL vault's records reach the sink one sealed chunk at a time
// (WriteChunk). A ledger beside the sink file lists the chunks written, each
// only after its lines are synced, so a restart writes every chunk the
// ledger does not list — a crash can repeat one chunk's lines but never
// lose them.
package jsonl

import (
	"bufio"
	"bytes"
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
	ledger   *os.File
	written  map[chunk.ChunkID]bool
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

// LedgerSuffix names the ledger of written chunks beside the sink file.
const LedgerSuffix = ".chunks"

// NewManager creates a JSONL sink manager at the given file path.
func NewManager(cfg Config) (*Manager, error) {
	if err := os.MkdirAll(filepath.Dir(cfg.Path), 0o750); err != nil {
		return nil, fmt.Errorf("create jsonl parent dir: %w", err)
	}
	if err := dropTornLine(cfg.Path); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(cfg.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open jsonl sink: %w", err)
	}
	written, err := readLedger(cfg.Path + LedgerSuffix)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	ledger, err := os.OpenFile(cfg.Path+LedgerSuffix, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("open jsonl sink ledger: %w", err)
	}
	return &Manager{
		file:    f,
		encoder: json.NewEncoder(f),
		ledger:  ledger,
		written: written,
		id:      chunk.NewChunkID(),
	}, nil
}

// dropTornLine truncates a partly written last line off the sink file: it
// belongs to a chunk the ledger does not list, which is written again whole.
func dropTornLine(path string) error {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read jsonl sink: %w", err)
	}
	if len(data) == 0 || data[len(data)-1] == '\n' {
		return nil
	}
	keep := bytes.LastIndexByte(data, '\n') + 1
	if err := os.Truncate(path, int64(keep)); err != nil {
		return fmt.Errorf("drop torn jsonl line: %w", err)
	}
	return nil
}

// readLedger returns the chunks the ledger lists. A torn last entry lists
// nothing: its chunk is written again.
func readLedger(path string) (map[chunk.ChunkID]bool, error) {
	written := make(map[chunk.ChunkID]bool)
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		if os.IsNotExist(err) {
			return written, nil
		}
		return nil, fmt.Errorf("open jsonl sink ledger: %w", err)
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if id, err := chunk.ParseChunkID(sc.Text()); err == nil {
			written[id] = true
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read jsonl sink ledger: %w", err)
	}
	return written, nil
}

// Written reports whether the ledger lists chunk id as written.
func (m *Manager) Written(id chunk.ChunkID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.written[id]
}

// WriteChunk appends sealed chunk id's records to the sink and lists the chunk
// in the ledger once its lines are synced. A chunk already listed is skipped.
// Reports whether it wrote the chunk.
func (m *Manager) WriteChunk(id chunk.ChunkID, next chunk.RecordIterator) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return false, errors.New("jsonl manager closed")
	}
	if m.written[id] {
		return false, nil
	}
	for {
		rec, err := next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			return false, fmt.Errorf("read chunk %s: %w", id, err)
		}
		if err := m.encodeLocked(rec); err != nil {
			return false, err
		}
	}
	if err := m.file.Sync(); err != nil {
		return false, fmt.Errorf("sync jsonl sink: %w", err)
	}
	if _, err := m.ledger.WriteString(id.String() + "\n"); err != nil {
		return false, fmt.Errorf("write jsonl sink ledger: %w", err)
	}
	if err := m.ledger.Sync(); err != nil {
		return false, fmt.Errorf("sync jsonl sink ledger: %w", err)
	}
	m.written[id] = true
	return true, nil
}

// NewFactory returns a chunk.ManagerFactory for JSONL sinks.
func NewFactory() chunk.ManagerFactory {
	return func(params map[string]string, _ *slog.Logger) (chunk.ChunkManager, error) {
		path := params["path"]
		if path == "" {
			return nil, errors.New("jsonl sink requires 'path' parameter")
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
	pos := uint64(m.count) //nolint:gosec // count is non-negative
	if err := m.encodeLocked(rec); err != nil {
		return chunk.ChunkID{}, 0, err
	}
	return m.id, pos, nil
}

// encodeLocked writes rec as one JSON line. A record's own write time is kept;
// one without gets the time it is written here. Caller holds m.mu.
func (m *Manager) encodeLocked(rec chunk.Record) error {
	raw := rec.Raw
	var rawStr string
	if utf8.Valid(raw) {
		rawStr = string(raw)
	} else {
		rawStr = base64.StdEncoding.EncodeToString(raw)
	}
	writeTS := rec.WriteTS
	if writeTS.IsZero() {
		writeTS = time.Now()
	}
	if err := m.encoder.Encode(record{
		SourceTS: rec.SourceTS,
		IngestTS: rec.IngestTS,
		WriteTS:  writeTS,
		Attrs:    rec.Attrs,
		Raw:      rawStr,
	}); err != nil {
		return fmt.Errorf("write jsonl: %w", err)
	}
	m.count++
	ts := rec.IngestTS
	if m.earliest.IsZero() || ts.Before(m.earliest) {
		m.earliest = ts
	}
	if ts.After(m.latest) {
		m.latest = ts
	}
	return nil
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
func (m *Manager) HasLocalContent(chunk.ChunkID) bool { return true }
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
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			return chunk.ChunkMeta{}, err
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
	var errs []error
	if m.ledger != nil {
		errs = append(errs, m.ledger.Close())
	}
	if m.file != nil {
		errs = append(errs, m.file.Close())
	}
	return errors.Join(errs...)
}
