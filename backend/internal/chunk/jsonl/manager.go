// Package jsonl implements a write-only JSONL sink. Every record
// is appended as a single JSON line to a file. No chunking, rotation,
// indexing, or searching.
//
// A JSONL vault's records reach the sink one sealed chunk at a time
// (WriteChunk). A ledger beside the sink file records the file's size when
// the sink first opened it, then each chunk written with the file's size
// once its lines were synced. Opening the sink cuts the file back to the last
// size the ledger records, so a chunk a crash interrupted loses its partial
// lines and is written again whole — each chunk once — and bytes the sink did
// not write are never touched.
package jsonl

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

// ledgerBase names the ledger's first entry: the sink file's size when the
// sink first opened it.
const ledgerBase = "base"

// NewManager creates a JSONL sink manager at the given file path.
func NewManager(cfg Config) (*Manager, error) {
	if err := os.MkdirAll(filepath.Dir(cfg.Path), 0o750); err != nil {
		return nil, fmt.Errorf("create jsonl parent dir: %w", err)
	}
	f, err := os.OpenFile(cfg.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open jsonl sink: %w", err)
	}
	ledgerPath := filepath.Clean(cfg.Path + LedgerSuffix)
	written, recorded, ok, err := readLedger(ledgerPath)
	if err == nil {
		err = settleSink(f, recorded, ok)
	}
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	ledger, err := os.OpenFile(ledgerPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("open jsonl sink ledger: %w", err)
	}
	m := &Manager{
		file:    f,
		encoder: json.NewEncoder(f),
		ledger:  ledger,
		written: written,
		id:      chunk.NewChunkID(),
	}
	if !ok {
		if err := m.recordLocked(ledgerBase); err != nil {
			_ = m.Close()
			return nil, err
		}
	}
	return m, nil
}

// settleSink cuts the sink file back to recorded, the last size the ledger
// records: what lies past it is the partial lines of a chunk a crash
// interrupted, which is written again whole. A file shorter than recorded was
// changed outside the sink and is left alone, as is a file the ledger has no
// record of (ok false).
func settleSink(f *os.File, recorded int64, ok bool) error {
	if !ok {
		return nil
	}
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat jsonl sink: %w", err)
	}
	if info.Size() <= recorded {
		return nil
	}
	if err := f.Truncate(recorded); err != nil {
		return fmt.Errorf("cut jsonl sink back to its last written chunk: %w", err)
	}
	return nil
}

// readLedger returns the chunks the ledger lists and the last sink size it
// records; ok is false when it records none. A torn last entry records
// nothing: its chunk is written again.
func readLedger(path string) (written map[chunk.ChunkID]bool, recorded int64, ok bool, err error) {
	written = make(map[chunk.ChunkID]bool)
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		if os.IsNotExist(err) {
			return written, 0, false, nil
		}
		return nil, 0, false, fmt.Errorf("open jsonl sink ledger: %w", err)
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		name, sizeText, found := strings.Cut(sc.Text(), " ")
		if !found {
			continue
		}
		size, err := strconv.ParseInt(sizeText, 10, 64)
		if err != nil || size < 0 {
			continue
		}
		if name != ledgerBase {
			id, err := chunk.ParseChunkID(name)
			if err != nil {
				continue
			}
			written[id] = true
		}
		recorded, ok = size, true
	}
	if err := sc.Err(); err != nil {
		return nil, 0, false, fmt.Errorf("read jsonl sink ledger: %w", err)
	}
	return written, recorded, ok, nil
}

// recordLocked appends a ledger entry naming what, with the sink file's
// current size, and syncs it. Caller holds m.mu or owns m exclusively.
func (m *Manager) recordLocked(what string) error {
	info, err := m.file.Stat()
	if err != nil {
		return fmt.Errorf("stat jsonl sink: %w", err)
	}
	if _, err := m.ledger.WriteString(what + " " + strconv.FormatInt(info.Size(), 10) + "\n"); err != nil {
		return fmt.Errorf("write jsonl sink ledger: %w", err)
	}
	if err := m.ledger.Sync(); err != nil {
		return fmt.Errorf("sync jsonl sink ledger: %w", err)
	}
	return nil
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
	info, err := m.file.Stat()
	if err != nil {
		return false, fmt.Errorf("stat jsonl sink: %w", err)
	}
	if err := m.writeChunkLocked(id, next); err != nil {
		// Cut the chunk's partial lines so the next chunk does not follow
		// them; this chunk is written again whole.
		if cutErr := m.file.Truncate(info.Size()); cutErr != nil {
			err = errors.Join(err, fmt.Errorf("cut partial chunk: %w", cutErr))
		}
		return false, err
	}
	m.written[id] = true
	return true, nil
}

func (m *Manager) writeChunkLocked(id chunk.ChunkID, next chunk.RecordIterator) error {
	for {
		rec, err := next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			return fmt.Errorf("read chunk %s: %w", id, err)
		}
		if err := m.encodeLocked(rec); err != nil {
			return err
		}
	}
	if err := m.file.Sync(); err != nil {
		return fmt.Errorf("sync jsonl sink: %w", err)
	}
	return m.recordLocked(id.String())
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
