package cloud

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
)

var (
	ErrSealedOnly = errors.New("cloud vaults are sealed-only: append not supported")
	ErrNotSealed  = errors.New("cloud vaults only contain sealed chunks")
)

// Manager implements chunk.ChunkManager for cloud-archived vaults.
// It is sealed-only — chunks are imported via ImportRecords, not appended.
type Manager struct {
	store   blobstore.Store
	vaultID uuid.UUID
	prefix  string // blob key prefix (e.g. "vault-{id}/")
	logger  *slog.Logger

	mu    sync.RWMutex
	metas map[chunk.ChunkID]chunk.ChunkMeta // cached metadata
}

// NewManager creates a cloud chunk manager.
func NewManager(store blobstore.Store, vaultID uuid.UUID, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Manager{
		store:   store,
		vaultID: vaultID,
		prefix:  fmt.Sprintf("vault-%s/", vaultID),
		logger:  logger,
		metas:   make(map[chunk.ChunkID]chunk.ChunkMeta),
	}
}

// blobKey returns the object key for a chunk.
func (m *Manager) blobKey(id chunk.ChunkID) string {
	return m.prefix + id.String() + ".glcb"
}

// chunkIDFromKey extracts the ChunkID from a blob key.
func (m *Manager) chunkIDFromKey(key string) (chunk.ChunkID, bool) {
	key = strings.TrimPrefix(key, m.prefix)
	key = strings.TrimSuffix(key, ".glcb")
	id, err := chunk.ParseChunkID(key)
	if err != nil {
		return chunk.ChunkID{}, false
	}
	return id, true
}

// blobMetaToChunkMeta converts blob object metadata to ChunkMeta.
func blobMetaToChunkMeta(id chunk.ChunkID, bm blobstore.BlobInfo) chunk.ChunkMeta {
	meta := chunk.ChunkMeta{
		ID:         id,
		Sealed:     true,
		Compressed: true,
		DiskBytes:  bm.Size,
		Bytes:      bm.Size, // overwritten below if raw_bytes is known
	}
	if v, ok := bm.Metadata["raw_bytes"]; ok {
		n, _ := strconv.ParseInt(v, 10, 64)
		if n > 0 {
			meta.Bytes = n
		}
	}
	if v, ok := bm.Metadata["record_count"]; ok {
		n, _ := strconv.ParseInt(v, 10, 64)
		meta.RecordCount = n
	}
	if v, ok := bm.Metadata["write_start"]; ok {
		t, _ := time.Parse(time.RFC3339Nano, v)
		meta.WriteStart = t
	}
	if v, ok := bm.Metadata["write_end"]; ok {
		t, _ := time.Parse(time.RFC3339Nano, v)
		meta.WriteEnd = t
	}
	if v, ok := bm.Metadata["ingest_start"]; ok {
		t, _ := time.Parse(time.RFC3339Nano, v)
		meta.IngestStart = t
	}
	if v, ok := bm.Metadata["ingest_end"]; ok {
		t, _ := time.Parse(time.RFC3339Nano, v)
		meta.IngestEnd = t
	}
	if v, ok := bm.Metadata["source_start"]; ok {
		t, _ := time.Parse(time.RFC3339Nano, v)
		meta.SourceStart = t
	}
	if v, ok := bm.Metadata["source_end"]; ok {
		t, _ := time.Parse(time.RFC3339Nano, v)
		meta.SourceEnd = t
	}
	return meta
}

// objectMetadata builds blob object metadata from BlobMeta for upload.
func objectMetadata(bm BlobMeta) map[string]string {
	md := map[string]string{
		"chunk_id":     bm.ChunkID.String(),
		"vault_id":     bm.VaultID.String(),
		"record_count": strconv.FormatUint(uint64(bm.RecordCount), 10),
		"raw_bytes":    strconv.FormatInt(bm.RawBytes, 10),
	}
	if !bm.WriteStart.IsZero() {
		md["write_start"] = bm.WriteStart.Format(time.RFC3339Nano)
	}
	if !bm.WriteEnd.IsZero() {
		md["write_end"] = bm.WriteEnd.Format(time.RFC3339Nano)
	}
	if !bm.IngestStart.IsZero() {
		md["ingest_start"] = bm.IngestStart.Format(time.RFC3339Nano)
	}
	if !bm.IngestEnd.IsZero() {
		md["ingest_end"] = bm.IngestEnd.Format(time.RFC3339Nano)
	}
	if !bm.SourceStart.IsZero() {
		md["source_start"] = bm.SourceStart.Format(time.RFC3339Nano)
	}
	if !bm.SourceEnd.IsZero() {
		md["source_end"] = bm.SourceEnd.Format(time.RFC3339Nano)
	}
	return md
}

// --- ChunkManager interface ---

func (m *Manager) Append(_ chunk.Record) (chunk.ChunkID, uint64, error) {
	return chunk.ChunkID{}, 0, ErrSealedOnly
}

func (m *Manager) AppendPreserved(_ chunk.Record) (chunk.ChunkID, uint64, error) {
	return chunk.ChunkID{}, 0, ErrSealedOnly
}

func (m *Manager) Seal() error { return nil }

func (m *Manager) Active() *chunk.ChunkMeta { return nil }

func (m *Manager) Meta(id chunk.ChunkID) (chunk.ChunkMeta, error) {
	m.mu.RLock()
	if meta, ok := m.metas[id]; ok {
		m.mu.RUnlock()
		return meta, nil
	}
	m.mu.RUnlock()

	info, err := m.store.Head(context.Background(), m.blobKey(id))
	if err != nil {
		return chunk.ChunkMeta{}, fmt.Errorf("head %s: %w", id, err)
	}
	meta := blobMetaToChunkMeta(id, info)

	m.mu.Lock()
	m.metas[id] = meta
	m.mu.Unlock()

	return meta, nil
}

func (m *Manager) List() ([]chunk.ChunkMeta, error) {
	blobs, err := m.store.List(context.Background(), m.prefix)
	if err != nil {
		return nil, fmt.Errorf("list blobs: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]chunk.ChunkMeta, 0, len(blobs))
	for _, blob := range blobs {
		id, ok := m.chunkIDFromKey(blob.Key)
		if !ok {
			continue
		}
		meta := blobMetaToChunkMeta(id, blob)
		m.metas[id] = meta
		result = append(result, meta)
	}
	return result, nil
}

func (m *Manager) Delete(id chunk.ChunkID) error {
	if err := m.store.Delete(context.Background(), m.blobKey(id)); err != nil {
		return fmt.Errorf("delete %s: %w", id, err)
	}

	m.mu.Lock()
	delete(m.metas, id)
	m.mu.Unlock()

	return nil
}

// downloadToTemp downloads a blob to a temporary file and seeks back to start.
// The caller is responsible for closing and removing the file (Reader.Close does this).
func (m *Manager) downloadToTemp(id chunk.ChunkID) (*os.File, error) {
	rc, err := m.store.Download(context.Background(), m.blobKey(id))
	if err != nil {
		return nil, fmt.Errorf("download %s: %w", id, err)
	}
	defer func() { _ = rc.Close() }()

	tmp, err := os.CreateTemp("", "glcb-*")
	if err != nil {
		return nil, fmt.Errorf("create temp file: %w", err)
	}

	if _, err := io.Copy(tmp, rc); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name()) //nolint:gosec // tmp is from os.CreateTemp, not user input
		return nil, fmt.Errorf("download %s to temp: %w", id, err)
	}

	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name()) //nolint:gosec // tmp is from os.CreateTemp, not user input
		return nil, err
	}

	return tmp, nil
}

func (m *Manager) OpenCursor(id chunk.ChunkID) (chunk.RecordCursor, error) {
	tmp, err := m.downloadToTemp(id)
	if err != nil {
		return nil, err
	}

	rd, err := NewReader(tmp)
	if err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name()) //nolint:gosec // tmp is from os.CreateTemp, not user input
		return nil, fmt.Errorf("open reader %s: %w", id, err)
	}

	return NewSeekableCursor(rd, id), nil
}

func (m *Manager) FindStartPosition(_ chunk.ChunkID, _ time.Time) (uint64, bool, error) {
	return 0, false, nil
}

func (m *Manager) FindIngestStartPosition(_ chunk.ChunkID, _ time.Time) (uint64, bool, error) {
	return 0, false, nil
}

func (m *Manager) FindSourceStartPosition(_ chunk.ChunkID, _ time.Time) (uint64, bool, error) {
	return 0, false, nil
}

func (m *Manager) ReadWriteTimestamps(id chunk.ChunkID, positions []uint64) ([]time.Time, error) {
	cursor, err := m.OpenCursor(id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cursor.Close() }()

	// Build position set for fast lookup.
	posSet := make(map[uint64]int, len(positions))
	for i, p := range positions {
		posSet[p] = i
	}

	result := make([]time.Time, len(positions))
	var pos uint64
	for {
		rec, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			return nil, err
		}
		if idx, ok := posSet[pos]; ok {
			result[idx] = rec.WriteTS
		}
		pos++
	}
	return result, nil
}

func (m *Manager) SetRotationPolicy(_ chunk.RotationPolicy) {}

func (m *Manager) CheckRotation() *string { return nil }

func (m *Manager) ImportRecords(next chunk.RecordIterator) (chunk.ChunkMeta, error) {
	chunkID := chunk.NewChunkID()
	w := NewWriter(chunkID, m.vaultID)

	for {
		rec, err := next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			return chunk.ChunkMeta{}, fmt.Errorf("read record: %w", err)
		}
		if err := w.Add(rec); err != nil {
			return chunk.ChunkMeta{}, fmt.Errorf("add record: %w", err)
		}
	}

	var buf bytes.Buffer
	if _, err := w.WriteTo(&buf); err != nil {
		return chunk.ChunkMeta{}, fmt.Errorf("compress %s: %w", chunkID, err)
	}

	bm := w.Meta()
	key := m.blobKey(chunkID)
	if err := m.store.Upload(context.Background(), key, bytes.NewReader(buf.Bytes()), objectMetadata(bm)); err != nil {
		return chunk.ChunkMeta{}, fmt.Errorf("upload %s: %w", chunkID, err)
	}

	info, err := m.store.Head(context.Background(), key)
	if err != nil {
		m.logger.Warn("failed to head after upload", "chunk", chunkID, "error", err)
	}

	meta := chunk.ChunkMeta{
		ID:          chunkID,
		WriteStart:     bm.WriteStart,
		WriteEnd:       bm.WriteEnd,
		RecordCount: int64(bm.RecordCount),
		Sealed:      true,
		Compressed:  true,
		IngestStart: bm.IngestStart,
		IngestEnd:   bm.IngestEnd,
		SourceStart: bm.SourceStart,
		SourceEnd:   bm.SourceEnd,
		Bytes:     bm.RawBytes,
		DiskBytes: info.Size,
	}

	m.mu.Lock()
	m.metas[chunkID] = meta
	m.mu.Unlock()

	m.logger.Info("chunk archived",
		"chunk", chunkID,
		"records", bm.RecordCount,
		"bytes", info.Size,
	)
	return meta, nil
}

func (m *Manager) ScanAttrs(id chunk.ChunkID, startPos uint64, fn func(writeTS time.Time, attrs chunk.Attributes) bool) error {
	cursor, err := m.OpenCursor(id)
	if err != nil {
		return err
	}
	defer func() { _ = cursor.Close() }()

	var pos uint64
	for {
		rec, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			return nil
		}
		if err != nil {
			return err
		}
		if pos >= startPos {
			if !fn(rec.WriteTS, rec.Attrs) {
				return nil
			}
		}
		pos++
	}
}

func (m *Manager) Close() error { return nil }

// --- seekableCursor: random-access cursor backed by seekable zstd ---

type seekableCursor struct {
	reader      *Reader
	id          chunk.ChunkID
	recordCount uint64
	fwdIndex    uint64
	revIndex    uint64
	fwdDone     bool
	revDone     bool
}

// NewSeekableCursor creates a seekable cursor from a Reader.
// Exported for testing; Manager.OpenCursor creates these internally.
func NewSeekableCursor(rd *Reader, id chunk.ChunkID) chunk.RecordCursor {
	return &seekableCursor{
		reader:      rd,
		id:          id,
		recordCount: uint64(rd.Meta().RecordCount),
		fwdIndex:    0,
		revIndex:    uint64(rd.Meta().RecordCount),
	}
}

func (c *seekableCursor) Next() (chunk.Record, chunk.RecordRef, error) {
	if c.fwdDone || c.fwdIndex >= c.recordCount {
		c.fwdDone = true
		return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
	}

	rec, err := c.reader.ReadRecord(uint32(c.fwdIndex)) //nolint:gosec // G115: bounded by recordCount
	if err != nil {
		return chunk.Record{}, chunk.RecordRef{}, err
	}

	ref := chunk.RecordRef{ChunkID: c.id, Pos: c.fwdIndex}
	c.fwdIndex++
	return rec, ref, nil
}

func (c *seekableCursor) Prev() (chunk.Record, chunk.RecordRef, error) {
	if c.revDone || c.revIndex == 0 {
		c.revDone = true
		return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
	}

	c.revIndex--
	rec, err := c.reader.ReadRecord(uint32(c.revIndex)) //nolint:gosec // G115: bounded by recordCount
	if err != nil {
		c.revIndex++
		return chunk.Record{}, chunk.RecordRef{}, err
	}

	return rec, chunk.RecordRef{ChunkID: c.id, Pos: c.revIndex}, nil
}

func (c *seekableCursor) Seek(ref chunk.RecordRef) error {
	c.fwdIndex = ref.Pos
	c.revIndex = ref.Pos
	c.fwdDone = false
	c.revDone = false
	return nil
}

func (c *seekableCursor) Close() error {
	if c.reader != nil {
		return c.reader.Close()
	}
	return nil
}
