package cloud

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
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
		ID:     id,
		Sealed: true,
		Bytes:  bm.Size,
	}
	if v, ok := bm.Metadata["record_count"]; ok {
		n, _ := strconv.ParseInt(v, 10, 64)
		meta.RecordCount = n
	}
	if v, ok := bm.Metadata["start_ts"]; ok {
		t, _ := time.Parse(time.RFC3339Nano, v)
		meta.StartTS = t
	}
	if v, ok := bm.Metadata["end_ts"]; ok {
		t, _ := time.Parse(time.RFC3339Nano, v)
		meta.EndTS = t
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
	}
	if !bm.StartTS.IsZero() {
		md["start_ts"] = bm.StartTS.Format(time.RFC3339Nano)
	}
	if !bm.EndTS.IsZero() {
		md["end_ts"] = bm.EndTS.Format(time.RFC3339Nano)
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

	// Fetch from blob store.
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

func (m *Manager) OpenCursor(id chunk.ChunkID) (chunk.RecordCursor, error) {
	rc, err := m.store.Download(context.Background(), m.blobKey(id))
	if err != nil {
		return nil, fmt.Errorf("download %s: %w", id, err)
	}

	rd, err := NewReader(rc)
	if err != nil {
		_ = rc.Close()
		return nil, fmt.Errorf("open reader %s: %w", id, err)
	}

	return &cloudCursor{
		reader: rd,
		closer: rc,
		id:     id,
	}, nil
}

func (m *Manager) FindStartPosition(_ chunk.ChunkID, _ time.Time) (uint64, bool, error) {
	// No time index — query engine falls back to scanning from position 0.
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

	// Compress to buffer, then upload. The blob is already buffered
	// in the Writer (record frames), so this adds minimal overhead.
	// Using a seekable buffer avoids issues with S3 checksum computation
	// on non-TLS connections.
	var buf bytes.Buffer
	if _, err := w.WriteTo(&buf); err != nil {
		return chunk.ChunkMeta{}, fmt.Errorf("compress %s: %w", chunkID, err)
	}

	bm := w.Meta()
	key := m.blobKey(chunkID)
	if err := m.store.Upload(context.Background(), key, bytes.NewReader(buf.Bytes()), objectMetadata(bm)); err != nil {
		return chunk.ChunkMeta{}, fmt.Errorf("upload %s: %w", chunkID, err)
	}

	// Fetch actual size from store.
	info, err := m.store.Head(context.Background(), key)
	if err != nil {
		m.logger.Warn("failed to head after upload", "chunk", chunkID, "error", err)
	}

	meta := chunk.ChunkMeta{
		ID:          chunkID,
		StartTS:     bm.StartTS,
		EndTS:       bm.EndTS,
		RecordCount: int64(bm.RecordCount),
		Sealed:      true,
		IngestStart: bm.IngestStart,
		IngestEnd:   bm.IngestEnd,
		SourceStart: bm.SourceStart,
		SourceEnd:   bm.SourceEnd,
		Bytes:       info.Size,
		DiskBytes:   info.Size,
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

// --- cloudCursor adapts cloud.Reader to chunk.RecordCursor ---

type cloudCursor struct {
	reader *Reader
	closer interface{ Close() error }
	id     chunk.ChunkID
	pos    uint64
}

func (c *cloudCursor) Next() (chunk.Record, chunk.RecordRef, error) {
	rec, err := c.reader.Next()
	if err != nil {
		return rec, chunk.RecordRef{}, err
	}
	ref := chunk.RecordRef{ChunkID: c.id, Pos: c.pos}
	c.pos++
	return rec, ref, nil
}

func (c *cloudCursor) Prev() (chunk.Record, chunk.RecordRef, error) {
	return chunk.Record{}, chunk.RecordRef{}, errors.New("cloud cursor does not support Prev")
}

func (c *cloudCursor) Seek(_ chunk.RecordRef) error {
	return errors.New("cloud cursor does not support Seek")
}

func (c *cloudCursor) Close() error {
	_ = c.reader.Close()
	return c.closer.Close()
}

