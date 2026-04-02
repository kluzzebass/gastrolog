package blobstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"
)

// MemoryConfig configures the in-memory blobstore's archival simulation.
type MemoryConfig struct {
	// Now returns the current time. Defaults to time.Now.
	// Inject a frozen or manually-advanced clock for deterministic tests.
	Now func() time.Time

	// RestoreDelay is how long a blob stays in "restoring" state after
	// Restore is called. During this window, IsRestoring returns true and
	// downloads still fail with ErrBlobArchived. Default 0 (instant restore).
	RestoreDelay time.Duration

	// RestoreExpiry is how long a restored blob stays readable before
	// automatically re-archiving (simulates S3's restore window). Default 0
	// means restored blobs stay readable forever until explicitly re-archived.
	RestoreExpiry time.Duration
}

// Memory is a thread-safe in-memory implementation of Store for testing.
// Also implements Archiver — blobs can be archived/restored to simulate
// Glacier/Archive storage class behavior without real cloud APIs.
type Memory struct {
	mu    sync.Mutex
	blobs map[string]memBlob
	cfg   MemoryConfig
}

type memBlob struct {
	data         []byte
	metadata     map[string]string
	storageClass string    // empty = standard; "GLACIER", "DEEP_ARCHIVE", "Archive" = archived
	restoreAt    time.Time // when Restore was called (zero = not restoring)
}

// NewMemory returns a new in-memory blobstore with default config (instant
// restore, no expiry).
func NewMemory() *Memory {
	return NewMemoryWithConfig(MemoryConfig{})
}

// NewMemoryWithConfig returns a new in-memory blobstore with the given config.
func NewMemoryWithConfig(cfg MemoryConfig) *Memory {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Memory{blobs: make(map[string]memBlob), cfg: cfg}
}

func (m *Memory) EnsureBucket(_ context.Context) error { return nil }

func (m *Memory) Upload(_ context.Context, key string, data io.Reader, metadata map[string]string) error {
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, data); err != nil {
		return err
	}
	meta := make(map[string]string, len(metadata))
	maps.Copy(meta, metadata)
	m.mu.Lock()
	m.blobs[key] = memBlob{data: buf.Bytes(), metadata: meta}
	m.mu.Unlock()
	return nil
}

func (m *Memory) Download(_ context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	blob, ok := m.blobs[key]
	if ok {
		m.checkExpiry(key, &blob)
	}
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrBlobNotFound, key)
	}
	if blob.isArchived(m.cfg.Now(), m.cfg.RestoreDelay) {
		return nil, fmt.Errorf("%w: %s", ErrBlobArchived, key)
	}
	return io.NopCloser(bytes.NewReader(blob.data)), nil
}

func (m *Memory) DownloadRange(_ context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	m.mu.Lock()
	blob, ok := m.blobs[key]
	if ok {
		m.checkExpiry(key, &blob)
	}
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrBlobNotFound, key)
	}
	if blob.isArchived(m.cfg.Now(), m.cfg.RestoreDelay) {
		return nil, fmt.Errorf("%w: %s", ErrBlobArchived, key)
	}
	end := min(offset+length, int64(len(blob.data)))
	return io.NopCloser(bytes.NewReader(blob.data[offset:end])), nil
}

func (m *Memory) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	delete(m.blobs, key)
	m.mu.Unlock()
	return nil
}

func (m *Memory) List(_ context.Context, prefix string, fn func(BlobInfo) error) error {
	m.mu.Lock()
	keys := make([]string, 0, len(m.blobs))
	for k := range m.blobs {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	m.mu.Unlock()
	sort.Strings(keys)
	for _, k := range keys {
		m.mu.Lock()
		blob, ok := m.blobs[k]
		m.mu.Unlock()
		if !ok {
			continue
		}
		if err := fn(BlobInfo{Key: k, Size: int64(len(blob.data)), Metadata: blob.metadata, StorageClass: blob.storageClass}); err != nil {
			if errors.Is(err, ErrStopIteration) {
				return nil
			}
			return err
		}
	}
	return nil
}

func (m *Memory) Head(_ context.Context, key string) (BlobInfo, error) {
	m.mu.Lock()
	blob, ok := m.blobs[key]
	m.mu.Unlock()
	if !ok {
		return BlobInfo{}, fmt.Errorf("%w: %s", ErrBlobNotFound, key)
	}
	return BlobInfo{Key: key, Size: int64(len(blob.data)), Metadata: blob.metadata, StorageClass: blob.storageClass}, nil
}

// isArchived returns true if this blob is in an offline storage class and
// not yet restored (or restore delay hasn't elapsed).
func (b memBlob) isArchived(now time.Time, restoreDelay time.Duration) bool {
	switch b.storageClass {
	case "GLACIER", "DEEP_ARCHIVE", "Archive", "archive":
		// not restoring at all
		if b.restoreAt.IsZero() {
			return true
		}
		// still within restore delay
		if now.Before(b.restoreAt.Add(restoreDelay)) {
			return true
		}
		return false
	}
	return false
}

// checkExpiry re-archives a restored blob if RestoreExpiry has elapsed.
// Must be called with m.mu held.
func (m *Memory) checkExpiry(key string, blob *memBlob) {
	if m.cfg.RestoreExpiry <= 0 || blob.restoreAt.IsZero() {
		return
	}
	expiresAt := blob.restoreAt.Add(m.cfg.RestoreDelay).Add(m.cfg.RestoreExpiry)
	if m.cfg.Now().After(expiresAt) {
		blob.restoreAt = time.Time{} // re-archive
		m.blobs[key] = *blob
	}
}

// Archive transitions a blob to the given storage class.
// Download/DownloadRange will return ErrBlobArchived until Restore is called
// and the restore delay elapses.
func (m *Memory) Archive(_ context.Context, key string, storageClass string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	blob, ok := m.blobs[key]
	if !ok {
		return fmt.Errorf("blob %q not found", key)
	}
	blob.storageClass = storageClass
	blob.restoreAt = time.Time{} // clear any active restore
	m.blobs[key] = blob
	return nil
}

// Restore initiates retrieval of an archived blob. The blob becomes readable
// after RestoreDelay elapses (0 = instant). It stays readable for RestoreExpiry
// (0 = forever). tier and days are recorded but only affect behavior through
// the configured delays.
func (m *Memory) Restore(_ context.Context, key string, tier string, days int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	blob, ok := m.blobs[key]
	if !ok {
		return fmt.Errorf("blob %q not found", key)
	}
	blob.restoreAt = m.cfg.Now()
	m.blobs[key] = blob
	return nil
}

// IsRestoring returns true if a restore has been initiated but the restore
// delay hasn't elapsed yet.
func (m *Memory) IsRestoring(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	blob, ok := m.blobs[key]
	if !ok {
		return false, fmt.Errorf("blob %q not found", key)
	}
	if blob.restoreAt.IsZero() {
		return false, nil
	}
	now := m.cfg.Now()
	return now.Before(blob.restoreAt.Add(m.cfg.RestoreDelay)), nil
}

var _ Store = (*Memory)(nil)
var _ Archiver = (*Memory)(nil)
