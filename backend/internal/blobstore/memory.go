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
)

// Memory is a thread-safe in-memory implementation of Store for testing.
type Memory struct {
	mu    sync.Mutex
	blobs map[string]memBlob
}

type memBlob struct {
	data     []byte
	metadata map[string]string
}

// NewMemory returns a new in-memory blobstore.
func NewMemory() *Memory {
	return &Memory{blobs: make(map[string]memBlob)}
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
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("blob %q not found", key)
	}
	return io.NopCloser(bytes.NewReader(blob.data)), nil
}

func (m *Memory) DownloadRange(_ context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	m.mu.Lock()
	blob, ok := m.blobs[key]
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("blob %q not found", key)
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
		if err := fn(BlobInfo{Key: k, Size: int64(len(blob.data)), Metadata: blob.metadata}); err != nil {
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
		return BlobInfo{}, fmt.Errorf("blob %q not found", key)
	}
	return BlobInfo{Key: key, Size: int64(len(blob.data)), Metadata: blob.metadata}, nil
}

var _ Store = (*Memory)(nil)
