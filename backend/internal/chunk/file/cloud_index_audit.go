package file

import (
	"context"
	"fmt"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
)

// ListCloudBlobs enumerates every object under this vault's cloud prefix.
//
// Read-only by contract: loadCloudBackedChunksFromStore walks the same listing
// but INSERTS what it finds, which is right at startup and wrong for an audit —
// indexing an object during the scan would erase the very divergence the audit
// is looking for.
//
// Objects whose key does not parse as a chunk ID are skipped rather than
// reported: the prefix is ours, but a key we cannot attribute to a chunk is not
// a chunk-level fact and there is nothing an operator could act on per-chunk.
func (m *Manager) ListCloudBlobs(ctx context.Context) ([]chunk.CloudBlobInfo, error) {
	if m.cfg.CloudStore == nil {
		return nil, chunk.ErrCloudStoreNotConfigured
	}
	var out []chunk.CloudBlobInfo
	err := m.cfg.CloudStore.List(ctx, m.cloudPrefix(), func(blob blobstore.BlobInfo) error {
		id, ok := m.chunkIDFromBlobKey(blob.Key)
		if !ok {
			return nil
		}
		out = append(out, chunk.CloudBlobInfo{
			ID:           id,
			Size:         blob.Size,
			StorageClass: blob.StorageClass,
			Archived:     blob.IsArchived(),
		})
		return nil
	})
	m.trackCloudResult(err)
	if err != nil {
		return nil, fmt.Errorf("list cloud blobs: %w", err)
	}
	return out, nil
}

// CloudIndexEntries returns the node's cached cloud-backed chunk metadata.
func (m *Manager) CloudIndexEntries() []chunk.ChunkMeta {
	if m.cloudIdx == nil {
		return nil
	}
	var out []chunk.ChunkMeta
	m.cloudIdxMu.Lock()
	err := m.cloudIdx.ForEach(func(_ chunk.ChunkID, meta *chunkMeta) bool {
		out = append(out, meta.toChunkMeta())
		return true
	})
	m.cloudIdxMu.Unlock()
	if err != nil {
		m.logger.Warn("cloud index: ForEach failed while listing entries", "error", err)
	}
	return out
}
