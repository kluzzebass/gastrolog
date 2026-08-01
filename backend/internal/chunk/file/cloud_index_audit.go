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

// RepairCloudIndex rebuilds this node's cloud index from the blob store:
// drops entries whose object is gone, resets sizes that drifted from the
// object they describe, and indexes objects the cache never recorded.
//
// Objects are never deleted. An object the cache cannot explain is the case
// where the cache is the thing that is wrong.
//
// Every removal is confirmed with a HEAD against the object rather than trusted
// from the listing. Without that, an upload that lands between the listing and
// the removal loses its fresh entry — the listing is a snapshot, and the index
// is written by the live upload path at the same time.
func (m *Manager) RepairCloudIndex(ctx context.Context) (chunk.CloudIndexRepair, error) {
	var repair chunk.CloudIndexRepair
	if m.cfg.CloudStore == nil {
		return repair, chunk.ErrCloudStoreNotConfigured
	}
	blobs, err := m.ListCloudBlobs(ctx)
	if err != nil {
		return repair, err
	}
	sizeByID := make(map[chunk.ChunkID]int64, len(blobs))
	for _, b := range blobs {
		sizeByID[b.ID] = b.Size
	}

	for _, cached := range m.CloudIndexEntries() {
		storeSize, present := sizeByID[cached.ID]
		if !present {
			if m.HeadCloudBlob(cached.ID) == nil {
				continue // raced an upload; the object is really there
			}
			if err := m.dropCloudIndexEntry(cached.ID); err != nil {
				return repair, err
			}
			repair.RemovedEntries++
			continue
		}
		if cached.CloudBytes == storeSize {
			continue
		}
		if err := m.correctCloudIndexSize(cached.ID, storeSize); err != nil {
			return repair, err
		}
		repair.CorrectedSizes++
	}

	// Re-indexing reuses the startup loader: it already skips what is indexed,
	// decodes object metadata, and falls back to the GLCB's own footer when that
	// metadata is unreadable. A second implementation here would be a second
	// place for "what does this object contain" to be answered differently.
	before := len(m.CloudIndexEntries())
	if err := m.loadCloudBackedChunksFromStore(); err != nil {
		return repair, err
	}
	repair.IndexedBlobs = len(m.CloudIndexEntries()) - before

	if !repair.Clean() {
		m.invalidateCloudListCache()
	}
	return repair, nil
}

// invalidateCloudListCache forces the next List() to re-derive its cloud-backed
// half from the index. Takes m.mu, so it must not be called while holding
// cloudIdxMu — the established order in this file acquires them in that
// sequence, never nested the other way.
func (m *Manager) invalidateCloudListCache() {
	m.mu.Lock()
	m.cloudListCache = nil
	m.mu.Unlock()
}

func (m *Manager) dropCloudIndexEntry(id chunk.ChunkID) error {
	m.cloudIdxMu.Lock()
	defer m.cloudIdxMu.Unlock()
	if _, err := m.cloudIdx.Delete(id); err != nil {
		return fmt.Errorf("drop stale cloud index entry %s: %w", id, err)
	}
	if err := m.cloudIdx.Sync(); err != nil {
		return fmt.Errorf("sync cloud index after dropping %s: %w", id, err)
	}
	return nil
}

func (m *Manager) correctCloudIndexSize(id chunk.ChunkID, storeSize int64) error {
	m.cloudIdxMu.Lock()
	defer m.cloudIdxMu.Unlock()
	meta, ok := m.cloudIdx.Lookup(id)
	if !ok {
		return nil // dropped underneath us; nothing to correct
	}
	meta.cloudBytes = storeSize
	if _, err := m.cloudIdx.Delete(id); err != nil {
		return fmt.Errorf("correct cloud index size for %s: %w", id, err)
	}
	if err := m.cloudIdx.Insert(id, meta); err != nil {
		return fmt.Errorf("reinsert corrected cloud index entry %s: %w", id, err)
	}
	if err := m.cloudIdx.Sync(); err != nil {
		return fmt.Errorf("sync cloud index after correcting %s: %w", id, err)
	}
	return nil
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
