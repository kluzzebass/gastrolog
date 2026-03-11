package cloud

import (
	"context"
	"log/slog"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
)

// NoopIndexManager is an IndexManager that returns ErrIndexNotFound for all
// lookups. Cloud vaults have no pre-built indexes — queries scan the full blob.
type NoopIndexManager struct{}

// NewIndexFactory returns a factory for no-op index managers.
func NewIndexFactory() index.ManagerFactory {
	return func(_ map[string]string, _ chunk.ChunkManager, _ *slog.Logger) (index.IndexManager, error) {
		return &NoopIndexManager{}, nil
	}
}

func (n *NoopIndexManager) BuildIndexes(_ context.Context, _ chunk.ChunkID) error { return nil }
func (n *NoopIndexManager) DeleteIndexes(_ chunk.ChunkID) error                   { return nil }

func (n *NoopIndexManager) OpenTokenIndex(_ chunk.ChunkID) (*index.Index[index.TokenIndexEntry], error) {
	return nil, index.ErrIndexNotFound
}

func (n *NoopIndexManager) OpenAttrKeyIndex(_ chunk.ChunkID) (*index.Index[index.AttrKeyIndexEntry], error) {
	return nil, index.ErrIndexNotFound
}

func (n *NoopIndexManager) OpenAttrValueIndex(_ chunk.ChunkID) (*index.Index[index.AttrValueIndexEntry], error) {
	return nil, index.ErrIndexNotFound
}

func (n *NoopIndexManager) OpenAttrKVIndex(_ chunk.ChunkID) (*index.Index[index.AttrKVIndexEntry], error) {
	return nil, index.ErrIndexNotFound
}

func (n *NoopIndexManager) OpenKVKeyIndex(_ chunk.ChunkID) (*index.Index[index.KVKeyIndexEntry], index.KVIndexStatus, error) {
	return nil, 0, index.ErrIndexNotFound
}

func (n *NoopIndexManager) OpenKVValueIndex(_ chunk.ChunkID) (*index.Index[index.KVValueIndexEntry], index.KVIndexStatus, error) {
	return nil, 0, index.ErrIndexNotFound
}

func (n *NoopIndexManager) OpenKVIndex(_ chunk.ChunkID) (*index.Index[index.KVIndexEntry], index.KVIndexStatus, error) {
	return nil, 0, index.ErrIndexNotFound
}

func (n *NoopIndexManager) OpenJSONPathIndex(_ chunk.ChunkID) (*index.Index[index.JSONPathIndexEntry], index.JSONIndexStatus, error) {
	return nil, 0, index.ErrIndexNotFound
}

func (n *NoopIndexManager) OpenJSONPVIndex(_ chunk.ChunkID) (*index.Index[index.JSONPVIndexEntry], index.JSONIndexStatus, error) {
	return nil, 0, index.ErrIndexNotFound
}

func (n *NoopIndexManager) IndexesComplete(_ chunk.ChunkID) (bool, error) {
	return false, nil
}

func (n *NoopIndexManager) FindIngestStartPosition(_ chunk.ChunkID, _ time.Time) (uint64, bool, error) {
	return 0, false, index.ErrIndexNotFound
}

func (n *NoopIndexManager) FindSourceStartPosition(_ chunk.ChunkID, _ time.Time) (uint64, bool, error) {
	return 0, false, index.ErrIndexNotFound
}

func (n *NoopIndexManager) IndexSizes(_ chunk.ChunkID) map[string]int64 {
	return nil
}
