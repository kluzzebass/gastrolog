package index

import (
	"context"

	"github.com/kluzzebass/gastrolog/internal/chunk"
)

type Indexer interface {
	// Name returns a stable identifier for this indexer
	// (e.g. "time", "source", "token").
	Name() string

	// Build builds the index for the given sealed chunk.
	// It is expected to:
	// - open its own cursor
	// - read records
	// - write its own index artifacts
	//
	// Build must be idempotent or overwrite existing artifacts.
	Build(ctx context.Context, chunkID chunk.ChunkID) error
}
