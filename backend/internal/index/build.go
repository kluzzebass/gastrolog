package index

import (
	"context"

	"github.com/kluzzebass/gastrolog/internal/chunk"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
)

// BuildHelper deduplicates concurrent BuildIndexes calls for the same chunkID
// and parallelizes individual indexers within a single build.
type BuildHelper struct {
	group singleflight.Group
}

func NewBuildHelper() *BuildHelper {
	return &BuildHelper{}
}

// Build runs all indexers for the given chunkID concurrently. If a build for the
// same chunkID is already in flight, this call blocks until it completes and
// shares the result. If the caller's context is cancelled while waiting, it
// returns the context error without cancelling the in-flight build.
func (h *BuildHelper) Build(ctx context.Context, chunkID chunk.ChunkID, indexers []Indexer) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	key := chunkID.String()
	ch := h.group.DoChan(key, func() (interface{}, error) {
		// Detach from the initiator's context so that cancelling one caller
		// does not abort the shared build. Individual callers still exit
		// early via the select on their own ctx.Done() below.
		g, gctx := errgroup.WithContext(context.WithoutCancel(ctx))
		for _, idx := range indexers {
			g.Go(func() error {
				return idx.Build(gctx, chunkID)
			})
		}
		return nil, g.Wait()
	})

	select {
	case res := <-ch:
		return res.Err
	case <-ctx.Done():
		return ctx.Err()
	}
}
