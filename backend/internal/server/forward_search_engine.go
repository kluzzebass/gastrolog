package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
)

// ForwardSearchEngine resolves the query engine for a ForwardSearch RPC: the
// vault's leader engine on this node, or nil when the request names no vault.
func ForwardSearchEngine(o *orchestrator.Orchestrator, req *apiv1.ForwardSearchRequest) (*query.Engine, error) {
	vaultID := glid.FromBytes(req.GetVaultId())
	if vaultID.IsZero() {
		return nil, nil
	}
	return o.LeaderQueryEngineForVault(vaultID)
}

// pageHistogramBuckets is the bucket count of a search's page histogram.
const pageHistogramBuckets = 50

// pageHistogramGrid is the grid a search's page histogram is counted on: q's
// window cut into pageHistogramBuckets equal buckets. ok is false when q has
// no window, or one narrower than a nanosecond per bucket, so there is no grid
// every node could count on.
func pageHistogramGrid(q query.Query) (grid *apiv1.HistogramGrid, ok bool) {
	start, end, ok := normalizedRange(q.Start, q.End)
	if !ok || end.Sub(start) < pageHistogramBuckets {
		return nil, false
	}
	return &apiv1.HistogramGrid{
		StartNanos: start.UnixNano(),
		EndNanos:   end.UnixNano(),
		Buckets:    pageHistogramBuckets,
	}, true
}

// histogramGridBounds returns the window and bucket count of grid, or an
// error when no node could count on it bucket for bucket.
func histogramGridBounds(grid *apiv1.HistogramGrid) (start, end time.Time, buckets int, err error) {
	if grid == nil {
		return time.Time{}, time.Time{}, 0, errors.New("histogram request carries no grid")
	}
	buckets = int(grid.GetBuckets())
	if buckets < 1 || buckets > query.MaxHistogramBuckets {
		return time.Time{}, time.Time{}, 0, fmt.Errorf("histogram grid of %d buckets, want 1 to %d", buckets, query.MaxHistogramBuckets)
	}
	start, end = time.Unix(0, grid.GetStartNanos()).UTC(), time.Unix(0, grid.GetEndNanos()).UTC()
	if !start.Before(end) || end.Sub(start) < time.Duration(buckets) {
		return time.Time{}, time.Time{}, 0, fmt.Errorf("histogram grid [%s, %s) is too narrow for %d buckets",
			start.Format(time.RFC3339Nano), end.Format(time.RFC3339Nano), buckets)
	}
	return start, end, buckets, nil
}

// ForwardSearchHistogram answers a ForwardSearch carrying a histogram grid:
// the vault's page histogram counted on exactly that grid, as the coordinator
// counts the vaults it leads itself. A grid no node could count on is refused,
// never replaced with a window this node derives: shares counted on different
// grids do not sum.
func ForwardSearchHistogram(ctx context.Context, eng *query.Engine, q query.Query, grid *apiv1.HistogramGrid) ([]*apiv1.HistogramBucket, error) {
	start, end, buckets, err := histogramGridBounds(grid)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	q.Start, q.End = start, end
	return HistogramToProto(eng.ComputeSearchPageHistogram(ctx, q, buckets)), nil
}
