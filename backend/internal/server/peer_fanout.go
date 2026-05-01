package server

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"gastrolog/internal/orchestrator"
)

// peerInspectorTimeout caps how long a single peer's inspector RPC may
// block before it's elided from the response. Sized to be comfortably
// above any healthy peer's round-trip on small metadata payloads
// (typical: tens of ms; warm-cache worst case: a few hundred ms) and
// well below any operator's tolerance for a frozen UI. A paused or
// partitioned peer hits this limit and is silently dropped from the
// merged result, leaving the UI usable instead of frozen. See
// gastrolog-csspr.
const peerInspectorTimeout = 3 * time.Second

// peerFanOut runs fn concurrently against each node and returns the
// successful results in node-order (results[i] corresponds to nodes[i]
// when fn(nodes[i]) succeeded; otherwise results[i] is the zero value
// of T and the slice's "ok" parallel array is false at i).
//
// Each invocation runs under its own context derived from ctx with a
// hard timeout of peerInspectorTimeout, so a hung or paused peer is
// elided from the merged view instead of blocking the whole handler.
// Errors are logged via the supplied logger with the operation name
// and node ID, and never surface to the caller — the entire point of
// this helper is degraded-but-responsive UI behaviour when one peer
// is unreachable.
//
// Use this for unary inspector RPCs only (small request/response).
// Streaming RPCs (SearchStream, Follow) have their own concurrency
// shape and do not need this helper — those return channels that
// naturally end when the parent context cancels. See gastrolog-csspr.
func peerFanOut[T any](
	ctx context.Context,
	logger *slog.Logger,
	op string,
	nodes []string,
	fn func(ctx context.Context, nodeID string) (T, error),
) (results []T, ok []bool) {
	results = make([]T, len(nodes))
	ok = make([]bool, len(nodes))
	if len(nodes) == 0 {
		return results, ok
	}
	var wg sync.WaitGroup
	for i, nodeID := range nodes {
		wg.Go(func() {
			peerCtx, cancel := context.WithTimeout(ctx, peerInspectorTimeout)
			defer cancel()
			val, err := fn(peerCtx, nodeID)
			if err != nil {
				// Demote benign placement-churn errors (peer no longer
				// owns the vault/tier) to Debug — these fire during
				// reconfiguration and aren't operational failures. See
				// gastrolog-5z607.
				level := slog.LevelWarn
				if orchestrator.IsPlacementChurnErr(err) {
					level = slog.LevelDebug
				}
				logger.Log(peerCtx, level, op+": remote node failed",
					"node", nodeID, "error", err)
				return
			}
			results[i] = val
			ok[i] = true
		})
	}
	wg.Wait()
	return results, ok
}
