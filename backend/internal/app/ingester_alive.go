package app

import (
	"context"
	"log/slog"
	"slices"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// clearStaleIngesterAlive walks every configured ingester and, for each one
// this node is NOT currently running but whose alive map still says it is,
// writes SetIngesterAlive(id, localNode, false). Covers the cases where the
// previous session crashed before its graceful alive=false write, or the
// ingester config was edited to exclude this node while it was down —
// without this, the alive map in Raft retains phantom "true" entries and the
// UI 3/4-style badge counts running nodes incorrectly.
//
// Runs from the ingester convergence sweep, never from the startup path: a
// clear is a cluster-wide store write, and readiness must not gate on write
// quorum — a node restarting while the store cannot accept proposals would
// otherwise stall its boot behind per-write timeouts until the liveness
// probe kills it, looping. Read-first so the steady state proposes nothing:
// only an entry that actually says alive=true for this node is cleared.
func clearStaleIngesterAlive(ctx context.Context, cfgStore system.Store, running []glid.GLID, localNodeID string, logger *slog.Logger) {
	ingesters, err := cfgStore.ListIngesters(ctx)
	if err != nil {
		logger.Warn("stale-alive cleanup: list ingesters failed", "error", err)
		return
	}
	for _, ing := range ingesters {
		if slices.Contains(running, ing.ID) {
			continue
		}
		alive, err := cfgStore.GetIngesterAlive(ctx, ing.ID)
		if err != nil {
			logger.Warn("stale-alive cleanup: read alive map failed",
				"ingester", ing.ID, "error", err)
			continue
		}
		if !alive[localNodeID] {
			continue
		}
		// Ingester exists in config, isn't running on this node, yet the
		// alive map says it is: a stale entry from a previous life.
		if err := cfgStore.SetIngesterAlive(ctx, ing.ID, localNodeID, false); err != nil {
			logger.Warn("stale-alive cleanup: clear failed",
				"ingester", ing.ID, "node", localNodeID, "error", err)
			continue
		}
		logger.Info("cleared stale ingester alive entry",
			"ingester", ing.ID, "name", ing.Name, "node", localNodeID)
	}
}
