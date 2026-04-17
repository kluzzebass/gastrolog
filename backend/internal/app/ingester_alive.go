package app

import (
	"context"
	"log/slog"
	"slices"

	"gastrolog/internal/orchestrator"
	"gastrolog/internal/system"
)

// clearStaleIngesterAlive walks every configured ingester and, for each one
// this node is NOT currently running, writes SetIngesterAlive(id, localNode,
// false). Idempotent — a clean previous session already wrote false on
// graceful exit; this exists for the cases where the previous session
// crashed or the node was down when the ingester config was edited to
// exclude it. Without this, the alive map in Raft retains phantom "true"
// entries and the UI 3/4-style badge counts running nodes incorrectly.
func clearStaleIngesterAlive(ctx context.Context, cfgStore system.Store, orch *orchestrator.Orchestrator, localNodeID string, logger *slog.Logger) {
	ingesters, err := cfgStore.ListIngesters(ctx)
	if err != nil {
		logger.Warn("startup: list ingesters for stale-alive cleanup failed", "error", err)
		return
	}
	running := orch.ListIngesters()
	for _, ing := range ingesters {
		if slices.Contains(running, ing.ID) {
			continue
		}
		// Ingester exists in config but isn't running on this node. Write
		// alive=false unconditionally — the store treats false as delete
		// (no-op when already absent).
		if err := cfgStore.SetIngesterAlive(ctx, ing.ID, localNodeID, false); err != nil {
			logger.Warn("startup: clear stale alive failed",
				"ingester", ing.ID, "node", localNodeID, "error", err)
		}
	}
}
