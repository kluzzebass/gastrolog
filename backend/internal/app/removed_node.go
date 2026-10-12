package app

import (
	"context"
	"log/slog"

	"gastrolog/internal/glid"
	"gastrolog/internal/home"
	"gastrolog/internal/server"
)

// removedFromCluster is the cause membership ends with when the cluster
// removes this node.
type removedFromCluster struct {
	removal nodeRemoval
}

func (e *removedFromCluster) Error() string {
	return "this node was removed from the cluster: " + e.removal.Reason
}

// serveRemovedNode runs a node the cluster removed until ctx is cancelled.
//
// The node does not rejoin: removal is the operator's decision, and the
// cluster refuses the identity for good. Nor does it start over as a cluster
// of its own. It stays up so the reason is there to read — in its log, its
// status and its readiness probe — instead of exiting into a restart loop.
func serveRemovedNode(ctx context.Context, logger *slog.Logger, cfg RunConfig, hd home.Dir, id glid.GLID, removal nodeRemoval) error {
	n := server.RemovedNode{NodeID: id.String(), RemovedAt: removal.At, Reason: removal.Reason}
	logger.Error("this node was removed from the cluster and takes no part in it. "+server.RemovedNodeRemedy,
		"node_id", n.NodeID, "removed_at", n.RemovedAt, "reason", n.Reason)
	return server.ServeRemovedNode(ctx, n, Version, cfg.ServerAddr, hd.SocketPath(), logger)
}
