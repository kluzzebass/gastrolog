package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/home"
	"gastrolog/internal/raftwal"
	"gastrolog/internal/server"
)

// errRemovedFromCluster is the cause membership ends with when the cluster
// removes this node.
var errRemovedFromCluster = errors.New("this node was removed from the cluster")

// membershipEnd is how a member learns it was removed — the leader tells it,
// or a member it calls refuses it as removed, which is how a node that was
// down at its removal finds out — and ends membership on the first word.
type membershipEnd struct {
	leave   context.CancelCauseFunc
	once    sync.Once
	removal atomic.Pointer[nodeRemoval]
}

func newMembershipEnd(leave context.CancelCauseFunc) *membershipEnd {
	return &membershipEnd{leave: leave}
}

// removed ends membership with the given reason; later calls change nothing.
// It only latches and cancels, so it is safe from any goroutine at any
// point, a stopping node included.
func (e *membershipEnd) removed(reason string) {
	e.once.Do(func() {
		e.removal.Store(&nodeRemoval{At: time.Now(), Reason: reason})
		e.leave(errRemovedFromCluster)
	})
}

// finishMembership runs once the member side has shut down. A removal
// learned while it ran is recorded in the home — its WAL is closed by now —
// so every later start finds it, and unless the process is stopping anyway
// the node goes on serving the removal.
func finishMembership(ctx context.Context, logger *slog.Logger, cfg RunConfig, hd home.Dir, id glid.GLID, end *membershipEnd, err error) error {
	removal := end.removal.Load()
	if removal == nil {
		return err
	}
	if recErr := recordRemovalInHome(hd, *removal); recErr != nil {
		logger.Error("could not record this node's removal in its home; its next start learns it again from the members that refuse it",
			"error", recErr)
	}
	if ctx.Err() != nil {
		return err
	}
	return serveRemovedNode(ctx, logger, cfg, hd, id, *removal)
}

// recordRemovalInHome persists the removal beside the node identity in the
// home's cluster-ctl WAL.
func recordRemovalInHome(hd home.Dir, removal nodeRemoval) error {
	walDir := hd.ClusterCtlWALDir()
	if _, err := os.Stat(walDir); err != nil {
		return fmt.Errorf("cluster-ctl WAL: %w", err)
	}
	wal, err := raftwal.Open(walDir)
	if err != nil {
		return fmt.Errorf("open cluster-ctl WAL: %w", err)
	}
	if err := recordNodeRemoval(wal.GroupStore("cluster-ctl"), removal); err != nil {
		_ = wal.Close()
		return err
	}
	return wal.Close()
}

// serveRemovedNode runs a node the cluster removed until ctx is cancelled.
//
// The node does not rejoin: removal is the operator's decision, and the
// cluster refuses the identity for good. Nor does it start over as a cluster
// of its own. It stays up so the reason is there to read — in its log, and
// through its Unix socket in `cluster status` — instead of exiting into a
// restart loop.
func serveRemovedNode(ctx context.Context, logger *slog.Logger, cfg RunConfig, hd home.Dir, id glid.GLID, removal nodeRemoval) error {
	n := server.RemovedNode{NodeID: id.String(), RemovedAt: removal.At, Reason: removal.Reason}
	logger.Error("this node was removed from the cluster and takes no part in it. "+server.RemovedNodeRemedy,
		"node_id", n.NodeID, "removed_at", n.RemovedAt, "reason", n.Reason)
	return server.ServeRemovedNode(ctx, n, Version, cfg.ServerAddr, hd.SocketPath(), logger)
}
