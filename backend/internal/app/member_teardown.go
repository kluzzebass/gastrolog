package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"gastrolog/internal/cluster"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/raftwal"
	"gastrolog/internal/server"
)

// memberTeardown stops what runMember has started, in the one safe order,
// however runMember returns: at the end of a normal run, or from any point
// of startup when membership ends or a step fails. A removed node must not
// keep cluster machinery running while it serves its removal.
//
// The order: orchestrator, broadcaster, vault Raft groups, cluster-ctl Raft
// (with its WAL), vault-ctl WAL, cluster server, API server. Raft must stop
// while the transport is alive, or the leader's replication goroutines
// block on dead connections.
type memberTeardown struct {
	logger *slog.Logger

	orch        *orchestrator.Orchestrator
	broadcaster *cluster.Broadcaster
	groupMgr    *raftgroup.GroupManager
	configStore io.Closer
	vaultWAL    *raftwal.WAL
	clusterSrv  *cluster.Server

	httpSrv     *server.Server
	httpServing sync.WaitGroup
}

// httpDrain bounds how long stopping the API server waits for in-flight
// requests; the context that ended the run is already cancelled by then.
const httpDrain = 300 * time.Millisecond

// serveHTTP serves srv on addr until the teardown stops it.
func (t *memberTeardown) serveHTTP(srv *server.Server, addr string) {
	t.httpSrv = srv
	t.httpServing.Go(func() {
		if err := srv.ServeTCP(addr); err != nil {
			t.logger.Error("server error", "error", err)
		}
	})
}

// run stops everything started so far and returns the orchestrator's stop
// error, if any.
func (t *memberTeardown) run() error {
	var stopErr error
	if t.orch != nil {
		t.logger.Info("shutting down orchestrator")
		if err := t.orch.Stop(); err != nil && !errors.Is(err, orchestrator.ErrNotRunning) {
			stopErr = err
		}
		t.orch.Close()
	}
	if t.broadcaster != nil {
		_ = t.broadcaster.Close()
	}
	if t.groupMgr != nil {
		t.logger.Info("shutting down vault multiraft groups")
		t.groupMgr.Shutdown()
	}
	if t.configStore != nil {
		t.logger.Info("shutting down cluster-ctl raft")
		_ = t.configStore.Close()
	}
	if t.vaultWAL != nil {
		if err := t.vaultWAL.Close(); err != nil {
			t.logger.Error("vault-ctl raftwal close failed", "error", err)
		}
	}
	if t.clusterSrv != nil {
		t.logger.Info("stopping cluster server")
		t.clusterSrv.Stop()
	}
	if t.httpSrv != nil {
		t.logger.Info("stopping server")
		stopCtx, cancel := context.WithTimeout(context.Background(), httpDrain)
		err := t.httpSrv.Stop(stopCtx)
		cancel()
		// A peer holding a long request across shutdown makes these the
		// expected outcomes, not failures.
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			t.logger.Error("server stop error", "error", err)
		}
		t.httpServing.Wait()
	}
	t.logger.Info("shutdown complete")
	return stopErr
}

// memberStartupHook, when set, runs at each startup stage of runMember with
// the node, its membership end and what has started so far. It is the seam
// tests deliver a removal through at a chosen point of startup.
var memberStartupHook func(stage, nodeID string, end *membershipEnd, td *memberTeardown)

// startupStage marks a point of runMember's startup. Startup does not go on
// past it once membership has ended (or the run was cancelled): runMember
// returns, and the teardown stops what had started.
func startupStage(ctx context.Context, stage, nodeID string, end *membershipEnd, td *memberTeardown) error {
	if memberStartupHook != nil {
		memberStartupHook(stage, nodeID, end, td)
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return nil
}
