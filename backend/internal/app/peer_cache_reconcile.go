package app

import (
	"log/slog"
)

// peer_cache_reconcile.go: scheduled periodic reconcile of every
// per-peer cache against current Raft membership. Belt-and-suspenders
// for the observer path (raft.go observePeerRemovals) — hraft does
// not fire PeerObservation when a config change is delivered via
// snapshot install (only on log apply), so a follower behind by a
// snapshot can miss removal events.
//
// Registered with the orchestrator's job scheduler so the operator
// can see it in the inspector's Scheduled view alongside the other
// periodic sweeps (cache-eviction, archival-sweep, retention, etc.).
// gastrolog-9ohip.

const (
	// peerCacheReconcileJobName is the operator-visible name shown
	// in the inspector. Keep stable across releases; changing it
	// breaks any saved filters or alerts the operator wired up.
	peerCacheReconcileJobName = "peer-cache-reconcile"

	// peerCacheReconcileSchedule runs every 30 seconds. 6-field cron
	// (with-seconds). Phase-offset from the existing per-minute
	// sweeps in orchestrator/ (retention=second 0, cache-eviction=23,
	// catchup=13/33/53) so simultaneous ticks don't pile up — though
	// at 30s cadence we will overlap with second-0 sweeps every
	// other minute, which is fine because all of these are
	// singleton-scheduled and short-running.
	peerCacheReconcileSchedule = "*/30 * * * * *"
)

// scheduledJobRegistry is the minimal contract this package needs
// from a scheduler — narrow interface so each migration's registrar
// can fake it in tests without depending on the orchestrator package.
// Satisfied by *orchestrator.Scheduler.
type scheduledJobRegistry interface {
	AddJob(name, cronExpr string, taskFn any, args ...any) error
	Describe(name, description string)
}

// startPeerCacheReconcile registers the reconcile work as a named
// scheduled job. Returns AddJob's error so the caller can decide
// whether to fatal or log-and-continue. On success, attaches a
// human-readable description for the inspector's Scheduled view.
//
// `src` is typically the system-Raft cluster.Server (which
// satisfies memberSource via its Servers() method); `caches` is the
// same set passed to observePeerRemovals so the observer (fast
// path) and the reconciler (backstop) operate on identical state.
func startPeerCacheReconcile(scheduler scheduledJobRegistry, src memberSource, logger *slog.Logger, caches ...peerCacheReconciler) error {
	task := func() {
		reconcilePeerCachesOnce(src, logger, caches...)
	}
	if err := scheduler.AddJob(peerCacheReconcileJobName, peerCacheReconcileSchedule, task); err != nil {
		return err
	}
	scheduler.Describe(peerCacheReconcileJobName,
		"Reconcile per-peer caches (PeerState, PeerJobState, PeerByteMetrics, Broadcaster, StatsCollector, RecordForwarder) against current Raft membership — backstop for snapshot-install config changes that don't fire PeerObservation (gastrolog-9ohip)")
	return nil
}
