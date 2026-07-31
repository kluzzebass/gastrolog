package app

import (
	"log/slog"
)

// peer_cache_reconcile.go: scheduled periodic reconcile of every
// per-peer cache against current Raft membership.
//
// On a FOLLOWER this is not a backstop — it is the only mechanism. The
// observer path (raft.go observePeerRemovals) consumes hraft's
// PeerObservation, which is the sole membership-bearing observation hraft
// emits, and both of its emission sites are inside startStopReplication:
// leader-only code managing leaderState.replState. A follower records
// membership changes in processConfigurationLogEntry and on snapshot
// restore, and emits nothing in either case. Verified against
// hashicorp/raft v1.7.3, which emits exactly eight observations —
// LeaderObservation, PeerObservation x2, RequestVote, RequestPreVote,
// RaftState, and the two heartbeat ones, all leader-side or carrying no
// membership.
//
// So there is no upstream event to fix and then retire this tick against.
// Do not delete it on the strength of the observer existing: without it,
// every follower keeps per-peer cache entries for removed nodes until
// restart.
//
// Registered with the orchestrator's job scheduler so the operator
// can see it in the inspector's Scheduled view alongside the other
// periodic sweeps (cache-eviction, archival-sweep, retention, etc.).

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
// `src` is typically the cluster-ctl cluster.Server (which
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
		"Reconcile per-peer caches (PeerState, PeerJobState, PeerByteMetrics, Broadcaster, StatsCollector) against current Raft membership — backstop for snapshot-install config changes that don't fire PeerObservation")
	return nil
}
