package app

import (
	"context"
	"log/slog"
	"os"
	"time"

	"gastrolog/internal/cluster"
)

// gastrolog-6bfwk: K8s scale-down via `kubectl scale` terminates pods
// but doesn't fire `cluster.RemoveNode`, leaving Raft voters and
// NodeConfig entries stranded. The supported `kubernetes-contract`
// recipe calls remove-node explicitly, but operators reach for
// `kubectl scale` constantly; nothing in the system catches up the
// difference. The reaper is the leader-side safety net: it watches
// peer-state heartbeats, identifies voters whose last contact has
// aged past the eviction threshold, and proposes removal via the
// same code path manual `cluster remove-node` uses.
//
// **Conservative semantics — load-bearing.** The reaper only evicts
// peers with positive evidence of past liveness (PeerState.LastSeen
// returns non-zero AND age > threshold). A peer that has NEVER
// broadcast — fresh-join bootstrap, network never connected — is
// operator territory; the reaper won't touch it. This avoids the
// classic auto-eviction failure mode where a transient partition or
// initialization race wipes legitimate voters.
//
// The threshold defaults to 10 minutes — well above the broadcast
// TTL (~15s) and the standard K8s rolling-redeploy window (~60s per
// pod × ordinal count). A transient partition under that bound is
// invisible to the reaper. Above it, the operator is paying
// availability cost regardless; cluster size correctness matters
// more.

const (
	// defaultStaleVoterThreshold is how long a voter can be silent
	// before the reaper evicts it. Override-resistant baseline; if
	// tuning per cluster is needed, surface via ServerSettings in a
	// follow-up.
	defaultStaleVoterThreshold = 10 * time.Minute

	// staleVoterReapInterval is the period between reaper passes.
	// Coarse enough not to spam Raft applies during a partition
	// recovery, fine enough that scale-downs settle within a tick or
	// two after the threshold expires.
	staleVoterReapInterval = 60 * time.Second
)

// raftMembership is the slice of cluster.Server methods the reaper
// reads. Extracted as an interface so tests don't need a live
// hashicorp/raft instance to exercise the tick.
type raftMembership interface {
	IsLeader() bool
	Servers() ([]cluster.RaftServer, error)
}

// peerLastSeener is the slice of cluster.PeerState methods the reaper
// reads. Same rationale as raftMembership.
type peerLastSeener interface {
	LastSeen(nodeID string) time.Time
}

type staleVoterReaper struct {
	clusterSrv  raftMembership
	peerState   peerLastSeener
	removeNode  func(ctx context.Context, nodeID string) error
	localNodeID string
	threshold   time.Duration
	interval    time.Duration
	logger      *slog.Logger
}

func newStaleVoterReaper(
	clusterSrv raftMembership,
	peerState peerLastSeener,
	removeNode func(ctx context.Context, nodeID string) error,
	localNodeID string,
	logger *slog.Logger,
) *staleVoterReaper {
	threshold := defaultStaleVoterThreshold
	interval := staleVoterReapInterval

	// GASTROLOG_STALE_VOTER_THRESHOLD overrides the default 10-minute
	// eviction window. Useful for test clusters and operators who
	// want different patience for transient partitions. Parse as a
	// Go duration ("30s", "5m", "2h"); silently ignore malformed
	// values so a typo can't take down the reaper.
	if raw := os.Getenv("GASTROLOG_STALE_VOTER_THRESHOLD"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			threshold = d
		} else {
			logger.Warn("stale-voter reaper: ignoring invalid GASTROLOG_STALE_VOTER_THRESHOLD",
				"raw", raw, "err", err)
		}
	}
	// GASTROLOG_STALE_VOTER_INTERVAL similarly overrides the 60-second
	// tick cadence. Coupled with threshold mostly for testing — in
	// production the interval can stay coarse because the threshold
	// dominates the response time.
	if raw := os.Getenv("GASTROLOG_STALE_VOTER_INTERVAL"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			interval = d
		} else {
			logger.Warn("stale-voter reaper: ignoring invalid GASTROLOG_STALE_VOTER_INTERVAL",
				"raw", raw, "err", err)
		}
	}

	return &staleVoterReaper{
		clusterSrv:  clusterSrv,
		peerState:   peerState,
		removeNode:  removeNode,
		localNodeID: localNodeID,
		threshold:   threshold,
		interval:    interval,
		logger:      logger.With("component", "stale-voter-reaper", "threshold", threshold, "interval", interval),
	}
}

// Run blocks until ctx cancellation, ticking once per interval and
// reaping any stale voter on the current leader. Safe to invoke on
// every node — the leader-only gate inside tick() makes followers a
// no-op so we don't have to coordinate with leader-change events.
func (r *staleVoterReaper) Run(ctx context.Context) {
	if r == nil || r.clusterSrv == nil || r.peerState == nil || r.removeNode == nil {
		return // not wired (single-node / memory mode)
	}
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.tick(ctx)
		}
	}
}

// tick runs one pass of the reaper. Leader-only; non-leaders return
// without doing anything.
func (r *staleVoterReaper) tick(ctx context.Context) {
	if !r.clusterSrv.IsLeader() {
		return
	}
	servers, err := r.clusterSrv.Servers()
	if err != nil {
		r.logger.Warn("list servers failed", "error", err)
		return
	}

	// Count current voters so we can gate evictions on quorum-preservation
	// (gastrolog-24iv4): refuse to evict if doing so would reduce the
	// cluster's failure tolerance to zero. Decremented after each
	// successful eviction within this tick.
	voterCount := 0
	for _, srv := range servers {
		if srv.Suffrage == "Voter" {
			voterCount++
		}
	}

	now := time.Now()
	for _, srv := range servers {
		if srv.ID == r.localNodeID {
			continue // never reap self
		}
		if srv.Suffrage != "Voter" {
			continue // only voters; staging/nonvoter stragglers are out of scope
		}
		lastSeen := r.peerState.LastSeen(srv.ID)
		if lastSeen.IsZero() {
			// No positive evidence of past liveness — fresh-join
			// bootstrap, never connected, etc. Operator territory.
			continue
		}
		age := now.Sub(lastSeen)
		if age < r.threshold {
			continue
		}
		// gastrolog-24iv4 quorum-preservation gate. On small clusters
		// (N≤3), reaper-driven eviction takes the cluster from
		// "1 voter unreachable but recoverable" to "1 voter
		// permanently removed AND zero failure tolerance" — strictly
		// worse than just waiting for the unreachable node to return.
		// We'd rather keep a ghost voter than auto-destroy the
		// cluster's redundancy for a node that may be in maintenance.
		// Operators with permanently-dead hardware still have manual
		// `cluster remove-node`.
		if !canSafelyEvict(voterCount) {
			r.logger.Info("skipping eviction — would reduce failure tolerance to zero",
				"node_id", srv.ID, "last_seen_ago", age, "voter_count", voterCount)
			continue
		}
		r.logger.Warn("evicting unreachable voter",
			"node_id", srv.ID, "last_seen_ago", age, "threshold", r.threshold)
		if err := r.removeNode(ctx, srv.ID); err != nil {
			r.logger.Warn("eviction failed",
				"node_id", srv.ID, "error", err)
			continue
		}
		r.logger.Info("voter evicted",
			"node_id", srv.ID, "last_seen_ago", age)
		voterCount--
	}
}

// canSafelyEvict reports whether the reaper can remove one voter from
// a cluster of `voterCount` without driving failure tolerance to zero.
//
// Raft voter count N → quorum = floor(N/2)+1 → failure tolerance =
// N − quorum. After eviction the cluster has N−1 voters with quorum =
// floor((N−1)/2)+1 and failure tolerance = (N−1) − quorum.
//
//	N=2: after=1, quorum=1, failure=0. Refuse.
//	N=3: after=2, quorum=2, failure=0. Refuse.
//	N=4: after=3, quorum=2, failure=1. Allow.
//	N=5: after=4, quorum=3, failure=1. Allow.
//	N≥4: always safe — at worst we step from N failure-tolerance to
//	N-1, never to 0.
//
// See gastrolog-24iv4 for the threat model and the alternative fixes
// considered (preStop hook, demote-to-non-voter, boot-time auto-rejoin).
func canSafelyEvict(voterCount int) bool {
	afterN := voterCount - 1
	if afterN < 2 {
		return false
	}
	quorumAfter := afterN/2 + 1
	failureToleranceAfter := afterN - quorumAfter
	return failureToleranceAfter >= 1
}
