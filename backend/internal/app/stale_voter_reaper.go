package app

import (
	"context"
	"log/slog"
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
	return &staleVoterReaper{
		clusterSrv:  clusterSrv,
		peerState:   peerState,
		removeNode:  removeNode,
		localNodeID: localNodeID,
		threshold:   defaultStaleVoterThreshold,
		interval:    staleVoterReapInterval,
		logger:      logger.With("component", "stale-voter-reaper"),
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
		r.logger.Warn("evicting unreachable voter",
			"node_id", srv.ID, "last_seen_ago", age, "threshold", r.threshold)
		if err := r.removeNode(ctx, srv.ID); err != nil {
			r.logger.Warn("eviction failed",
				"node_id", srv.ID, "error", err)
			continue
		}
		r.logger.Info("voter evicted",
			"node_id", srv.ID, "last_seen_ago", age)
	}
}
