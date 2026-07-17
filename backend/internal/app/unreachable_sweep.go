package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/cluster"
	"gastrolog/internal/system"
)

const (
	// defaultUnreachableThreshold is the heartbeat-lapse window after
	// which the sweep transitions a Live node to Unreachable. Overridable
	// via GLOG_UNREACHABLE_THRESHOLD (any time.ParseDuration string).
	defaultUnreachableThreshold = 5 * time.Minute

	// defaultUnreachableAlertThreshold is the time-in-Unreachable window
	// after which a warning alert fires for the node. Independent from
	// the detection threshold because operators want a separate dial:
	// "flip to Unreachable quickly so placement reroutes, but only page
	// us when it stays Unreachable for longer." Overridable via
	// GLOG_UNREACHABLE_ALERT_THRESHOLD.
	defaultUnreachableAlertThreshold = 5 * time.Minute

	// unreachableSweepJobName is the operator-visible name shown in
	// the inspector's Scheduled view. Keep stable across releases.
	unreachableSweepJobName = "node-unreachable-sweep"

	// unreachableSweepSchedule runs every 30 seconds. Low frequency by
	// design — the transition phase proposes Raft commands, so faster
	// ticks just add log churn without helping detection (PeerState's
	// TTL is the actual detection floor). 6-field cron (with-seconds).
	unreachableSweepSchedule = "*/30 * * * * *"

	// unreachableAlertID is the stable per-node alert ID prefix. Format:
	// "node-unreachable:<nodeID>". One Set/Clear pair per node.
	unreachableAlarmType = "node-unreachable"
)

// unreachableSweep transitions nodes between Live and Unreachable based
// on PeerState heartbeat freshness. Runs on the cluster-ctl leader only.
//
// Heartbeat-driven gating closes the RF=1 redeploy bug (gastrolog-2i1g9):
// when a node briefly disappears (pod restart, network blip), the sweep
// flips its NodeConfig.State to Unreachable, which the placement guard
// (placement.go) reads to refuse leader rotation. The chunks stay where
// they live; placement does not move them off the absent node.
//
// State transitions handled here:
//   - Live → Unreachable: lastSeen older than threshold (heartbeat lapse)
//   - Unreachable → Live: lastSeen within threshold (heartbeat resume,
//     a.k.a. auto-clear)
//
// Operator-set states (Maintenance, Draining, Decommissioning) are
// never touched by the sweep — they are sticky and cleared only by
// explicit operator action (e.g. `cluster online`, drain completion,
// DeleteNode). Unreachable is the only auto-set state, so the
// auto-set vs operator-set distinction is implicit in the state name:
// no StateSource field is needed.
type unreachableSweep struct {
	cfgStore       system.Store
	clusterSrv     *cluster.Server
	peerState      *cluster.PeerState
	localNodeID    string
	threshold      time.Duration
	alertThreshold time.Duration
	alerts         *alert.Collector
	logger         *slog.Logger
	now            func() time.Time
}

// newUnreachableSweep wires the sweep with the configured thresholds.
// `threshold` (GLOG_UNREACHABLE_THRESHOLD) gates Live↔Unreachable
// transitions; `alertThreshold` (GLOG_UNREACHABLE_ALERT_THRESHOLD)
// gates the per-node warning alert. Both fall back to their defaults
// if the env var is missing or unparseable.
func newUnreachableSweep(cfgStore system.Store, clusterSrv *cluster.Server, peerState *cluster.PeerState, localNodeID string, alerts *alert.Collector, logger *slog.Logger) *unreachableSweep {
	threshold := durationFromEnv(logger, "GLOG_UNREACHABLE_THRESHOLD", defaultUnreachableThreshold)
	alertThreshold := durationFromEnv(logger, "GLOG_UNREACHABLE_ALERT_THRESHOLD", defaultUnreachableAlertThreshold)
	return &unreachableSweep{
		cfgStore:       cfgStore,
		clusterSrv:     clusterSrv,
		peerState:      peerState,
		localNodeID:    localNodeID,
		threshold:      threshold,
		alertThreshold: alertThreshold,
		alerts:         alerts,
		logger:         logger,
		now:            time.Now,
	}
}

// startUnreachableSweep registers the sweep with the supplied
// scheduler as a recurring job. Returns the scheduler's AddJob
// error if any. On success, attaches a Describe text for the
// inspector's Scheduled view so the operator can see what the job
// does + which phases run on which nodes.
//
// The job task closes over ctx so the per-tick FSM reads and
// proposals share the app's lifetime context; the scheduler handles
// cadence and singleton concurrency.
func startUnreachableSweep(ctx context.Context, scheduler scheduledJobRegistry, sweep *unreachableSweep) error {
	task := func() { sweep.tickOnce(ctx) }
	if err := scheduler.AddJob(unreachableSweepJobName, unreachableSweepSchedule, task); err != nil {
		return err
	}
	scheduler.Describe(unreachableSweepJobName,
		"Heartbeat-driven Live↔Unreachable node-state sweep. Two phases per tick: (1) transitions — leader-only, proposes SetNodeState commands when PeerState lastSeen crosses the threshold; (2) alerts — every node, raises a warning when a node has been Unreachable for longer than the alert threshold. Tunable via GLOG_UNREACHABLE_THRESHOLD and GLOG_UNREACHABLE_ALERT_THRESHOLD.")
	return nil
}

func durationFromEnv(logger *slog.Logger, key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		logger.Warn("unreachable_sweep: invalid env override, using default",
			"key", key, "value", v, "default", fallback)
		return fallback
	}
	return d
}

// tickOnce runs both phases of the sweep — the scheduler handles
// cadence, so the loop and ticker that used to live here are gone.
//
//   - Transition phase (leader-only): scans NodeConfig records and
//     proposes Live↔Unreachable transitions based on PeerState
//     heartbeat freshness. Only the cluster-ctl leader proposes so
//     concurrent followers don't issue duplicate transitions.
//   - Alert phase (every node): scans NodeConfig records and raises
//     or clears the per-node warning alert based on time-in-Unreachable.
//     Runs on every node because alerts live in the local
//     alert.Collector — the UI on each node needs its own copy.
func (s *unreachableSweep) tickOnce(ctx context.Context) {
	if s.clusterSrv != nil && s.clusterSrv.IsLeader() {
		s.tick(ctx)
	}
	s.alertTick(ctx)
}

// alertTick evaluates Unreachable-duration alerts on every node. The
// FSM-replicated NodeConfig (with StateSince set by every transition
// command) is the source of truth, so every node sees the same value
// and arrives at the same alert decision — no leader gating required.
//
// Maintenance, Draining, and Decommissioning intentionally do not
// alert: operators set those deliberately, so the UI tone alone
// (informational badge, no warning) communicates intent without
// pestering them.
func (s *unreachableSweep) alertTick(ctx context.Context) {
	if s.alerts == nil {
		return
	}
	nodes, err := s.cfgStore.ListNodes(ctx)
	if err != nil {
		s.logger.Debug("unreachable_sweep: alert list nodes", "error", err)
		return
	}
	now := s.now()
	for _, n := range nodes {
		state := n.EffectiveState()
		if state != system.NodeStateUnreachable {
			s.alerts.Clear(unreachableAlarmType, n.ID.String())
			continue
		}
		if n.StateSince.IsZero() {
			continue
		}
		elapsed := now.Sub(n.StateSince)
		if elapsed < s.alertThreshold {
			s.alerts.Clear(unreachableAlarmType, n.ID.String())
			continue
		}
		label := n.Name
		if label == "" {
			label = n.ID.String()
		}
		s.alerts.Raise(unreachableAlarmType, n.ID.String(),
			fmt.Sprintf("node %s has been Unreachable for %s", label, elapsed.Round(time.Second)))
	}
}

// tick evaluates every NodeConfig once and proposes one of:
//   - SetNodeState(Unreachable) for Live nodes with stale heartbeat
//   - SetNodeState(Live) for Unreachable nodes with current heartbeat
//
// The leader's own row is skipped because PeerState carries no entry
// for the local node — Heartbeats are broadcast TO peers, not back to
// self — so its LastSeen would always be zero and the sweep would
// otherwise flip the leader Unreachable as soon as the cluster started.
//
// A zero LastSeen is interpreted as "no positive evidence yet" (per
// the documented contract on PeerState.LastSeen) and never produces a
// Live→Unreachable transition: cold-start nodes that have never been
// observed are operator territory, not auto-eviction territory.
func (s *unreachableSweep) tick(ctx context.Context) {
	nodes, err := s.cfgStore.ListNodes(ctx)
	if err != nil {
		s.logger.Error("unreachable_sweep: list nodes", "error", err)
		return
	}
	now := s.now()
	for _, n := range nodes {
		id := n.ID.String()
		if id == s.localNodeID {
			continue
		}
		lastSeen := s.peerState.LastSeen(id)
		switch n.EffectiveState() {
		case system.NodeStateUnknown:
			// EffectiveState maps Unknown→Live; this branch is
			// unreachable but required by the exhaustive-switch lint.
		case system.NodeStateMaintenance, system.NodeStateDraining, system.NodeStateDecommissioning:
			// Operator-sticky states: the sweep never touches them.
			// They clear only via explicit operator action (e.g.
			// `cluster online`, drain completion, DeleteNode). See
			// docs/node-lifecycle-design.md "Behavior gates by state".
		case system.NodeStateLive:
			if lastSeen.IsZero() {
				continue
			}
			elapsed := now.Sub(lastSeen)
			if elapsed <= s.threshold {
				continue
			}
			// StateSince records when the node actually went silent,
			// not when the sweep noticed. Anchors the inspector's
			// "unreachable Xm" duration to the same moment the
			// client-side offline tracker started counting
			// (gastrolog-778iv) so the badge transition is seamless.
			if err := s.cfgStore.SetNodeState(ctx, n.ID, system.NodeStateUnreachable, lastSeen); err != nil {
				s.logger.Warn("unreachable_sweep: propose Unreachable",
					"node", id, "elapsed", elapsed, "error", err)
				continue
			}
			s.logger.Info("unreachable_sweep: node → Unreachable",
				"node", id, "elapsed", elapsed, "state_since", lastSeen)
		case system.NodeStateUnreachable:
			if lastSeen.IsZero() {
				continue
			}
			elapsed := now.Sub(lastSeen)
			if elapsed > s.threshold {
				continue
			}
			if err := s.cfgStore.SetNodeState(ctx, n.ID, system.NodeStateLive, now); err != nil {
				s.logger.Warn("unreachable_sweep: propose Live (auto-clear)",
					"node", id, "elapsed", elapsed, "error", err)
				continue
			}
			s.logger.Info("unreachable_sweep: node → Live (auto-clear)",
				"node", id, "elapsed", elapsed)
		}
	}
}
