package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/alert"
	"gastrolog/internal/cluster"
	"gastrolog/internal/notify"
	"gastrolog/internal/system"
)

const (
	// defaultUnreachableThreshold is the contact-lapse window after which
	// the sweep transitions a Live node to Unreachable. Overridable via
	// GLOG_UNREACHABLE_THRESHOLD (any time.ParseDuration string).
	//
	// "Contact" is PeerState.LastSeen: the most recent Raft contact on any
	// group shared with the peer, or the arrival of its stats broadcast,
	// whichever is newer. The transition phase runs on
	// the cluster-ctl leader, and cluster-ctl spans every node, so the
	// deciding node is replicating to every peer several times a second —
	// this threshold is measured against a signal that refreshes far faster
	// than it does.
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
	// evidence freshness is the actual detection floor). 6-field cron
	// (with-seconds).
	//
	// This job carries ONLY the Live→Unreachable direction, and it is not a
	// compensator for a missed event. Absence emits nothing: "LastSeen is
	// older than the threshold" is a clock comparison with no upstream event
	// to fix. hraft's FailedHeartbeatObservation marks the ONSET of absence
	// at roughly HeartbeatTimeout — two orders of magnitude below this
	// threshold — so it cannot drive the transition without flapping, and a
	// per-peer one-shot armed at LastSeen+threshold would be the same clock
	// comparison holding more state.
	//
	// Nor can the verdict be derived at read time instead of stored: each
	// node's LastSeen differs under an asymmetric partition, so placement
	// guards on different nodes would disagree. It has to be one
	// FSM-replicated value.
	//
	// Auto-clear does have an event and is NOT here — see nodeLiveness.Run.
	unreachableSweepSchedule = "*/30 * * * * *"

	// unreachableAlertID is the stable per-node alert ID prefix. Format:
	// "node-unreachable:<nodeID>". One Set/Clear pair per node.
	unreachableAlarmType = "node-unreachable"
)

// nodeLiveness transitions nodes between Live and Unreachable based
// on PeerState.LastSeen freshness — Raft contact on any shared group, or the
// peer's stats broadcast, whichever is newer.
//
// Three decisions live here, split by whether an upstream event exists:
//
//   - Live → Unreachable is a threshold on ABSENCE, which emits nothing, so
//     it runs on the scheduled sweep (see unreachableSweepSchedule for why
//     that is not a workaround).
//   - Unreachable → Live is a threshold on PRESENCE, and presence is an
//     event: the returning node's own traffic feeds LastSeen. Evaluated on
//     the broadcast fabric (see Run), not on a tick.
//   - The per-node alert is elapsed absence, so it stays on the sweep for the
//     same reason as Live → Unreachable, plus one of its own: the only events
//     available are broadcasts, and they arrive FROM peers. With a single peer
//     that is the absent node, an event-driven alert would wait on traffic
//     from the very node it exists to report.
//
// Contact-driven gating keeps RF=1 vaults intact across redeploys: when
// a node briefly disappears (pod restart, network blip), the sweep
// flips its NodeConfig.State to Unreachable, which the placement guard
// (placement.go) reads to refuse leader rotation. The chunks stay where
// they live; placement does not move them off the absent node.
//
// Operator-set states (Maintenance, Draining, Decommissioning) are
// never touched by the sweep — they are sticky and cleared only by
// explicit operator action (e.g. `cluster online`, drain completion,
// DeleteNode). Unreachable is the only auto-set state, so the
// auto-set vs operator-set distinction is implicit in the state name:
// no StateSource field is needed.
type nodeLiveness struct {
	cfgStore system.Store
	// isLeader reports cluster-ctl leadership. Injected rather than holding a
	// *cluster.Server because leadership is the only thing this needs from it,
	// and a func keeps the real-raft leader path reachable from tests — the
	// same shape promotionGroup.isLeader uses.
	isLeader       func() bool
	peerState      *cluster.PeerState
	localNodeID    string
	threshold      time.Duration
	alertThreshold time.Duration
	alerts         *alert.Collector
	logger         *slog.Logger
	now            func() time.Time
	wake           *notify.Signal
}

// newNodeLiveness wires the monitor with the configured thresholds.
// `threshold` (GLOG_UNREACHABLE_THRESHOLD) gates Live↔Unreachable
// transitions; `alertThreshold` (GLOG_UNREACHABLE_ALERT_THRESHOLD)
// gates the per-node warning alert. Both fall back to their defaults
// if the env var is missing or unparseable.
func newNodeLiveness(cfgStore system.Store, isLeader func() bool, peerState *cluster.PeerState, localNodeID string, alerts *alert.Collector, logger *slog.Logger) *nodeLiveness {
	threshold := durationFromEnv(logger, "GLOG_UNREACHABLE_THRESHOLD", defaultUnreachableThreshold)
	alertThreshold := durationFromEnv(logger, "GLOG_UNREACHABLE_ALERT_THRESHOLD", defaultUnreachableAlertThreshold)
	return &nodeLiveness{
		cfgStore:       cfgStore,
		isLeader:       isLeader,
		peerState:      peerState,
		localNodeID:    localNodeID,
		threshold:      threshold,
		alertThreshold: alertThreshold,
		alerts:         alerts,
		logger:         logger,
		now:            time.Now,
		wake:           notify.NewSignal(),
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
func startUnreachableSweep(ctx context.Context, scheduler scheduledJobRegistry, sweep *nodeLiveness) error {
	task := func() { sweep.tickOnce(ctx) }
	if err := scheduler.AddJob(unreachableSweepJobName, unreachableSweepSchedule, task); err != nil {
		return err
	}
	scheduler.Describe(unreachableSweepJobName,
		"Elapsed-absence node-state sweep. Contact is Raft last-contact or the peer's stats broadcast, whichever is newer. Two phases per tick: (1) Live→Unreachable — leader-only, proposes SetNodeState once PeerState lastSeen crosses the threshold; (2) alerts — every node, warns when a node has been Unreachable for longer than the alert threshold. Recovery (Unreachable→Live) is NOT on this schedule: it is driven by the returning node's own traffic. Tunable via GLOG_UNREACHABLE_THRESHOLD and GLOG_UNREACHABLE_ALERT_THRESHOLD.")
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

// tickOnce runs the two elapsed-absence phases once; the scheduler owns
// cadence.
//
//   - Live → Unreachable (leader-only): proposes SetNodeState when LastSeen
//     crosses the threshold. Leader-gated so concurrent followers don't
//     issue duplicate transitions.
//   - Alert (every node): raises or clears the per-node warning from
//     time-in-Unreachable. Every node because alerts live in the local
//     alert.Collector — each node's UI needs its own copy.
//
// Auto-clear is deliberately absent: it has an event and runs on the wake
// signal instead, so a node that comes back is not held Unreachable — and
// refused leader rotation by the placement guard — for the remainder of a
// cron interval.
func (s *nodeLiveness) tickOnce(ctx context.Context) {
	if s.isLeader != nil && s.isLeader() {
		s.sweepUnreachable(ctx)
	}
	s.alertTick(ctx)
}

// Run evaluates auto-clear once immediately, then on every trigger until ctx
// is cancelled. Intended to run in its own goroutine.
//
// The wake channel is re-armed BEFORE each evaluation so a trigger arriving
// during autoClear is not lost.
func (s *nodeLiveness) Run(ctx context.Context) {
	wake := s.wake.C()
	s.autoClearIfLeader(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-wake:
			wake = s.wake.C()
			s.autoClearIfLeader(ctx)
		}
	}
}

// autoClearIfLeader is Run's leader gate, mirroring tickOnce: the transition
// is a Raft proposal, so concurrent followers would duplicate it. The gate
// lives at the entry point rather than inside autoClear so the phase stays
// directly callable from tests, matching sweepUnreachable.
func (s *nodeLiveness) autoClearIfLeader(ctx context.Context) {
	if s.isLeader != nil && s.isLeader() {
		s.autoClear(ctx)
	}
}

// trigger requests an auto-clear pass. Non-blocking; coalesces with any other
// pending trigger.
func (s *nodeLiveness) trigger() { s.wake.Notify() }

// onBroadcast is a cluster-broadcast subscriber. Any NodeStats arrival
// triggers, not just the returning node's own, because autoClear re-reads
// every Unreachable node rather than acting on the message.
//
// This covers only half the evidence: LastSeen is the max of broadcast
// arrival and Raft contact, and PeerState's contact-resumed hook supplies the
// other half.
func (s *nodeLiveness) onBroadcast(msg *gastrologv1.BroadcastMessage) {
	if msg.GetNodeStats() != nil {
		s.trigger()
	}
}

// autoClear proposes Unreachable → Live for every node whose contact is
// current again.
//
// A zero LastSeen is "no positive evidence yet" and never clears — a node
// that has never been observed is operator territory.
func (s *nodeLiveness) autoClear(ctx context.Context) {
	nodes, err := s.cfgStore.ListNodes(ctx)
	if err != nil {
		s.logger.Debug("node_liveness: auto-clear list nodes", "error", err)
		return
	}
	now := s.now()
	for _, n := range nodes {
		id := n.ID.String()
		if id == s.localNodeID || n.EffectiveState() != system.NodeStateUnreachable {
			continue
		}
		lastSeen := s.peerState.LastSeen(id)
		if lastSeen.IsZero() {
			continue
		}
		elapsed := now.Sub(lastSeen)
		if elapsed > s.threshold {
			continue
		}
		if err := s.cfgStore.SetNodeState(ctx, n.ID, system.NodeStateLive, now); err != nil {
			s.logger.Warn("node_liveness: propose Live (auto-clear)",
				"node", id, "elapsed", elapsed, "error", err)
			continue
		}
		s.logger.Info("node_liveness: node → Live (auto-clear)",
			"node", id, "elapsed", elapsed)
	}
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
func (s *nodeLiveness) alertTick(ctx context.Context) {
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
// for the local node — a node neither broadcasts to itself nor runs Raft
// RPCs against itself — so its LastSeen would always be zero and the sweep
// would otherwise flip the leader Unreachable as soon as the cluster
// started.
//
// A zero LastSeen is interpreted as "no positive evidence yet" (per
// the documented contract on PeerState.LastSeen) and never produces a
// Live→Unreachable transition: cold-start nodes that have never been
// observed are operator territory, not auto-eviction territory.
func (s *nodeLiveness) sweepUnreachable(ctx context.Context) {
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
			// client-side offline tracker started counting, so the
			// badge transition is seamless.
			if err := s.cfgStore.SetNodeState(ctx, n.ID, system.NodeStateUnreachable, lastSeen); err != nil {
				s.logger.Warn("unreachable_sweep: propose Unreachable",
					"node", id, "elapsed", elapsed, "error", err)
				continue
			}
			s.logger.Info("unreachable_sweep: node → Unreachable",
				"node", id, "elapsed", elapsed, "state_since", lastSeen)
		case system.NodeStateUnreachable:
			// Recovery is autoClear's job, on the wake signal.
		}
	}
}
