package raftgroup

import (
	"log/slog"
	"sync/atomic"
	"time"

	hraft "github.com/hashicorp/raft"
)

// LivenessCounters accumulates node-level Raft liveness events across every
// group observed on this node (gastrolog-1io54g). Broadcast in NodeStats so
// election storms and heartbeat failures announce themselves instead of
// requiring log archaeology.
type LivenessCounters struct {
	// ElectionsStarted counts candidate-state transitions (a node started or
	// restarted an election for some group).
	ElectionsStarted atomic.Uint64
	// LeaderLosses counts leader observations with an empty leader — a group
	// lost its leader from this node's perspective.
	LeaderLosses atomic.Uint64
	// FailedHeartbeats counts leader-side failed replication heartbeats to a
	// peer (the precursor signal to followers timing out).
	FailedHeartbeats atomic.Uint64
}

func (c *LivenessCounters) observe(obs hraft.Observation) {
	if c == nil {
		return
	}
	switch d := obs.Data.(type) {
	case hraft.RaftState:
		if d == hraft.Candidate {
			c.ElectionsStarted.Add(1)
		}
	case hraft.LeaderObservation:
		if d.LeaderID == "" {
			c.LeaderLosses.Add(1)
		}
	case hraft.FailedHeartbeatObservation:
		c.FailedHeartbeats.Add(1)
	}
}

// Snapshot returns the current counter values.
func (c *LivenessCounters) Snapshot() (elections, leaderLosses, failedHeartbeats uint64) {
	if c == nil {
		return 0, 0, 0
	}
	return c.ElectionsStarted.Load(), c.LeaderLosses.Load(), c.FailedHeartbeats.Load()
}

type raftObservationSource interface {
	CurrentTerm() uint64
	State() hraft.RaftState
}

// ObserveRaftDiagnostics registers Raft observers for leadership and replication
// signals (leader changes, heartbeat lease failures, state transitions). Logger
// must carry a structured "group" attribute identifying the Raft group.
func ObserveRaftDiagnostics(r *hraft.Raft, logger *slog.Logger, leaderLeaseTimeout time.Duration, counters *LivenessCounters) {
	ch := make(chan hraft.Observation, 32)
	r.RegisterObserver(hraft.NewObserver(ch, true, func(o *hraft.Observation) bool {
		switch o.Data.(type) {
		case hraft.LeaderObservation, hraft.FailedHeartbeatObservation, hraft.ResumedHeartbeatObservation, hraft.RaftState:
			return true
		default:
			return false
		}
	}))
	go func() {
		for obs := range ch {
			counters.observe(obs)
			logRaftObservation(logger, r, obs, leaderLeaseTimeout)
		}
	}()
}

func logRaftObservation(logger *slog.Logger, r raftObservationSource, obs hraft.Observation, leaderLeaseTimeout time.Duration) {
	term := r.CurrentTerm()
	switch d := obs.Data.(type) {
	case hraft.LeaderObservation:
		if d.LeaderID == "" {
			logger.Warn("raft lost leader",
				"term", term,
				"raft_state", r.State().String(),
			)
		} else {
			logger.Info("raft leader elected",
				"node_id", string(d.LeaderID),
				"addr", string(d.LeaderAddr),
				"term", term,
			)
		}
	case hraft.FailedHeartbeatObservation:
		sinceContact := time.Since(d.LastContact)
		logger.Warn("raft replication heartbeat failed",
			"peer_id", string(d.PeerID),
			"last_contact", d.LastContact,
			"since_last_contact_ms", sinceContact.Milliseconds(),
			"lease_timeout_ms", leaderLeaseTimeout.Milliseconds(),
			"term", term,
			"raft_state", r.State().String(),
		)
	case hraft.ResumedHeartbeatObservation:
		logger.Info("raft replication heartbeat resumed",
			"peer_id", string(d.PeerID),
			"term", term,
		)
	case hraft.RaftState:
		logger.Info("raft state transition",
			"state", d.String(),
			"term", term,
		)
	}
}
