package raftgroup

import (
	"log/slog"
	"time"

	hraft "github.com/hashicorp/raft"
)

type raftObservationSource interface {
	CurrentTerm() uint64
	State() hraft.RaftState
}

// ObserveRaftDiagnostics registers Raft observers for leadership and replication
// signals (leader changes, heartbeat lease failures, state transitions). Logger
// must carry a structured "group" attribute identifying the Raft group.
func ObserveRaftDiagnostics(r *hraft.Raft, logger *slog.Logger, leaderLeaseTimeout time.Duration) {
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
