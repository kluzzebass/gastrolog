package cluster

import (
	"sync"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
)

type peerJobEntry struct {
	jobs     []*gastrologv1.Job
	received time.Time
}

// PeerJobState stores the most recent job list from each cluster peer.
// Entries expire after a configurable TTL (typically 3× the broadcast interval).
type PeerJobState struct {
	mu      sync.RWMutex
	entries map[string]peerJobEntry
	ttl     time.Duration
}

// NewPeerJobState creates a PeerJobState with the given TTL.
func NewPeerJobState(ttl time.Duration) *PeerJobState {
	return &PeerJobState{
		entries: make(map[string]peerJobEntry),
		ttl:     ttl,
	}
}

// Update stores or replaces the job list for the given sender.
func (p *PeerJobState) Update(senderID string, jobs []*gastrologv1.Job, received time.Time) {
	p.mu.Lock()
	p.entries[senderID] = peerJobEntry{jobs: jobs, received: received}
	p.mu.Unlock()
}

// GetAll returns all non-expired peer job lists, keyed by sender node ID.
func (p *PeerJobState) GetAll() map[string][]*gastrologv1.Job {
	p.mu.RLock()
	defer p.mu.RUnlock()

	now := time.Now()
	result := make(map[string][]*gastrologv1.Job, len(p.entries))
	for id, e := range p.entries {
		if now.Sub(e.received) <= p.ttl {
			result[id] = e.jobs
		}
	}
	return result
}

// HandleBroadcast is a subscriber callback for the cluster broadcast system.
// It extracts NodeJobs from the broadcast message and stores them.
func (p *PeerJobState) HandleBroadcast(msg *gastrologv1.BroadcastMessage) {
	if nj := msg.GetNodeJobs(); nj != nil {
		received := time.Now()
		if msg.Timestamp != nil {
			received = msg.Timestamp.AsTime()
		}
		p.Update(msg.SenderId, nj.Jobs, received)
	}
}
