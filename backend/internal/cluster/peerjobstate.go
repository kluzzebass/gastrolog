package cluster

import (
	"sync"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/notify"
)

type peerJobEntry struct {
	jobs     []*gastrologv1.Job
	received time.Time
}

// PeerJobState stores the most recent job list from each cluster peer.
// Entries expire after a configurable TTL (typically 3× the broadcast interval).
type PeerJobState struct {
	// localNodeID is this node, so its own broadcast is not folded into the
	// peer half. Empty means "no self known", preserving pre-loopback
	// behaviour; callers that deliver local broadcasts must set it.
	localNodeID string

	mu      sync.RWMutex
	entries map[string]peerJobEntry
	ttl     time.Duration
	// changes fires every time the entries map is mutated (Update, Delete).
	// Subscribers (e.g. WatchJobs) use it to know when to re-send the
	// merged local+peer job list without polling.
	changes *notify.Signal
}

// NewPeerJobState creates a PeerJobState with the given TTL.
func NewPeerJobState(ttl time.Duration) *PeerJobState {
	return &PeerJobState{
		entries: make(map[string]peerJobEntry),
		ttl:     ttl,
		changes: notify.NewSignal(),
	}
}

// Changes returns a signal fired every time peer-job state mutates. Use
// with notify.Signal's close-and-recreate receive pattern: read once per
// wakeup, re-call Changes() after each wakeup to get the next channel.
func (p *PeerJobState) Changes() *notify.Signal { return p.changes }

// Update stores or replaces the job list for the given sender.
func (p *PeerJobState) Update(senderID string, jobs []*gastrologv1.Job, received time.Time) {
	p.mu.Lock()
	p.entries[senderID] = peerJobEntry{jobs: jobs, received: received}
	p.mu.Unlock()
	p.changes.Notify()
}

// Delete removes a peer's entry entirely. Used when the node is permanently
// removed from the Raft configuration so the entries map doesn't retain
// zombie data from departed peers.
func (p *PeerJobState) Delete(senderID string) {
	p.mu.Lock()
	delete(p.entries, senderID)
	p.mu.Unlock()
	p.changes.Notify()
}

// ReconcilePeers drops any entry whose peer is not in keep. Backstop
// for the observer path when hraft delivers a config change via
// snapshot install (no PeerObservation fires).
func (p *PeerJobState) ReconcilePeers(keep map[string]struct{}) {
	p.mu.Lock()
	removed := false
	for id := range p.entries {
		if _, ok := keep[id]; !ok {
			delete(p.entries, id)
			removed = true
		}
	}
	p.mu.Unlock()
	if removed {
		p.changes.Notify()
	}
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

// SetLocalNodeID tells PeerJobState which node it runs on, so it can ignore
// its own broadcasts. Wire at construction, before any broadcast is handled.
func (p *PeerJobState) SetLocalNodeID(id string) {
	p.mu.Lock()
	p.localNodeID = id
	p.mu.Unlock()
}

// HandleBroadcast is a subscriber callback for the cluster broadcast system.
// It extracts NodeJobs from the broadcast message and stores them.
func (p *PeerJobState) HandleBroadcast(msg *gastrologv1.BroadcastMessage) {
	// Own broadcasts reach this subscriber now, but this cache is the PEER
	// half: JobServer reads local jobs straight from the scheduler, which is
	// live, and merging a broadcast-delayed copy of them in here would both
	// duplicate rows and make this node's own jobs the stalest in the list.
	if p.localNodeID != "" && string(msg.SenderId) == p.localNodeID {
		return
	}
	if nj := msg.GetNodeJobs(); nj != nil {
		received := time.Now()
		if msg.Timestamp != nil {
			received = msg.Timestamp.AsTime()
		}
		p.Update(string(msg.SenderId), nj.Jobs, received)
	}
}
