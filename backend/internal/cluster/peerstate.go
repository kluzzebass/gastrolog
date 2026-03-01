package cluster

import (
	"sync"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
)

type peerEntry struct {
	stats    *gastrologv1.NodeStats
	received time.Time
}

// PeerState stores the most recent NodeStats from each cluster peer.
// Entries expire after a configurable TTL (typically 3× the broadcast interval).
type PeerState struct {
	mu      sync.RWMutex
	entries map[string]peerEntry
	ttl     time.Duration
}

// NewPeerState creates a PeerState with the given TTL.
func NewPeerState(ttl time.Duration) *PeerState {
	return &PeerState{
		entries: make(map[string]peerEntry),
		ttl:     ttl,
	}
}

// Update stores or replaces the stats for the given sender.
func (p *PeerState) Update(senderID string, stats *gastrologv1.NodeStats, received time.Time) {
	p.mu.Lock()
	p.entries[senderID] = peerEntry{stats: stats, received: received}
	p.mu.Unlock()
}

// Get returns the latest stats for the given sender, or nil if absent or expired.
func (p *PeerState) Get(senderID string) *gastrologv1.NodeStats {
	p.mu.RLock()
	e, ok := p.entries[senderID]
	p.mu.RUnlock()
	if !ok || time.Since(e.received) > p.ttl {
		return nil
	}
	return e.stats
}

// FindVaultStats scans all live peers for a VaultStats matching the given ID.
// Returns nil if no peer reports stats for this vault.
func (p *PeerState) FindVaultStats(vaultID string) *gastrologv1.VaultStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	for _, e := range p.entries {
		if now.Sub(e.received) > p.ttl || e.stats == nil {
			continue
		}
		for _, vs := range e.stats.Vaults {
			if vs.Id == vaultID {
				return vs
			}
		}
	}
	return nil
}

// HandleBroadcast is a subscriber callback for the cluster broadcast system.
// It extracts NodeStats from the broadcast message and stores it.
func (p *PeerState) HandleBroadcast(msg *gastrologv1.BroadcastMessage) {
	if ns := msg.GetNodeStats(); ns != nil {
		received := time.Now()
		if msg.Timestamp != nil {
			received = msg.Timestamp.AsTime()
		}
		p.Update(msg.SenderId, ns, received)
	}
}
