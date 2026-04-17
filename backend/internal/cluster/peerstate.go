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

// MarkUnreachable immediately expires a peer so LivePeers() stops including
// it. Called when the record forwarder detects a dead stream — no need to
// wait for the TTL. The next broadcast from the peer will restore it.
func (p *PeerState) MarkUnreachable(nodeID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.entries[nodeID]; ok {
		e.received = time.Time{} // zero time = always expired
		p.entries[nodeID] = e
	}
}

// Delete removes a peer's entry entirely. Unlike MarkUnreachable (transient
// — a future broadcast restores the entry), Delete is for permanent removal
// (e.g. the node was dropped from the Raft configuration) so the entry never
// comes back on its own. Used by the Raft peer-removal observer to keep the
// entries map from growing unboundedly across cluster scale-downs.
func (p *PeerState) Delete(nodeID string) {
	p.mu.Lock()
	delete(p.entries, nodeID)
	p.mu.Unlock()
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
			if string(vs.Id) == vaultID {
				return vs
			}
		}
	}
	return nil
}

// FindIngesterStats scans all live peers for an IngesterNodeStats matching the given ID.
// Returns nil if no peer reports stats for this ingester.
func (p *PeerState) FindIngesterStats(ingesterID string) *gastrologv1.IngesterNodeStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	for _, e := range p.entries {
		if now.Sub(e.received) > p.ttl || e.stats == nil {
			continue
		}
		for _, is := range e.stats.Ingesters {
			if string(is.Id) == ingesterID {
				return is
			}
		}
	}
	return nil
}

// CollectIngesterAlive returns a map of nodeID → running for the given ingester
// across all live peers. Only includes peers that report stats for this ingester.
func (p *PeerState) CollectIngesterAlive(ingesterID string) map[string]bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make(map[string]bool)
	now := time.Now()
	for nodeID, e := range p.entries {
		if now.Sub(e.received) > p.ttl || e.stats == nil {
			continue
		}
		for _, is := range e.stats.Ingesters {
			if string(is.Id) == ingesterID {
				result[nodeID] = is.Running
				break
			}
		}
	}
	return result
}

// AggregateRouteStats sums route stats from all live peers.
// Returns per-peer totals merged into a single snapshot.
func (p *PeerState) AggregateRouteStats() (ingested, dropped, routed int64, filterActive bool, vaultStats []*gastrologv1.VaultRouteStats, routeStats []*gastrologv1.PerRouteStats) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()

	// Merge per-vault and per-route stats across peers.
	vaultMap := make(map[string]*gastrologv1.VaultRouteStats)
	routeMap := make(map[string]*gastrologv1.PerRouteStats)

	for _, e := range p.entries {
		if now.Sub(e.received) > p.ttl || e.stats == nil {
			continue
		}
		ingested += e.stats.RouteStatsIngested
		dropped += e.stats.RouteStatsDropped
		routed += e.stats.RouteStatsRouted
		if e.stats.RouteStatsFilterActive {
			filterActive = true
		}
		for _, vs := range e.stats.RouteVaultStats {
			key := string(vs.VaultId)
			existing, ok := vaultMap[key]
			if !ok {
				vaultMap[key] = &gastrologv1.VaultRouteStats{
					VaultId:          vs.VaultId,
					RecordsMatched:   vs.RecordsMatched,
					RecordsForwarded: vs.RecordsForwarded,
				}
			} else {
				existing.RecordsMatched += vs.RecordsMatched
				existing.RecordsForwarded += vs.RecordsForwarded
			}
		}
		for _, rs := range e.stats.RoutePerRouteStats {
			rkey := string(rs.RouteId)
			existing, ok := routeMap[rkey]
			if !ok {
				routeMap[rkey] = &gastrologv1.PerRouteStats{
					RouteId:          rs.RouteId,
					RecordsMatched:   rs.RecordsMatched,
					RecordsForwarded: rs.RecordsForwarded,
				}
			} else {
				existing.RecordsMatched += rs.RecordsMatched
				existing.RecordsForwarded += rs.RecordsForwarded
			}
		}
	}

	for _, vs := range vaultMap {
		vaultStats = append(vaultStats, vs)
	}
	for _, rs := range routeMap {
		routeStats = append(routeStats, rs)
	}
	return
}

// LivePeers returns the node IDs of all peers whose stats have not expired.
func (p *PeerState) LivePeers() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	var live []string
	for id, e := range p.entries {
		if now.Sub(e.received) <= p.ttl {
			live = append(live, id)
		}
	}
	return live
}

// HandleBroadcast is a subscriber callback for the cluster broadcast system.
// It extracts NodeStats from the broadcast message and stores it.
func (p *PeerState) HandleBroadcast(msg *gastrologv1.BroadcastMessage) {
	if ns := msg.GetNodeStats(); ns != nil {
		received := time.Now()
		if msg.Timestamp != nil {
			received = msg.Timestamp.AsTime()
		}
		p.Update(string(msg.SenderId), ns, received)
	}
}
