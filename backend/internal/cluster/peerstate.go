package cluster

import (
	"bytes"
	"sort"
	"sync"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
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

// ReconcilePeers drops any entry whose peer is not in keep. Backstop
// for the observer path when hraft delivers a config change via
// snapshot install (no PeerObservation fires).
func (p *PeerState) ReconcilePeers(keep map[string]struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id := range p.entries {
		if _, ok := keep[id]; !ok {
			delete(p.entries, id)
		}
	}
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

// FindStorageState scans all live peers for a StorageState matching the
// given ID (gastrolog-3cobq4). storageID is the GLID's canonical String()
// form (matches every other resolver in this codebase, e.g. resolve() /
// FindVaultStats callers) — parsed here and compared against the wire's raw
// GLID bytes, never a raw-bytes-vs-string comparison (that mismatch bit
// StatsVaultRouteSnapshot before it was fixed to carry glid.GLID directly).
// A storage is only ever reported by its owning node (only that node can
// statfs the volume), so this returns at most one match. Returns nil if no
// live peer reports state for this storage — e.g. the owning node is down,
// or storageID doesn't parse as a GLID.
func (p *PeerState) FindStorageState(storageID string) *gastrologv1.StorageState {
	id, err := glid.Parse(storageID)
	if err != nil {
		return nil
	}
	want := id.ToProto()
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	for _, e := range p.entries {
		if now.Sub(e.received) > p.ttl || e.stats == nil {
			continue
		}
		for _, ss := range e.stats.Storages {
			if bytes.Equal(ss.Id, want) {
				return ss
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

// AggregateIngesterStats sums an ingester's counters across all live peers.
// For parallel ingesters (running on every node) the sum is the cluster
// total; for singleton ingesters only one node carries non-zero counters
// so the sum still gives the correct number. anyRunning is true when any
// peer reports the ingester as running.
func (p *PeerState) AggregateIngesterStats(ingesterID string) (messages, bytes, errors uint64, anyRunning bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	for _, e := range p.entries {
		if now.Sub(e.received) > p.ttl || e.stats == nil {
			continue
		}
		for _, is := range e.stats.Ingesters {
			if string(is.Id) != ingesterID {
				continue
			}
			messages += is.MessagesIngested
			bytes += is.BytesIngested
			errors += is.Errors
			if is.Running {
				anyRunning = true
			}
		}
	}
	return
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

// AggregateRouteTotals returns the summed cumulative route counters across
// TTL-live peers plus a fingerprint of exactly which peers contributed —
// taken under one lock so the sums and the fingerprint can never disagree.
// The stats collector's summed window re-anchors when the fingerprint
// changes, so a peer's stats expiring and later resuming can never read as
// a throughput spike (gastrolog-mliwrd).
func (p *PeerState) AggregateRouteTotals() (routed, matched int64, members []string) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	for id, e := range p.entries {
		if now.Sub(e.received) > p.ttl || e.stats == nil {
			continue
		}
		routed += e.stats.RouteStatsRouted
		matched += e.stats.RouteStatsMatched
		members = append(members, id)
	}
	sort.Strings(members)
	return routed, matched, members
}

// AggregateRouteStats sums route stats from all live peers.
// Returns per-peer totals merged into a single snapshot.
func (p *PeerState) AggregateRouteStats() (routed, unmatched, matched int64, routeTableActive bool, vaultStats []*gastrologv1.VaultRouteStats, routeStats []*gastrologv1.PerRouteStats) {
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
		routed += e.stats.RouteStatsRouted
		unmatched += e.stats.RouteStatsUnmatched
		matched += e.stats.RouteStatsMatched
		if e.stats.RouteStatsRouteTableActive {
			routeTableActive = true
		}
		for _, vs := range e.stats.RouteVaultStats {
			key := string(vs.VaultId)
			existing, ok := vaultMap[key]
			if !ok {
				vaultMap[key] = &gastrologv1.VaultRouteStats{
					VaultId:        vs.VaultId,
					RecordsMatched: vs.RecordsMatched,
				}
			} else {
				existing.RecordsMatched += vs.RecordsMatched
			}
		}
		for _, rs := range e.stats.RoutePerRouteStats {
			rkey := string(rs.RouteId)
			existing, ok := routeMap[rkey]
			if !ok {
				routeMap[rkey] = &gastrologv1.PerRouteStats{
					RouteId:        rs.RouteId,
					RecordsMatched: rs.RecordsMatched,
				}
			} else {
				existing.RecordsMatched += rs.RecordsMatched
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

// AggregateRouteRates sums live peers' rolling-window routing rates from
// their NodeStats broadcasts, per horizon. Sparks are omitted: per-node tick
// phases differ, so an element-wise sum would fabricate a series no node
// observed. The caller adds the local node's own rates (gastrolog-4eh5ns).
func (p *PeerState) AggregateRouteRates() (routed, matched *gastrologv1.ThroughputRate) {
	routed = &gastrologv1.ThroughputRate{}
	matched = &gastrologv1.ThroughputRate{}
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	for _, e := range p.entries {
		if now.Sub(e.received) > p.ttl || e.stats == nil {
			continue
		}
		addThroughput(routed, e.stats.RouteRouted)
		addThroughput(matched, e.stats.RouteMatched)
	}
	return routed, matched
}

// addThroughput accumulates src's per-horizon rates into dst (nil src is a
// node that has not broadcast rate fields yet).
func addThroughput(dst, src *gastrologv1.ThroughputRate) {
	if src == nil {
		return
	}
	dst.InstantPerSec += src.InstantPerSec
	dst.Avg_1MPerSec += src.Avg_1MPerSec
	dst.Avg_5MPerSec += src.Avg_5MPerSec
	dst.Avg_15MPerSec += src.Avg_15MPerSec
}

// PeerVaultPipelineDisk is one peer node's broadcast pipeline disk counts for a vault.
type PeerVaultPipelineDisk struct {
	NodeID                string
	Working               int
	CompletedStaging      int
	Head                  int
	PreHead               int
	WorkingBytes          int64
	CompletedStagingBytes int64
	HeadBytes             int64
	PreHeadBytes          int64
}

// AggregatePipelineDisk collects per-vault pipeline disk counts from all live peers.
func (p *PeerState) AggregatePipelineDisk() map[glid.GLID][]PeerVaultPipelineDisk {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()

	out := make(map[glid.GLID][]PeerVaultPipelineDisk)
	for nodeID, e := range p.entries {
		if now.Sub(e.received) > p.ttl || e.stats == nil {
			continue
		}
		for _, vd := range e.stats.VaultPipelineDisk {
			vid := glid.FromBytes(vd.GetVaultId())
			out[vid] = append(out[vid], PeerVaultPipelineDisk{
				NodeID:           nodeID,
				Working:          int(vd.GetWorkingSegments()),
				CompletedStaging: int(vd.GetCompletedStagingSegments()),
				Head:             int(vd.GetHeadSegments()),
				PreHead:          int(vd.GetPreHeadSegments()),
			})
		}
	}
	return out
}

// VaultStorageProtected reports whether any live peer has a storage backing
// this vault below its free-space floor (gastrolog-9akebz: renamed from
// VaultDiskProtected — the thresholds moved from VaultConfig to the storage
// entity a vault's placements reference). Combined with the local guard,
// this makes per-vault admission cluster-consistent: the starved storage is
// usually on a different node than the front door taking the records.
func (p *PeerState) VaultStorageProtected(vaultID glid.GLID) bool {
	return p.vaultListedByAnyPeer(vaultID, func(ns *gastrologv1.NodeStats) [][]byte {
		return ns.StorageProtectedVaultIds
	})
}

// VaultSizeCapped reports whether any live peer has this vault at its local
// max-size bound. Same cluster-consistency contract as VaultDiskProtected.
func (p *PeerState) VaultSizeCapped(vaultID glid.GLID) bool {
	return p.vaultListedByAnyPeer(vaultID, func(ns *gastrologv1.NodeStats) [][]byte {
		return ns.SizeCappedVaultIds
	})
}

// VaultAgeBoundCapped reports whether any live peer's retention runner has
// swept and failed to clear this vault's max-age bound, on a policy with
// refuse=true (gastrolog-5yfaqj). Same cluster-consistency contract as
// VaultDiskProtected: only the retention leader for a vault instance
// derives this, so a peer that only fronts ingest for the vault needs the
// broadcast.
func (p *PeerState) VaultAgeBoundCapped(vaultID glid.GLID) bool {
	return p.vaultListedByAnyPeer(vaultID, func(ns *gastrologv1.NodeStats) [][]byte {
		return ns.AgeBoundVaultIds
	})
}

// VaultChunkCountBoundCapped is VaultAgeBoundCapped's max-chunks sibling.
func (p *PeerState) VaultChunkCountBoundCapped(vaultID glid.GLID) bool {
	return p.vaultListedByAnyPeer(vaultID, func(ns *gastrologv1.NodeStats) [][]byte {
		return ns.ChunkCountBoundVaultIds
	})
}

// VaultStorageProtectedNodes returns the live peers currently reporting a
// storage backing this vault under disk protect — the WHO to
// VaultStorageProtected's whether. The placement manager uses it to name
// the degraded home in the vault-home-cannot-store alarm (gastrolog-38bm9t).
// Renamed from VaultDiskProtectedNodes (gastrolog-9akebz).
func (p *PeerState) VaultStorageProtectedNodes(vaultID glid.GLID) []string {
	want := vaultID.ToProto()
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	var nodes []string
	for nodeID, e := range p.entries {
		if now.Sub(e.received) > p.ttl || e.stats == nil {
			continue
		}
		for _, id := range e.stats.StorageProtectedVaultIds {
			if string(id) == string(want) {
				nodes = append(nodes, nodeID)
				break
			}
		}
	}
	return nodes
}

// VaultStorageProtectedNodeNames is VaultStorageProtectedNodes' operator-
// facing sibling: the same live peers, named instead of ID-keyed, for the
// admission-detail signal's "reported by <name>" text (gastrolog-9akebz).
// Deliberately a SEPARATE method from VaultStorageProtectedNodes rather
// than a repurposing of it — the placement manager compares that method's
// output against raw node IDs for set membership (vaultStorageProtectedSet
// in backend/internal/app/placement.go), so swapping its return value to
// names would silently break that match.
//
// The name comes from each peer's OWN broadcast NodeStats.NodeName —
// already resident in this entry, no config-store lookup — falling back to
// the node ID when a peer hasn't reported a name yet. Sorted so the joined
// "reported by a, b" string is stable between reads (map iteration order
// is not).
func (p *PeerState) VaultStorageProtectedNodeNames(vaultID glid.GLID) []string {
	want := vaultID.ToProto()
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	var names []string
	for nodeID, e := range p.entries {
		if now.Sub(e.received) > p.ttl || e.stats == nil {
			continue
		}
		for _, id := range e.stats.StorageProtectedVaultIds {
			if string(id) == string(want) {
				name := e.stats.NodeName
				if name == "" {
					name = nodeID
				}
				names = append(names, name)
				break
			}
		}
	}
	sort.Strings(names)
	return names
}

func (p *PeerState) vaultListedByAnyPeer(vaultID glid.GLID, list func(*gastrologv1.NodeStats) [][]byte) bool {
	want := vaultID.ToProto()
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	for _, e := range p.entries {
		if now.Sub(e.received) > p.ttl || e.stats == nil {
			continue
		}
		for _, id := range list(e.stats) {
			if string(id) == string(want) {
				return true
			}
		}
	}
	return false
}

// LastSeen returns the timestamp of the most recent broadcast received
// from the named peer, or the zero time if no broadcast has ever been
// observed. Used by the stale-voter reaper (gastrolog-6bfwk) to detect
// voters that have been unreachable for longer than the eviction
// threshold — distinct from LivePeers which only answers the
// short-window "is it currently reachable" question (~TTL seconds).
//
// A zero return is a deliberate "no positive evidence" signal: the
// reaper must NOT evict on it. Genuinely-never-up nodes are operator
// territory (manual cluster remove-node), not automatic.
func (p *PeerState) LastSeen(nodeID string) time.Time {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if e, ok := p.entries[nodeID]; ok {
		return e.received
	}
	return time.Time{}
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
// Two payload types update peer liveness here:
//   - NodeStats: full state from the heavy 5s broadcast — replaces both
//     the cached stats and the last-seen timestamp.
//   - Heartbeat: empty marker from the lightweight 1s broadcast — only
//     refreshes last-seen so cached stats from the most recent NodeStats
//     remain queryable. This is what makes paused-peer detection fast
//     without making the bulky payload fly every second. See
//     gastrolog-2kio8.
func (p *PeerState) HandleBroadcast(msg *gastrologv1.BroadcastMessage) {
	received := time.Now()
	if msg.Timestamp != nil {
		received = msg.Timestamp.AsTime()
	}
	if ns := msg.GetNodeStats(); ns != nil {
		p.Update(string(msg.SenderId), ns, received)
		return
	}
	if msg.GetHeartbeat() != nil {
		p.Touch(string(msg.SenderId), received)
		return
	}
}

// Touch refreshes the last-seen timestamp for senderID without changing
// the cached NodeStats. Used by Heartbeat broadcasts (which don't carry
// stats) to extend the TTL of an already-known peer. If senderID has no
// existing entry, a stub entry with nil stats is created so liveness is
// trackable for new peers before their first NodeStats arrives.
func (p *PeerState) Touch(senderID string, received time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.entries[senderID]
	e.received = received
	p.entries[senderID] = e
}
