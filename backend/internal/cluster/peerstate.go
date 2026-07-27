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

// raftEntry is the Raft-derived reachability evidence for one peer, folded
// across every Raft group on this node (gastrolog-1lbifx).
//
// # The aggregation rule
//
// Raft contact is per group — cluster-ctl plus one vault-ctl group per vault —
// and a node can hold a different role in each. These two timestamps are the
// ANY-GROUP, MOST-RECENT-WINS fold over all of them:
//
//	lastContact = max over groups G, over both directions, of the last time
//	              this node exchanged Raft traffic with the peer in G
//	lastProbe   = max over groups G of the last time this node ATTEMPTED an
//	              outbound Raft RPC to the peer in G (success or failure)
//
// Why max/any-group and not min/all-groups: contact in ONE group proves the
// peer's process is up and its cluster listener is serving, which is the whole
// question. Silence in another group proves only that no Raft edge exists
// there right now — this node is not its leader, or the peer is not a member,
// or the group has not started yet. Positive evidence aggregates; absence does
// not. An all-groups rule would read a peer as dead the instant leadership
// moved or a vault was created, which is false by construction.
//
// # Why lastProbe exists
//
// Raft's per-group topology is a star: followers exchange traffic only with
// their leader. Two co-followers therefore have NO Raft edge and never will,
// until one of them leads something. Their mutual silence is not evidence of
// death, and a rule that read it that way would declare half a healthy cluster
// unreachable. lastProbe is what separates the two cases: this node only
// probes peers it is actively replicating to, so a fresh lastProbe with a
// stale lastContact means "we are asking and getting nothing back" — real,
// authoritative negative evidence — while a stale lastProbe means "we are not
// asking", and the verdict falls back to the NodeStats broadcast.
//
// lastProbe also retracts itself: a node that loses leadership of a group
// stops probing its members, so the timestamp ages out on its own within
// raftTTL. No leadership or membership bookkeeping is mirrored here — which is
// the point, since a mirrored copy of Raft's configuration is exactly the kind
// of second synced copy this codebase keeps getting bitten by.
type raftEntry struct {
	lastContact time.Time
	lastProbe   time.Time
}

// PeerState stores the most recent NodeStats from each cluster peer, plus the
// Raft-derived reachability evidence that drives peer liveness.
//
// Two independent freshness clocks, deliberately not merged into one:
//
//   - ttl bounds how long a peer's cached NodeStats stays QUERYABLE. Every
//     stats reader (Get, FindVaultStats, VaultStorageProtected, the Aggregate*
//     family, …) uses it, and it is anchored on the NodeStats broadcast
//     interval because that is what refreshes the payload.
//   - raftTTL bounds how long Raft evidence stays ADMISSIBLE for the liveness
//     verdict. It is anchored on the Raft heartbeat timeout, because that is
//     what refreshes it — a leader probes each follower roughly every
//     HeartbeatTimeout/10.
//
// Keeping them apart is what lets liveness be fast (raftTTL, ~seconds) without
// making cached stats expire at the same rate, and vice versa. Before
// gastrolog-1lbifx a dedicated 1s Heartbeat broadcast supplied the fast clock;
// Raft's own per-group traffic already carried it, so the extra broadcast was
// deleted rather than kept as a third opinion.
type PeerState struct {
	mu      sync.RWMutex
	entries map[string]peerEntry
	raft    map[string]raftEntry
	ttl     time.Duration
	raftTTL time.Duration
}

// MarkUnreachable immediately expires a peer so LivePeers() stops including
// it. Called when the record forwarder detects a dead stream — no need to
// wait for the TTL. The next broadcast, or the next Raft contact, restores it.
//
// Both evidence clocks are cleared: leaving the Raft contact standing would
// let it out-vote the forwarder's first-hand knowledge that the peer's stream
// just died, and the whole point of this call is "don't wait, we know".
func (p *PeerState) MarkUnreachable(nodeID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.entries[nodeID]; ok {
		e.received = time.Time{} // zero time = always expired
		p.entries[nodeID] = e
	}
	delete(p.raft, nodeID)
}

// Delete removes a peer's entry entirely. Unlike MarkUnreachable (transient
// — a future broadcast restores the entry), Delete is for permanent removal
// (e.g. the node was dropped from the Raft configuration) so the entry never
// comes back on its own. Used by the Raft peer-removal observer to keep the
// entries map from growing unboundedly across cluster scale-downs.
func (p *PeerState) Delete(nodeID string) {
	p.mu.Lock()
	delete(p.entries, nodeID)
	delete(p.raft, nodeID)
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
	for id := range p.raft {
		if _, ok := keep[id]; !ok {
			delete(p.raft, id)
		}
	}
}

// NewPeerState creates a PeerState.
//
// statsTTL bounds how long a peer's cached NodeStats stays queryable; anchor
// it on the NodeStats broadcast interval. raftContactTTL bounds how long Raft
// reachability evidence stays admissible for the liveness verdict; anchor it
// on the Raft heartbeat timeout. A zero or negative raftContactTTL disables
// the Raft input entirely, leaving liveness on broadcast freshness alone —
// the shape the cluster had before gastrolog-1lbifx, and what a test that
// only exercises broadcast paths should pass.
func NewPeerState(statsTTL, raftContactTTL time.Duration) *PeerState {
	return &PeerState{
		entries: make(map[string]peerEntry),
		raft:    make(map[string]raftEntry),
		ttl:     statsTTL,
		raftTTL: raftContactTTL,
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
// or storageID doesn't parse as a GLID. If a storage's config moves to a
// different node (rare — an operator edits NodeStorageConfig), the two
// nodes' guard ticks aren't synchronized, so there's a transient window of
// up to ~two broadcast intervals where this can return the OLD node's
// stale cached entry (until its next tick drops it via
// retainStorageGuards) and then nil (until the new node's next tick picks
// it up via SetStorageGuard and actually samples it) — never a fabricated
// blend of the two, but not instantaneously consistent either.
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

// RecordRaftContact folds positive Raft reachability evidence for peerID into
// the any-group maximum. Implements multiraft.ContactRecorder; called from the
// Raft transport whenever an outbound RPC to the peer completes cleanly or an
// inbound RPC from it is handled, on any group.
//
// Monotone: an out-of-order or duplicated record can never move the timestamp
// backwards, so a slow group's late callback cannot un-see a fresher one.
func (p *PeerState) RecordRaftContact(peerID, _ string, at time.Time) {
	if peerID == "" {
		return
	}
	p.mu.Lock()
	e := p.raft[peerID]
	if at.After(e.lastContact) {
		e.lastContact = at
		p.raft[peerID] = e
	}
	p.mu.Unlock()
}

// RecordRaftProbe folds an outbound Raft RPC ATTEMPT for peerID into the
// any-group maximum. Implements multiraft.ContactRecorder.
//
// This is what makes a lapse in RecordRaftContact meaningful — see raftEntry's
// doc comment. The group ID is accepted (and ignored) because the aggregation
// is a max over groups: keeping a per-group map would store strictly more
// state to compute the same number, and would then need its own eviction path
// when groups come and go.
func (p *PeerState) RecordRaftProbe(peerID, _ string, at time.Time) {
	if peerID == "" {
		return
	}
	p.mu.Lock()
	e := p.raft[peerID]
	if at.After(e.lastProbe) {
		e.lastProbe = at
		p.raft[peerID] = e
	}
	p.mu.Unlock()
}

// LastSeen returns the most recent moment this node had ANY positive evidence
// that the named peer was alive — the max of its last Raft contact and its
// last received broadcast — or the zero time if neither has ever been
// observed.
//
// Deliberately a pure max over positive evidence, with no probe-authority
// rule: this is the long-horizon accessor. The unreachable sweep
// (gastrolog-2i1g9, five-minute threshold) and the stale-voter reaper
// (gastrolog-6bfwk) both ask "has this node been silent for a very long
// time", and for that question a failing Raft probe must not be allowed to
// discard the fact that the peer was broadcasting a second ago. The
// short-window "is it reachable right now" question is LivePeers/IsLive,
// which does apply probe authority.
//
// A zero return is a deliberate "no positive evidence" signal: the reaper must
// NOT evict on it. Genuinely-never-up nodes are operator territory (manual
// cluster remove-node), not automatic.
func (p *PeerState) LastSeen(nodeID string) time.Time {
	p.mu.RLock()
	defer p.mu.RUnlock()
	last := p.entries[nodeID].received
	if c := p.raft[nodeID].lastContact; c.After(last) {
		last = c
	}
	return last
}

// IsLive reports whether the named peer is reachable right now.
//
// The verdict, in priority order:
//
//  1. Fresh Raft contact (within raftTTL) → live. Positive evidence from any
//     group wins outright, including evidence that arrived inbound while our
//     own probes to the peer were failing: a peer we can hear is alive, even
//     under an asymmetric partition.
//  2. Otherwise, if we are currently probing the peer over Raft (a probe
//     within raftTTL) → NOT live. This is the fast-detection path and the only
//     place absence counts as evidence. It requires roughly raftTTL worth of
//     consecutive unanswered heartbeats, so a single blip cannot trip it.
//     Note the deliberate consequence: a peer whose broadcasts still arrive
//     but whose Raft lane has gone silent reads as not live. That is the
//     honest answer for every consumer of this method — a node the cluster-ctl
//     leader cannot replicate to cannot hold vault leadership or accept
//     replicated writes, whatever else it is still doing.
//  3. Otherwise there is no Raft edge to this peer at all — the boot window
//     before groups exist, a peer we neither lead nor follow, or a cluster
//     whose Raft transport has no contact recorder wired. Fall back to
//     NodeStats broadcast freshness, which is full-mesh by construction and is
//     what liveness rested on before Raft evidence existed.
//
// A peer with no evidence of any kind is not live.
func (p *PeerState) IsLive(nodeID string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.isLiveLocked(nodeID, time.Now())
}

func (p *PeerState) isLiveLocked(nodeID string, now time.Time) bool {
	if p.raftTTL > 0 {
		rc := p.raft[nodeID]
		if !rc.lastContact.IsZero() && now.Sub(rc.lastContact) <= p.raftTTL {
			return true
		}
		if !rc.lastProbe.IsZero() && now.Sub(rc.lastProbe) <= p.raftTTL {
			return false
		}
	}
	received := p.entries[nodeID].received
	return !received.IsZero() && now.Sub(received) <= p.ttl
}

// LivePeers returns the node IDs of every peer IsLive currently answers true
// for. A peer known only through Raft contact (no NodeStats broadcast yet)
// counts — liveness and stats freshness are separate questions.
func (p *PeerState) LivePeers() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	seen := make(map[string]struct{}, len(p.entries)+len(p.raft))
	var live []string
	for id := range p.entries {
		seen[id] = struct{}{}
	}
	for id := range p.raft {
		seen[id] = struct{}{}
	}
	for id := range seen {
		if p.isLiveLocked(id, now) {
			live = append(live, id)
		}
	}
	return live
}

// HandleBroadcast is a subscriber callback for the cluster broadcast system.
// NodeStats is the only payload that lands here: it replaces both the cached
// stats and the last-received timestamp.
//
// There used to be a second case. An empty Heartbeat message flew every second
// purely to refresh last-seen, because PeerState had no faster liveness input
// than the 5s NodeStats payload (gastrolog-2kio8). Raft's own per-group
// heartbeats already prove the same thing on the same wire, so gastrolog-1lbifx
// deleted the extra message rather than keeping a third opinion about whether
// a peer is up. Fast liveness now arrives through RecordRaftContact /
// RecordRaftProbe, and this broadcast carries observability payload only.
func (p *PeerState) HandleBroadcast(msg *gastrologv1.BroadcastMessage) {
	ns := msg.GetNodeStats()
	if ns == nil {
		return
	}
	received := time.Now()
	if msg.Timestamp != nil {
		received = msg.Timestamp.AsTime()
	}
	p.Update(string(msg.SenderId), ns, received)
}
