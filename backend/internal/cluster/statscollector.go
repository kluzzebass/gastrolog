package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/alert"
	"gastrolog/internal/glid"
	"gastrolog/internal/notify"
	"gastrolog/internal/sysmetrics"
)

// StatsVaultSnapshot is the stats collector's view of a vault.
// Mirrors orchestrator.VaultSnapshot without importing it.
//
// ID is the canonical glid.GLID. Earlier shapes stored it as the
// String() form and then cast `[]byte(s.ID)` for the broadcast,
// which silently encoded the ASCII bytes of the base32 string —
// breaking round-trip on the receiver. The canonical type closes
// that hole; statscollector emits the raw proto bytes via ToProto().
type StatsVaultSnapshot struct {
	ID               glid.GLID
	Name             string
	RecordCount      int64
	ChunkCount       int
	SealedChunks     int
	DataBytes        int64
	Enabled          bool
	RaftAppliedIndex uint64
}

// StatsVaultAppendSnapshot captures one vault's cumulative pipeline stage
// counters for broadcast; the collector's rolling windows turn them into
// per-second rates (gastrolog-4eh5ns, gastrolog-10n6k8). Append counters are
// origin-side; Collected/Sealed are home-side and zero on nodes without that
// role for the vault.
type StatsVaultAppendSnapshot struct {
	VaultID          glid.GLID
	RecordsAppended  uint64
	BytesAppended    uint64
	RecordsDurable   uint64
	QueueDepth       int
	QueueCap         int
	CollectedRecords uint64
	CollectedBytes   uint64
	SealedRecords    uint64
	SealedBytes      uint64

	// Discrete pipeline stage-count milestones (gastrolog-4r784a), monotonic
	// per vault. Origin-owned: SegmentsCompleted, SegmentsPublished. Home-owned:
	// ChunksBuilt, HeadPurges, GLCBPullsAttempted/Failed, RetentionDeletes.
	// Leader-owned: ChunksPlanned, ChunksSealed, SegmentsReleased. Cluster
	// totals are the sum across nodes (each milestone counted once by its
	// owner, so summing never double-counts).
	SegmentsCompleted  uint64
	SegmentsPublished  uint64
	SegmentsReleased   uint64
	ChunksPlanned      uint64
	ChunksBuilt        uint64
	ChunksSealed       uint64
	HeadPurges         uint64
	GLCBPullsAttempted uint64
	GLCBPullsFailed    uint64
	RetentionDeletes   uint64
}

// StatsRouteSnapshot captures route stats for broadcast.
type StatsRouteSnapshot struct {
	Routed           int64
	Unmatched        int64
	Matched          int64
	RouteTableActive bool
	VaultStats       []StatsVaultRouteSnapshot
	RouteStats       []StatsPerRouteSnapshot
}

// StatsVaultRouteSnapshot captures per-vault route stats.
//
// VaultID is the canonical glid.GLID type (NOT the String() form). The
// marshal step uses VaultID.ToProto() to emit raw 16-byte proto bytes;
// stuffing the String() form into a string field and then casting to
// []byte produced ASCII bytes of the base32hex GLID string, which broke
// round-tripping and caused the dedup map at the consumer to miss
// (resulting in duplicate per-vault rows in the inspector).
type StatsVaultRouteSnapshot struct {
	VaultID glid.GLID
	Matched int64
}

// StatsPerRouteSnapshot captures per-route stats. Same GLID-encoding
// note as StatsVaultRouteSnapshot.
type StatsPerRouteSnapshot struct {
	RouteID glid.GLID
	Matched int64
}

// StatsVaultPipelineDiskSnapshot is local on-disk pipeline segment counts
// for one vault on this node, broadcast via NodeStats.
type StatsVaultPipelineDiskSnapshot struct {
	VaultID          glid.GLID
	Working          int
	CompletedStaging int
	Head             int
	PreHead          int
}

// StatsProvider abstracts the orchestrator for stats collection.
// Defined here at the consumer site to avoid importing orchestrator.
type StatsProvider interface {
	IngestQueueDepth() int
	IngestQueueCapacity() int
	VaultSnapshots() []StatsVaultSnapshot
	IngesterIDs() []string
	IngesterStats(id string) (name string, messages, bytes, errors int64, running bool)
	RouteStats() StatsRouteSnapshot
	VaultAppendStats() []StatsVaultAppendSnapshot
	PipelineDiskSnapshots() []StatsVaultPipelineDiskSnapshot
	LocalStorageBytes() int64
	// DiskProtectedVaults lists vaults whose local backing volume is below
	// its free-space floor. Broadcast so every node's admission gate can
	// honor the cluster-wide union.
	DiskProtectedVaults() []glid.GLID
	// SizeCappedVaults lists vaults at their local max-size budget —
	// broadcast for the same cluster-wide admission union.
	SizeCappedVaults() []glid.GLID
}

// RaftLivenessProvider exposes aggregated Raft WAL append latency and
// liveness counters across every Raft instance on this node
// (gastrolog-1io54g). Totals are pure reads; TakeWALAppendMax resets the
// max and must only be called from the ticking path.
type RaftLivenessProvider interface {
	WALAppendTotals() (count, totalNanos uint64)
	TakeWALAppendMax() (maxNanos uint64)
	RaftLiveness() (elections, leaderLosses, failedHeartbeats uint64)
}

// RaftStatsProvider exposes local Raft stats for the collector.
type RaftStatsProvider interface {
	LocalStats() map[string]string
}

// PeerConnSnapshotProvider exposes managed outbound connection telemetry.
type PeerConnSnapshotProvider interface {
	Snapshot() []PeerConnSnapshot
	// ResetPurposeWindows clears per-connection purpose activity windows after
	// a stats broadcast tick. Snapshot must not reset windows — lifecycle
	// polls CollectLocalSnapshot between ticks.
	ResetPurposeWindows()
}

// AlertProvider exposes active system alerts for broadcast.
// Satisfied by *alert.Collector.
type AlertProvider interface {
	ActiveAlerts() []alert.AlertInfo
}

// JobsProvider returns the current job list for broadcast.
// Defined at the consumer site to avoid importing orchestrator/server.
type JobsProvider interface {
	ListJobsProto() []*gastrologv1.Job
}

// StatsCollectorConfig configures a StatsCollector.
type StatsCollectorConfig struct {
	Broadcaster  *Broadcaster
	RaftStats    RaftStatsProvider
	Stats        StatsProvider
	PeerConns    PeerConnSnapshotProvider // optional; nil disables peer conn stats
	RaftLiveness RaftLivenessProvider     // optional; nil disables Raft liveness stats (gastrolog-1io54g)
	// ClusterRouteTotals returns the cluster-wide cumulative route counters
	// (local node + live peers' broadcast totals). The collector windows the
	// SUMMED counters so cluster rates and their spark history are computed
	// server-side from system data — never accumulated client-side, and
	// never fabricated by summing phase-skewed per-node spark arrays
	// (gastrolog-4eh5ns). Optional; nil disables cluster route rates.
	// The membership string fingerprints the contributor set behind the
	// sums (sorted live-peer IDs + self). The summed window re-anchors on
	// any change so contributors entering/leaving the sum can never read
	// as traffic (gastrolog-mliwrd).
	ClusterRouteTotals func() (routed, matched int64, membership string)
	Alerts             AlertProvider // optional; nil if no alert collector
	Jobs               JobsProvider  // optional; nil in single-node mode
	NodeID             string
	NodeNameFn         func() string // lazily resolved node name
	Version            string
	StartTime          time.Time
	Interval           time.Duration  // heavy NodeStats broadcast cadence (default 5s)
	HeartbeatInterval  time.Duration  // lightweight liveness ping cadence (default 1s); 0 disables
	ApiAddress         string         // HTTP API listen address (e.g. ":4564")
	PprofAddress       string         // pprof listen address, empty if disabled
	StatsSignal        *notify.Signal // fired after each broadcast to notify WatchSystemStatus streams
	Logger             *slog.Logger
}

// StatsCollector periodically gathers local node statistics and
// broadcasts them to all cluster peers via the Broadcaster.
type StatsCollector struct {
	cfg StatsCollectorConfig

	mu sync.Mutex
	// rates holds every rolling rate/spark window keyed by role-namespaced
	// string (peerconn:/peertotal:/append:/durable:/collect:/seal:/segstage:/
	// chunkstage:/route:/clusterroute:/wal:/live:). Each entry is ONE honest
	// counter's series; the tx/rx-pair window it replaced is gone (each side is
	// now its own series). See rateseries.go.
	rates map[string]*rateSeries
	// clusterRouted/clusterMatched cache the latest cluster-total route
	// rate series for the RPC/stream builders (gastrolog-4eh5ns).
	clusterRouted  *gastrologv1.ThroughputRate
	clusterMatched *gastrologv1.ThroughputRate
	// walMaxNanos caches the last tick's WAL append max so snapshot reads
	// between ticks see it without consuming the accumulator (gastrolog-1io54g).
	walMaxNanos uint64
	// electionStormActive is the storm/calm hysteresis state for the
	// transition-edge logging in collectRaftLiveness. Guarded by mu.
	electionStormActive bool
	// lastPublishedPurposeWindows holds purposes_window from the most recent
	// CollectLocalTick (5s broadcast). CollectLocalSnapshot overlays this onto
	// read-only snapshots briefly after each tick so WatchSystemStatus still
	// shows the completed interval after ResetPurposeWindows runs.
	lastPublishedPurposeWindows map[string][]string
}

// NewStatsCollector creates a collector with the given system.
func NewStatsCollector(cfg StatsCollectorConfig) *StatsCollector {
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 1 * time.Second
	}
	return &StatsCollector{
		cfg:   cfg,
		rates: make(map[string]*rateSeries),
	}
}

// Delete drops every rate series belonging to a removed node. Called from the
// peer-removal observer (raft.go runPeerRemovalLoop) so the map doesn't
// accumulate dead entries forever. Naming aligns with the peerEvictor interface.
func (c *StatsCollector) Delete(peer string) {
	c.mu.Lock()
	for k := range c.rates {
		if p, ok := rateSeriesPeerID(k); ok && p == peer {
			delete(c.rates, k)
		}
	}
	c.mu.Unlock()
}

func (c *StatsCollector) ReconcilePeers(keep map[string]struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.rates {
		if p, ok := rateSeriesPeerID(k); ok {
			if _, keepIt := keep[p]; !keepIt {
				delete(c.rates, k)
			}
		}
	}
}

// BroadcastStats gathers local NodeStats, broadcasts them to all cluster
// peers, and advances rolling rate windows. Registered as a scheduler job
// from app.startStatsCollectorJobs.
func (c *StatsCollector) BroadcastStats(ctx context.Context) {
	now := time.Now()
	stats := c.CollectLocalTick(now)
	if c.cfg.Broadcaster != nil {
		c.cfg.Broadcaster.Send(ctx, &gastrologv1.BroadcastMessage{
			SenderId:  []byte(c.cfg.NodeID),
			Timestamp: timestamppb.Now(),
			Payload:   &gastrologv1.BroadcastMessage_NodeStats{NodeStats: stats},
		})
		c.BroadcastJobs(ctx)
	}
	if c.cfg.StatsSignal != nil {
		c.cfg.StatsSignal.Notify()
	}
}

// BroadcastHeartbeat sends the lightweight peer-liveness marker. Registered as
// a separate scheduler job so it cannot be delayed by BroadcastStats work.
func (c *StatsCollector) BroadcastHeartbeat(ctx context.Context) {
	if c.cfg.Broadcaster == nil {
		return
	}
	c.cfg.Broadcaster.Send(ctx, &gastrologv1.BroadcastMessage{
		SenderId:  []byte(c.cfg.NodeID),
		Timestamp: timestamppb.Now(),
		Payload:   &gastrologv1.BroadcastMessage_Heartbeat{Heartbeat: &gastrologv1.Heartbeat{}},
	})
}

// CollectLocalSnapshot gathers a NodeStats snapshot for the local node without
// advancing any rolling windows. Used by the lifecycle server for "real-time"
// reads so opening the inspector doesn't skew rate calculations.
func (c *StatsCollector) CollectLocalSnapshot() *gastrologv1.NodeStats {
	stats := c.collectLocal(time.Now(), false)
	c.applyPublishedPurposeWindows(stats)
	return stats
}

// CollectLocalTick gathers NodeStats and advances rolling windows. Called
// by the periodic stats broadcast loop.
func (c *StatsCollector) CollectLocalTick(now time.Time) *gastrologv1.NodeStats {
	return c.collectLocal(now, true)
}

func (c *StatsCollector) collectLocal(now time.Time, stepWindows bool) *gastrologv1.NodeStats {
	cpu := sysmetrics.CPUPercent()
	mem := sysmetrics.Memory()

	stats := &gastrologv1.NodeStats{
		CpuPercent:         cpu,
		MemoryInuse:        uint64(mem.Inuse),              //nolint:gosec // always positive
		MemoryRss:          uint64(mem.RSS),                //nolint:gosec // always positive
		MemoryHeapAlloc:    uint64(mem.HeapAlloc),          //nolint:gosec // always positive
		MemorySys:          uint64(mem.Sys),                //nolint:gosec // always positive
		Goroutines:         uint32(runtime.NumGoroutine()), //nolint:gosec // always small
		NodeName:           c.cfg.NodeNameFn(),
		Version:            c.cfg.Version,
		UptimeSeconds:      int64(now.Sub(c.cfg.StartTime).Seconds()),
		MemoryHeapIdle:     uint64(mem.HeapIdle),     //nolint:gosec // always positive
		MemoryHeapReleased: uint64(mem.HeapReleased), //nolint:gosec // always positive
		MemoryStackInuse:   uint64(mem.StackInuse),   //nolint:gosec // always positive
		MemoryHeapObjects:  mem.HeapObjects,
		NumGc:              mem.NumGC,
		ApiAddress:         c.cfg.ApiAddress,
		PprofAddress:       c.cfg.PprofAddress,
	}

	// Queue stats.
	if c.cfg.Stats != nil {
		stats.IngestQueueDepth = uint32(c.cfg.Stats.IngestQueueDepth())       //nolint:gosec
		stats.IngestQueueCapacity = uint32(c.cfg.Stats.IngestQueueCapacity()) //nolint:gosec

		// Vault snapshots.
		for _, v := range c.cfg.Stats.VaultSnapshots() {
			stats.Vaults = append(stats.Vaults, &gastrologv1.VaultStats{
				Id:               v.ID.ToProto(),
				Name:             v.Name,
				RecordCount:      v.RecordCount,
				ChunkCount:       int64(v.ChunkCount),
				SealedChunks:     int64(v.SealedChunks),
				DataBytes:        v.DataBytes,
				Enabled:          v.Enabled,
				RaftAppliedIndex: v.RaftAppliedIndex,
			})
		}

		// Per-vault segmentation append throughput (gastrolog-4eh5ns).
		// Rates come from rolling windows over the writer's cumulative
		// counters; totals and queue gauges pass through as-is.
		vaultByID := make(map[string]*gastrologv1.VaultStats, len(stats.Vaults))
		for _, v := range stats.Vaults {
			vaultByID[string(v.Id)] = v
		}
		for _, as := range c.cfg.Stats.VaultAppendStats() {
			v := vaultByID[string(as.VaultID.ToProto())]
			if v == nil {
				v = &gastrologv1.VaultStats{Id: as.VaultID.ToProto()}
				stats.Vaults = append(stats.Vaults, v)
			}
			id := as.VaultID.String()
			v.AppendRecords = c.emitRate(now, "append:records:"+id, int64(as.RecordsAppended), stepWindows) //nolint:gosec // counters < 2^63
			v.AppendBytes = c.emitRate(now, "append:bytes:"+id, int64(as.BytesAppended), stepWindows)       //nolint:gosec // counters < 2^63
			v.AppendDurable = c.emitRate(now, "durable:"+id, int64(as.RecordsDurable), stepWindows)         //nolint:gosec // counter < 2^63
			v.AppendRecordsTotal = as.RecordsAppended
			v.AppendBytesTotal = as.BytesAppended
			v.AppendQueueDepth = uint32(as.QueueDepth)  //nolint:gosec
			v.AppendQueueCapacity = uint32(as.QueueCap) //nolint:gosec

			v.CollectedRecords = c.emitRate(now, "collect:records:"+id, int64(as.CollectedRecords), stepWindows) //nolint:gosec // counters < 2^63
			v.CollectedBytes = c.emitRate(now, "collect:bytes:"+id, int64(as.CollectedBytes), stepWindows)       //nolint:gosec // counters < 2^63
			v.SealedRecords = c.emitRate(now, "seal:records:"+id, int64(as.SealedRecords), stepWindows)          //nolint:gosec // counters < 2^63
			v.SealedBytes = c.emitRate(now, "seal:bytes:"+id, int64(as.SealedBytes), stepWindows)                //nolint:gosec // counters < 2^63

			// Discrete pipeline stage-count milestones (gastrolog-4r784a):
			// cumulative totals pass through as-is; rates for the throughput
			// milestones come from the collector's rolling windows over the
			// totals, paired two-per-window like every other rate here.
			v.SegmentsCompletedTotal = as.SegmentsCompleted
			v.SegmentsPublishedTotal = as.SegmentsPublished
			v.SegmentsReleasedTotal = as.SegmentsReleased
			v.ChunksPlannedTotal = as.ChunksPlanned
			v.ChunksBuiltTotal = as.ChunksBuilt
			v.ChunksSealedTotal = as.ChunksSealed
			v.HeadPurgesTotal = as.HeadPurges
			v.GlcbPullsAttemptedTotal = as.GLCBPullsAttempted
			v.GlcbPullsFailedTotal = as.GLCBPullsFailed
			v.RetentionDeletesTotal = as.RetentionDeletes

			v.SegmentsCompletedRate = c.emitRate(now, "segstage:completed:"+id, int64(as.SegmentsCompleted), stepWindows) //nolint:gosec // counters < 2^63
			v.SegmentsPublishedRate = c.emitRate(now, "segstage:published:"+id, int64(as.SegmentsPublished), stepWindows) //nolint:gosec // counters < 2^63
			v.ChunksBuiltRate = c.emitRate(now, "chunkstage:built:"+id, int64(as.ChunksBuilt), stepWindows)               //nolint:gosec // counters < 2^63
			v.ChunksSealedRate = c.emitRate(now, "chunkstage:sealed:"+id, int64(as.ChunksSealed), stepWindows)            //nolint:gosec // counters < 2^63
		}

		// Ingester stats.
		for _, id := range c.cfg.Stats.IngesterIDs() {
			name, msgs, bytes, errs, running := c.cfg.Stats.IngesterStats(id)
			stats.Ingesters = append(stats.Ingesters, &gastrologv1.IngesterNodeStats{
				Id:               []byte(id),
				Name:             name,
				MessagesIngested: uint64(msgs),  //nolint:gosec
				BytesIngested:    uint64(bytes), //nolint:gosec
				Errors:           uint64(errs),  //nolint:gosec
				Running:          running,
			})
		}

		// Route stats.
		rs := c.cfg.Stats.RouteStats()
		stats.RouteStatsRouted = rs.Routed
		stats.RouteStatsUnmatched = rs.Unmatched
		stats.RouteStatsMatched = rs.Matched
		stats.RouteStatsRouteTableActive = rs.RouteTableActive
		c.collectClusterRouteRates(now, stepWindows)

		// Node-level routing throughput windows (gastrolog-4eh5ns).
		stats.RouteRouted = c.emitRate(now, "route:routed", rs.Routed, stepWindows)
		stats.RouteMatched = c.emitRate(now, "route:matched", rs.Matched, stepWindows)
		for _, vs := range rs.VaultStats {
			stats.RouteVaultStats = append(stats.RouteVaultStats, &gastrologv1.VaultRouteStats{
				VaultId:        vs.VaultID.ToProto(),
				RecordsMatched: vs.Matched,
			})
		}
		for _, ps := range rs.RouteStats {
			stats.RoutePerRouteStats = append(stats.RoutePerRouteStats, &gastrologv1.PerRouteStats{
				RouteId:        ps.RouteID.ToProto(),
				RecordsMatched: ps.Matched,
			})
		}

		for _, pd := range c.cfg.Stats.PipelineDiskSnapshots() {
			stats.VaultPipelineDisk = append(stats.VaultPipelineDisk, &gastrologv1.VaultPipelineNodeDisk{
				VaultId:                  pd.VaultID.ToProto(),
				WorkingSegments:          uint32(pd.Working),          //nolint:gosec
				CompletedStagingSegments: uint32(pd.CompletedStaging), //nolint:gosec
				HeadSegments:             uint32(pd.Head),             //nolint:gosec
				PreHeadSegments:          uint32(pd.PreHead),          //nolint:gosec
			})
		}

		stats.StorageBytes = c.cfg.Stats.LocalStorageBytes()
		stats.DiskProtectedVaultIds = glidsToProto(c.cfg.Stats.DiskProtectedVaults())
		stats.SizeCappedVaultIds = glidsToProto(c.cfg.Stats.SizeCappedVaults())
	}

	if c.cfg.PeerConns != nil {
		for _, pc := range c.cfg.PeerConns.Snapshot() {
			key := peerConnStatsKey(pc)
			tx := c.emitRate(now, rateKeyPeerConnTx+key, pc.BytesSent, stepWindows)
			rx := c.emitRate(now, rateKeyPeerConnRx+key, pc.BytesRecv, stepWindows)
			stats.PeerConnections = append(stats.PeerConnections, &gastrologv1.PeerConnStat{
				Peer:           pc.PeerNodeID,
				Lane:           pc.Lane,
				GroupId:        pc.GroupID,
				Purposes:       append([]string(nil), pc.Purposes...),
				PurposesWindow: append([]string(nil), pc.PurposesWindow...),
				Connectivity:   pc.Connectivity,
				PoolIndex:      int32(pc.PoolIndex), //nolint:gosec
				BytesSent:      pc.BytesSent,
				BytesReceived:  pc.BytesRecv,
				TxBytesPerSec:  tx.InstantPerSec,
				RxBytesPerSec:  rx.InstantPerSec,
				TxSpark:        tx.Spark,
				RxSpark:        rx.Spark,
			})
		}
		if stepWindows {
			c.storePublishedPurposeWindows(stats.PeerConnections)
			c.cfg.PeerConns.ResetPurposeWindows()
		}
	}
	c.appendPeerTrafficTotals(stats, now, stepWindows)
	c.collectRaftLiveness(stats, now, stepWindows)

	// Active alerts.
	if c.cfg.Alerts != nil {
		for _, a := range c.cfg.Alerts.ActiveAlerts() {
			stats.Alerts = append(stats.Alerts, &gastrologv1.SystemAlert{
				Id:        []byte(a.ID),
				Severity:  gastrologv1.AlertSeverity(a.Severity), //nolint:gosec // bounded enum
				Source:    a.Source,
				Message:   a.Message,
				FirstSeen: timestamppb.New(a.FirstSeen),
				LastSeen:  timestamppb.New(a.LastSeen),
			})
		}
	}

	// Raft stats.
	if c.cfg.RaftStats != nil {
		if m := c.cfg.RaftStats.LocalStats(); m != nil {
			stats.RaftState = m["state"]
			stats.RaftTerm = parseUint64(m["term"])
			stats.RaftCommitIndex = parseUint64(m["commit_index"])
			stats.RaftAppliedIndex = parseUint64(m["applied_index"])
			stats.RaftLastContact = m["last_contact"]
			stats.RaftFsmPending = parseUint64(m["fsm_pending"])
		}
	}

	return stats
}

func (c *StatsCollector) appendPeerTrafficTotals(stats *gastrologv1.NodeStats, now time.Time, stepWindows bool) {
	totals := make(map[string]struct {
		sent int64
		recv int64
	})
	for _, row := range stats.PeerConnections {
		t := totals[row.Peer]
		t.sent += row.BytesSent
		t.recv += row.BytesReceived
		totals[row.Peer] = t
	}
	for peer, sum := range totals {
		tx := c.emitRate(now, rateKeyPeerTotalTx+peer, sum.sent, stepWindows)
		rx := c.emitRate(now, rateKeyPeerTotalRx+peer, sum.recv, stepWindows)
		stats.PeerTrafficTotals = append(stats.PeerTrafficTotals, &gastrologv1.PeerTrafficTotal{
			Peer:          peer,
			BytesSent:     sum.sent,
			BytesReceived: sum.recv,
			TxBytesPerSec: tx.InstantPerSec,
			RxBytesPerSec: rx.InstantPerSec,
			TxSpark:       tx.Spark,
			RxSpark:       rx.Spark,
		})
	}
	sort.Slice(stats.PeerTrafficTotals, func(i, j int) bool {
		return stats.PeerTrafficTotals[i].Peer < stats.PeerTrafficTotals[j].Peer
	})
}

// glidsToProto maps a GLID slice to its broadcast wire form.
func glidsToProto(ids []glid.GLID) [][]byte {
	out := make([][]byte, 0, len(ids))
	for _, id := range ids {
		out = append(out, id.ToProto())
	}
	return out
}

func peerConnStatsKey(s PeerConnSnapshot) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%d", s.PeerNodeID, s.Lane, s.GroupID, s.PoolIndex)
}

func peerConnStatKey(row *gastrologv1.PeerConnStat) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%d", row.Peer, row.Lane, row.GroupId, row.PoolIndex)
}

func (c *StatsCollector) storePublishedPurposeWindows(rows []*gastrologv1.PeerConnStat) {
	published := make(map[string][]string)
	for _, row := range rows {
		if len(row.PurposesWindow) == 0 {
			continue
		}
		published[peerConnStatKey(row)] = append([]string(nil), row.PurposesWindow...)
	}
	c.mu.Lock()
	c.lastPublishedPurposeWindows = published
	c.mu.Unlock()
}

func (c *StatsCollector) applyPublishedPurposeWindows(stats *gastrologv1.NodeStats) {
	c.mu.Lock()
	published := c.lastPublishedPurposeWindows
	c.mu.Unlock()
	if len(published) == 0 {
		return
	}
	for _, row := range stats.PeerConnections {
		if pw, ok := published[peerConnStatKey(row)]; ok && len(pw) > 0 {
			row.PurposesWindow = append([]string(nil), pw...)
		} else {
			row.PurposesWindow = nil
		}
	}
}

// Raft liveness alert thresholds (gastrolog-1io54g). Storm: sustained
// elections at a rate no healthy cluster shows (the 2026-07-04 incident ran
// 7-13/min). Calm clears with hysteresis so a single quiet tick doesn't
// flap the alert. WAL max latency: one slow append is normal on shared
// disks; above a second, Raft replication RTTs and lease checks are in the
// danger zone.
const (
	raftElectionStormPerMin = 3.0
	raftElectionCalmPerMin  = 0.5
	walAppendMaxAlertMs     = 1000.0
	walAppendMaxClearMs     = 250.0
)

// collectRaftLiveness populates the Raft WAL latency and election liveness
// fields and maintains the degraded-liveness alerts. Alert evaluation lives
// here, not in a component, because the rolling-window rates only exist in
// the collector (gastrolog-1io54g).
func (c *StatsCollector) collectRaftLiveness(stats *gastrologv1.NodeStats, now time.Time, stepWindows bool) {
	if c.cfg.RaftLiveness == nil {
		return
	}
	count, totalNanos := c.cfg.RaftLiveness.WALAppendTotals()
	elections, losses, failedHB := c.cfg.RaftLiveness.RaftLiveness()

	stats.RaftWalAppendsTotal = count
	stats.RaftElectionsTotal = elections
	stats.RaftLeaderLossesTotal = losses
	stats.RaftFailedHeartbeatsTotal = failedHB

	walAppendsPerSec := c.observeRateInstant(now, "wal:count", int64(count), stepWindows)    //nolint:gosec // counter < 2^63
	walNanosPerSec := c.observeRateInstant(now, "wal:nanos", int64(totalNanos), stepWindows) //nolint:gosec // counter < 2^63
	if walAppendsPerSec > 0 {
		stats.RaftWalAppendAvgMs = walNanosPerSec / walAppendsPerSec / 1e6
	}
	electionsPerSec := c.observeRateInstant(now, "live:elections", int64(elections), stepWindows) //nolint:gosec // counter < 2^63
	stats.RaftElectionsPerMin = electionsPerSec * 60

	c.mu.Lock()
	if stepWindows {
		c.walMaxNanos = c.cfg.RaftLiveness.TakeWALAppendMax()
	}
	maxNanos := c.walMaxNanos
	c.mu.Unlock()
	stats.RaftWalAppendMaxMs = float64(maxNanos) / 1e6

	if c.cfg.Alerts == nil || !stepWindows {
		return
	}
	alerts, ok := c.cfg.Alerts.(interface {
		Set(id string, severity alert.Severity, source, message string)
		Clear(id string)
	})
	if !ok {
		return
	}
	// Election churn is a diagnostic, not an alarm: there is no direct
	// operator action, and the rate already ships in stats for the health
	// surfaces (EEMUA 191 actionability test, gastrolog-29380r). Log on the
	// storm/calm transitions only — the same hysteresis the alert had — so
	// a sustained storm is one line, not one per tick.
	c.mu.Lock()
	stormWas := c.electionStormActive
	switch {
	case stats.RaftElectionsPerMin >= raftElectionStormPerMin:
		c.electionStormActive = true
	case stats.RaftElectionsPerMin < raftElectionCalmPerMin:
		c.electionStormActive = false
	}
	stormNow := c.electionStormActive
	c.mu.Unlock()
	if logger := c.cfg.Logger; logger != nil && stormNow != stormWas {
		if stormNow {
			logger.Warn("Raft election storm: consensus is churning on this node",
				"elections_per_min", stats.RaftElectionsPerMin)
		} else {
			logger.Info("Raft election rate back to calm",
				"elections_per_min", stats.RaftElectionsPerMin)
		}
	}
	switch {
	case stats.RaftWalAppendMaxMs >= walAppendMaxAlertMs:
		alerts.Set("raft-wal-latency", alert.Warning, "raft",
			fmt.Sprintf("Raft WAL append latency degraded: max %.0fms since last tick — bulk I/O may be starving consensus", stats.RaftWalAppendMaxMs))
	case stats.RaftWalAppendMaxMs < walAppendMaxClearMs:
		alerts.Clear("raft-wal-latency")
	}
}

// collectClusterRouteRates windows the SUMMED cluster route counters; each
// resulting series (including spark) is this node's honest observation of
// cluster rate at its tick cadence. Counter drops (peer TTL expiry, node
// restart) re-anchor via the series' reset guard, and any change in the
// contributor-set fingerprint re-anchors both series (gastrolog-4eh5ns,
// gastrolog-mliwrd).
func (c *StatsCollector) collectClusterRouteRates(now time.Time, stepWindows bool) {
	if c.cfg.ClusterRouteTotals == nil {
		return
	}
	crouted, cmatched, membership := c.cfg.ClusterRouteTotals()
	routed := c.emitRateM(now, "clusterroute:routed", crouted, membership, stepWindows)
	matched := c.emitRateM(now, "clusterroute:matched", cmatched, membership, stepWindows)
	c.mu.Lock()
	c.clusterRouted = routed
	c.clusterMatched = matched
	c.mu.Unlock()
}

// ClusterRouteRates returns the latest cluster-total route throughput series
// (instant/30s/1m + spark), computed server-side from summed cluster counters
// at each stats tick. Safe between ticks; returns empty rates before the
// first tick. Proto messages carry an internal mutex, so the copies are
// rebuilt field-by-field rather than dereferenced (gastrolog-4eh5ns).
func (c *StatsCollector) ClusterRouteRates() (routed, matched *gastrologv1.ThroughputRate) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clusterRouted == nil {
		return &gastrologv1.ThroughputRate{}, &gastrologv1.ThroughputRate{}
	}
	return copyThroughputRate(c.clusterRouted), copyThroughputRate(c.clusterMatched)
}

func copyThroughputRate(r *gastrologv1.ThroughputRate) *gastrologv1.ThroughputRate {
	return &gastrologv1.ThroughputRate{
		InstantPerSec: r.InstantPerSec,
		Avg_1MPerSec:  r.Avg_1MPerSec,
		Avg_5MPerSec:  r.Avg_5MPerSec,
		Avg_15MPerSec: r.Avg_15MPerSec,
		Spark:         append([]float64(nil), r.Spark...),
	}
}

// BroadcastJobs sends the current job list to all cluster peers.
// Called on every tick for periodic sync, and directly by the scheduler's
// onJobChange callback for immediate notification.
func (c *StatsCollector) BroadcastJobs(ctx context.Context) {
	if c.cfg.Broadcaster == nil || c.cfg.Jobs == nil {
		return
	}
	c.cfg.Broadcaster.Send(ctx, &gastrologv1.BroadcastMessage{
		SenderId:  []byte(c.cfg.NodeID),
		Timestamp: timestamppb.Now(),
		Payload: &gastrologv1.BroadcastMessage_NodeJobs{NodeJobs: &gastrologv1.NodeJobs{
			Jobs: c.cfg.Jobs.ListJobsProto(),
		}},
	})
}

func parseUint64(s string) uint64 {
	v, _ := strconv.ParseUint(s, 10, 64)
	return v
}
