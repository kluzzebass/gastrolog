package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"runtime"
	"sort"
	"strconv"
	"strings"
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
}

// StatsRouteSnapshot captures route stats for broadcast.
type StatsRouteSnapshot struct {
	Ingested     int64
	Dropped      int64
	Routed       int64
	FilterActive bool
	VaultStats   []StatsVaultRouteSnapshot
	RouteStats   []StatsPerRouteSnapshot
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
	ClusterRouteTotals func() (ingested, routed int64, membership string)
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

	mu               sync.Mutex
	peerConnStats    map[string]*peerConnStatsWindow
	peerTrafficStats map[string]*peerConnStatsWindow // keyed by peer node ID
	// vaultAppendStats holds per-vault append-rate windows, two entries per
	// vault ("append:<id>" records+bytes, "durable:<id>" durable records).
	// routeRateStats holds the single node-level routing window ("route"
	// ingested+routed). Same mechanics as the peer traffic windows.
	vaultAppendStats map[string]*peerConnStatsWindow
	routeRateStats   map[string]*peerConnStatsWindow
	// clusterIngested/clusterRouted cache the latest cluster-total route
	// rate series for the RPC/stream builders (gastrolog-4eh5ns).
	clusterIngested *gastrologv1.ThroughputRate
	clusterRouted   *gastrologv1.ThroughputRate
	// raftLiveStats windows: "wal" (tx=append count, rx=append nanos) and
	// "live" (tx=elections, rx=failed heartbeats). walMaxNanos caches the
	// last tick's max so snapshot reads between ticks see it without
	// consuming the WAL-side accumulator (gastrolog-1io54g).
	raftLiveStats map[string]*peerConnStatsWindow
	walMaxNanos   uint64
	// lastPublishedPurposeWindows holds purposes_window from the most recent
	// CollectLocalTick (5s broadcast). CollectLocalSnapshot overlays this onto
	// read-only snapshots briefly after each tick so WatchSystemStatus still
	// shows the completed interval after ResetPurposeWindows runs.
	lastPublishedPurposeWindows map[string][]string
}

const peerConnStatsSparkPoints = 20

type peerConnStatsWindow struct {
	lastSent int64
	lastRecv int64
	lastAt   time.Time
	// membership fingerprints the contributor set behind a SUMMED series
	// (cluster totals from TTL-live peer broadcasts). A contributor whose
	// stats expired and later resumed rejoins the sum as a one-tick upward
	// jump indistinguishable from real traffic — 5m EWMA read 138K/s from a
	// 40K/s source (gastrolog-mliwrd). Any fingerprint change re-anchors the
	// window exactly like a counter reset: no sample, EWMAs preserved.
	// Per-entity windows pass a constant fingerprint and never trigger it.
	membership string
	// Unix-load-style EWMAs (one float per horizon, no history buffer):
	// each step folds the instantaneous rate in with e^(-dt/tau) decay,
	// tau = 1m/5m/15m (gastrolog-4eh5ns).
	txEwma  [3]float64
	rxEwma  [3]float64
	txRates []float64
	rxRates []float64
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
		cfg:              cfg,
		peerConnStats:    make(map[string]*peerConnStatsWindow),
		peerTrafficStats: make(map[string]*peerConnStatsWindow),
		vaultAppendStats: make(map[string]*peerConnStatsWindow),
		routeRateStats:   make(map[string]*peerConnStatsWindow),
		raftLiveStats:    make(map[string]*peerConnStatsWindow),
	}
}

// Delete drops the per-peer rate window for a removed node. Called
// from the peer-removal observer (raft.go runPeerRemovalLoop) so the
// peerBytes map doesn't accumulate dead entries forever. Naming
// aligns with the peerEvictor interface.
func (c *StatsCollector) Delete(peer string) {
	c.mu.Lock()
	for k := range c.peerConnStats {
		if strings.HasPrefix(k, peer+"\x00") || strings.HasPrefix(k, peer+":") {
			delete(c.peerConnStats, k)
		}
	}
	delete(c.peerTrafficStats, peer)
	c.mu.Unlock()
}

func (c *StatsCollector) ReconcilePeers(keep map[string]struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.peerConnStats {
		peer := strings.SplitN(k, "\x00", 2)[0]
		if _, ok := keep[peer]; !ok {
			delete(c.peerConnStats, k)
		}
	}
	for peer := range c.peerTrafficStats {
		if _, ok := keep[peer]; !ok {
			delete(c.peerTrafficStats, peer)
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
			ar := c.observeTrafficWindowRates(now, "append:"+as.VaultID.String(),
				int64(as.RecordsAppended), int64(as.BytesAppended), "", stepWindows, c.vaultAppendStats) //nolint:gosec // counters < 2^63
			dr := c.observeTrafficWindowRates(now, "durable:"+as.VaultID.String(),
				int64(as.RecordsDurable), 0, "", stepWindows, c.vaultAppendStats) //nolint:gosec // counter < 2^63
			v.AppendRecords = &gastrologv1.ThroughputRate{
				InstantPerSec: ar.txPerSec, Avg_1MPerSec: ar.txEwma[0], Avg_5MPerSec: ar.txEwma[1], Avg_15MPerSec: ar.txEwma[2], Spark: ar.txSpark,
			}
			v.AppendBytes = &gastrologv1.ThroughputRate{
				InstantPerSec: ar.rxPerSec, Avg_1MPerSec: ar.rxEwma[0], Avg_5MPerSec: ar.rxEwma[1], Avg_15MPerSec: ar.rxEwma[2], Spark: ar.rxSpark,
			}
			v.AppendDurable = &gastrologv1.ThroughputRate{
				InstantPerSec: dr.txPerSec, Avg_1MPerSec: dr.txEwma[0], Avg_5MPerSec: dr.txEwma[1], Avg_15MPerSec: dr.txEwma[2], Spark: dr.txSpark,
			}
			v.AppendRecordsTotal = as.RecordsAppended
			v.AppendBytesTotal = as.BytesAppended
			v.AppendQueueDepth = uint32(as.QueueDepth)  //nolint:gosec
			v.AppendQueueCapacity = uint32(as.QueueCap) //nolint:gosec

			cr := c.observeTrafficWindowRates(now, "collect:"+as.VaultID.String(),
				int64(as.CollectedRecords), int64(as.CollectedBytes), "", stepWindows, c.vaultAppendStats) //nolint:gosec // counters < 2^63
			v.CollectedRecords = throughputRateProto(cr, true)
			v.CollectedBytes = throughputRateProto(cr, false)
			sr := c.observeTrafficWindowRates(now, "seal:"+as.VaultID.String(),
				int64(as.SealedRecords), int64(as.SealedBytes), "", stepWindows, c.vaultAppendStats) //nolint:gosec // counters < 2^63
			v.SealedRecords = throughputRateProto(sr, true)
			v.SealedBytes = throughputRateProto(sr, false)
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
		stats.RouteStatsIngested = rs.Ingested
		stats.RouteStatsDropped = rs.Dropped
		stats.RouteStatsRouted = rs.Routed
		stats.RouteStatsFilterActive = rs.FilterActive
		c.collectClusterRouteRates(now, stepWindows)

		// Node-level routing throughput windows (gastrolog-4eh5ns).
		rr := c.observeTrafficWindowRates(now, "route",
			rs.Ingested, rs.Routed, "", stepWindows, c.routeRateStats)
		stats.RouteIngested = &gastrologv1.ThroughputRate{
			InstantPerSec: rr.txPerSec, Avg_1MPerSec: rr.txEwma[0], Avg_5MPerSec: rr.txEwma[1], Avg_15MPerSec: rr.txEwma[2], Spark: rr.txSpark,
		}
		stats.RouteRouted = &gastrologv1.ThroughputRate{
			InstantPerSec: rr.rxPerSec, Avg_1MPerSec: rr.rxEwma[0], Avg_5MPerSec: rr.rxEwma[1], Avg_15MPerSec: rr.rxEwma[2], Spark: rr.rxSpark,
		}
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
	}

	if c.cfg.PeerConns != nil {
		for _, pc := range c.cfg.PeerConns.Snapshot() {
			key := peerConnStatsKey(pc)
			txPerSec, rxPerSec, txSpark, rxSpark := c.observePeerConnStats(now, key, pc.BytesSent, pc.BytesRecv, stepWindows)
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
				TxBytesPerSec:  txPerSec,
				RxBytesPerSec:  rxPerSec,
				TxSpark:        txSpark,
				RxSpark:        rxSpark,
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
		txPerSec, rxPerSec, txSpark, rxSpark := c.observePeerTrafficTotal(now, peer, sum.sent, sum.recv, stepWindows)
		stats.PeerTrafficTotals = append(stats.PeerTrafficTotals, &gastrologv1.PeerTrafficTotal{
			Peer:          peer,
			BytesSent:     sum.sent,
			BytesReceived: sum.recv,
			TxBytesPerSec: txPerSec,
			RxBytesPerSec: rxPerSec,
			TxSpark:       txSpark,
			RxSpark:       rxSpark,
		})
	}
	sort.Slice(stats.PeerTrafficTotals, func(i, j int) bool {
		return stats.PeerTrafficTotals[i].Peer < stats.PeerTrafficTotals[j].Peer
	})
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

func (c *StatsCollector) observePeerConnStats(now time.Time, key string, sent, recv int64, step bool) (txPerSec, rxPerSec float64, txSpark, rxSpark []float64) {
	return c.observeTrafficWindow(now, key, sent, recv, step, c.peerConnStats)
}

func (c *StatsCollector) observePeerTrafficTotal(now time.Time, peer string, sent, recv int64, step bool) (txPerSec, rxPerSec float64, txSpark, rxSpark []float64) {
	return c.observeTrafficWindow(now, peer, sent, recv, step, c.peerTrafficStats)
}

func (c *StatsCollector) observeTrafficWindow(now time.Time, key string, sent, recv int64, step bool, store map[string]*peerConnStatsWindow) (txPerSec, rxPerSec float64, txSpark, rxSpark []float64) {
	r := c.observeTrafficWindowRates(now, key, sent, recv, "", step, store)
	return r.txPerSec, r.rxPerSec, r.txSpark, r.rxSpark
}

// ewmaTaus are the Unix-load-average horizons for sustained rates.
var ewmaTaus = [3]time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute}

// trafficRates is one window observation: instantaneous rates with spark
// history (burst shape), and 1m/5m/15m EWMAs (sustained rates).
type trafficRates struct {
	txPerSec, rxPerSec float64
	txEwma, rxEwma     [3]float64
	txSpark, rxSpark   []float64
}

func (c *StatsCollector) observeTrafficWindowRates(now time.Time, key string, sent, recv int64, membership string, step bool, store map[string]*peerConnStatsWindow) trafficRates {
	if key == "" {
		return trafficRates{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	w := store[key]
	if w == nil {
		w = &peerConnStatsWindow{lastSent: sent, lastRecv: recv, lastAt: now, membership: membership}
		store[key] = w
		return trafficRates{}
	}

	if !step {
		return w.snapshotRates()
	}

	dt := now.Sub(w.lastAt).Seconds()
	if dt <= 0 {
		return w.snapshotRates()
	}

	if membership != w.membership {
		// Contributor set changed under a summed series: this tick's delta
		// mixes real traffic with counters entering/leaving the sum, so it
		// is not a measurable sample. Re-anchor, preserve EWMAs and spark
		// (gastrolog-mliwrd).
		w.membership = membership
		w.lastSent = sent
		w.lastRecv = recv
		w.lastAt = now
		return trafficRates{
			txEwma: w.txEwma, rxEwma: w.rxEwma,
			txSpark: append([]float64(nil), w.txRates...),
			rxSpark: append([]float64(nil), w.rxRates...),
		}
	}

	if sent < w.lastSent || recv < w.lastRecv {
		// Counter reset (process restart, peer expiry in summed series):
		// re-anchor the counters but PRESERVE the EWMAs and spark — the
		// sustained-rate history is still true; only the delta baseline
		// moved. This tick has no measurable delta, so instant reads 0 and
		// the EWMAs are not updated (no sample, rather than a fake zero).
		w.lastSent = sent
		w.lastRecv = recv
		w.lastAt = now
		return trafficRates{
			txEwma: w.txEwma, rxEwma: w.rxEwma,
			txSpark: append([]float64(nil), w.txRates...),
			rxSpark: append([]float64(nil), w.rxRates...),
		}
	}

	txPerSec := float64(sent-w.lastSent) / dt
	rxPerSec := float64(recv-w.lastRecv) / dt
	for i, tau := range ewmaTaus {
		decay := math.Exp(-dt / tau.Seconds())
		w.txEwma[i] = w.txEwma[i]*decay + txPerSec*(1-decay)
		w.rxEwma[i] = w.rxEwma[i]*decay + rxPerSec*(1-decay)
	}

	w.lastSent = sent
	w.lastRecv = recv
	w.lastAt = now

	w.txRates = append(w.txRates, txPerSec)
	w.rxRates = append(w.rxRates, rxPerSec)
	if len(w.txRates) > peerConnStatsSparkPoints {
		w.txRates = w.txRates[len(w.txRates)-peerConnStatsSparkPoints:]
	}
	if len(w.rxRates) > peerConnStatsSparkPoints {
		w.rxRates = w.rxRates[len(w.rxRates)-peerConnStatsSparkPoints:]
	}

	return trafficRates{
		txPerSec: txPerSec, rxPerSec: rxPerSec,
		txEwma: w.txEwma, rxEwma: w.rxEwma,
		txSpark: append([]float64(nil), w.txRates...),
		rxSpark: append([]float64(nil), w.rxRates...),
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

	wal := c.observeTrafficWindowRates(now, "wal",
		int64(count), int64(totalNanos), "", stepWindows, c.raftLiveStats) //nolint:gosec // counters < 2^63
	if wal.txPerSec > 0 {
		stats.RaftWalAppendAvgMs = wal.rxPerSec / wal.txPerSec / 1e6
	}
	live := c.observeTrafficWindowRates(now, "live",
		int64(elections), int64(failedHB), "", stepWindows, c.raftLiveStats) //nolint:gosec // counters < 2^63
	stats.RaftElectionsPerMin = live.txPerSec * 60

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
	switch {
	case stats.RaftElectionsPerMin >= raftElectionStormPerMin:
		alerts.Set("raft-liveness-elections", alert.Error, "raft",
			fmt.Sprintf("Raft election storm: %.1f elections/min on this node — consensus is churning (see gastrolog-1io54g)", stats.RaftElectionsPerMin))
	case stats.RaftElectionsPerMin < raftElectionCalmPerMin:
		alerts.Clear("raft-liveness-elections")
	}
	switch {
	case stats.RaftWalAppendMaxMs >= walAppendMaxAlertMs:
		alerts.Set("raft-wal-latency", alert.Warning, "raft",
			fmt.Sprintf("Raft WAL append latency degraded: max %.0fms since last tick — bulk I/O may be starving consensus", stats.RaftWalAppendMaxMs))
	case stats.RaftWalAppendMaxMs < walAppendMaxClearMs:
		alerts.Clear("raft-wal-latency")
	}
}

// throughputRateProto converts one side (tx or rx) of a window observation
// into the wire series.
func throughputRateProto(r trafficRates, tx bool) *gastrologv1.ThroughputRate {
	if tx {
		return &gastrologv1.ThroughputRate{
			InstantPerSec: r.txPerSec, Avg_1MPerSec: r.txEwma[0], Avg_5MPerSec: r.txEwma[1], Avg_15MPerSec: r.txEwma[2], Spark: r.txSpark,
		}
	}
	return &gastrologv1.ThroughputRate{
		InstantPerSec: r.rxPerSec, Avg_1MPerSec: r.rxEwma[0], Avg_5MPerSec: r.rxEwma[1], Avg_15MPerSec: r.rxEwma[2], Spark: r.rxSpark,
	}
}

// snapshotRates returns the last stepped observation without advancing the
// window (read paths between broadcast ticks).
func (w *peerConnStatsWindow) snapshotRates() trafficRates {
	r := trafficRates{
		txEwma: w.txEwma, rxEwma: w.rxEwma,
		txSpark: append([]float64(nil), w.txRates...),
		rxSpark: append([]float64(nil), w.rxRates...),
	}
	if len(r.txSpark) > 0 {
		r.txPerSec = r.txSpark[len(r.txSpark)-1]
	}
	if len(r.rxSpark) > 0 {
		r.rxPerSec = r.rxSpark[len(r.rxSpark)-1]
	}
	return r
}

// collectClusterRouteRates windows the SUMMED cluster route counters; the
// resulting series (including spark) is this node's honest observation of
// cluster rate at its tick cadence. Counter drops (peer TTL expiry, node
// restart) re-anchor via the window's reset guard (gastrolog-4eh5ns).
func (c *StatsCollector) collectClusterRouteRates(now time.Time, stepWindows bool) {
	if c.cfg.ClusterRouteTotals == nil {
		return
	}
	cin, crouted, membership := c.cfg.ClusterRouteTotals()
	cr := c.observeTrafficWindowRates(now, "clusterroute",
		cin, crouted, membership, stepWindows, c.routeRateStats)
	c.mu.Lock()
	c.clusterIngested = &gastrologv1.ThroughputRate{
		InstantPerSec: cr.txPerSec, Avg_1MPerSec: cr.txEwma[0], Avg_5MPerSec: cr.txEwma[1], Avg_15MPerSec: cr.txEwma[2], Spark: cr.txSpark,
	}
	c.clusterRouted = &gastrologv1.ThroughputRate{
		InstantPerSec: cr.rxPerSec, Avg_1MPerSec: cr.rxEwma[0], Avg_5MPerSec: cr.rxEwma[1], Avg_15MPerSec: cr.rxEwma[2], Spark: cr.rxSpark,
	}
	c.mu.Unlock()
}

// ClusterRouteRates returns the latest cluster-total route throughput series
// (instant/30s/1m + spark), computed server-side from summed cluster counters
// at each stats tick. Safe between ticks; returns empty rates before the
// first tick. Proto messages carry an internal mutex, so the copies are
// rebuilt field-by-field rather than dereferenced (gastrolog-4eh5ns).
func (c *StatsCollector) ClusterRouteRates() (ingested, routed *gastrologv1.ThroughputRate) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clusterIngested == nil {
		return &gastrologv1.ThroughputRate{}, &gastrologv1.ThroughputRate{}
	}
	return copyThroughputRate(c.clusterIngested), copyThroughputRate(c.clusterRouted)
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
