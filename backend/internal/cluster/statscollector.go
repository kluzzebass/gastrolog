package cluster

import (
	"context"
	"fmt"
	"log/slog"
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

// StatsVaultAppendSnapshot captures one vault's cumulative segmentation
// append counters for broadcast; the collector's rolling windows turn them
// into per-second rates (gastrolog-4eh5ns).
type StatsVaultAppendSnapshot struct {
	VaultID         glid.GLID
	RecordsAppended uint64
	BytesAppended   uint64
	RecordsDurable  uint64
	QueueDepth      int
	QueueCap        int
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
	Broadcaster       *Broadcaster
	RaftStats         RaftStatsProvider
	Stats             StatsProvider
	PeerConns         PeerConnSnapshotProvider // optional; nil disables peer conn stats
	Alerts            AlertProvider            // optional; nil if no alert collector
	Jobs              JobsProvider             // optional; nil in single-node mode
	NodeID            string
	NodeNameFn        func() string // lazily resolved node name
	Version           string
	StartTime         time.Time
	Interval          time.Duration  // heavy NodeStats broadcast cadence (default 5s)
	HeartbeatInterval time.Duration  // lightweight liveness ping cadence (default 1s); 0 disables
	ApiAddress        string         // HTTP API listen address (e.g. ":4564")
	PprofAddress      string         // pprof listen address, empty if disabled
	StatsSignal       *notify.Signal // fired after each broadcast to notify WatchSystemStatus streams
	Logger            *slog.Logger
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
	// lastPublishedPurposeWindows holds purposes_window from the most recent
	// CollectLocalTick (5s broadcast). CollectLocalSnapshot overlays this onto
	// read-only snapshots briefly after each tick so WatchSystemStatus still
	// shows the completed interval after ResetPurposeWindows runs.
	lastPublishedPurposeWindows map[string][]string
}

const peerConnStatsSparkPoints = 20

type trafficSample struct {
	at   time.Time
	sent int64
	recv int64
}

type peerConnStatsWindow struct {
	lastSent int64
	lastRecv int64
	lastAt   time.Time
	// samples retains (tick time, counters) for trailing-average rates:
	// counter delta over the retained sample closest to now-horizon
	// (gastrolog-4eh5ns). Capped at peerConnStatsSparkPoints (~100s at the
	// 5s broadcast cadence).
	samples []trafficSample
	tx30    float64
	rx30    float64
	tx60    float64
	rx60    float64
	txRates  []float64
	rxRates  []float64
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
				int64(as.RecordsAppended), int64(as.BytesAppended), stepWindows, c.vaultAppendStats) //nolint:gosec // counters < 2^63
			dr := c.observeTrafficWindowRates(now, "durable:"+as.VaultID.String(),
				int64(as.RecordsDurable), 0, stepWindows, c.vaultAppendStats) //nolint:gosec // counter < 2^63
			v.AppendRecords = &gastrologv1.ThroughputRate{
				InstantPerSec: ar.txPerSec, Avg_30SPerSec: ar.tx30, Avg_60SPerSec: ar.tx60, Spark: ar.txSpark,
			}
			v.AppendBytes = &gastrologv1.ThroughputRate{
				InstantPerSec: ar.rxPerSec, Avg_30SPerSec: ar.rx30, Avg_60SPerSec: ar.rx60, Spark: ar.rxSpark,
			}
			v.AppendDurable = &gastrologv1.ThroughputRate{
				InstantPerSec: dr.txPerSec, Avg_30SPerSec: dr.tx30, Avg_60SPerSec: dr.tx60, Spark: dr.txSpark,
			}
			v.AppendRecordsTotal = as.RecordsAppended
			v.AppendBytesTotal = as.BytesAppended
			v.AppendQueueDepth = uint32(as.QueueDepth)  //nolint:gosec
			v.AppendQueueCapacity = uint32(as.QueueCap) //nolint:gosec
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
		// Node-level routing throughput windows (gastrolog-4eh5ns).
		rr := c.observeTrafficWindowRates(now, "route",
			rs.Ingested, rs.Routed, stepWindows, c.routeRateStats)
		stats.RouteIngested = &gastrologv1.ThroughputRate{
			InstantPerSec: rr.txPerSec, Avg_30SPerSec: rr.tx30, Avg_60SPerSec: rr.tx60, Spark: rr.txSpark,
		}
		stats.RouteRouted = &gastrologv1.ThroughputRate{
			InstantPerSec: rr.rxPerSec, Avg_30SPerSec: rr.rx30, Avg_60SPerSec: rr.rx60, Spark: rr.rxSpark,
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
	r := c.observeTrafficWindowRates(now, key, sent, recv, step, store)
	return r.txPerSec, r.rxPerSec, r.txSpark, r.rxSpark
}

// trafficRates is one window observation: instantaneous rates, trailing
// averages (~30s / ~60s counter deltas), and the per-tick spark history.
type trafficRates struct {
	txPerSec, rxPerSec float64
	tx30, rx30         float64
	tx60, rx60         float64
	txSpark, rxSpark   []float64
}

func (c *StatsCollector) observeTrafficWindowRates(now time.Time, key string, sent, recv int64, step bool, store map[string]*peerConnStatsWindow) trafficRates {
	if key == "" {
		return trafficRates{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	w := store[key]
	if w == nil {
		w = &peerConnStatsWindow{lastSent: sent, lastRecv: recv, lastAt: now,
			samples: []trafficSample{{at: now, sent: sent, recv: recv}}}
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

	if sent < w.lastSent || recv < w.lastRecv {
		// Counter reset (process restart): re-anchor everything.
		*w = peerConnStatsWindow{lastSent: sent, lastRecv: recv, lastAt: now,
			samples: []trafficSample{{at: now, sent: sent, recv: recv}}}
		return trafficRates{}
	}

	txPerSec := float64(sent-w.lastSent) / dt
	rxPerSec := float64(recv-w.lastRecv) / dt
	w.tx30, w.rx30 = trailingRates(w.samples, now, sent, recv, 30*time.Second)
	w.tx60, w.rx60 = trailingRates(w.samples, now, sent, recv, 60*time.Second)

	w.lastSent = sent
	w.lastRecv = recv
	w.lastAt = now

	w.samples = append(w.samples, trafficSample{at: now, sent: sent, recv: recv})
	if len(w.samples) > peerConnStatsSparkPoints {
		w.samples = w.samples[len(w.samples)-peerConnStatsSparkPoints:]
	}
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
		tx30: w.tx30, rx30: w.rx30, tx60: w.tx60, rx60: w.rx60,
		txSpark: append([]float64(nil), w.txRates...),
		rxSpark: append([]float64(nil), w.rxRates...),
	}
}

// snapshotRates returns the last stepped observation without advancing the
// window (read paths between broadcast ticks).
func (w *peerConnStatsWindow) snapshotRates() trafficRates {
	r := trafficRates{
		tx30: w.tx30, rx30: w.rx30, tx60: w.tx60, rx60: w.rx60,
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

// trailingRates computes average rates over roughly horizon: the counter
// delta between now and the retained sample whose age is closest to horizon.
// With a young window (uptime < horizon) it falls back to the oldest sample —
// an average over the available span rather than a fabricated one.
func trailingRates(samples []trafficSample, now time.Time, sent, recv int64, horizon time.Duration) (tx, rx float64) {
	if len(samples) == 0 {
		return 0, 0
	}
	target := now.Add(-horizon)
	best := samples[0]
	bestDiff := absDuration(best.at.Sub(target))
	for _, s := range samples[1:] {
		if d := absDuration(s.at.Sub(target)); d < bestDiff {
			best, bestDiff = s, d
		}
	}
	dt := now.Sub(best.at).Seconds()
	if dt <= 0 {
		return 0, 0
	}
	return float64(sent-best.sent) / dt, float64(recv-best.recv) / dt
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
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
