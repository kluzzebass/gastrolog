package cluster

import (
	"context"
	"log/slog"
	"runtime"
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
	PipelineDiskSnapshots() []StatsVaultPipelineDiskSnapshot
}

// RaftStatsProvider exposes local Raft stats for the collector.
type RaftStatsProvider interface {
	LocalStats() map[string]string
}

// PeerBytesProvider exposes cumulative per-peer inter-node gRPC byte
// counters. Satisfied by *PeerByteMetrics.
type PeerBytesProvider interface {
	Snapshot() []PeerByteCounter
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
	PeerBytes    PeerBytesProvider // optional; nil disables per-peer byte stats
	Alerts       AlertProvider           // optional; nil if no alert collector
	Jobs         JobsProvider            // optional; nil in single-node mode
	NodeID            string
	NodeNameFn        func() string // lazily resolved node name
	Version           string
	StartTime         time.Time
	Interval          time.Duration // heavy NodeStats broadcast cadence (default 5s)
	HeartbeatInterval time.Duration // lightweight liveness ping cadence (default 1s); 0 disables
	ApiAddress        string         // HTTP API listen address (e.g. ":4564")
	PprofAddress      string         // pprof listen address, empty if disabled
	StatsSignal       *notify.Signal // fired after each broadcast to notify WatchSystemStatus streams
	Logger            *slog.Logger
}

// StatsCollector periodically gathers local node statistics and
// broadcasts them to all cluster peers via the Broadcaster.
type StatsCollector struct {
	cfg StatsCollectorConfig

	mu        sync.Mutex
	peerBytes map[string]*peerBytesWindow
}

const peerBytesSparkPoints = 20

type peerBytesWindow struct {
	lastSent int64
	lastRecv int64
	lastAt   time.Time
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
		cfg:       cfg,
		peerBytes: make(map[string]*peerBytesWindow),
	}
}

// Delete drops the per-peer rate window for a removed node. Called
// from the peer-removal observer (raft.go runPeerRemovalLoop) so the
// peerBytes map doesn't accumulate dead entries forever. Naming
// aligns with the peerEvictor interface.
func (c *StatsCollector) Delete(peer string) {
	c.mu.Lock()
	delete(c.peerBytes, peer)
	c.mu.Unlock()
}

// ReconcilePeers drops any rate window whose peer is not in keep.
// Backstop for the observer path when hraft delivers a config
// change via snapshot install (no PeerObservation fires).
func (c *StatsCollector) ReconcilePeers(keep map[string]struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for p := range c.peerBytes {
		if _, ok := keep[p]; !ok {
			delete(c.peerBytes, p)
		}
	}
}

// Run starts the periodic broadcast loops. Blocks until ctx is cancelled.
//
// Two cadences run in parallel:
//   - Stats ticker (cfg.Interval, default 5s): full NodeStats payload.
//     Heavy — carries vault stats, ingester stats, route stats, peer
//     bytes, alerts, raft state, etc.
//   - Heartbeat ticker (cfg.HeartbeatInterval, default 1s): empty
//     Heartbeat marker. Just refreshes peer last-seen so paused/wedged
//     peers fall out of LivePeers within a few seconds without making
//     the heavy payload fly every second. See gastrolog-2kio8.
func (c *StatsCollector) Run(ctx context.Context) {
	statsTicker := time.NewTicker(c.cfg.Interval)
	defer statsTicker.Stop()
	heartbeatTicker := time.NewTicker(c.cfg.HeartbeatInterval)
	defer heartbeatTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case tickAt := <-statsTicker.C:
			stats := c.CollectLocalTick(tickAt)
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
		case <-heartbeatTicker.C:
			if c.cfg.Broadcaster != nil {
				c.cfg.Broadcaster.Send(ctx, &gastrologv1.BroadcastMessage{
					SenderId:  []byte(c.cfg.NodeID),
					Timestamp: timestamppb.Now(),
					Payload:   &gastrologv1.BroadcastMessage_Heartbeat{Heartbeat: &gastrologv1.Heartbeat{}},
				})
			}
		}
	}
}

// CollectLocalSnapshot gathers a NodeStats snapshot for the local node without
// advancing any rolling windows. Used by the lifecycle server for "real-time"
// reads so opening the inspector doesn't skew rate calculations.
func (c *StatsCollector) CollectLocalSnapshot() *gastrologv1.NodeStats {
	return c.collectLocal(time.Now(), false)
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
				HeadSegments:             uint32(pd.Head),               //nolint:gosec
				PreHeadSegments:            uint32(pd.PreHead),            //nolint:gosec
			})
		}
	}

	// Per-peer inter-node byte counters. See gastrolog-47u85.
	if c.cfg.PeerBytes != nil {
		for _, pc := range c.cfg.PeerBytes.Snapshot() {
			txPerSec, rxPerSec, txSpark, rxSpark := c.observePeerBytes(now, pc.Peer, pc.Sent, pc.Received, stepWindows)
			stats.PeerBytes = append(stats.PeerBytes, &gastrologv1.PeerBytesStat{
				Peer:          pc.Peer,
				BytesSent:     pc.Sent,
				BytesReceived: pc.Received,
				TxBytesPerSec: txPerSec,
				RxBytesPerSec: rxPerSec,
				TxSpark:       txSpark,
				RxSpark:       rxSpark,
			})
		}
	}

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

func (c *StatsCollector) observePeerBytes(now time.Time, peer string, sent, recv int64, step bool) (txPerSec, rxPerSec float64, txSpark, rxSpark []float64) {
	if peer == "" {
		return 0, 0, nil, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	w := c.peerBytes[peer]
	if w == nil {
		w = &peerBytesWindow{lastSent: sent, lastRecv: recv, lastAt: now}
		c.peerBytes[peer] = w
		return 0, 0, nil, nil
	}

	// Snapshot-only reads should not advance the window (prevents UI/opening
	// inspector from skewing rates with tiny dt).
	if !step {
		txSpark = append([]float64(nil), w.txRates...)
		rxSpark = append([]float64(nil), w.rxRates...)
		if len(txSpark) > 0 {
			txPerSec = txSpark[len(txSpark)-1]
		}
		if len(rxSpark) > 0 {
			rxPerSec = rxSpark[len(rxSpark)-1]
		}
		return txPerSec, rxPerSec, txSpark, rxSpark
	}

	dt := now.Sub(w.lastAt).Seconds()
	if dt <= 0 {
		return 0, 0, append([]float64(nil), w.txRates...), append([]float64(nil), w.rxRates...)
	}

	// Handle counter resets (process restart) or any monotonicity violation.
	if sent < w.lastSent || recv < w.lastRecv {
		w.lastSent = sent
		w.lastRecv = recv
		w.lastAt = now
		return 0, 0, append([]float64(nil), w.txRates...), append([]float64(nil), w.rxRates...)
	}

	txPerSec = float64(sent-w.lastSent) / dt
	rxPerSec = float64(recv-w.lastRecv) / dt

	w.lastSent = sent
	w.lastRecv = recv
	w.lastAt = now

	w.txRates = append(w.txRates, txPerSec)
	w.rxRates = append(w.rxRates, rxPerSec)
	if len(w.txRates) > peerBytesSparkPoints {
		w.txRates = w.txRates[len(w.txRates)-peerBytesSparkPoints:]
	}
	if len(w.rxRates) > peerBytesSparkPoints {
		w.rxRates = w.rxRates[len(w.rxRates)-peerBytesSparkPoints:]
	}

	txSpark = append([]float64(nil), w.txRates...)
	rxSpark = append([]float64(nil), w.rxRates...)
	return txPerSec, rxPerSec, txSpark, rxSpark
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
