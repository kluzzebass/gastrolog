package cluster

import (
	"context"
	"log/slog"
	"runtime"
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/alert"
	"gastrolog/internal/notify"
	"gastrolog/internal/sysmetrics"
)

// StatsVaultSnapshot is the stats collector's view of a vault.
// Mirrors orchestrator.VaultSnapshot without importing it.
type StatsVaultSnapshot struct {
	ID           string
	Name         string
	RecordCount  int64
	ChunkCount   int
	SealedChunks int
	DataBytes    int64
	Enabled      bool
}

// StatsRouteSnapshot captures route stats for broadcast.
type StatsRouteSnapshot struct {
	Ingested       int64
	Dropped        int64
	Routed         int64
	FilterActive   bool
	VaultStats     []StatsVaultRouteSnapshot
	RouteStats     []StatsPerRouteSnapshot
}

// StatsVaultRouteSnapshot captures per-vault route stats.
type StatsVaultRouteSnapshot struct {
	VaultID   string
	Matched   int64
	Forwarded int64
}

// StatsPerRouteSnapshot captures per-route stats.
type StatsPerRouteSnapshot struct {
	RouteID   string
	Matched   int64
	Forwarded int64
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
}

// RaftStatsProvider exposes local Raft stats for the collector.
type RaftStatsProvider interface {
	LocalStats() map[string]string
}

// ForwardingStatsProvider exposes record forwarding counters.
type ForwardingStatsProvider interface {
	ForwardingStats() (sent, received int64)
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
	Forwarding        ForwardingStatsProvider // optional; nil if no forwarding
	Alerts            AlertProvider            // optional; nil if no alert collector
	Jobs              JobsProvider // optional; nil in single-node mode
	NodeID            string
	NodeNameFn        func() string // lazily resolved node name
	Version           string
	StartTime         time.Time
	Interval          time.Duration
	ApiAddress        string // HTTP API listen address (e.g. ":4564")
	PprofAddress      string // pprof listen address, empty if disabled
	StatsSignal       *notify.Signal // fired after each broadcast to notify WatchSystemStatus streams
	Logger            *slog.Logger
}

// StatsCollector periodically gathers local node statistics and
// broadcasts them to all cluster peers via the Broadcaster.
type StatsCollector struct {
	cfg StatsCollectorConfig
}

// NewStatsCollector creates a collector with the given system.
func NewStatsCollector(cfg StatsCollectorConfig) *StatsCollector {
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}
	return &StatsCollector{cfg: cfg}
}

// Run starts the periodic collection loop. Blocks until ctx is cancelled.
func (c *StatsCollector) Run(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats := c.CollectLocal()
			if c.cfg.Broadcaster != nil {
				c.cfg.Broadcaster.Send(ctx, &gastrologv1.BroadcastMessage{
					SenderId:  c.cfg.NodeID,
					Timestamp: timestamppb.Now(),
					Payload:   &gastrologv1.BroadcastMessage_NodeStats{NodeStats: stats},
				})
				c.BroadcastJobs(ctx)
			}
			if c.cfg.StatsSignal != nil {
				c.cfg.StatsSignal.Notify()
			}
		}
	}
}

// CollectLocal gathers a NodeStats snapshot for the local node.
// Called directly by the lifecycle server for real-time stats (not stale broadcast).
func (c *StatsCollector) CollectLocal() *gastrologv1.NodeStats {
	cpu := sysmetrics.CPUPercent()
	mem := sysmetrics.Memory()

	stats := &gastrologv1.NodeStats{
		CpuPercent:          cpu,
		MemoryInuse:         uint64(mem.Inuse),          //nolint:gosec // always positive
		MemoryRss:           uint64(mem.RSS),             //nolint:gosec // always positive
		MemoryHeapAlloc:     uint64(mem.HeapAlloc),       //nolint:gosec // always positive
		MemorySys:           uint64(mem.Sys),             //nolint:gosec // always positive
		Goroutines:          uint32(runtime.NumGoroutine()), //nolint:gosec // always small
		NodeName:            c.cfg.NodeNameFn(),
		Version:             c.cfg.Version,
		UptimeSeconds:       int64(time.Since(c.cfg.StartTime).Seconds()),
		MemoryHeapIdle:      uint64(mem.HeapIdle),        //nolint:gosec // always positive
		MemoryHeapReleased:  uint64(mem.HeapReleased),    //nolint:gosec // always positive
		MemoryStackInuse:    uint64(mem.StackInuse),      //nolint:gosec // always positive
		MemoryHeapObjects:   mem.HeapObjects,
		NumGc:               mem.NumGC,
		ApiAddress:          c.cfg.ApiAddress,
		PprofAddress:        c.cfg.PprofAddress,
	}

	// Queue stats.
	if c.cfg.Stats != nil {
		stats.IngestQueueDepth = uint32(c.cfg.Stats.IngestQueueDepth())       //nolint:gosec
		stats.IngestQueueCapacity = uint32(c.cfg.Stats.IngestQueueCapacity()) //nolint:gosec

		// Vault snapshots.
		for _, v := range c.cfg.Stats.VaultSnapshots() {
			stats.Vaults = append(stats.Vaults, &gastrologv1.VaultStats{
				Id:           v.ID,
				Name:         v.Name,
				RecordCount:  v.RecordCount,
				ChunkCount:   int64(v.ChunkCount),
				SealedChunks: int64(v.SealedChunks),
				DataBytes:    v.DataBytes,
				Enabled:      v.Enabled,
			})
		}

		// Ingester stats.
		for _, id := range c.cfg.Stats.IngesterIDs() {
			name, msgs, bytes, errs, running := c.cfg.Stats.IngesterStats(id)
			stats.Ingesters = append(stats.Ingesters, &gastrologv1.IngesterNodeStats{
				Id:               id,
				Name:             name,
				MessagesIngested: uint64(msgs),  //nolint:gosec
				BytesIngested:    uint64(bytes),  //nolint:gosec
				Errors:           uint64(errs),   //nolint:gosec
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
				VaultId:          vs.VaultID,
				RecordsMatched:   vs.Matched,
				RecordsForwarded: vs.Forwarded,
			})
		}
		for _, ps := range rs.RouteStats {
			stats.RoutePerRouteStats = append(stats.RoutePerRouteStats, &gastrologv1.PerRouteStats{
				RouteId:          ps.RouteID,
				RecordsMatched:   ps.Matched,
				RecordsForwarded: ps.Forwarded,
			})
		}
	}

	// Forwarding stats.
	if c.cfg.Forwarding != nil {
		stats.ForwardedSent, stats.ForwardedReceived = c.cfg.Forwarding.ForwardingStats()
	}

	// Active alerts.
	if c.cfg.Alerts != nil {
		for _, a := range c.cfg.Alerts.ActiveAlerts() {
			stats.Alerts = append(stats.Alerts, &gastrologv1.SystemAlert{
				Id:        a.ID,
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

// BroadcastJobs sends the current job list to all cluster peers.
// Called on every tick for periodic sync, and directly by the scheduler's
// onJobChange callback for immediate notification.
func (c *StatsCollector) BroadcastJobs(ctx context.Context) {
	if c.cfg.Broadcaster == nil || c.cfg.Jobs == nil {
		return
	}
	c.cfg.Broadcaster.Send(ctx, &gastrologv1.BroadcastMessage{
		SenderId:  c.cfg.NodeID,
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
