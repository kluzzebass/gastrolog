package cluster

import (
	"context"
	"log/slog"
	"runtime"
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
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

// StatsProvider abstracts the orchestrator for stats collection.
// Defined here at the consumer site to avoid importing orchestrator.
type StatsProvider interface {
	IngestQueueDepth() int
	IngestQueueCapacity() int
	VaultSnapshots() []StatsVaultSnapshot
	IngesterIDs() []string
	IngesterStats(id string) (name string, messages, bytes, errors int64, running bool)
}

// RaftStatsProvider exposes local Raft stats for the collector.
type RaftStatsProvider interface {
	LocalStats() map[string]string
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
	Jobs              JobsProvider // optional; nil in single-node mode
	NodeID            string
	NodeNameFn        func() string // lazily resolved node name
	Version           string
	StartTime         time.Time
	Interval          time.Duration
	Logger            *slog.Logger
}

// StatsCollector periodically gathers local node statistics and
// broadcasts them to all cluster peers via the Broadcaster.
type StatsCollector struct {
	cfg StatsCollectorConfig
}

// NewStatsCollector creates a collector with the given config.
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
