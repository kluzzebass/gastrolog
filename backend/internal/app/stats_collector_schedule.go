package app

import (
	"context"
	"fmt"
	"time"

	"gastrolog/internal/cluster"
	"gastrolog/internal/orchestrator"
)

const clusterStatsBroadcastJobName = "cluster-stats-broadcast"

// startStatsCollectorJobs registers the peer stats broadcast on the
// orchestrator scheduler.
//
// It used to register a second job alongside it — a lightweight peer heartbeat
// on its own schedule, split out (gastrolog-2kio8, gastrolog-2vqw3) so a slow
// NodeStats collection pass could not delay peer liveness. That job is gone:
// Raft's own per-group heartbeats already carry peer liveness
// (gastrolog-1lbifx), so the starvation the split protected against no longer
// has anything to starve. This broadcast now carries observability payload
// only; nothing in the cluster waits on its cadence to notice a dead peer.
func startStatsCollectorJobs(
	scheduler scheduledJobRegistry,
	collector *cluster.StatsCollector,
	ctx context.Context,
	broadcastInterval time.Duration,
) error {
	if err := scheduler.AddJob(
		clusterStatsBroadcastJobName,
		orchestrator.CronEvery(broadcastIntervalOr(defaultBroadcastInterval, broadcastInterval)),
		func() { collector.BroadcastStats(ctx) },
	); err != nil {
		return fmt.Errorf("cluster stats broadcast job: %w", err)
	}
	scheduler.Describe(clusterStatsBroadcastJobName,
		"Broadcast local NodeStats to all cluster peers (vault, route, ingest queue, alerts)")
	return nil
}

func broadcastIntervalOr(fallback, interval time.Duration) time.Duration {
	if interval <= 0 {
		return fallback
	}
	return interval
}
