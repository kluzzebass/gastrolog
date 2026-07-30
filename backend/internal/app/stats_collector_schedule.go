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
// The broadcast carries observability payload only. Peer liveness rides
// on Raft's own per-group heartbeats, so nothing in the cluster waits on
// this cadence to notice a dead peer and a slow NodeStats collection pass
// cannot delay liveness detection.
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
