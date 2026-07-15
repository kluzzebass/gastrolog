package app

import (
	"context"
	"fmt"
	"time"

	"gastrolog/internal/cluster"
	"gastrolog/internal/orchestrator"
)

const (
	clusterStatsBroadcastJobName = "cluster-stats-broadcast"
	clusterPeerHeartbeatJobName  = "cluster-peer-heartbeat"
)

// startStatsCollectorJobs registers peer stats broadcast and heartbeat as
// separate scheduled jobs on the orchestrator scheduler. Splitting them
// ensures a slow NodeStats collection pass cannot delay peer liveness
// heartbeats (gastrolog-2kio8, gastrolog-2vqw3 follow-up).
func startStatsCollectorJobs(
	scheduler scheduledJobRegistry,
	collector *cluster.StatsCollector,
	ctx context.Context,
	broadcastInterval, heartbeatInterval time.Duration,
) error {
	if err := scheduler.AddJob(
		clusterStatsBroadcastJobName,
		orchestrator.CronEvery(broadcastIntervalOr(5*time.Second, broadcastInterval)),
		func() { collector.BroadcastStats(ctx) },
	); err != nil {
		return fmt.Errorf("cluster stats broadcast job: %w", err)
	}
	scheduler.Describe(clusterStatsBroadcastJobName,
		"Broadcast local NodeStats to all cluster peers (vault, route, ingest queue, alerts)")

	if heartbeatInterval <= 0 {
		return nil
	}
	if err := scheduler.AddJob(
		clusterPeerHeartbeatJobName,
		orchestrator.CronEvery(heartbeatIntervalOr(time.Second, heartbeatInterval)),
		func() { collector.BroadcastHeartbeat(ctx) },
	); err != nil {
		return fmt.Errorf("cluster peer heartbeat job: %w", err)
	}
	scheduler.Describe(clusterPeerHeartbeatJobName,
		"Broadcast lightweight peer liveness heartbeat (refreshes PeerState last-seen)")
	return nil
}

func broadcastIntervalOr(fallback, interval time.Duration) time.Duration {
	if interval <= 0 {
		return fallback
	}
	return interval
}

func heartbeatIntervalOr(fallback, interval time.Duration) time.Duration {
	if interval <= 0 {
		return fallback
	}
	return interval
}
