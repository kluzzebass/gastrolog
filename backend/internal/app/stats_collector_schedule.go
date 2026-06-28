package app

import (
	"context"
	"fmt"
	"time"

	"gastrolog/internal/cluster"
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
		everyCron(broadcastInterval, 5*time.Second),
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
		everyCron(heartbeatInterval, time.Second),
		func() { collector.BroadcastHeartbeat(ctx) },
	); err != nil {
		return fmt.Errorf("cluster peer heartbeat job: %w", err)
	}
	scheduler.Describe(clusterPeerHeartbeatJobName,
		"Broadcast lightweight peer liveness heartbeat (refreshes PeerState last-seen)")
	return nil
}

func everyCron(interval, fallback time.Duration) string {
	if interval <= 0 {
		interval = fallback
	}
	return "@every " + interval.String()
}
