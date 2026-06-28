package app

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/cluster"
)

type fakeScheduledJobRegistry struct {
	jobs []string
}

func (f *fakeScheduledJobRegistry) AddJob(name, cronExpr string, taskFn any, args ...any) error {
	f.jobs = append(f.jobs, name+":"+cronExpr)
	return nil
}

func (f *fakeScheduledJobRegistry) Describe(string, string) {}

func TestStartStatsCollectorJobsRegistersSeparateCadences(t *testing.T) {
	t.Parallel()

	sched := &fakeScheduledJobRegistry{}
	collector := cluster.NewStatsCollector(cluster.StatsCollectorConfig{
		NodeID: "node-a",
	})

	if err := startStatsCollectorJobs(sched, collector, context.Background(), 5*time.Second, time.Second); err != nil {
		t.Fatalf("startStatsCollectorJobs: %v", err)
	}
	if len(sched.jobs) != 2 {
		t.Fatalf("jobs = %v, want stats + heartbeat", sched.jobs)
	}
	if sched.jobs[0] != clusterStatsBroadcastJobName+":@every 5s" {
		t.Fatalf("stats job = %q", sched.jobs[0])
	}
	if sched.jobs[1] != clusterPeerHeartbeatJobName+":@every 1s" {
		t.Fatalf("heartbeat job = %q", sched.jobs[1])
	}
}

func TestStartStatsCollectorJobsSkipsHeartbeatWhenDisabled(t *testing.T) {
	t.Parallel()

	sched := &fakeScheduledJobRegistry{}
	collector := cluster.NewStatsCollector(cluster.StatsCollectorConfig{NodeID: "node-a"})

	if err := startStatsCollectorJobs(sched, collector, context.Background(), 5*time.Second, 0); err != nil {
		t.Fatalf("startStatsCollectorJobs: %v", err)
	}
	if len(sched.jobs) != 1 {
		t.Fatalf("jobs = %v, want stats only", sched.jobs)
	}
}
