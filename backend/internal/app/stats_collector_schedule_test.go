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

// One broadcast job, and only one. There used to be a second — a lightweight
// peer heartbeat on its own cadence, split out so slow stats collection could
// not starve peer liveness (gastrolog-2kio8). gastrolog-1lbifx moved liveness
// onto Raft last-contact and deleted it, so this asserts the absence: a
// re-added liveness broadcast would be a third opinion about whether a peer is
// up, which is exactly what that issue removed.
func TestStartStatsCollectorJobsRegistersOnlyTheStatsBroadcast(t *testing.T) {
	t.Parallel()

	sched := &fakeScheduledJobRegistry{}
	collector := cluster.NewStatsCollector(cluster.StatsCollectorConfig{
		NodeID: "node-a",
	})

	if err := startStatsCollectorJobs(sched, collector, context.Background(), 5*time.Second); err != nil {
		t.Fatalf("startStatsCollectorJobs: %v", err)
	}
	if len(sched.jobs) != 1 {
		t.Fatalf("jobs = %v, want the stats broadcast alone", sched.jobs)
	}
	if sched.jobs[0] != clusterStatsBroadcastJobName+":*/5 * * * * *" {
		t.Fatalf("stats job = %q", sched.jobs[0])
	}
}

// An unset broadcast interval must fall back to the shipped default rather
// than registering a cron of "*/0", which the scheduler would reject or run
// pathologically often.
func TestStartStatsCollectorJobsFallsBackToDefaultInterval(t *testing.T) {
	t.Parallel()

	sched := &fakeScheduledJobRegistry{}
	collector := cluster.NewStatsCollector(cluster.StatsCollectorConfig{NodeID: "node-a"})

	if err := startStatsCollectorJobs(sched, collector, context.Background(), 0); err != nil {
		t.Fatalf("startStatsCollectorJobs: %v", err)
	}
	if len(sched.jobs) != 1 || sched.jobs[0] != clusterStatsBroadcastJobName+":*/5 * * * * *" {
		t.Fatalf("jobs = %v, want the stats broadcast at the 5s default", sched.jobs)
	}
}
