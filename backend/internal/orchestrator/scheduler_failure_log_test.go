package orchestrator

// A scheduled job that fails must say so in the log.
//
// It did not. completeOneTimeJob set the progress record and published a
// JobEventFailed, and the success path logged "job finished" — so from the log
// alone, a job that died and a job that never ran were the same thing: silence.
// Post-seal rides a one-time job, which is why a chunk stranded in Sealing left
// no evidence anywhere and why that member of gastrolog-231ik stayed
// unfalsifiable across runs.

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestFailedOneTimeJobIsLogged(t *testing.T) {
	t.Parallel()
	var out syncBuffer
	sched, err := newScheduler(slog.New(slog.NewTextHandler(&out, nil)), 4, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sched.Stop() })

	notified := notifyChan(sched)

	const name = "doomed"
	const boom = "post-seal blew up"
	if err := sched.RunOnce(name, func(context.Context) error {
		return errors.New(boom)
	}); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	awaitNotifications(t, notified, 1, "the failing job to reach a terminal state")

	got := out.String()
	if !strings.Contains(got, "one-time job failed") {
		t.Errorf("a failed job produced no failure line; log was:\n%s", got)
	}
	// The name and the underlying error are the two things that make the line
	// actionable — "a job failed" with neither is barely better than silence.
	if !strings.Contains(got, name) {
		t.Errorf("failure line does not name the job %q; log was:\n%s", name, got)
	}
	if !strings.Contains(got, boom) {
		t.Errorf("failure line drops the task error %q; log was:\n%s", boom, got)
	}
}

// The success path must stay quiet at WARN. A fix that logged every terminal
// event would bury the failures it exists to surface.
func TestSucceedingOneTimeJobLogsNoFailure(t *testing.T) {
	t.Parallel()
	var out syncBuffer
	sched, err := newScheduler(slog.New(slog.NewTextHandler(&out, nil)), 4, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sched.Stop() })

	notified := notifyChan(sched)

	if err := sched.RunOnce("fine", func(context.Context) error { return nil }); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	awaitNotifications(t, notified, 1, "the successful job to reach a terminal state")

	if got := out.String(); strings.Contains(got, "one-time job failed") {
		t.Errorf("a successful job logged a failure; log was:\n%s", got)
	}
}
